import { ConnectionDefinition, ModelDefinition } from "../types/objects";
import assert from "assert";
import {
  BaseChannel,
  DestinationChannel,
  EnrichmentChannel,
  ExecutionContext,
  HaltMessage,
  LogMessage,
  MessageHandler,
  StreamPersistenceStore,
} from "@syncmaven/protocol";
import { stringifyZodError } from "../lib/zod";
import { compileProject, configureEnvVars, readProject, unfoldSyncs, untildify } from "../lib/project";
import { createErrorThreshold } from "../lib/error-threshold";
import { createParser, SchemaBasedParser, stringifyParseError } from "../lib/uniparser";
import { StdInOutChannel } from "../docker/docker-channel";
import { createDatasource, DataSource } from "../datasources";
import { PostgresStore, SqliteStore } from "../lib/store";
import path from "path";
import { SqlQuery } from "../lib/sql";
import { GenericColumnType } from "../datasources/types";
import fs from "fs";
import { trackEvent } from "../lib/telemetry";
import JSON5 from "json5";
import { PackageOpts } from "./index";
import { SetRequired } from "type-fest";
import { load } from "js-yaml";
import { HandlebarsTemplateEngine } from "../lib/template";
import { ConfigurationObject, Project, RawProject } from "../types/project";
import { requireDefined } from "../../__tests__/lib/preconditions";
import os from "node:os";
import { execSync } from "child_process";

function deleteDirBeforeExit(tmpDir: string) {
  process.on("exit", () => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log(`Deleted temporary directory: ${tmpDir}`);
  });
}

function getNpmBinary() {
  if (process.env.npm_execpath) {
    return process.env.npm_execpath;
  } else if (process.env.NODE) {
    const npmPath = path.join(path.dirname(process.env.NODE), "npm");
    if (fs.existsSync(npmPath)) {
      return npmPath;
    }
  }
  return "npm";
}

function splitPackageName(packageName: string): { name: string; version?: string } {
  const match = packageName.match(/^(?<name>@?[^@]+\/?[^@]*)@?(?<version>.+)?$/);
  if (match && match.groups) {
    const { name, version } = match.groups;
    return { name, version: version || undefined };
  }
  throw new Error(`Invalid package name format: ${packageName}`);
}

export async function downloadAndUnpackPackage(pkg: string) {
  const { name, version } = splitPackageName(pkg);
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "syncmaven-"));
  deleteDirBeforeExit(tmpDir);
  const npmBinary = getNpmBinary();
  const command = `${npmBinary} install ${pkg} --prefix ${tmpDir}`;
  console.log(`Downloading and installing ${pkg} to ${tmpDir}`);
  console.debug(`Running command: ${command}`);
  execSync(command);
  return tmpDir + "/node_modules/" + name;
}

export async function getDestinationChannel(
  pkg: ConnectionDefinition["package"],
  messagesHandler: MessageHandler
): Promise<DestinationChannel> {
  if (pkg.type === "npm") {
    let { command, dir, package: packageName } = pkg;
    if (!dir) {
      if (packageName) {
        dir = await downloadAndUnpackPackage(packageName);
      } else {
        throw new Error("Either dir or package is required for npm package type");
      }
    }

    if (!command) {
      console.debug(`Looking for package.json in ${dir}, cwd: ${process.cwd()}`);
      //get command from package.json
      const pkgJsonPath = path.join(dir, "package.json");
      assert(fs.existsSync(pkgJsonPath), `package.json not found in ${dir}`);
      const packageJson = JSON.parse(fs.readFileSync(pkgJsonPath, "utf-8"));
      assert(packageJson.main, `${pkgJsonPath} should have main field`);
      command = `${process.execPath} ${packageJson.main}`;
    }
    return new StdInOutChannel({ command: { exec: command, dir } }, messagesHandler);
  } else if (pkg.type === "docker") {
    const image = pkg.image;
    assert(image, "Docker image is required if package type is docker");
    return new StdInOutChannel({ dockerImage: image }, messagesHandler);
  } else {
    throw new Error(`Unsupported package type: ${pkg.type}`);
  }
}

export function getEnrichmentProvider(en: ConnectionDefinition, messagesHandler: MessageHandler): EnrichmentChannel {
  throw new Error("Package-based enrichments are not yet supported");
}

export async function createStore(_state: string): Promise<StreamPersistenceStore> {
  console.log(`Creating state store defined by ${_state}`);
  const state = new HandlebarsTemplateEngine().compile(_state, { fileName: "(unknown)" })({ env: process.env });
  if (state.startsWith("postgres://") || state.startsWith("postgresql://")) {
    const pgStore = new PostgresStore(state);
    await pgStore.init();
    return pgStore;
  } else {
    const sqliteStore = new SqliteStore(path.join(state));
    await sqliteStore.init();
    return sqliteStore;
  }
}

/**
 * Reads JSON from a resource. Resource can be
 *   * Either pointer to a JSON or YAML file (starts with @ or file://)
 *   * Or a JSON string
 *   * Or just a string, in that case nonJsonHandler should be used to parse it
 */
function readJson(_resource: any, nonJsonHandler: (val: string) => any) {
  if (!_resource) {
    throw new Error("Resource is required");
  }
  if (typeof _resource === "object") {
    return _resource;
  }
  const resource = _resource + "";
  const prefixes = ["@", "file://"];
  for (const prefix of prefixes) {
    if (resource.startsWith(prefix)) {
      const filePath = path.resolve(resource.slice(prefix.length));
      console.debug(`Reading json resource from ${filePath}`);
      const content = fs.readFileSync(filePath, "utf-8");
      if (filePath.endsWith(".yaml") || filePath.endsWith(".yml")) {
        return load(content, { filename: filePath, json: true });
      }
      return JSON5.parse(content);
    }
  }
  try {
    return JSON5.parse(resource);
  } catch (e) {
    return nonJsonHandler(resource + "");
  }
}

//reads project either from the directory or from source and destination
function getProjectModel(
  opts: Required<Pick<SyncCommandOpts, "projectDir">> &
    Pick<
      SyncCommandOpts,
      "model" | "streamOptions" | "credentials" | "datasource" | "stream" | "checkpointEvery" | "syncId"
    > &
    Partial<PackageOpts>
): RawProject {
  if (opts.model) {
    assert(opts.credentials, "--credentials options should be set for ad-hoc runs");
    if (!opts.package) {
      throw new Error("Package is required if model is provided. See -p and -t flags");
    }
    const source = readJson(opts.model, (val: string) => ({ query: val })) as ModelDefinition;
    if (!source.id) {
      source.id = "source";
    }
    if (!source.datasource) {
      assert(
        opts.datasource,
        "Either define datasource in the model JSON as datasource field, or provide it with --datasource flag"
      );
      source.datasource = opts.datasource;
    }
    const destinationId = opts.package;

    const packageType = opts.packageType || "docker";
    assert(opts.package, `Please specify a package for the destination with -p flag`);
    const connectionCredentials = readJson(opts.credentials, (val: string) => ({ apiKey: val }));

    const syncId = opts.syncId || "sync";
    const rawProject = {
      models: [
        {
          type: "model" as ConfigurationObject["type"],
          relativeFileName: "(cli)",
          fileId: source.id,
          content: source,
        },
      ],
      connections: [
        {
          type: "connection" as ConfigurationObject["type"],
          relativeFileName: "(cli)",
          fileId: destinationId,
          content: {
            package: {
              type: packageType,
              ...(packageType === "docker" ? { image: opts.package } : { dir: opts.package }),
            },
            credentials: connectionCredentials,
          },
        },
      ],
      syncs: [
        {
          type: "sync" as ConfigurationObject["type"],
          relativeFileName: "(cli)",
          fileId: syncId,
          content: {
            model: "source",
            id: syncId,
            destination: destinationId,
            stream: opts.stream,
            options: opts.streamOptions ? JSON5.parse(opts.streamOptions) : {},
            checkpointEvery: opts.checkpointEvery,
          },
        },
      ],
    };
    unfoldSyncs(rawProject);
    return rawProject;
  }
  return readProject(untildify(opts.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd()));
}

export type SyncCommandOpts = {
  projectDir?: string;
  state?: string;
  select?: string;
  source?: string;
  model?: string;
  checkpointEvery?: number;
  credentials?: string;
  datasource?: string;
  streamOptions?: string;
  stream?: string;
  fullRefresh?: boolean;
  env?: string[];
  syncId?: string;
};

function dockerProjectDirWarning(projectDir?: string) {
  if (process.env.IN_DOCKER) {
    console.warn(
      `Project dir is set explicitly, but Syncmaven is running in Docker. It may not work as you expect. Mount it with -v flag: -v ${projectDir}:/project instead`
    );
  }
}

function patchOptsWithProjectDir(
  _opts: SyncCommandOpts & Partial<PackageOpts>,
  _projectDir: string
): SetRequired<SyncCommandOpts, "projectDir"> {
  return {
    ..._opts,
    projectDir: _projectDir || _opts.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd(),
  };
}

export async function sync(_projectDir: string, _opts: SyncCommandOpts & Partial<PackageOpts>) {
  dockerProjectDirWarning(_projectDir || _opts.projectDir);

  //patch opts with projectDir if specified as an argument, also make a copy
  const opts = patchOptsWithProjectDir(_opts, _projectDir);
  console.debug(`Running sync with following options: ${JSON.stringify(opts, null, 2)}`);
  await trackEvent("sync-command", { fullRefresh: !!opts.fullRefresh });

  configureEnvVars([opts.projectDir, "."], opts.env || []);
  const rawProject = getProjectModel(opts);
  console.debug(`Raw project: ${JSON.stringify(rawProject, null, 2)}`);
  const project = compileProject(rawProject);
  console.debug(`Compiled project: ${JSON.stringify(project, null, 2)}`);
  const syncIds = opts.select ? opts.select.split(",") : Object.keys(project.syncs);
  let store: StreamPersistenceStore;
  const storeCfg = opts.state || process.env.SYNCMAVEN_STATE || path.join(opts.projectDir, ".state");
  try {
    store = await createStore(storeCfg);
  } catch (e: any) {
    console.error(`Failed to init store: ${storeCfg}`, e);
    process.exit(1);
  }
  let errors = 0;
  for (const syncId of syncIds) {
    try {
      await runSync({ project, syncId, store });
    } catch (e: any) {
      errors++;
      console.error(`Failed to run sync: ${syncId}`, e);
    }
  }
  if (errors == 0) {
    console.debug(`All syncs finished`);
  } else {
    console.error(`${errors}/${syncIds.length} syncs failed. See log messages above`);
    process.exit(1);
  }
}

type CursorState = {
  type: GenericColumnType;
  val: number | string | null | Date;
};

function inferCursorType(type: string): GenericColumnType {
  return "string";
}

/**
 * Nulls considered as the smallest value
 */
function compareCursors(type: CursorState["type"], val1: CursorState["val"], val2: CursorState["val"]): number {
  if (val1 === val2) {
    return 0;
  }
  //nulls is the smallest value
  if (val1 === null) {
    return -1;
  }
  if (val2 === null) {
    return 1;
  }
  return val1 > val2 ? 1 : -1;
}

function stringifyCursor(val: any) {
  if (val instanceof Date) {
    return val.toISOString();
  }
}

function processLogMessage(logMessage: LogMessage, syncId: string) {
  const params = logMessage.payload.params?.length
    ? ` ${logMessage.payload.params.map(p => JSON.stringify(p)).join(", ")}`
    : "";
  const logLevel = logMessage.payload.level.toLowerCase();
  const logFunction = ["debug", "info", "warn", "error"].includes(logLevel) ? console[logLevel] : console.log;
  //sometimes message + params is an empty string. In this case we display a raw payload
  const stringMessage = `${logMessage.payload.message}${params}` || JSON.stringify(logMessage);
  logFunction(`<${syncId}> ${stringMessage} - ${JSON.stringify(logMessage)}`);
}

export async function runSync(opts: {
  project: Project;
  syncId: string;
  store: StreamPersistenceStore;
  fullRefresh?: boolean;
}) {
  const { project, syncId, store } = opts;
  const sync = requireDefined(
    project.syncs[syncId],
    `Can't find sync ${syncId} in the project. Available syncs: ${Object.keys(project.syncs).join(", ")}`
  );
  const startedAt = Date.now();
  console.info(`Running sync \`${syncId}\``);
  const modelId = sync.model;
  const checkpointEvery = sync.checkpointEvery;

  const model = requireDefined(
    project.models[modelId],
    `Can't find model ${modelId} in the project. Available models: ${Object.keys(project.models).join(", ")}`
  );
  const destinationId = sync.destination;
  const destination = requireDefined(
    project.connections[destinationId],
    `Can't find destination ${destinationId} in the project. Available destinations: ${Object.keys(project.connections).join(", ")}`
  );
  const context: ExecutionContext = { store };

  let halt = false;
  let haltError: any;

  const messageListener = message => {
    switch (message.type) {
      case "log":
        processLogMessage(message, syncId);
        break;
      case "halt":
        const haltMes = message as HaltMessage;
        halt = true;
        if (haltMes.payload.status == "error") {
          haltError = new Error(haltMes.payload.message);
          console.error(
            `HALT [${syncId}] ERROR ${haltMes.payload.message} data: ${haltMes.payload.data ? JSON.stringify(haltMes.payload.data) : ""}`
          );
        } else {
          console.log(
            `HALT [${syncId}] OK ${haltMes.payload.message} data: ${haltMes.payload.data ? JSON.stringify(haltMes.payload.data) : ""}`
          );
        }
        break;
      default:
        console.error(
          `${message.type?.toUpperCase} [${syncId}] Unexpected message type: '${message.type}' payload: ${JSON.stringify(message)}`
        );
    }
  };

  const destinationChannel: DestinationChannel = await getDestinationChannel(destination.package, messageListener);
  const enrichments: EnrichmentChannel[] = [];
  let datasource: DataSource | undefined = undefined;
  try {
    const connectionSpec = await destinationChannel.describe();
    const connectionCredentialsParser = createParser(connectionSpec.payload.connectionCredentials);
    const parsedCredentials = connectionCredentialsParser.safeParse(destination.credentials);
    if (!parsedCredentials.success) {
      console.debug(
        `Malformed destination config. Will fail, see error message below:\n${JSON.stringify(destination, null, 2)}`
      );
      throw new Error(
        `Invalid credentials for destination ${destinationId}: ${stringifyParseError(parsedCredentials.error)}`
      );
    }
    const streamsSpec = await destinationChannel.streams({
      type: "describe-streams",
      payload: {
        credentials: parsedCredentials,
      },
    });
    const streamId = sync.stream || streamsSpec.payload.defaultStream;
    const streamSpec = streamsSpec.payload.streams.find(s => s.name === streamId);
    if (!streamSpec) {
      throw new Error(`Stream ${streamId} not found in destination ${destinationId}`);
    }
    console.debug(`Stream spec: ${JSON.stringify(streamSpec)}`);
    const rowSchemaParser = createParser(streamSpec.rowType);

    const enrichmentSettings = sync.enrichments || (sync.enrichment ? [sync.enrichment] : []);

    for (const enrichmentRef of enrichmentSettings) {
      const connection = project.connections[enrichmentRef.connection];
      // TODO: probably enrichments need to have their own messageListener. not sure yet
      const enrichmentProvider = getEnrichmentProvider(connection, messageListener);
      enrichments.push(enrichmentProvider);
      await enrichmentProvider.startEnrichment(
        {
          type: "enrichment-connect",
          payload: {
            credentials: connection.credentials,
            options: enrichmentRef.options,
          },
        },
        context
      );
    }

    let totalRows = 0;
    let enrichedRows = 0;
    const errorThreshold = createErrorThreshold();
    datasource = await createDatasource(model);
    console.debug(`Will run query (raw): '${model.query}' on ${datasource.type()}`);
    const query = new SqlQuery(model.query, datasource.type(), datasource.toQueryParameter);
    if (model.cursor && !query.getNamedParameters().includes("cursor")) {
      throw new Error(
        `Cursor field (${model.cursor}) is defined in the model, but :cursor is not referenced from the query. Read more about cursors and incremental syncs at https://syncmaven.sh/incremental`
      );
    }
    let maxCursorVal: CursorState | undefined = undefined;
    const cursorStoreKey = [`syncId=${syncId}`, `$lastCursor=${model.cursor}`];
    if (model.cursor && opts.fullRefresh) {
      await store.del(cursorStoreKey);
    }
    const lastMaxCursor = model.cursor ? ((await store.get(cursorStoreKey)) as CursorState) : null;
    if (lastMaxCursor?.val && lastMaxCursor.type === "date") {
      lastMaxCursor.val = new Date(lastMaxCursor.val);
    }

    let streamStarted = false;

    async function checkpoint(completed: boolean) {
      const res = await destinationChannel.stopStream();
      if (model.cursor) {
        console.debug(`Max cursor value: ${JSON.stringify(maxCursorVal)}`);
        await store.set(cursorStoreKey, maxCursorVal);
      }
      console.info(
        `Sync ${syncId} ${completed ? "is finished" : "is checkpointing"} in ${(Date.now() - startedAt) / 1000}s. Source rows ${totalRows}, enriched: ${enrichedRows}, channel stats:`
      );
      for (const [k, v] of Object.entries(res.payload as any)) {
        if (typeof v === "number") {
          console.info(`  ${k}: ${v}`);
        } else {
          console.info(`  ${k}: ${JSON.stringify(v)}`);
        }
      }
    }

    let previousCursorValue: any = undefined;
    const compiledQuery = model.cursor ? query.compile({ cursor: lastMaxCursor?.val || null }) : query.compile({});
    console.debug(`Query was compiled to: ${compiledQuery}`);
    await datasource.executeQuery({
      query: compiledQuery,
      handler: {
        header: async header => {
          console.debug(`Header: ${JSON.stringify(header)}`);
          if (model.cursor) {
            const cursorColumn = header.columns.find(c => c.name === model.cursor);
            if (!cursorColumn) {
              throw new Error(`Cursor field ${model.cursor} not found in the header of the query result`);
            }
            maxCursorVal = { type: cursorColumn.type.genericType, val: null };
          }
        },
        row: async row => {
          if (!streamStarted) {
            await destinationChannel.startStream(
              {
                type: "start-stream",
                payload: {
                  stream: streamId,
                  connectionCredentials: parsedCredentials.data,
                  streamOptions: sync.options || {},
                  syncId,
                  fullRefresh: !!opts.fullRefresh,
                },
              },
              context
            );
            streamStarted = true;
          }
          const parseResult = rowSchemaParser.safeParse(row);
          if (model.cursor) {
            const cursorVal = row[model.cursor];
            if (
              previousCursorValue !== undefined &&
              compareCursors(maxCursorVal!.type, previousCursorValue, cursorVal) > 0
            ) {
              throw new Error(
                `The model should be sorted in ascending order by the \`${model.cursor}\` field. Prev value ${stringifyCursor(previousCursorValue)} > current value ${stringifyCursor(cursorVal)}. Try to add \`ORDER BY ${model.cursor} ASC\` to the query.`
              );
            }
            previousCursorValue = cursorVal;
            if (compareCursors(maxCursorVal!.type, cursorVal, maxCursorVal!.val) > 0) {
              maxCursorVal!.val = cursorVal;
            }
          }
          if (parseResult.success) {
            let rows = [parseResult.data];
            for (const enrichment of enrichments) {
              rows = await applyEnrichment(rowSchemaParser, enrichment, rows, context);
            }
            if (rows.length > 1) {
              console.debug(
                `Enrichment expanded row ${JSON.stringify(parseResult.data)} to ${rows.length} rows: ${JSON.stringify(rows)}`
              );
            }
            enrichedRows += rows.length;
            for (const row of rows) {
              if (halt) {
                break;
              }
              await destinationChannel.row({ type: "row", payload: { row } });
            }
          } else {
            const zodError = stringifyZodError(parseResult.error);
            const errorMessage = `Invalid format of a row: ${zodError}. Row: ${JSON.stringify(row)}.`;
            if (errorThreshold.fail()) {
              throw new Error(errorMessage + `. Failed because of too many errors - ${errorThreshold.summary()}`);
            } else {
              console.warn(errorMessage + `. Skipping the row ${errorThreshold.summary()}`);
            }
          }
          totalRows++;
          if (checkpointEvery && totalRows % checkpointEvery === 0) {
            await checkpoint(false);
            streamStarted = false;
          }
        },
        finalize: async () => {},
      },
    });
    await checkpoint(true);
  } catch (e: any) {
    throw e;
  } finally {
    console.debug(`Closing all communications channels of sync '${syncId}'. It might take a while`);
    await closeChannels([...enrichments, destinationChannel]);
    console.debug(`All channels of '${syncId}' has been closed`);
    if (datasource && datasource.close) {
      await datasource.close();
    }
  }
}

async function applyEnrichment(
  rowType: SchemaBasedParser,
  enrichment: EnrichmentChannel,
  rows: any[],
  ctx: ExecutionContext
): Promise<any[]> {
  const res: any[] = [];
  for (const row of rows) {
    try {
      const item = await enrichment.row({
        type: "enrichment-request",
        payload: { row },
      });
      const parseResult = rowType.safeParse(item.payload.row);
      if (parseResult.success) {
        res.push(parseResult.data);
      } else {
        console.warn(
          `Enrichment returned invalid row: ${stringifyZodError(parseResult.error)}. Will skip it. Row: ${JSON.stringify(item)}`
        );
      }
    } catch (e: any) {
      console.error(`Enrichment error: ${e.message} on row: ${JSON.stringify(row)}`);
    }
  }
  return res;
}

async function closeChannels(channels: (BaseChannel | undefined)[]) {
  for (const channel of channels) {
    if (channel && channel.close) {
      try {
        await channel.close();
      } catch (e: any) {
        console.warn(`Failed to close channel`, e);
      }
    }
  }
}
