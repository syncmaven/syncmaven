import { ConnectionDefinition, SyncDefinition } from "../types/objects";
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
import { configureEnvVars, readProject, untildify } from "../lib/project";
import { createErrorThreshold } from "../lib/error-threshold";
import { Project } from "../types/project";
import { createParser, SchemaBasedParser, stringifyParseError } from "../lib/uniparser";
import { DockerChannel } from "../docker/docker-channel";
import { createDatasource, DataSource } from "../datasources";
import { PostgresStore, SqliteStore } from "../lib/store";
import path from "path";
import { SqlQuery } from "../lib/sql";
import { GenericColumnType } from "../datasources/types";
import fs from "fs";
import { trackEvent } from "../lib/telemetry";

export function getDestinationChannelFromPackage(
  { package: pkg, packageType = "docker" }: { package: string; packageType?: string },
  messagesHandler: MessageHandler
): DestinationChannel {
  if (packageType === "docker") {
    return new DockerChannel({ dockerImage: pkg }, messagesHandler);
  } else if (packageType === "npm") {
    const packageDir = pkg.split("@")[0];
    const packageJson = JSON.parse(fs.readFileSync(path.join(packageDir, "package.json"), "utf-8"));
    if (!packageJson.main) {
      throw new Error(`Package ${pkg} does not have a main entry point`);
    }
    return new DockerChannel({ command: { exec: `node ${packageJson.main}`, dir: packageDir } }, messagesHandler);
  } else {
    throw new Error(`Unsupported package type ${packageType}`);
  }
}

export function getDestinationChannelFromDefinition(
  destination: ConnectionDefinition,
  messagesHandler: MessageHandler
): DestinationChannel {
  if (destination.package.type === "sub-process") {
    const { command, commandDir } = destination.package;
    assert(command, "Command is required if package type is sub-process");
    assert(commandDir, "commandDir is required if package type is sub-process");
    return new DockerChannel({ command: { exec: command, dir: commandDir } }, messagesHandler);
  } else if (destination.package.type === "docker") {
    const image = destination.package.image;
    assert(image, "Docker image is required if package type is docker");
    return new DockerChannel({ dockerImage: image }, messagesHandler);
  } else {
    throw new Error(`Unsupported package type: ${destination.package.type} for destination ${destination.id}`);
  }
}

export function getEnrichmentProvider(en: ConnectionDefinition, messagesHandler: MessageHandler): EnrichmentChannel {
  throw new Error("Package-based enrichments are not yet supported");
}

export async function createStore(state: string): Promise<StreamPersistenceStore> {
  console.log(`Creating store in ${state}`);
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

export async function sync(
  projectDir: string,
  opts: {
    projectDir?: string;
    state?: string;
    select?: string;
    fullRefresh?: boolean;
    env?: string[];
  }
) {
  projectDir = untildify(projectDir || opts.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd());
  await trackEvent("sync-command", { fullRefresh: !!opts.fullRefresh });
  if ((projectDir || opts.projectDir) && process.env.IN_DOCKER) {
    console.warn(
      `Project dir is set explicitly, but Syncmaven is running in Docker. It may not work as you expect. Mount it with -v flag: -v ${projectDir || opts.projectDir}:/project instead`
    );
  }
  configureEnvVars([projectDir, "."], opts.env || []);
  const project = readProject(projectDir);
  const syncIds = opts.select ? opts.select.split(",") : Object.keys(project.syncs);
  let store: StreamPersistenceStore;
  const storeCfg = opts.state || path.join(projectDir, ".state");
  try {
    store = await createStore(storeCfg);
  } catch (e: any) {
    console.error(`Failed to init store: ${storeCfg}`, e);
    process.exit(1);
  }
  let errors = false;
  for (const syncId of syncIds) {
    try {
      await runSync({ project, syncId, store });
    } catch (e: any) {
      errors = true;
      console.error(`Failed to run sync: ${syncId}`, e);
    }
  }
  if (!errors) {
    console.debug(`All syncs finished`);
  } else {
    console.error(`Some syncs failed`);
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

function compareCursors(type: CursorState["type"], val1: CursorState["val"], val2: CursorState["val"]): number {
  if (val1 === val2) {
    return 0;
  }
  if (val1 === null) {
    return -1;
  }
  if (val2 === null) {
    return 1;
  }
  return val1 < val2 ? -1 : 1;
}

export async function runSync(opts: {
  project: Project;
  syncId: string;
  store: StreamPersistenceStore;
  fullRefresh?: boolean;
}) {
  const { project, syncId, store } = opts;
  const syncFactory = project.syncs[syncId];
  if (!syncFactory) {
    throw new Error(
      `Sync with id \`${syncId}\` not found in the project, or it's disabled. Available syncs: ${Object.keys(project.syncs).join(", ")}`
    );
  }
  const sync: SyncDefinition = syncFactory();
  console.info(`Running sync \`${syncId}\``, sync);
  const modelId = sync.model;
  const checkpointEvery = sync.checkpointEvery;

  const modelFactory = project.models[modelId];
  assert(modelFactory, `Model with id ${modelId} referenced from sync ${syncId} not found in the project`);
  const model = modelFactory();

  const destinationId = sync.destination;
  const destinationFactory = project.connection[destinationId];
  assert(
    destinationFactory,
    `Destination with id \`${destinationId}\` referenced from sync \`${syncId}\` not found in the project`
  );
  const destination = destinationFactory();
  const context: ExecutionContext = { store };

  let halt = false;
  let haltError: any;

  const messageListener = message => {
    switch (message.type) {
      case "log":
        const logMes = message as LogMessage;
        const params = logMes.payload.params?.length
          ? ` ${logMes.payload.params.map(p => JSON.stringify(p)).join(", ")}`
          : "";
        console.log(`LOG [${syncId}] ${logMes.payload.level.toUpperCase()} ${logMes.payload.message}${params}`);
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

  const destinationChannel: DestinationChannel = getDestinationChannelFromDefinition(destination, messageListener);
  const enrichments: EnrichmentChannel[] = [];
  let datasource: DataSource | undefined = undefined;
  try {
    const connectionSpec = await destinationChannel.describe();
    const connectionCredentialsParser = createParser(connectionSpec.payload.connectionCredentials);
    const parsedCredentials = connectionCredentialsParser.safeParse(destination.credentials);
    if (!parsedCredentials.success) {
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
      const connection = project.connection[enrichmentRef.connection]();
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
        console.debug(`Max cursor value: ${maxCursorVal}`);
        await store.set(cursorStoreKey, maxCursorVal);
      }
      console.info(
        `Sync ${syncId} ${completed ? "is finished" : "is checkpointing"}. Source rows ${totalRows}, enriched: ${enrichedRows}, channel stats:`
      );
      for (const [k, v] of Object.entries(res.payload as any)) {
        if (typeof v === "number") {
          console.info(`  ${k}: ${v}`);
        } else {
          console.info(`  ${k}: ${JSON.stringify(v)}`);
        }
      }
    }

    await datasource.executeQuery({
      query: model.cursor ? query.compile({ cursor: lastMaxCursor?.val || null }) : query.compile({}),
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
