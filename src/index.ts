import { Command } from "commander";
import { initializeConsoleLogging } from "./log";
import fs from "fs";
import path from "path";
import type { Project } from "./types/project";
import { z, ZodError, type ZodIssue, ZodSchema } from "zod";
import { load } from "js-yaml";
import { ConnectionDefinition, ModelDefinition, SyncDefinition } from "./types/objects";
import { merge, omit, set } from "lodash";
import { Client, type QueryResult } from "pg";
import Cursor from "pg-cursor";
import assert from "assert";
import { ExecutionContext } from "./connections/types";
import { SqliteStore } from "./lib/store";
import { getDestinationChannel, getEnrichmentProvider } from "./connections";
import { maskPassword } from "./lib/util";
import dotenv from "dotenv";
import { OAuth2Client } from "google-auth-library";
import express from "express";
import http from "http";

import nunjucks from "nunjucks";
import { RpcError } from "./lib/rpc";
import {
  ConnectionSpecMessage,
  ComponentChannel,
  Message,
  StreamSpecMessage,
  processMessages,
  processMessageWithResult,
} from "./types/protocol";
import { stringifyZodError } from "./lib/zod";
import * as os from "node:os";

initializeConsoleLogging();

function untildify(filePath: string): string {
  const home = process.env.HOME || os.homedir() || "/";
  if (filePath.startsWith("~")) {
    return path.join(home, filePath.slice(1));
  }
  return filePath;
}

type TableColumn = {
  name: string;
  type: string;
};

type TableHeader = {
  columns: TableColumn[];
};

type SQLValue = any;

type StreamingHandler = {
  header: (header: TableHeader) => void | Promise<void>;
  row: (row: Record<string, SQLValue>) => void | Promise<void>;
  finalize: () => void | Promise<void>;
};

async function read(cursor: Cursor, batchSize: number): Promise<QueryResult> {
  return new Promise((resolve, reject) => {
    cursor.read(batchSize, (err, _, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}

async function executeQuery(param: { handler: StreamingHandler; datasource: string; query: string }): Promise<void> {
  const [protocol] = param.datasource.split("://");
  if (protocol !== "postgresql") {
    throw new Error(`Unsupported protocol ${protocol} in datasource ${param.datasource}`);
  }

  const client = new Client({
    connectionString: param.datasource,
  });
  console.debug(`Connecting`);

  await client.connect();
  console.debug(`Connected to database ${maskPassword(param.datasource)}. Executing query: ${param.query}`);
  const cursor = client.query(new Cursor(param.query));
  let cursorBatchSize = 100;
  let result = await read(cursor, cursorBatchSize);
  await param.handler.header({ columns: result.fields.map(f => ({ name: f.name, type: f.format })) });
  while (result?.rows && result.rows.length > 0) {
    for (const row of result.rows) {
      await param.handler.row(row);
    }
    result = await read(cursor, cursorBatchSize);
  }
  await param.handler.finalize();
}

type ErrorThreshold = {
  success(): void;
  /**
   * @returns true if the error if error should considered as a failure
   */
  fail(): boolean;

  summary(): string;
};

function createErrorThreshold({
  maxRatio = 0.2,
  minTotal = 100,
}: { maxRatio?: number; minTotal?: number } = {}): ErrorThreshold {
  let errors = 0;
  let success = 0;
  return {
    success() {
      success++;
    },
    fail() {
      const total = errors + success;
      errors++;
      return total >= minTotal && errors / total >= maxRatio;
    },
    summary() {
      const total = errors + success;
      return `${errors}/${total} - ${((errors / total) * 100).toFixed(2)}%`;
    },
  };
}

async function applyEnrichment(
  rowType: ZodSchema,
  enrichment: ComponentChannel,
  rows: any[],
  ctx: ExecutionContext
): Promise<any[]> {
  const res: any[] = [];
  for (const row of rows) {
    const replyMessages = await processMessages(enrichment, { type: "enrichment-request", payload: { row } }, ctx);

    for (const item of replyMessages) {
      if (item.type !== "enrichment-response") {
        console.warn(
          `Enrichment returned unexpected message type ${item.type}. Expected 'enrichment-response'. Will skip it. Message: ${JSON.stringify(item)}`
        );
        continue;
      }
      const parseResult = rowType.safeParse(item.payload.row);
      if (parseResult.success) {
        res.push(parseResult.data);
      } else {
        console.warn(
          `Enrichment returned invalid row: ${stringifyZodError(parseResult.error)}. Will skip it. Row: ${JSON.stringify(item)}`
        );
      }
    }
  }
  return res;
}

function isZodSchema(obj: any): boolean {
  return obj && obj._def && obj._def.typeName;
}

function isJsonSchema(obj: any): boolean {
  return obj && obj.$schema && obj.type;
}

function haltIfNeeded(m: Message | Message[], subject: string) {
  const arr = Array.isArray(m) ? m : [m];
  for (const item of arr) {
    if (item.type === "halt") {
      throw new Error(`${subject} execution of sync: ${item.payload.message}`);
    }
  }
}

async function sync(opts: {
  projectDir?: string;
  state?: string;
  select?: string;
  fullRefresh?: boolean;
  env?: string[];
}) {
  const projectDir = untildify(opts.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd());
  const envFileNames = [".env", ".env.local"];
  dotenv.config({
    path: [
      ...envFileNames.map(file => path.join(projectDir, file)),
      ...envFileNames.map(file => path.join(process.cwd(), file)),
      ...(opts.env || []),
    ].map(untildify),
  });
  const project = readProject(projectDir);
  const syncIds = opts.select ? opts.select.split(",") : Object.keys(project.syncs);
  for (const syncId of syncIds) {
    const syncFactory = project.syncs[syncId];
    if (!syncFactory) {
      throw new Error(
        `Sync with id \`${syncId}\` not found in the project, or it's disabled. Available syncs: ${Object.keys(project.syncs).join(", ")}`
      );
    }
    console.info(`Running sync \`${syncId}\``);
    const sync: SyncDefinition = syncFactory();
    const modelId = sync.model;
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
    const context: ExecutionContext = {
      store: new SqliteStore(path.join(projectDir, ".state")),
    };

    const destinationChannel: ComponentChannel | undefined = getDestinationChannel(destination.kind);
    if (!destinationChannel) {
      throw new Error(`Destination provider ${destination.kind} referenced from ${syncId} not found`);
    }
    const connectionSpec = await processMessageWithResult(
      destinationChannel,
      { type: "describe" },
      context,
      ConnectionSpecMessage
    );
    const streamsSpec = await processMessageWithResult(
      destinationChannel,
      { type: "describe-streams" },
      context,
      StreamSpecMessage
    );
    const streamId = sync.stream || streamsSpec.payload.defaultStream;
    const streamSpec = streamsSpec.payload.streams.find(s => s.name === streamId);
    if (!streamSpec) {
      throw new Error(`Stream ${streamId} not found in destination ${destinationId}`);
    }
    console.info(`Row type`, streamSpec.rowType);
    console.info(`Row type type`, typeof streamSpec.rowType);
    console.info(`Row type type`, typeof streamSpec.rowType);
    if (!isZodSchema(streamSpec.rowType)) {
      throw new Error(
        `Row type of stream ${streamId} is not based on Zod. We don't support JSON schema yet. Schema: ${JSON.stringify(streamSpec.rowType)}`
      );
    }
    const rowSchema = streamSpec.rowType as ZodSchema;
    if (!isZodSchema(connectionSpec.payload.connectionCredentials)) {
      throw new Error(
        `Connection credentials schema of ${destinationId} is not based on Zod. We don't support JSON schema yet`
      );
    }
    const connectionCredentialsType = connectionSpec.payload.connectionCredentials as ZodSchema;
    const parsedCredentials = connectionCredentialsType.safeParse(destination.credentials);
    if (!parsedCredentials.success) {
      throw new Error(
        `Invalid credentials for destination ${destinationId}: ${stringifyZodError(parsedCredentials.error)}`
      );
    }
    haltIfNeeded(
      await processMessages(
        destinationChannel,
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
      ),
      `Destination ${destinationId} initialization`
    );
    const enrichmentSettings = sync.enrichments || (sync.enrichment ? [sync.enrichment] : []);

    const enrichments: ComponentChannel[] = [];
    for (const enrichmentRef of enrichmentSettings) {
      const connection = project.connection[enrichmentRef.connection]();
      const enrichmentProvider = getEnrichmentProvider(connection.kind);
      if (!enrichmentProvider) {
        throw new Error(`Enrichment provider ${connection.kind} referenced from ${syncId} not found`);
      }
      enrichments.push(enrichmentProvider);
      haltIfNeeded(
        await processMessages(
          enrichmentProvider,
          {
            type: "enrichment-connect",
            payload: { credentials: connection.credentials, options: enrichmentRef.options },
          },
          context
        ),
        `Enrichment ${enrichmentRef.connection} initialization`
      );
    }

    let totalRows = 0;
    let enrichedRows = 0;
    const errorThreshold: ErrorThreshold = createErrorThreshold();
    await executeQuery({
      query: model.query,
      datasource: model.datasource,
      handler: {
        header: async header => {
          //await streamHandler.setup(header, context);
        },
        row: async row => {
          const parseResult = rowSchema.safeParse(row);
          if (parseResult.success) {
            let rows = [parseResult.data];
            for (const enrichment of enrichments) {
              rows = await applyEnrichment(rowSchema, enrichment, rows, context);
            }
            if (rows.length > 1) {
              console.debug(
                `Enrichment expanded row ${JSON.stringify(parseResult.data)} to ${rows.length} rows: ${JSON.stringify(rows)}`
              );
            }
            enrichedRows += rows.length;
            for (const row of rows) {
              const messages = await processMessages(destinationChannel, { type: "row", payload: { row } }, context);
              for (const message of messages) {
                if (message.type === "halt") {
                  throw new Error(`Destination ${destinationId} halted the stream: ${message.payload.message}`);
                } else if (message.type === "log") {
                  console[message.payload.level](message.payload.message, ...(message.payload.params || []));
                } else {
                  console.warn(
                    `Destination ${destinationId} replied with unexpected message type ${message.type}`,
                    message
                  );
                }
              }
            }
          } else {
            const zodError = stringifyZodError(parseResult.error);
            const errorMessage = `Invalid format of a row: ${zodError}. Row: ${JSON.stringify(row)}`;
            if (errorThreshold.fail()) {
              throw new Error(errorMessage + `. Failed because of too many errors - ${errorThreshold.summary()}`);
            } else {
              console.warn(errorMessage + `. Skipping the row ${errorThreshold.summary()}`);
            }
          }
          totalRows++;
        },
        finalize: async () => {
          haltIfNeeded(
            await processMessages(destinationChannel, { type: "end-stream", reason: "success" }, context),
            `Destination ${destinationId} finalization`
          );
        },
      },
    });
    console.info(`Sync ${syncId} is finished. Source rows ${totalRows}, enriched rows ${enrichedRows}`);
  }
  console.debug(`All syncs finished`);
}

function help() {}

function splitFileName(fileName: string): [string, string | undefined] {
  const parts = fileName.split(".");
  if (parts.length === 1) {
    return [parts[0], undefined];
  }
  return [parts.slice(0, -1).join("."), parts[parts.length - 1]];
}

type CallbackFunction = (varName: string, defaultVal?: string) => string;

function replaceExpressions(obj: any, callback: CallbackFunction): any {
  // Regular expression to find ${varName[:defaultVal]} patterns
  // This regex allows varName to include letters, numbers, underscores, dashes, and dots
  const regex = /\$\{([a-zA-Z0-9_.-]+)(?::([^}]*))?\}/g;

  // Function to recursively process an object
  function processValue(value: any): any {
    if (Array.isArray(value)) {
      // Process each item in the array
      return value.map(item => processValue(item));
    } else if (value !== null && typeof value === "object") {
      // Process each property in the object
      const processedObject: Record<string, any> = {};
      for (const key of Object.keys(value)) {
        processedObject[key] = processValue(value[key]);
      }
      return processedObject;
    } else if (typeof value === "string") {
      // Replace expressions in the string
      return value.replace(regex, (match, varName, defaultVal) => {
        return callback(varName.trim(), defaultVal?.trim());
      });
    } else {
      // Return other types unchanged
      return value;
    }
  }

  return processValue(obj);
}

type WithId = { id: string };
type MakeRequired<T, K extends keyof T> = Omit<T, K> & Required<Pick<T, K>>;

function makeFactory<T extends WithId>(
  obj: any,
  file: { fullPath: string; idFromName: string },
  zodSchema: ZodSchema<T>
): () => MakeRequired<T, "id"> {
  let cached: MakeRequired<T, "id"> | undefined = undefined;

  return () => {
    if (cached) {
      return cached;
    }
    obj = replaceExpressions(obj, (varName, defaultVal) => {
      const [prefix, ...rest] = varName.split(".");
      if (prefix === "env") {
        const value = process.env[rest.join(".")];
        if (value === undefined) {
          if (defaultVal === undefined) {
            throw new Error(`Environment variable ${varName} is not set. It's used in ${file.fullPath}`);
          } else {
            return defaultVal;
          }
        }
        return value;
      } else {
        throw new Error(
          `Unsupported placeholder \${${varName}} in ${file.fullPath}. Only \${env.NAME} placeholders are supported. Did you mean \${env.${varName}}?`
        );
      }
    });
    const { success, error, data } = zodSchema.safeParse(obj);
    if (!success) {
      throw new Error(`Error parsing ${file.fullPath}: ${stringifyZodError(error)}`);
    }
    return (cached = { id: data.id || file.idFromName, ...omit(data, "id") });
  };
}

function readObjectsFromDirector<T>(dir: string, zodSchema: ZodSchema<T>): Record<string, () => T> {
  const result: Record<string, () => T> = {};
  for (const child of fs.readdirSync(dir)) {
    const fullPath = path.join(dir, child);
    const pathStat = fs.statSync(fullPath);
    if (pathStat.isDirectory()) {
      console.warn(`Only files are supported in ${dir}. Skipping directry ${child}`);
    }
    const [baseName, extension] = splitFileName(child);
    if (extension === undefined) {
      console.warn(`Only files with extensions are supported in ${dir}. Skipping file ${child}`);
    } else if (extension === "sql") {
      const content = fs.readFileSync(fullPath, "utf-8");
      const templateEngine = new nunjucks.Environment();
      let config: any = {};
      templateEngine.addGlobal("config", function (arg1, arg2) {
        if (arg2 === undefined && typeof arg1 === "object") {
          config = merge(config, arg1);
        }
        if (typeof arg1 === "string" && typeof arg2 === "string") {
          set(config, arg1, arg2);
        } else {
          throw new Error(`Unsupported config() call with arguments ${typeof arg1}, ${typeof arg2}`);
        }
      });
      try {
        config.query = templateEngine.renderString(content, { env: process.env });
      } catch (e: any) {
        const message = (e?.message || "unknown error").replace("(unknown path)", fullPath);

        throw new Error(`Unable to parse template expression in ${child} model file: ${message}`, { cause: e });
      }

      if (!config.disabled) {
        const id = config.id || baseName;
        result[id] = makeFactory<any>(config, { fullPath, idFromName: baseName }, zodSchema) as any;
      }
    } else if (extension === "yaml" || extension === "yml") {
      const content = fs.readFileSync(fullPath, "utf-8");
      const yamlRaw = load(content, { filename: fullPath, json: true }) as any;
      if (!content || !yamlRaw) {
        throw new Error(`Error parsing ${dir}/${child}. File seems to be empty or invalid`);
      }
      if (!yamlRaw.disabled) {
        const id = yamlRaw.id || baseName;
        result[id] = makeFactory<any>(yamlRaw, { fullPath, idFromName: baseName }, zodSchema) as any;
      }
    } else if (extension === "ts") {
      console.warn(`TypeScript models are not supported yet. Skipping ${dir}/${child}`);
    } else {
      console.warn(`Unsupported file extension ${extension} in ${dir}/${child}. Skipping file`);
    }
  }
  return result;
}

function readProject(dir: string): Project {
  console.debug("Reading project from", dir);
  if (!fs.existsSync(dir)) {
    throw new Error(`Project directory ${dir} does not exist`);
  }
  const modelDir = path.join(dir, "models");
  const syncDir = path.join(dir, "syncs");
  const connectionsDir = path.join(dir, "connections");
  if (!fs.existsSync(modelDir)) {
    throw new Error(`Model directory ./model does not exist in the project directory ${dir}`);
  }
  if (!fs.existsSync(syncDir)) {
    throw new Error(`Model directory ./sync does not exist in the project directory ${dir}`);
  }
  if (!fs.existsSync(connectionsDir)) {
    throw new Error(`Model directory ./destinations does not exist in the project directory ${dir}`);
  }
  return {
    syncs: readObjectsFromDirector<SyncDefinition>(syncDir, SyncDefinition),
    models: readObjectsFromDirector<ModelDefinition>(modelDir, ModelDefinition),
    connection: readObjectsFromDirector<ConnectionDefinition>(connectionsDir, ConnectionDefinition),
  };
}

function waitForRequest(port: number): Promise<express.Request> {
  return new Promise((resolve, reject) => {
    const app = express();
    const server = http.createServer(app);

    app.use((req, res, next) => {
      resolve(req); // Resolve the promise with the request object
      res.status(200).send("Please, see your token in the console. You can close this window");
      server.close(err => {
        // Close the server
        if (err) {
          reject(err);
        }
      });
    });

    server.listen(port, () => {
      //console.log(`Server is running on http://localhost:${port}`);
    });

    server.on("error", error => {
      reject(error);
    });
  });
}

const defaultOauthRedirectURIPort = 4512;

async function triggerOauthFlow(opts: { projectDir?: string; connection: string; port: string }) {
  const port = opts.port ? parseInt(opts.port) : defaultOauthRedirectURIPort;
  const projectDir = opts.projectDir || process.cwd();
  const project = readProject(projectDir);
  const connectionFactory = project.connection[opts.connection];
  assert(connectionFactory, `Connection with id ${opts.connection} not found in the project`);
  const connection = connectionFactory();
  const redirectUrl = `http://localhost:${port}`;
  if (connection.kind === "google-ads") {
    const oauth2Client = new OAuth2Client(
      connection.credentials.clientId,
      connection.credentials.clientSecret,
      redirectUrl
    );
    const url = oauth2Client.generateAuthUrl({
      access_type: "offline",
      scope: ["https://www.googleapis.com/auth/adwords"],
      prompt: "consent",
    });
    console.debug(
      `Make sure you have set up the redirect URI in the Google Cloud Console. Redirect URL: ${redirectUrl}`
    );
    console.log(`Open this URL in your browser to authenticate: ${url}`);
    const request = await waitForRequest(port);
    const code = request.query.code;
    console.log(`Oauth code`, code);
    const { tokens } = await oauth2Client.getToken(code);
    console.log(`Oauth tokens`, tokens);
    if (tokens.expiry_date) {
      console.log(`Expiration date`, new Date(tokens.expiry_date));
    }
  } else {
    throw new Error(`OAuth flow is not supported for connection kind ${connection.kind}`);
  }
}

export async function main(argv: string[] = process.argv) {
  const program = new Command();
  program.name("audience-sync").description("Synchronize data from your database to external services.");
  const syncCommand = program.command("sync");
  const authHelper = program.command("auth-helper");

  syncCommand
    .description("Run all or selected syncs of a given project")
    .option(
      "-e, --env <file...>",
      "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options"
    )
    .option("-h, --help", "Display this help message")
    .option(
      "-d, --project-dir <project-directory>",
      "Which directory to look in for the project. If not specified, a current directory will be used"
    )
    .option("-t, --state <state-directory>", "Where to store state of syncs. Default is <project-directory>/.state")
    .option(
      "-s, --select <syncs>",
      "Which syncs to run. Can be a sync id or list of sync ids separated by comma. If not provided, all syncs will be run"
    )
    .option("--debug", "Enable extra logging for debugging purposes")
    .option(
      "-f, --full-refresh",
      "If sync supports incremental mode, this option will force full refresh. Will apply to all selected syncs"
    )
    .action(sync);

  authHelper
    .description("Trigger an oauth flow for a given connection to generate credentials")
    .option(
      "-e, --env <file...>",
      "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options"
    )
    .option("-d, --project-dir <project-directory>", "Which directory to look in for the project")
    .option("-c, --connection <connectionId>", "Connection id")
    .option("-p, --port <port>", `Port where to run server (${defaultOauthRedirectURIPort}) to default`)
    .option("--debug", "Enable extra logging for debugging purposes")
    .action(triggerOauthFlow);

  const debug = argv.includes("--debug");
  try {
    await program.parseAsync(argv);
    process.exit(0);
  } catch (e: any) {
    if (e instanceof RpcError) {
      console.error(
        `The command failed due to unhandled error in the RPC call to ${e.url}. Status code ${e.statusCode}. Response`,
        e.response
      );
    }
    if (debug) {
      console.error(e);
    } else {
      console.error(`Failed: ${e?.message || "Unknown error"}`);
    }
    process.exit(1);
  }
}

main();
