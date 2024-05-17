import dotenv from "dotenv";
import path from "path";
import { SyncDefinition } from "../types/objects";
import assert from "assert";
import { ExecutionContext } from "../connections/types";
import { SqliteStore } from "../lib/store";
import {
  BaseChannel,
  DestinationChannel, EnrichmentChannel,
  HaltMessage,
  LogMessage,
} from "../types/protocol";
import { getDestinationChannel, getEnrichmentProvider } from "../connections";
import { stringifyZodError } from "../lib/zod";
import { readProject, untildify } from "../lib/project";
import { executeQuery } from "../lib/datasource";
import { createErrorThreshold } from "../lib/error-threshold";
import { Project } from "../types/project";
import { createParser, SchemaBasedParser, stringifyParseError } from "../lib/uniparser";

export async function sync(projectDir: string, opts: {
  projectDir?: string;
  state?: string;
  select?: string;
  fullRefresh?: boolean;
  env?: string[];
}) {
  projectDir = untildify(projectDir || opts.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd());
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
    await runSync(project, syncId, projectDir, opts);
  }
  console.debug(`All syncs finished`);
}

async function runSync(project: Project, syncId: string, projectDir: string, opts: { projectDir?: string; state?: string; select?: string; fullRefresh?: boolean; env?: string[] }) {
  const syncFactory = project.syncs[syncId];
  if (!syncFactory) {
    throw new Error(
      `Sync with id \`${syncId}\` not found in the project, or it's disabled. Available syncs: ${Object.keys(project.syncs).join(", ")}`,
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
    `Destination with id \`${destinationId}\` referenced from sync \`${syncId}\` not found in the project`,
  );
  const destination = destinationFactory();
  const context: ExecutionContext = {
    store: new SqliteStore(path.join(projectDir, ".state")),
  };

  let halt = false
  let haltError:any

  const messageListener = message => {
    switch (message.type) {
      case "log":
        const logMes = message as LogMessage
        const params = logMes.payload.params?.length ? ` ${logMes.payload.params.map(p => JSON.stringify(p)).join(", ")}` : ""
        console.log(`LOG [${syncId}] ${logMes.payload.level.toUpperCase()} ${logMes.payload.message}${params}`)
        break;
      case "halt":
        const haltMes = message as HaltMessage
        halt = true
        if (haltMes.payload.status == "error") {
          haltError = new Error(haltMes.payload.message)
          console.error(`HALT [${syncId}] ERROR ${haltMes.payload.message} data: ${haltMes.payload.data ?JSON.stringify(haltMes.payload.data): ""}`)
        } else {
          console.log(`HALT [${syncId}] OK ${haltMes.payload.message} data: ${haltMes.payload.data ?JSON.stringify(haltMes.payload.data): ""}`)
        }
        break
      default:
        console.error(`${message.type?.toUpperCase} [${syncId}] Unexpected message type: '${message.type}' payload: ${JSON.stringify(message)}`)
    }
  }

  const destinationChannel: DestinationChannel | undefined = getDestinationChannel(destination, messageListener);
  const enrichments: EnrichmentChannel[] = [];
  try {

    if (!destinationChannel) {
      throw new Error(`Destination provider ${destination.kind} referenced from ${syncId} not found`);
    }
    const connectionSpec = await destinationChannel.describe()
    const streamsSpec = await destinationChannel.streams()
    const streamId = sync.stream || streamsSpec.payload.defaultStream;
    const streamSpec = streamsSpec.payload.streams.find(s => s.name === streamId);
    if (!streamSpec) {
      throw new Error(`Stream ${streamId} not found in destination ${destinationId}`);
    }
    console.debug(`Stream spec: ${JSON.stringify(streamSpec)}`);
    const rowSchemaParser = createParser(streamSpec.rowType);
    const connectionCredentialsParser = createParser(connectionSpec.payload.connectionCredentials);
    const parsedCredentials = connectionCredentialsParser.safeParse(destination.credentials);
    if (!parsedCredentials.success) {
      throw new Error(
        `Invalid credentials for destination ${destinationId}: ${stringifyParseError(parsedCredentials.error)}`,
      );
    }
    await destinationChannel.startStream({
      type: "start-stream",
      payload: {
        stream: streamId,
        connectionCredentials: parsedCredentials.data,
        streamOptions: sync.options || {},
        syncId,
        fullRefresh: !!opts.fullRefresh,
      },
    }, context)


    const enrichmentSettings = sync.enrichments || (sync.enrichment ? [sync.enrichment] : []);

    for (const enrichmentRef of enrichmentSettings) {
      const connection = project.connection[enrichmentRef.connection]();
      const enrichmentProvider = getEnrichmentProvider(connection, () => {});
      if (!enrichmentProvider) {
        throw new Error(`Enrichment provider ${connection.kind} referenced from ${syncId} not found`);
      }
      enrichments.push(enrichmentProvider);
      await enrichmentProvider.startEnrichment({
        type: "enrichment-connect",
        payload: { credentials: connection.credentials, options: enrichmentRef.options },
      }, context)
    }

    let totalRows = 0;
    let enrichedRows = 0;
    const errorThreshold = createErrorThreshold();
    await executeQuery({
      query: model.query,
      datasource: model.datasource,
      handler: {
        header: async header => {
          //await streamHandler.setup(header, context);
        },
        row: async row => {
          const parseResult = rowSchemaParser.safeParse(row);
          if (parseResult.success) {
            let rows = [parseResult.data];
            for (const enrichment of enrichments) {
              rows = await applyEnrichment(rowSchemaParser, enrichment, rows, context);
            }
            if (rows.length > 1) {
              console.debug(
                `Enrichment expanded row ${JSON.stringify(parseResult.data)} to ${rows.length} rows: ${JSON.stringify(rows)}`,
              );
            }
            enrichedRows += rows.length;
            for (const row of rows) {
              if (halt) {
                break
              }
              await destinationChannel.row({ type: "row", payload: { row } })
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
        },
        finalize: async () => {
        },
      },
    });
    const res = await destinationChannel.stopStream()
    console.info(`Sync ${syncId} is finished. Source rows ${totalRows}, enriched: ${enrichedRows}, channel stats: ${JSON.stringify(res.payload)}`);
  } finally {
    console.debug(`Closing all communications channels of sync '${syncId}'. It might take a while`);
    await closeChannels([...enrichments, destinationChannel]);
    console.debug(`All channels of '${syncId}' has been closed`);
  }
}

async function applyEnrichment(
  rowType: SchemaBasedParser,
  enrichment: EnrichmentChannel,
  rows: any[],
  ctx: ExecutionContext,
): Promise<any[]> {
  const res: any[] = [];
  for (const row of rows) {
    try {
      const item = await enrichment.row({ type: "enrichment-request", payload: { row } })
      const parseResult = rowType.safeParse(item.payload.row);
      if (parseResult.success) {
        res.push(parseResult.data);
      } else {
        console.warn(
          `Enrichment returned invalid row: ${stringifyZodError(parseResult.error)}. Will skip it. Row: ${JSON.stringify(item)}`,
        );
      }
    } catch (e: any) {
      console.error(`Enrichment error: ${e.message} on row: ${JSON.stringify(row)}`)
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