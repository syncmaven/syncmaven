import dotenv from "dotenv";
import path from "path";
import { SyncDefinition } from "../types/objects";
import assert from "assert";
import { ExecutionContext } from "../connections/types";
import { SqliteStore } from "../lib/store";
import { ComponentChannel, ConnectionSpecMessage, Message, processMessages, processMessageWithResult, StreamSpecMessage } from "../types/protocol";
import { getDestinationChannel, getEnrichmentProvider } from "../connections";
import { ZodSchema } from "zod";
import { stringifyZodError } from "../lib/zod";
import { readProject, untildify } from "../lib/project";
import { executeQuery } from "../lib/datasource";
import { createErrorThreshold } from "../lib/error-threshold";
import { Project } from "../types/project";

async function applyEnrichment(
  rowType: ZodSchema,
  enrichment: ComponentChannel,
  rows: any[],
  ctx: ExecutionContext,
): Promise<any[]> {
  const res: any[] = [];
  for (const row of rows) {
    const replyMessages = await processMessages(enrichment, { type: "enrichment-request", payload: { row } }, ctx);

    for (const item of replyMessages) {
      if (item.type !== "enrichment-response") {
        console.warn(
          `Enrichment returned unexpected message type ${item.type}. Expected 'enrichment-response'. Will skip it. Message: ${JSON.stringify(item)}`,
        );
        continue;
      }
      const parseResult = rowType.safeParse(item.payload.row);
      if (parseResult.success) {
        res.push(parseResult.data);
      } else {
        console.warn(
          `Enrichment returned invalid row: ${stringifyZodError(parseResult.error)}. Will skip it. Row: ${JSON.stringify(item)}`,
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

async function closeChannels(channels: (ComponentChannel | undefined)[]) {
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

  const destinationChannel: ComponentChannel | undefined = getDestinationChannel(destination);
  const enrichments: ComponentChannel[] = [];
  try {

    if (!destinationChannel) {
      throw new Error(`Destination provider ${destination.kind} referenced from ${syncId} not found`);
    }
    const connectionSpec = await processMessageWithResult(
      destinationChannel,
      { type: "describe" },
      context,
      ConnectionSpecMessage,
    );
    const streamsSpec = await processMessageWithResult(
      destinationChannel,
      { type: "describe-streams" },
      context,
      StreamSpecMessage,
    );
    const streamId = sync.stream || streamsSpec.payload.defaultStream;
    const streamSpec = streamsSpec.payload.streams.find(s => s.name === streamId);
    if (!streamSpec) {
      throw new Error(`Stream ${streamId} not found in destination ${destinationId}`);
    }
    if (!isZodSchema(streamSpec.rowType)) {
      throw new Error(
        `Row type of stream ${streamId} is not based on Zod. We don't support JSON schema yet. Schema: ${JSON.stringify(streamSpec.rowType)}`,
      );
    }
    const rowSchema = streamSpec.rowType as ZodSchema;
    if (!isZodSchema(connectionSpec.payload.connectionCredentials)) {
      throw new Error(
        `Connection credentials schema of ${destinationId} is not based on Zod. We don't support JSON schema yet`,
      );
    }
    const connectionCredentialsType = connectionSpec.payload.connectionCredentials as ZodSchema;
    const parsedCredentials = connectionCredentialsType.safeParse(destination.credentials);
    if (!parsedCredentials.success) {
      throw new Error(
        `Invalid credentials for destination ${destinationId}: ${stringifyZodError(parsedCredentials.error)}`,
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
        context,
      ),
      `Destination ${destinationId} initialization`,
    );
    const enrichmentSettings = sync.enrichments || (sync.enrichment ? [sync.enrichment] : []);

    for (const enrichmentRef of enrichmentSettings) {
      const connection = project.connection[enrichmentRef.connection]();
      const enrichmentProvider = getEnrichmentProvider(connection);
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
          context,
        ),
        `Enrichment ${enrichmentRef.connection} initialization`,
      );
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
          const parseResult = rowSchema.safeParse(row);
          if (parseResult.success) {
            let rows = [parseResult.data];
            for (const enrichment of enrichments) {
              rows = await applyEnrichment(rowSchema, enrichment, rows, context);
            }
            if (rows.length > 1) {
              console.debug(
                `Enrichment expanded row ${JSON.stringify(parseResult.data)} to ${rows.length} rows: ${JSON.stringify(rows)}`,
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
                    message,
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
            `Destination ${destinationId} finalization`,
          );
          // if (destinationChannel.close) {
          //   console.debug(`Closing destination channel`);
          //   await destinationChannel.close();
          // }
        },
      },
    });
    console.info(`Sync ${syncId} is finished. Source rows ${totalRows}, enriched rows ${enrichedRows}`);
  } finally {
    await closeChannels([...enrichments, destinationChannel]);
  }
}

export async function sync(opts: {
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
    await runSync(project, syncId, projectDir, opts);
  }
  console.debug(`All syncs finished`);
}
