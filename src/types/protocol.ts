import { z, ZodType } from "zod";
import { Simplify } from "type-fest";
import {
  DestinationProvider,
  DestinationStream,
  EnrichmentProvider,
  ExecutionContext,
  StreamEnrichment,
} from "../connections/types";
import { stringifyZodError } from "../lib/zod";

const MessageBase = z.object({
  type: z.string(),
  direction: z.enum(["incoming", "reply"]).optional(),
  payload: z.unknown(),
});

type MessageBase = z.infer<typeof MessageBase>;

export const DescribeConnectionMessage = MessageBase.merge(
  z.object({
    type: z.literal("describe"),
    direction: z.literal("incoming").default("incoming").optional(),
    payload: z.never().optional(),
  })
);

export type DescribeConnectionMessage = z.infer<typeof DescribeConnectionMessage>;

export const ConnectionSpecMessage = MessageBase.merge(
  z.object({
    type: z.literal("spec"),
    direction: z.literal("reply").default("reply").optional(),
    payload: z.object({
      roles: z.array(z.enum(["enrichment", "destination"])),
      connectionCredentials: z.any(),
    }),
  })
);

export type ConnectionSpecMessage = z.infer<typeof ConnectionSpecMessage>;

export const DescribeStreamsMessage = MessageBase.merge(
  z.object({
    type: z.literal("describe-streams"),
    direction: z.literal("incoming").default("incoming").optional(),
    payload: z.never().optional(),
  })
);

export type DescribeStreamsMessage = z.infer<typeof DescribeStreamsMessage>;

export const StreamSpecMessage = MessageBase.merge(
  z.object({
    type: z.literal("stream-spec"),
    direction: z.literal("reply").default("reply").optional(),
    payload: z.object({
      roles: z
        .array(z.enum(["destination"]))
        .optional()
        .default(["destination"]),
      defaultStream: z.string(),
      streams: z.array(
        z.object({
          name: z.string(),
          rowType: z.any(),
        })
      ),
    }),
  })
);

export type StreamSpecMessage = z.infer<typeof StreamSpecMessage>;

export const StartStreamMessage = MessageBase.merge(
  z.object({
    type: z.literal("start-stream"),
    direction: z.literal("incoming").default("incoming").optional(),
    payload: z.object({
      stream: z.string(),
      connectionCredentials: z.any(),
      streamOptions: z.any(),
      syncId: z.string(),
      fullRefresh: z.boolean().optional().default(false),
    }),
  })
);

export type StartStreamMessage = z.infer<typeof StartStreamMessage>;

export const RowMessage = MessageBase.merge(
  z.object({
    type: z.literal("row"),
    direction: z.literal("incoming").default("incoming").optional(),
    payload: z.object({
      row: z.any(),
    }),
  })
);

export type RowMessage = z.infer<typeof RowMessage>;

export const EndStreamMessage = MessageBase.merge(
  z.object({
    type: z.literal("end-stream"),
    direction: z.literal("incoming").default("incoming").optional(),
    reason: z.enum(["success", "error"]),
  })
);

export type EndStreamMessage = z.infer<typeof EndStreamMessage>;

export const LogMessage = MessageBase.merge(
  z.object({
    type: z.literal("log"),
    direction: z.literal("reply").default("reply").optional(),
    payload: z.object({
      level: z.enum(["debug", "info", "warn", "error"]),
      message: z.string(),
      params: z.array(z.any()).optional(),
    }),
  })
);

export type LogMessage = z.infer<typeof LogMessage>;

export const HaltMessage = MessageBase.merge(
  z.object({
    type: z.literal("halt").optional(),
    direction: z.literal("reply").default("reply").optional(),
    payload: z.object({
      message: z.string().optional(),
      data: z.any().optional(),
    }),
  })
);

export type HaltMessage = z.infer<typeof HaltMessage>;

export const EnrichmentRequest = MessageBase.merge(
  z.object({
    type: z.literal("enrichment-request"),
    direction: z.literal("incoming").default("incoming").optional(),
    payload: z.object({
      row: z.any(),
    }),
  })
);

export type EnrichmentRequest = z.infer<typeof EnrichmentRequest>;

export const EnrichmentResponse = MessageBase.merge(
  z.object({
    type: z.literal("enrichment-response"),
    direction: z.literal("reply").default("reply").optional(),
    payload: z.object({
      row: z.any(),
    }),
  })
);

export const EnrichmentConnect = MessageBase.merge(
  z.object({
    type: z.literal("enrichment-connect"),
    direction: z.literal("incoming").default("incoming").optional(),
    payload: z.object({
      credentials: z.any(),
      options: z.any(),
    }),
  })
);

export type EnrichmentConnect = z.infer<typeof EnrichmentConnect>;

export const Message = z.discriminatedUnion("type", [
  DescribeConnectionMessage,
  ConnectionSpecMessage,
  DescribeStreamsMessage,
  StreamSpecMessage,
  StartStreamMessage,
  EndStreamMessage,
  RowMessage,
  LogMessage,
  HaltMessage,
  EnrichmentRequest,
  EnrichmentResponse,
  EnrichmentConnect,
]);

export type Message = Simplify<z.infer<typeof Message>>;

export type ReplyChannel = {
  send: (message: Message) => Promise<void>;
};

export interface ComponentChannel {
  handleMessage: (messages: Message, channel: ReplyChannel, ctx: ExecutionContext) => Promise<void> | void;
}

export async function processMessages(
  ch: ComponentChannel,
  message: Message,
  ctx: ExecutionContext
): Promise<Message[]> {
  const messages: Message[] = [];
  await ch.handleMessage(
    message,
    {
      send: async reply => {
        messages.push(reply);
      },
    },
    ctx
  );
  return messages;
}

export async function processMessageWithResult<T>(
  ch: ComponentChannel,
  message: Message,
  ctx: ExecutionContext,
  expectedMessageType: ZodType<T>
): Promise<T> {
  const messages = await processMessages(ch, message, ctx);
  if (messages.length !== 1) {
    throw new Error(`Expected exactly one in reply to a message ${message.type}, got ${messages.length}`);
  } else {
    try {
      //we parsing it for validation onlu, we should return the original message since it could contain some fields which
      //are not serializable
      expectedMessageType.parse(messages[0]);
      return messages[0] as T;
    } catch (e) {
      throw new Error(
        `Error while parsing a reply to '${message.type}' message: ${stringifyZodError(e)}. Reply: ${JSON.stringify(messages[0])}`
      );
    }
  }
}

export function createEnrichmentChannel(provider: EnrichmentProvider): ComponentChannel {
  let enrichment: StreamEnrichment | undefined;
  return {
    async handleMessage(message: Message, channel: ReplyChannel, ctx: ExecutionContext) {
      try {
        if (message.type === "describe") {
          await channel.send({
            type: "spec",
            payload: {
              roles: ["enrichment"],
              connectionCredentials: provider.credentialsType,
            },
          });
        } else if (message.type === "enrichment-connect") {
          if (enrichment) {
            await channel.send({ type: "halt", payload: { message: `Enrichment already connected` } });
            return;
          }
          enrichment = await provider.createEnrichment(
            {
              credentials: message.payload.credentials,
              options: message.payload.options,
            },
            ctx
          );
        } else if (message.type === "enrichment-request") {
          if (!enrichment) {
            await channel.send({ type: "halt", payload: { message: `Enrichment not connected` } });
            return;
          }

          const enrichedRows = await enrichment.enrichRow(message.payload.row, ctx);
          if (enrichedRows) {
            const rows = Array.isArray(enrichedRows) ? enrichedRows : [enrichedRows];

            for (const enrichedRow of rows) {
              await channel.send({ type: "enrichment-response", payload: { row: enrichedRow } });
            }
          }
        }
      } catch (e: any) {
        console.error(`Unhandled error while handling message ${message.type}`, e);
        await channel.send({ type: "halt", payload: { message: e?.message } });
      }
    },
  };
}

export function createDestinationChannel(provider: DestinationProvider): ComponentChannel {
  let outputStream;
  let stream: DestinationStream;

  return {
    async handleMessage(message: Message, channel: ReplyChannel, ctx: ExecutionContext) {
      try {
        if (message.type === "describe") {
          await channel.send({
            type: "spec",
            payload: {
              roles: ["destination"],
              connectionCredentials: provider.credentialsType,
            },
          });
        } else if (message.type === "describe-streams") {
          await channel.send({
            type: "stream-spec",
            payload: {
              roles: ["destination"],
              defaultStream: provider.defaultStream,
              streams: provider.streams.map(s => ({ name: s.name, rowType: s.rowType })),
            },
          });
        } else if (message.type === "start-stream") {
          stream = provider.streams.find(s => s.name === message.payload.stream)!;
          if (!stream) {
            await channel.send({
              type: "halt",
              payload: { message: `Stream ${message.payload.stream} not found in ${provider.name} manifest` },
            });
            return;
          }
          if (outputStream) {
            await channel.send({
              type: "halt",
              payload: { message: `Stream ${message.payload.stream} already started in ${provider.name}` },
            });
            return;
          }
          outputStream = await stream.createOutputStream(
            {
              streamId: message.payload.stream,
              options: message.payload.streamOptions,
              credentials: message.payload.connectionCredentials,
              syncId: message.payload.syncId,
              fullRefresh: message.payload.fullRefresh,
            },
            ctx
          );
        } else if (message.type === "end-stream") {
          if (!outputStream) {
            await channel.send({ type: "halt", payload: { message: `Stream not started in ${provider.name}` } });
            return;
          }
          await outputStream.finish(ctx);
          outputStream = undefined;
        } else if (message.type === "row") {
          if (!outputStream) {
            await channel.send({ type: "halt", payload: { message: `Stream not started in ${provider.name}` } });
            return;
          }
          try {
            message.payload = stream.rowType.parse(message.payload.row);
          } catch (e) {
            await channel.send({
              type: "halt",
              payload: { message: `Row cannot be parsed`, data: { row: message.payload.row } },
            });
            return;
          }

          await outputStream.handleRow(message.payload, ctx);
        }
      } catch (e: any) {
        console.error(`Unhandled error while handling message ${message.type}`, e);
        await channel.send({ type: "halt", payload: { message: e?.message } });
      }
    },
  };
}
