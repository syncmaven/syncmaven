import { z } from "zod";
import { Simplify } from "type-fest";

export type StorageKey = string | string[];

export type Entry = {
  key: StorageKey;
  value: any;
};

export interface StreamPersistenceStore {
  init(): Promise<void>;

  get(key: StorageKey): Promise<any>;

  set(key: StorageKey, value: any): Promise<void>;

  del(key: StorageKey): Promise<void>;

  list(prefix: StorageKey): Promise<Entry[]>;

  stream(prefix: StorageKey, cb: (entry: Entry) => Promise<void> | void): Promise<any>;

  streamBatch(prefix: StorageKey, cb: (batch: Entry[]) => Promise<void> | void, maxBatchSize: number): Promise<any>;

  deleteByPrefix(prefix: StorageKey): Promise<void>;

  size(prefix: StorageKey): Promise<number>;
}

export type ExecutionContext = {
  store: StreamPersistenceStore;
};
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
    payload: z.object({
      credentials: z.any(),
    }),
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

export const StreamResultMessage = MessageBase.merge(
  z.object({
    type: z.literal("stream-result"),
    direction: z.literal("reply").default("reply").optional(),
    payload: z.object({
      received: z.number(),
      success: z.number(),
      skipped: z.number(),
      failed: z.number(),
    }),
  })
);

export type StreamResultMessage = z.infer<typeof StreamResultMessage>;

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
      status: z.enum(["ok", "error"]),
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

export type EnrichmentResponse = z.infer<typeof EnrichmentResponse>;

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

export const IncomingMessage = z.discriminatedUnion("type", [
  DescribeConnectionMessage,
  DescribeStreamsMessage,
  StartStreamMessage,
  EndStreamMessage,
  RowMessage,
  EnrichmentRequest,
  EnrichmentConnect,
]);

export type IncomingMessage = Simplify<z.infer<typeof IncomingMessage>>;

export const ReplyMessage = z.discriminatedUnion("type", [
  ConnectionSpecMessage,
  StreamSpecMessage,
  StreamResultMessage,
  LogMessage,
  HaltMessage,
  EnrichmentResponse,
]);

export type ReplyMessage = Simplify<z.infer<typeof ReplyMessage>>;

export const Message = z.union([IncomingMessage, ReplyMessage]);

/**
 * How message should be processed by the channel
 * The logic is somewhat similar to HTTP connection processing types
 */
export type MessageProcessingMode =
  //the process should send open channel, listen to replies and wait for channel to be closed
  | "singleton"
  //the process should send message, set up listener and keep channel open
  | "keep-alive"
  //assumes that the
  | "close";

export const messageProcessingFlow: Record<
  IncomingMessage["type"],
  { mode: MessageProcessingMode; expectReply?: boolean }
> = {
  describe: { mode: "singleton" },
  "describe-streams": { mode: "singleton" },
  "start-stream": { mode: "keep-alive" },
  "end-stream": { mode: "close" },
  row: { mode: "singleton" },

  //not working right now, we should not support it
  "enrichment-request": { mode: "keep-alive", expectReply: true },
  "enrichment-connect": { mode: "keep-alive" },
};

/**
 * Messages that are not replies to any particular message, but are system messages
 */
export const systemMessageTypes: ReplyMessage["type"][] = ["halt", "log"];

export type Message = Simplify<z.infer<typeof Message>>;

export type MessageHandler = (message: Message) => Promise<void> | void;
export type SingletonMessageHandler = (message: Message) => Promise<"done" | void> | "done" | void;

export type ReplyChannel = {
  dispatchReplyMessage: (message: Message) => Promise<void>;
};

export interface BaseChannel {
  init(messagesListener?: MessageHandler);

  describe: () => Promise<ConnectionSpecMessage>;
  close?: () => Promise<void> | void;
}

export interface DestinationChannel extends BaseChannel {
  streams: (msg: DescribeStreamsMessage) => Promise<StreamSpecMessage>;
  startStream: (startStreamMessage: StartStreamMessage, ctx: ExecutionContext) => Promise<void>;
  row: (rowMessage: RowMessage) => Promise<void>;
  stopStream: () => Promise<StreamResultMessage>;

  //dispatchMessage: (messages: IncomingMessage, channel: ReplyChannel, ctx: ExecutionContext) => Promise<void> | void;
}

export interface EnrichmentChannel extends BaseChannel {
  startEnrichment: (startStreamMessage: EnrichmentConnect, ctx: ExecutionContext) => Promise<void>;
  row: (rowMessage: EnrichmentRequest) => Promise<EnrichmentResponse>;
}
