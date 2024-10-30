import readline from "readline";
import { DestinationProvider, OutputStream, rpc } from "./index";
import { zodToJsonSchema } from "zod-to-json-schema";
import { Entry, ExecutionContext, StartStreamMessage, StorageKey } from "@syncmaven/protocol";

let readLine: readline.Interface;
let stdProtocolEnabled = true;

function log(level: "debug" | "info" | "warn" | "error", message: string, ...params: any[]) {
  process.stdout.write(
    JSON.stringify({
      type: "log",
      payload: {
        level,
        message,
        params: params.length > 0 ? params : undefined,
      },
    }) + "\n"
  );
}

function reply(type: string, payload: any) {
  process.stdout.write(JSON.stringify({ type, payload }) + "\n");
}

function fatal(reason: string) {
  reply("halt", { message: reason, status: "error" });
  process.exit(1);
}

export async function stdProtocol(provider: DestinationProvider) {
  if (!stdProtocolEnabled) {
    return;
  }
  if (process.env.STD_PROTOCOL_DISABLED === "true") {
    return;
  }
  let received = 0;
  let skipped = 0;
  let failed = 0;
  let success = 0;
  let currentOutputStream: OutputStream | undefined = undefined;
  let ctx: ExecutionContext | undefined = undefined;

  const oldConsole = console;
  try {
    console = {
      ...console,
      log: (...args: any[]) => log("info", args[0], ...args.slice(1)),
      info: (...args: any[]) => log("info", args[0], ...args.slice(1)),
      warn: (...args: any[]) => log("warn", args[0], ...args.slice(1)),
      error: (...args: any[]) => log("error", args[0], ...args.slice(1)),
      debug: (...args: any[]) => log("debug", args[0], ...args.slice(1)),
    };

    readLine = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: false,
    });

    process.on("SIGINT", () => {
      readLine.close();
    });
    process.on("SIGTERM", () => {
      readLine.close();
    });

    for await (const line of readLine) {
      if (line.trim() === "") {
        return;
      }
      let message: any;
      try {
        message = JSON.parse(line);
      } catch (error) {
        log("error", "Message received cannot be parsed: " + line, { error });
        process.exit(1);
      }
      if (!message.type) {
        log("error", "Message received does not have a type", { message });
        process.exit(1);
      }
      log("debug", `Received message ${message.type}`, { message });
      if (message.type === "describe") {
        reply("spec", {
          description: provider.name,
          roles: ["destination"],
          connectionCredentials: zodToJsonSchema(provider.credentialsType),
        });
      } else if (message.type === "describe-streams") {
        reply("stream-spec", {
          roles: ["destination"],
          defaultStream: provider.defaultStream,
          streams: provider.streams.map(s => ({
            name: s.name,
            rowType: zodToJsonSchema(s.rowType),
            streamOptions: s.streamOptions ? zodToJsonSchema(s.streamOptions) : undefined,
          })),
        });
      } else if (message.type === "start-stream") {
        try {
          const streamName = message.payload.stream;
          const stream = provider.streams.find(s => s.name === streamName);
          if (!stream) {
            fatal(`Unknown stream ${streamName}`);
          } else {
            const payload = (message as StartStreamMessage).payload;
            if (!ctx) {
              ctx = createContext();
            }
            currentOutputStream = await stream.createOutputStream(
              {
                streamId: payload.stream,
                credentials: payload.connectionCredentials,
                syncId: payload.syncId,
                fullRefresh: payload.fullRefresh,
                options: payload.streamOptions,
              },
              ctx
            );
          }
        } catch (e: any) {
          fatal(`Failed to start stream: ${e.toString()}`);
        }
      } else if (message.type === "end-stream") {
        if (currentOutputStream) {
          log("info", "Received end-stream message. Bye!");
          if (currentOutputStream.finish) {
            try {
              await currentOutputStream.finish(ctx!);
            } catch (e: any) {
              fatal(`Failed to finish stream: ${e?.message || "unknown error"}${e.stack ? "\n" + e.stack : ""}`);
            }
          }
          setTimeout(() => {
            reply("stream-result", { received, skipped, success, failed });
            process.exit(0);
          }, 1000);
        } else {
          log("error", "There is no started stream.");
        }
      } else if (message.type === "row") {
        received++;
        if (currentOutputStream) {
          const row = message.payload.row;
          try {
            await currentOutputStream.handleRow(row, ctx!);
            success++;
          } catch (e: any) {
            failed++;
            log("error", `Failed to process row: ${JSON.stringify(row)} error: ${e.toString()}`);
          }
        } else {
          log("error", "There is no started stream.");
        }
      } else {
        log("warn", `Unknown message type ${message.type}`, { message });
      }
    }
  } finally {
    console = oldConsole;
  }
}

function createContext(): ExecutionContext {
  return {
    store: {
      init(): Promise<void> {
        return Promise.resolve();
      },
      async get(key: StorageKey): Promise<any> {
        return rpcCall("state.get", { key });
      },
      async set(key: StorageKey, value: any): Promise<void> {
        return rpcCall("state.set", {
          key,
          value,
        });
      },
      async del(key: StorageKey): Promise<void> {
        return rpcCall("state.del", { key });
      },
      async deleteByPrefix(prefix: StorageKey): Promise<void> {
        return rpcCall("state.deleteByPrefix", {
          prefix,
        });
      },
      async size(prefix: StorageKey): Promise<number> {
        const r = await rpcCall("state.size", { prefix });
        return r.size;
      },
      async list(prefix: StorageKey): Promise<Entry[]> {
        return rpcCall("state.list", {
          prefix,
        });
      },

      async stream(prefix: StorageKey, cb: (entry: Entry) => Promise<void> | void): Promise<any> {
        const res = (await rpcCall("state.list", {
          prefix,
        })) as Entry[];
        for (const e of res) {
          await cb(e);
        }
      },
      async streamBatch(
        prefix: StorageKey,
        cb: (batch: Entry[]) => Promise<void> | void,
        maxBatchSize: number
      ): Promise<any> {
        const res = (await rpcCall("state.list", {
          prefix,
        })) as Entry[];
        let batch: Entry[] = [];
        for (const e of res) {
          batch.push(e);
          if (batch.length >= maxBatchSize) {
            await cb(batch);
            batch = [];
          }
        }
        await cb(batch);
      },
    },
  };
}

async function rpcCall(method: string, body: any): Promise<any> {
  return rpc(`${process.env.RPC_URL}/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(process.env.RPC_TOKEN && {
        Authorization: "Bearer " + process.env.RPC_TOKEN,
      }),
    },
    body: body,
  });
}

export function disableStdProtocol() {
  stdProtocolEnabled = false;
  if (readLine) {
    readLine.close();
  }
}
