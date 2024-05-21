import {
  ConnectionSpecMessage,
  DestinationChannel,
  HaltMessage,
  MessageHandler,
  RowMessage,
  StartStreamMessage,
  StreamResultMessage,
  StreamSpecMessage,
  ExecutionContext,
  Entry,
} from "@syncmaven/protocol";
import express from "express";
import type { Response } from "express";

import http from "http";
import { DockerContainer } from "./container";
import { undefined } from "zod";

export type RpcHandler = (
  ctx: ExecutionContext | undefined,
  opts: {
    body: any;
    path: string;
    query: any;
  },
  res: Response,
) => Promise<any>;

type RpcServer = { port: number; close: () => Promise<void> | void };

export class DockerChannel implements DestinationChannel {
  private image: string;
  private rpcServer?: RpcServer;
  private dockerContainer?: DockerContainer;
  private ctx?: ExecutionContext;
  private inited: boolean = false;
  private messagesListener?: MessageHandler;

  constructor(image: string, messagesListener?: MessageHandler) {
    this.image = image;
    this.messagesListener = messagesListener;
  }

  async init() {
    if (!this.inited) {
      this.rpcServer = await this.createRpcServer();
      this.dockerContainer = new DockerContainer(this.image, [
        `RPC_URL=http://host.docker.internal:${this.rpcServer.port}`,
      ]);
      this.inited = true;
    }
  }

  async describe(): Promise<ConnectionSpecMessage> {
    await this.init();
    await this.dockerContainer?.start(this.messagesListener);
    let promiseResolve;
    let promiseReject;
    const promise = new Promise<ConnectionSpecMessage>((resolve, reject) => {
      promiseResolve = resolve;
      promiseReject = reject;
    }).finally(async () => {
      await this.dockerContainer?.stop();
    });
    this.dockerContainer?.dispatchMessage(
      { type: "describe" },
      async (message) => {
        switch (message.type) {
          case "spec":
            promiseResolve(message as ConnectionSpecMessage);
            return "done";
          case "halt":
            promiseReject(new Error((message as HaltMessage).payload.message));
            return "done";
        }
      },
    );
    return promise;
  }

  async streams(): Promise<StreamSpecMessage> {
    await this.init();
    await this.dockerContainer?.start(this.messagesListener);
    let promiseResolve;
    let promiseReject;
    const promise = new Promise<StreamSpecMessage>((resolve, reject) => {
      promiseResolve = resolve;
      promiseReject = reject;
    }).finally(async () => {
      await this.dockerContainer?.stop();
    });
    this.dockerContainer?.dispatchMessage(
      { type: "describe-streams" },
      async (message) => {
        switch (message.type) {
          case "stream-spec":
            promiseResolve(message as StreamSpecMessage);
            return "done";
          case "halt":
            promiseReject(new Error((message as HaltMessage).payload.message));
            return "done";
        }
      },
    );
    return promise;
  }

  async startStream(
    startStreamMessage: StartStreamMessage,
    ctx: ExecutionContext,
  ): Promise<void> {
    await this.init();
    this.ctx = ctx;
    await this.dockerContainer?.start(this.messagesListener);
    await this.dockerContainer?.dispatchMessage(startStreamMessage);
  }

  async stopStream() {
    let promiseResolve;
    let promiseReject;
    const promise = new Promise<StreamResultMessage>((resolve, reject) => {
      promiseResolve = resolve;
      promiseReject = reject;
    }).finally(async () => {
      await this.dockerContainer?.stop();
    });
    this.dockerContainer?.dispatchMessage(
      { type: "end-stream", reason: "success" },
      async (message) => {
        switch (message.type) {
          case "stream-result":
            promiseResolve(message as StreamResultMessage);
            return "done";
          case "halt":
            promiseReject(new Error((message as HaltMessage).payload.message));
            return "done";
        }
      },
    );
    return promise;
  }

  async row(rowMessage: RowMessage): Promise<void> {
    await this.dockerContainer?.dispatchMessage(rowMessage);
  }

  async close(): Promise<void> {
    await this.dockerContainer?.close();
    await this.rpcServer?.close();
    this.inited = false;
  }

  async handleRpcRequest(
    opts: {
      body: any;
      path: string;
      query: any;
    },
    res: Response,
  ): Promise<any> {
    const ctx = this.ctx;
    console.log(
      `RPC path:${opts.path} query:${JSON.stringify(opts.query)} body:${JSON.stringify(opts.body)} ctx:${JSON.stringify(ctx)}`,
    );
    if (!ctx) {
      throw new Error("Context is not set");
    }
    const key = opts.body.key;
    switch (opts.path) {
      case "/state.get":
        const v = await ctx.store.get(opts.body.key);
        return v || {};
      case "/state.set":
        await ctx.store.set(opts.body.key, opts.body.value);
        return {};
      case "/state.del":
        await ctx.store.del(opts.body.key);
        return {};
      case "/state.deleteByPrefix":
        await ctx.store.deleteByPrefix(opts.body.prefix);
        return {};
      case "/state.size":
        const n = await ctx.store.size(opts.body.prefix);
        return { size: n };
      case "/state.list":
        res.setHeader("Content-Type", "application/x-ndjson");
        await ctx.store.stream(opts.body.prefix, (e) => {
          res.write(JSON.stringify(e));
          res.write("\n");
        });
        res.end();
    }
    return {};
  }

  async createRpcServer(): Promise<RpcServer> {
    const chan = this;
    return new Promise((resolve) => {
      const app = express();
      app.use(express.json());
      const server = http.createServer(app);
      app.use((req, res, next) => {
        const body = req.body;
        const path = req.path;
        const query = req.query;
        chan
          .handleRpcRequest({ body, path, query }, res)
          .then((result) => {
            if (typeof result !== "undefined") {
              res.json(result);
            }
          })
          .catch((error) => {
            console.error(error);
            res.status(500).json({ error: error.message });
          });
        next();
      });

      server.listen(0, () => {
        const port = (server.address() as any).port;
        console.log(`Started one-time RPC server on http://localhost:${port}`);

        resolve({
          port,
          close: () => {
            return new Promise((resolve) => {
              server.close((err) => {
                if (err) {
                  resolve();
                  console.error(`Failed to close server: ${err.message}`);
                } else {
                  resolve();
                }
              });
            });
          },
        });
      });
    });
  }
}
