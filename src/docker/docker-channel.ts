import {
  ComponentChannel,
  IncomingMessage,
  LogMessage,
  Message,
  messageProcessingFlow,
  MessageProcessingMode,
  ReplyChannel, ReplyMessage,
  systemMessageTypes,
} from "../types/protocol";
import Docker from "dockerode";
import express from "express";
import http from "http";
import { ExecutionContext } from "../connections/types";
import JSON5 from "json5";
import readline from "readline";
import assert from "assert";


export type RpcHandler = (ctx: ExecutionContext | undefined, opts: {
  body: any;
  path: string;
  query: Record<string, string>;
}) => Promise<any>;

type RpcServer = { port: number, close: () => Promise<void> | void };


export class DockerChannel implements ComponentChannel {
  private image: string;
  private docker: Docker;
  private ctx?: ExecutionContext;
  private rpcServer?: RpcServer;
  private container: any;
  private containerStream?: any;
  private lineReader?: readline.Interface;

  constructor(image: string) {
    this.image = image;
    this.docker = new Docker({
      socketPath: "/var/run/docker.sock",
    });
  }

  async init() {
    this.rpcServer = await this.createRpcServer(this.handleRpcRequest);
    console.log(`RPC server started on port ${this.rpcServer.port}. Pulling image ${this.image}`);
    await this.docker.pull(this.image, (err) => {
      if (err) {
        console.error(`Failed to pull image ${this.image}: ${err.message}`);
        return;
      }
    });
    console.log(`Image ${this.image} pulled. Creating container...`);
    // Create and start the container
    // see https://docs.docker.com/engine/api/v1.45/#tag/Container/operation/ContainerCreate
    this.container = await this.docker.createContainer({
      Image: this.image,
      AttachStdin: true,
      AttachStdout: true,
      AttachStderr: true,
      OpenStdin: true,
      StdinOnce: false,
      ExtraHosts: ["host.docker.internal:host-gateway"],
      Tty: false,
      env: [`RPC_URL=http://host.docker.internal:${this.rpcServer.port}`],
      name: `syncmaven-${new Date().toISOString().replace(/[T\:\-]/g, "").replace(/\./, "-")}`,
    });
    console.log(`Container created. Id: ${this.container.id}`);
  }


  async handleRpcRequest(ctx: ExecutionContext | undefined, opts: { body: any; path: string; query: Record<string, string> }): Promise<any> {
    console.log(`RPC path:${opts.path} query:${JSON.stringify(opts.query)} body:${JSON.stringify(opts.body)} ctx:${JSON.stringify(ctx)}`);
    if (!ctx) {
      throw new Error("Context is not set");
    }
    const key = opts.body.key;
    if (key) {
      switch (opts.path) {
        case "/state.get":
          const res = await ctx.store.get(key);
          console.log(`GET ${key}: ${JSON.stringify(res)}`);
          return res || {};
        case "/state.set":
          await ctx.store.set(key, opts.body.value);
          return {};
      }
    }
    return {};
  }


  /**
   * Wait for container to stop. Returns result, or undefined if container is still running
   */
  async await<T>(cb: () => Promise<T>, timeoutMs: number): Promise<{ result: T, timedOut: false } | { result?: never, timedOut: true }> {
    return new Promise((resolve) => {
      let timedOut = false;
      const timeout = setTimeout(() => {
        timedOut = true;
        resolve({ timedOut: true });
      }, timeoutMs);
      cb().then((result) => {
        clearTimeout(timeout);
        resolve({ result, timedOut: false });
      });
    });
  }

  async isContainerRunning() {
    try {
      const inspected = await this.container.inspect();
      //console.debug(`Container inspection status: ${JSON.stringify(inspected, null, 2)}`);
      return inspected.State.Running;
    } catch (e) {
      console.error(`Failed to inspect container ${this.container?.id} of ${this.image} to check if it's running. We assume it's not running`, { cause: e });
      return false;
    }
  }

  async startContainer(stdoutHandler: (line: string) => Promise<void> | void) {
    if (await this.isContainerRunning()) {
      throw new Error(`Container ${this.container.id} of ${this.image} is already running. Can't start it`);
    }
    if (this.containerStream) {
      console.warn(`Illegal state: container stream is set, but container is not running. Cleaning up...`);
    }
    this.containerStream = await this.container.attach({
      stream: true,
      stdin: true,
      stdout: true,
      stderr: true,
      hijack: true,
    });
    this.lineReader = readline.createInterface({ input: this.containerStream });
    this.lineReader.on("line", async (data) => {
      //console.debug(`Got '${data}' from container ${this.container.id} of ${this.image}`);
      if (data.trim() !== "") {
        await stdoutHandler(data);
      }
    });

    console.log(`Starting container ${this.container.id} of ${this.image}...`);
    await this.container.start();
    console.log(`Container ${this.container.id} of ${this.image} has been started`);
  }

  /**
   * Forcefully stops the container
   */
  async stopContainer() {
    if (await this.isContainerRunning()) {
      console.log(`Stopping container ${this.container.id} of ${this.image}...`);
      await this.runCleanup(() => this.container.stop());
      await this.runCleanup(() => this.lineReader?.close());
      await this.runCleanup(() => this.containerStream?.close());
      this.containerStream = undefined;
      this.lineReader = undefined;
      console.log(`Container ${this.container?.id} of ${this.image} has been stopped`);
    } else {
      console.log(`Container ${this.container?.id} of ${this.image} is already stopped`);
    }
  }

  /**
   * Waits until container stops
   */
  async waitForContainerStop(opts: {
    //if specified and returns true signals that the waiting processes must be interrupted
    interrupt?: () => boolean
    //maximum wait time. Not implemented
    timeoutMs?: number
    //how often to check for a status
    pullIntervalMs?: number
  } = {}) {
    const {interrupt = () => false, timeoutMs = Number.MAX_VALUE, pullIntervalMs = 1000} = opts;
    while (!interrupt()) {
      if (!await this.isContainerRunning()) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, pullIntervalMs));
    }
  }

  /**
   * Parses message. If message malfromed, just ignores it
   * @param json
   */
  parseRawMessage(json: any): Message | undefined {
    try {
      return  Message.parse(json);
    } catch (e) {
      //just ignore invalid messages
      console.error(`Failed to parse message: ${JSON.stringify(json)}`, { cause: e });
      return undefined;
    }
  }

  async dispatchMessageInternal(opts: {
    //message to dispatch
    incomingMessage: IncomingMessage,
    mode: MessageProcessingMode,
    //only for keep-alive mode. If true, waits for next non-system (see systemMessageTypes) from container and invokes the callback
    //on this message
    waitForResult?: boolean,
    //callback to handle a message. The callback can throw an exception, that means that dispatchMessageInternal
    //should throw an exception as well. Exceptions from callbacks are truly non-recoverable
    handler?: (replyMessage: Message) => Promise<void> | void,
  }): Promise<ReplyMessage | undefined> {
    const { incomingMessage, mode, handler, waitForResult } = opts;
    const running = await this.isContainerRunning();
    if (mode === "singleton" && running) {
      throw new Error(`Can't dispatch '${incomingMessage.type}' message to ${this.image} in singleton mode. Container is already running`);
    }

    if (mode === "singleton") {
      assert(!waitForResult, `waitForResult should not be set in singleton mode`);
      assert(!running, `Can't dispatch '${incomingMessage.type}' message to ${this.image} in singleton mode. Container is already running`);
      assert(handler, `Handler must be set in singleton mode`);
      try {
        let failedError: (Error | undefined) = undefined;
        await this.startContainer(async (data) => {

          const message = this.parseRawMessage(this.parseLine(data));
          console.debug(`Received message from container ${this.container.id} of ${this.image}: ${JSON.stringify(message)}`);
          if (!message) {
            return;
          }
          try {
            await handler(message);
          } catch (e: any) {
            console.error(`Error occurred while handling message`, { cause: e });
            failedError = e instanceof Error ? e : new Error(e?.message || "Unknown error", { cause: e });
          }
        });
        await this.containerStream.write(JSON.stringify(incomingMessage) + "\n");
        //this.containerStream.end();
        console.log(`Waiting for container ${this.container.id} of ${this.image} to conclude it's work on '${incomingMessage.type}' message`);
        await this.waitForContainerStop({interrupt: () => !!failedError});
        if (failedError) {
          throw new Error(`Container ${this.container.id} of ${this.image} failed with non-recoverable error`, { cause: failedError });
        }
      } finally {
        await this.stopContainer();
      }
    } else if (mode === "keep-alive") {
      if (!running) {
        assert(handler, `Handler must be set in keep-alive mode for the first message`);
        await this.startContainer(async (data) => {
          const message = this.parseRawMessage(this.parseLine(data));
          if (!message) {
            return;
          }

          try {
            await handler(message);
          } catch (e: any) {
            console.error(`Error occurred while handling message`, { cause: e });
            //we can't do anything here, just log the error. Whoever send a first message should handle it
          }
        })
      } else {
        assert(!handler, `Line handler should be passed only with a fist message in keep-alive mode. Subsequent message cannot set up handlers`);

      }
      assert(this.containerStream, `Container stream must be defined in keep-alive mode at this point`)
      assert(this.lineReader, `Container lineReader must be defined in keep-alive mode at this point`)
      await this.containerStream.write(JSON.stringify(incomingMessage) + "\n");
      if (waitForResult) {
        return new Promise((resolve) => {
          let listener;
          this.lineReader?.on("line", listener = async (data) => {
            if (data.trim() === "") {
              return;
            }
            const message = this.parseRawMessage(this.parseLine(data));
            if (message && message.type && !(systemMessageTypes as string[]).includes(message.type as string)) {
              this.lineReader?.off("line", listener);
              resolve(message as ReplyMessage);
            }
          })
        })
      }
    } else {
      //mode === close
      assert(!handler, `Handler must not be set in close mode`);
      await this.containerStream.write(JSON.stringify(incomingMessage) + "\n");
      await this.waitForContainerStop();
    }
  }

  /**
   * Parses incoming line into a message. Wraps non-valid JSON into
   * log message
   */
  parseLine(data: string): any {
    while (data.charAt(0) != "{" && data.length > 0) {
      data = data.slice(1);
    }
    try {
      return JSON5.parse(data);
    } catch (e) {
      return { type: "log", payload: { level: "info", message: data } };
    }
  }


  async dispatchMessage(incomingMessage: IncomingMessage, channel: ReplyChannel, ctx) {
    if (!this.container) {
      //lazy init container on a first message
      await this.init();
    }
    const flow = messageProcessingFlow[incomingMessage.type];
    console.debug(`Dispatching message ${incomingMessage.type} to ${this.image} with flow '${flow.mode}'. Expecting reply: ${!!flow.expectReply}`);
    const processingResult = await this.dispatchMessageInternal({
      incomingMessage,
      mode: flow.mode,
      handler: async (replyMessage) => {
        await channel.dispatchReplyMessage(replyMessage);
      },
      waitForResult: flow.expectReply,
    });
    if (processingResult) {
      await channel.dispatchReplyMessage(processingResult);
    }
  }

  async runCleanup(cb?: () => Promise<void> | void) {
    if (!cb) {
      return;
    }
    try {
      await cb();
    } catch (e: any) {
      console.warn(`Cleanup error, can be ignored - ${e?.message || "unknown error"}`, { cause: e });
    }
  }

  async close() {
    console.info(`Stopping container ${this.container.id} of ${this.image}...`);
    await this.runCleanup(() => this.container?.stop());
    console.info(`Container stopped. Removing container ${this.container.id} of ${this.image}`);
    await this.runCleanup(() => this.container?.remove());
    console.info(`Docker cleanup done for ${this.image}`);
    await this.runCleanup(() => this.rpcServer?.close());
  }

  private handleLogMessage(msg: any) {
    try {
      const parsedMessage = LogMessage.parse(msg);
      if (parsedMessage.payload.message?.trim()?.length > 0) {
        console[parsedMessage.payload.level](`[🐳${this.image}] ${parsedMessage.payload.message}`, ...(parsedMessage.payload.params || []));
      }
    } catch (e) {
      console.warn(`Invalid log message received: ${JSON.stringify(msg)}`, { cause: e });
    }

  }


  async createRpcServer(rpcHandler: RpcHandler): Promise<RpcServer> {
    const ctx = this.ctx;
    return new Promise((resolve) => {
      const app = express();
      app.use(express.json());
      const server = http.createServer(app);
      app.use((req, res, next) => {
        const body = req.body;
        const path = req.path;
        const query = req.query;
        rpcHandler(ctx, { body, path, query })
          .then((result) => {
            res.json(result);
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
