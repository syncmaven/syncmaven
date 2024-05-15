import { ComponentChannel, IncomingMessage, LogMessage, Message, ReplyChannel, replyTable } from "../types/protocol";
import Docker from "dockerode";
import express from "express";
import http from "http";
import { ExecutionContext } from "../connections/types";
import JSON5 from "json5";
import readline from "readline";
import assert from "assert";

export class DockerChannel implements ComponentChannel {
  private image: string;
  private docker: Docker;
  private ctx?: ExecutionContext;
  private rpcServer?: RpcServer;
  private container: any;
  private containerStream?: any;
  private lineReader?: readline.Interface;
  private shouldHalt = false;

  constructor(image: string) {
    this.image = image;
    this.docker = new Docker({
      socketPath: "/var/run/docker.sock",
    });
  }

  async init() {
    this.rpcServer = await createRpcServer(this.handleRpcRequest);
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
      Tty: false,
      env: [`RPC_URL=http://localhost:${this.rpcServer.port}`],
    });
    console.log(`Container created. Id: ${this.container.id}`);


    this.containerStream = await this.container.attach({
      stream: true,
      stdin: true,
      stdout: true,
      stderr: true,
      hijack: true,
    });
    assert(this.containerStream, "Container stream is not set");
    this.lineReader = readline.createInterface({ input: this.containerStream });
    this.lineReader.on("line", (data) => {
      let msg: any;
      try {
        msg = this.parseLine(data);
      } catch (e) {
        //if we can't parse the message, we'll just log it. Con
        this.handleLogMessage({ type: "log", payload: { level: "info", message: data } } );
        return;
      }
      if (msg.type === "log") {
        this.handleLogMessage(msg);
      } else if (msg.type === "halt") {
        //we can't halt here, setting flag instead
        console.log("Received halt message. Stopping process...");
        this.shouldHalt = true;
      }
    });

    // Start the container
    await this.container.start();
  }

  haltIfRequested() {
    if (this.shouldHalt) {
      throw new Error(`${this.image} requested full stop by sending 'halt' message`);
    }
  }

  parseLine(str: string): any {
    while (str.charAt(0) != "{" && str.length > 0) {
      str = str.slice(1);
    }
    return JSON5.parse(str);
  }

  async handleRpcRequest(opts: { body: any; path: string; query: Record<string, string> }): Promise<any> {
    if (!this.ctx) {
      throw new Error("Context is not set");
    }
  }

  /**
   * Waits until certain message is received from the channel
   * @param opts
   */
  async waitForMessage<T = any>(opts: {
    //sends an initial message. invoked after listener is set up
    init: () => Promise<void> | void;
    //parses a line into a message
    parser: (line: string) => any;
    //only messages that pass this filter, will be considered. If not set, all messages are accepted
    accept?: (message: T) => boolean;
    //not implemented yet. If no acceptable message is received, the promise will be rejected after this timeout
    timeoutMs?: number;
  }): Promise<T> {
    const reader = this.lineReader!;
    let listener;
    try {
      const messageAwaiter = new Promise<T>((resolve, reject) => {
        listener = (line) => {
          try {
            const message = opts.parser(line);
            if (!opts.accept || opts.accept(message)) {
              resolve(message);
            }
          } catch (e) {
            reject(new Error(`Failed to parse message: ${line}`, { cause: e }));
          }
        };
        reader.on("line", listener);
      });
      await opts.init();
      return await messageAwaiter;
    } finally {
      if (listener) {
        reader.off("line", listener);
      }
    }
  }

  async dispatchMessage(incomingMessage: IncomingMessage, channel: ReplyChannel, ctx) {
    this.haltIfRequested();
    if (!this.ctx) {
      console.log("Received first message, lazy-initializing docker image");
      this.ctx = ctx;
      await this.init();
    }
    assert(this.containerStream, "Container stream is not set");
    assert(this.lineReader, "Line reader is not set");
    const expectReply = !!replyTable[incomingMessage.type];
    if (expectReply) {
      console.debug(`Will send '${incomingMessage.type}' to channel, expecting reply...`);
      const replyMessage = await this.waitForMessage({
        init: () => {
          this.containerStream?.write(JSON.stringify(incomingMessage) + "\n");
          console.debug(`Send '${incomingMessage.type}' to channel, expecting reply. Message sent: '${JSON.stringify(incomingMessage)}'`);
        },
        parser: (line) => this.parseLine(line),
        accept: (reply) => replyTable[incomingMessage.type] === reply.type,
      });
      let parsedMessage: Message;
      try {
        console.debug(`Received reply to '${incomingMessage.type}' message: ${JSON.stringify(replyMessage)}`);
        parsedMessage = Message.parse(replyMessage);
      } catch (e) {
        throw new Error(`Error parsing message: ${JSON.stringify(incomingMessage)}`, { cause: e });
      }
      this.haltIfRequested();
      try {
        await channel.dispatchReplyMessage(parsedMessage);
      } catch (e) {
        throw new Error(`Error dispatch`, { cause: e });
      }
    } else {
      this.containerStream?.write(JSON.stringify(incomingMessage) + "\n");
    }
    this.haltIfRequested();
  }


  async close() {
    try {
      await this.rpcServer?.close();
    } catch (e: any) {
      console.warn(`Failed to stop server: ${e?.message}`);
    }
    try {
      console.info(`Stopping container ${this.container.id} of ${this.image}...`);
      await this.container?.stop();
      console.info(`Container stopped. Removing container ${this.container.id} of ${this.image}`);
      await this.docker.removeContainer(this.container.id);
      console.info(`Docker cleanup done for ${this.image}`);
    } catch (e: any) {
      console.warn(`Failed to stop server: ${e?.message}`);
    }
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
}

export type RpcHandler = (opts: {
  body: any;
  path: string;
  query: Record<string, string>;
}) => Promise<any>;

type RpcServer = { port: number, close: () => Promise<void> | void };

async function createRpcServer(rpcHandler: RpcHandler): Promise<RpcServer> {
  return new Promise((resolve) => {
    const app = express();
    const server = http.createServer(app);
    app.use((req, res, next) => {
      const body = req.body;
      const path = req.path;
      const query = req.query;
      rpcHandler({ body, path, query })
        .then((result) => {
          res.json(result);
        })
        .catch((error) => {
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