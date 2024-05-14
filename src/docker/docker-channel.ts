import { ComponentChannel, Message, ReplyChannel } from "../types/protocol";
import Docker from "dockerode";
import express from "express";
import http from "http";
import { ExecutionContext } from "../connections/types";
import JSON5 from "json5";
import readline from "readline";

export class DockerChannel implements ComponentChannel {
  private image: string;
  private docker: Docker;
  private ctx?: ExecutionContext;
  private rpcServer?: RpcServer;
  private container: any;
  private messageQueue: Message[] = [];
  private containerStream?: any;

  constructor(image: string) {
    this.image = image;
    this.docker = new Docker({
      socketPath: "/var/run/docker.sock"
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
      env: [`RPC_URL=http://host.docker.internal:${this.rpcServer.port}`],
    });
    console.log(`Container created. Id: ${this.container.id}`);


    this.containerStream = await this.container.attach({
      stream: true,
      stdin: true,
      stdout: true,
      stderr: true,
      hijack: true,
    });
    const rl = readline.createInterface({ input: this.containerStream!, });
    rl!.on("line", (data) => {
      let rawMessage: any;
      let messageStr = data.toString();
      while (messageStr.charAt(0) != "{" && messageStr.length > 0) {
        messageStr = messageStr.slice(1);
      }

      try {
        rawMessage = JSON5.parse(messageStr);
      } catch (error) {
        console.error(`Received message is not in JSON format: '${messageStr}'`, error);
        return;
      }
      let message: Message;
      try {
        message = Message.parse(rawMessage);
      } catch (error) {
        console.error(`Received message doesn't match schema: ${messageStr}`, error);
        return;
      }
      this.handleParsedMessage(message);

    });

    // Start the container
    await this.container.start();
  }

  handleParsedMessage(message: Message) {
    if (message.type === "log") {
      const params = message.payload.params;
      const msgText = `${message.payload.message}`;
      if (params) {
        console[message.payload.level](msgText, ...(Array.isArray(params) ? params : [params]));
      } else {
        console[message.payload.level](msgText);
      }
    } else if (message.type === "halt") {
      console.error(`Received halt message. Closing...`);
      this.close();
    }
    this.messageQueue.push(message);
  }

  async handleRpcRequest(opts: { body: any; path: string; query: Record<string, string> }): Promise<any> {
    if (!this.ctx) {
      throw new Error("Context is not set");
    }
  }

  async handleMessage(message, channel: ReplyChannel, ctx) {
    if (!this.ctx) {
      console.log("Received first message, lazy-initializing docker image");
      this.ctx = ctx;
      await this.init();
    }
    console.log(`Sent ${message.type}: ${JSON.stringify(message)}. Will wait for ${message.result} messages to be processed from`);
  }


  async close() {
    try {
      await this.rpcServer?.close();
    } catch (e: any) {
      console.warn(`Failed to stop server: ${e?.message}`);
    }
    try {
      await this.container?.stop();
      await this.container?.remove();
    } catch (e: any) {
      console.warn(`Failed to stop server: ${e?.message}`);
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