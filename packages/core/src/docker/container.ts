import {
  IncomingMessage,
  Message,
  MessageHandler,
  SingletonMessageHandler,
} from "@syncmaven/protocol";
import Docker from "dockerode";
import readline from "readline";
import JSON5 from "json5";

import { exec, spawn, ChildProcessWithoutNullStreams, ChildProcess } from "child_process";
import assert from "assert";

export interface StdIoContainer {
  init(): Promise<void>;

  start(messagesHandler?: MessageHandler): Promise<any>;

  dispatchMessage(incomingMessage: IncomingMessage, messagesHandler?: SingletonMessageHandler): Promise<void>;

  /**
   * Forcefully stops the container
   */
  stop(): Promise<void>;

  close(): Promise<void>;
}

/**
 * Parses message. If message malfromed, just ignores it
 * @param json
 */
function parseRawMessage(json: any): Message | undefined {
  try {
    return Message.parse(json);
  } catch (e) {
    //just ignore invalid messages
    console.error(`Failed to parse message: ${JSON.stringify(json)}`, { cause: e });
    return undefined;
  }
}

/**
 * Parses incoming line into a message. Wraps non-valid JSON into
 * log message
 */
function parseLine(data: string): any {
  while (data.charAt(0) != "{" && data.length > 0) {
    data = data.slice(1);
  }
  try {
    return JSON5.parse(data);
  } catch (e) {
    return { type: "log", payload: { level: "info", message: data } };
  }
}

async function runCleanup(cb?: () => Promise<void> | void) {
  if (!cb) {
    return;
  }
  try {
    await cb();
  } catch (e: any) {
    //console.warn(`Cleanup error, can be ignored - ${e?.message || "unknown error"}`, { cause: e });
  }
}

export class CommandContainer implements StdIoContainer {
  private command: string;
  private messageHandler: MessageHandler | undefined = undefined;
  private oneTimeMessageHandler: SingletonMessageHandler | undefined = undefined;
  private proc: ChildProcessWithoutNullStreams | undefined;
  private lineReader?: readline.Interface;
  private cwd: string;

  constructor(command: string, cwd: string, envs: Record<string, string> = {}) {
    this.command = command;
    this.cwd = cwd;
  }

  init(): Promise<void> {
    //nothing to init, just run the command
    return Promise.resolve();
  }

  async start(messagesHandler?: MessageHandler | undefined) {
    if (messagesHandler) {
      this.messageHandler = messagesHandler;
    }
    console.debug(`Starting command container with command: ${this.command} in ${this.cwd}`);
    //there's a bug here, if bin contains spaces, it will not work. need to take into account escaping
    const [bin, ...args] = this.command.split(" ");
    this.proc = spawn(bin, args, {
      cwd: this.cwd,
      //stdio: "inherit"
    }) as ChildProcessWithoutNullStreams;
    assert(this.proc.stdout, "spawned process stdout is not defined");
    assert(this.proc.stdin, "spawned process stdout is not defined");
    this.lineReader = readline.createInterface({ input: this.proc.stdout });
    this.lineReader.on("line", async data => {
      if (data.trim() !== "") {
        const message = parseRawMessage(parseLine(data.trim()));
        console.debug(
          `Received message from container child process: ${JSON.stringify(message)}`
        );
        if (!message || (!this.messageHandler && !this.oneTimeMessageHandler)) {
          return;
        }
        try {
          if (this.oneTimeMessageHandler) {
            const s = await this.oneTimeMessageHandler(message);
            if (s === "done") {
              this.oneTimeMessageHandler = undefined;
              // oneTimeMessageHandler consumed message.
              // No need to pass it to global messageHandler
            } else if (this.messageHandler) {
              await this.messageHandler(message);
            }
          } else if (this.messageHandler) {
            await this.messageHandler(message);
          }
        } catch (e: any) {
          console.error(`Error occurred while handling message`, e);
        }
      }
    });
  }

  dispatchMessage(
    incomingMessage: IncomingMessage,
    messagesHandler?: SingletonMessageHandler | undefined
  ): Promise<void> {
    if (messagesHandler) {
      this.oneTimeMessageHandler = messagesHandler;
    }
    if (!this.proc) {
      throw new Error(`Illegal state: process is not running`);
    }
    console.debug(`Sending message to child process: ${JSON.stringify(incomingMessage)}`);
    this.proc.stdin.write(JSON.stringify(incomingMessage) + "\n");
    return Promise.resolve();
  }

  stop(): Promise<void> {
    if (this.proc) {
      this.proc.kill();
    }
    return Promise.resolve();
  }

  close(): Promise<void> {
    throw new Error("Method not implemented.");
  }
}

export class DockerContainer implements StdIoContainer {
  private image: string;
  private docker: Docker;
  private envs: string[];
  private container: any;
  private containerStream?: any;
  private lineReader?: readline.Interface;
  private messageHandler: MessageHandler | undefined = undefined;
  private oneTimeMessageHandler: SingletonMessageHandler | undefined =
    undefined;

  constructor(image: string, envs: string[]) {
    this.image = image;
    this.envs = envs;
    this.docker = new Docker({
      socketPath: "/var/run/docker.sock",
    });
  }

  async init() {
    const pullStream = await this.docker.pull(this.image);
    await new Promise((res) =>
      this.docker.modem.followProgress(pullStream, res),
    );

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
      env: this.envs,
      //env: [`RPC_URL=http://host.docker.internal:${this.rpcServer.port}`],
      name: `syncmaven-${new Date()
        .toISOString()
        .replace(/[T\:\-]/g, "")
        .replace(/\./, "-")}`,
    });
    console.log(`Container created. Id: ${this.container.id}`);
  }

  async start(messagesHandler?: MessageHandler) {
    if (messagesHandler) {
      this.messageHandler = messagesHandler;
    }

    return await this.startContainer(async line => {
      const message = parseRawMessage(parseLine(line.trim()));
      console.debug(
        `Received message from container ${this.container.id} of ${this.image}: ${JSON.stringify(message)}`,
      );
      if (!message || (!this.messageHandler && !this.oneTimeMessageHandler)) {
        return;
      }
      try {
        if (this.oneTimeMessageHandler) {
          const s = await this.oneTimeMessageHandler(message);
          if (s === "done") {
            this.oneTimeMessageHandler = undefined;
            // oneTimeMessageHandler consumed message.
            // No need to pass it to global messageHandler
          } else if (this.messageHandler) {
            await this.messageHandler(message);
          }
        } else if (this.messageHandler) {
          await this.messageHandler(message);
        }
      } catch (e: any) {
        console.error(`Error occurred while handling message`, e);
      }
    });
  }

  async dispatchMessage(
    incomingMessage: IncomingMessage,
    messagesHandler?: SingletonMessageHandler,
  ) {
    if (!this.containerStream) {
      throw new Error(
        `Illegal state: container is running but container stream is not`,
      );
    }
    if (messagesHandler) {
      this.oneTimeMessageHandler = messagesHandler;
    }
    console.debug(
      `Sending message to container ${this.container.id} of ${this.image}: ${JSON.stringify(incomingMessage)}`,
    );
    await this.containerStream.write(JSON.stringify(incomingMessage) + "\n");
  }

  async isContainerRunning() {
    try {
      const inspected = await this.container.inspect();
      //console.debug(`Container inspection status: ${JSON.stringify(inspected, null, 2)}`);
      return inspected.State.Running;
    } catch (e) {
      console.error(
        `Failed to inspect container ${this.container?.id} of ${this.image} to check if it's running. We assume it's not running`,
        { cause: e },
      );
      return false;
    }
  }

  async startContainer(stdoutHandler: (line: string) => Promise<void> | void) {
    if (!this.container) {
      //lazy init container on a first message
      await this.init();
    }
    if (await this.isContainerRunning()) {
      console.info(
        `Container ${this.container.id} of ${this.image} is already running.`,
      );
      return;
    }
    if (this.containerStream) {
      console.warn(
        `Illegal state: container stream is set, but container is not running. Cleaning up...`,
      );
    }
    // Workaround for https://github.com/apocas/dockerode/issues/742
    this.container.modem = new Proxy(this.container.modem, {
      get(target, prop) {
        const origMethod = target[prop];
        if (prop === "dial") {
          return function (...args) {
            if (args[0].path.endsWith("/attach?")) {
              args[0].file = Buffer.alloc(0);
            }
            return origMethod.apply(target, args);
          };
        } else {
          return origMethod;
        }
      },
    });
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
    console.log(
      `Container ${this.container.id} of ${this.image} has been started`,
    );
  }

  /**
   * Forcefully stops the container
   */
  async stop() {
    if (await this.isContainerRunning()) {
      console.log(`Stopping container ${this.container.id} of ${this.image}...`);
      await runCleanup(() => this.container.stop());
      console.log(`Container ${this.container?.id} of ${this.image} has been stopped`);
    } else {
      console.log(
        `Container ${this.container?.id} of ${this.image} is already stopped`,
      );
    }
    await runCleanup(() => this.lineReader?.close());
    await runCleanup(() => this.containerStream?.close());
    this.containerStream = undefined;
    this.lineReader = undefined;
  }

  /**
   * Waits until container stops
   */
  async waitForContainerStop(
    opts: {
      //if specified and returns true signals that the waiting processes must be interrupted
      interrupt?: () => boolean;
      //maximum wait time. Not implemented
      timeoutMs?: number;
      //how often to check for a status
      pullIntervalMs?: number;
    } = {},
  ) {
    const {
      interrupt = () => false,
      timeoutMs = Number.MAX_VALUE,
      pullIntervalMs = 1000,
    } = opts;
    while (!interrupt()) {
      if (!(await this.isContainerRunning())) {
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
      return Message.parse(json);
    } catch (e) {
      //just ignore invalid messages
      console.error(`Failed to parse message: ${JSON.stringify(json)}`, {
        cause: e,
      });
      return undefined;
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

  async runCleanup(cb?: () => Promise<void> | void) {
    if (!cb) {
      return;
    }
    try {
      await cb();
    } catch (e: any) {
      //console.warn(`Cleanup error, can be ignored - ${e?.message || "unknown error"}`, { cause: e });
    }
  }

  async close() {
    console.info(`Closing container ${this.container.id} of ${this.image}...`);
    await this.stop();
    console.info(`Container stopped. Removing container ${this.container.id} of ${this.image}`);
    await runCleanup(() => this.container?.remove());
    console.info(`Docker cleanup done for ${this.image}`);
  }
}
