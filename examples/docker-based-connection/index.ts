import readline from "readline";

import JSON5 from "json5";

function log(level: "debug" | "info" | "warn" | "error", message: string, ...params: any[]) {
  process.stdout.write(JSON.stringify({ type: "log", payload: { level, message, params: params.length > 0 ? params : undefined } }) + "\n");
}

function reply(type: string, payload: any) {
  process.stdout.write(JSON.stringify({ type, payload }) + "\n");
}

const credentialsSchema = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  properties: {
    requiredKey: {
      type: "string",
    },
    optionalKey: {
      type: "string",
    },
  },
  required: ["requiredKey"],
};

const rowSchema = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  properties: {
    email: {
      type: "string",
    },
  },
  required: ["requiredKey"],
};

function rpc(method: string, body: any): Promise<any> {
  return fetch(`${process.env.RPC_URL}/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(process.env.RPC_TOKEN && { Authorization: "Bearer " + process.env.RPC_TOKEN }),
    },
    body: JSON.stringify(body),
  })
    .then(response => {
      if (!response.ok) {
        log("error", "Failed to call RPC method", { status: response.status, body: response.body });
        return;
      }
      return response.json();
    })
    .catch(error => {
      log("error", "Failed to call RPC method", { error });
      return Promise.resolve();
    });
}

(async function main() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false,
  });

  let streamStarted = false;

  rl.on("line", line => {
    if (line.trim() === "") {
      return;
    }
    let message: any;
    try {
      message = JSON5.parse(line);
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
        description: "This is an example of a Docker-based connection",
        roles: ["destination"],
        connectionCredentials: credentialsSchema,
      });
    } else if (message.type === "describe-streams") {
      reply("stream-spec", {
        roles: ["destination"],
        defaultStream: "stream1",
        streams: [{ name: "stream1", rowType: rowSchema }],
      });
    } else if (message.type === "start-stream") {
      const stream = message.payload.stream;
      if (stream !== "stream1") {
        log("error", "Unknown stream", { stream });
        reply("halt", { message: `Unknown stream ${stream}` });
        process.exit(1);
      }
    } else if (message.type === "stop-stream") {
      setTimeout(() => {
        log("info", "Received stop-stream message. Bye!");
        process.exit(0);
      }, 1000);
    } else if (message.type === "row") {
      const row = message.payload.row;
      if (!row.email) {
        log("error", "Row does not have an email", { row });
      } else {
        if (process.env.RPC_URL) {
          console.log("Sending email to RPC", row.email);
          const key = `email=${row.email}`;
          rpc(`state.get`, { key: [key] }).then(json => {
            if (json) {
              const { counter } = json;
              log("info", `Got counter for ${row.email}`, { counter });
              rpc(`state.set`, { key: [key], value: { counter: counter + 1 } }).then(json => {
                if (json) {
                  log("info", `Counter for ${row.email} has been incremented`);
                }
              });
            }
          });
        }
      }
    } else {
      log("warn", `Unknown message type ${message.type}`, { message });
    }
  });

  // rl.once("close", () => {
  //   log("info", "Received shutdown signal");
  //   process.exit(0);
  // });

  //await new Promise((resolve) => setTimeout(resolve, 5000));
})();
