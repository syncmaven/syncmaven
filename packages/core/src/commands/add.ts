import { CommonOpts, PackageOpts, ProjectDirOpt, StateOpt } from "./index";
import path from "path";
import { untildify } from "../lib/project";
import { fmt, out, rewriteSeverityLevel, startLoading } from "../log";
import { ConnectionSpecMessage, StreamSpecMessage } from "@syncmaven/protocol";
import clr from "picocolors";
import prompts from "prompts";
import Ajv from "ajv";
import { SchemaObject } from "ajv/dist/types";
import fs from "fs";
import { dump } from "js-yaml";
import { getDestinationChannel } from "./sync";
import picocolors from "picocolors";

function pickId(projectDir: string, packageName: string) {
  const connectionsDir = path.join(projectDir, "connections");
  if (!fs.existsSync(connectionsDir)) {
    fs.mkdirSync(connectionsDir, { recursive: true });
  }
  const base = packageName
    .split("/")
    .pop()
    ?.replace(/[^a-zA-Z0-9]/g, "-")
    .toLowerCase();
  let cnt = 1;
  let fileName = base;
  while (fs.existsSync(path.join(connectionsDir, fileName + ".yml"))) {
    fileName = base + "-" + cnt++;
  }
  return fileName;
}

export async function add(args: string[], opts: CommonOpts & ProjectDirOpt & StateOpt & PackageOpts) {
  rewriteSeverityLevel("INFO", "DEBUG");
  const projectDir = path.resolve(untildify(opts?.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd()));
  const type = args?.[0]?.toLowerCase()?.trim();
  console.debug(`Adding ${type} to ${projectDir}`);
  if (type === "connection") {
    let packageName = args[1];
    if (!packageName) {
      throw new Error(`Missing package name. It should be provided as syncmaven add connection <package-name>`);
    }
    const packageType = opts.packageType || "docker";
    if (packageName.indexOf("/") < 0) {
      packageName = `syncmaven/${packageName}`;
    }
    const channel = await getDestinationChannel(
      {
        type: packageType,
        [packageType === "docker" ? "image" : "dir"]: packageName,
      },
      () => {}
    );
    let stopLoader = startLoading(
      `Getting metadata for ${clr.bold(packageName)}. For the first time, it might take a while...`
    );
    let connection: ConnectionSpecMessage;
    try {
      connection = await channel.describe();
    } finally {
      stopLoader();
    }
    const spec = connection.payload;
    if (!spec.roles.includes("destination")) {
      throw new Error(`Package ${packageName} does not support destination role`);
    }
    if (!spec.connectionCredentials.$schema) {
      throw new Error("Unsupported connection spec format. Credentials should be a JSON schema");
    }
    const credentialsSchema = spec.connectionCredentials as SchemaObject;
    console.debug(`Got credentials schema for ${packageName}`, JSON.stringify(credentialsSchema, null, 2));
    const ajv = new Ajv();
    const validator = ajv.compile(credentialsSchema);
    const config: Record<string, any> = {};
    for (const [key, prop] of Object.entries(credentialsSchema.properties) as [string, any][]) {
      const required = credentialsSchema.required?.includes(key);
      out(`Enter the value for ${clr.bold(key)}${required ? picocolors.red(" (required)") : ""}`);
      if (prop.description) {
        out(`Documentation: ${clr.gray(prop.description)}`);
      }
      const response = await prompts({
        type: prop.type === "string" ? "text" : prop.type,
        name: "value",
        instructions: prop.description,
        //required: credentialsSchema.required?.includes(key),
        message: `${key} `,
      });
      config[key] = response.value;
    }
    if (!validator(config)) {
      throw new Error(`Invalid configuration: ${ajv.errorsText(validator.errors)}`);
    }
    stopLoader = startLoading(`Validating credentials for ${clr.bold(packageName)}`);
    let availableStreams: StreamSpecMessage;
    try {
      availableStreams = await channel.streams({ type: "describe-streams", payload: { credentials: config } });
    } finally {
      stopLoader();
    }
    const connectionId = pickId(projectDir, packageName);
    const connectionYaml = {
      package: {
        type: packageType,
        [packageType === "docker" ? "image" : "dir"]: packageName,
      },
      credentials: config,
    };
    const connectionFile = path.join(projectDir, "connections", connectionId + ".yml");
    fs.writeFileSync(connectionFile, dump(connectionYaml), "utf-8");

    out([
      `${clr.green("✔")} Connection ${clr.bold(packageName)} added.`,
      `  ${clr.magenta("➔")} It's written to file: ${clr.bold(connectionFile)}`,
      `  ${clr.magenta("➔")} Available streams: ${availableStreams.payload.streams.map(s => clr.bold(s.name)).join(", ")}`,
      `  ${clr.magenta("➔")} Get detailed information about streams with \`${clr.bold(`syncmaven streams ${args[1]}`)}\``,
    ]);
  } else {
    throw new Error(`Unknown object type: ${type}. Available types: connection, model, sync`);
  }
}
