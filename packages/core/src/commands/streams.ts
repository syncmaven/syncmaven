import { CommonOpts, PackageOpts } from "./index";
import fs from "fs";
import JSON5 from "json5";
import { ConnectionDefinition } from "../types/objects";
import { getDestinationChannel } from "./sync";
import assert from "assert";
import { DescribeStreamsMessage } from "@syncmaven/protocol";
import { configureEnvVars } from "../lib/project";
import { fmt, out, rewriteSeverityLevel } from "../log";
import { SchemaObject } from "ajv/dist/types";
import { displayProperties } from "./destination";
import path from "path";
import { load } from "js-yaml";

function getChannelAndMessage(
  opts: CommonOpts &
    PackageOpts & { credentials?: string; connectionFile?: string } & { projectDir?: string; connectionId?: string }
) {
  let connectionFile = opts.connectionFile;
  if (opts.connectionId) {
    connectionFile = path.join(opts.projectDir || process.cwd(), "connections", `${opts.connectionId}.yml`);
    //TODO: it's possible to re-define connectionId within the file
    //we should read a whole project here
    if (!fs.existsSync(connectionFile)) {
      throw new Error(`Connection ${opts.connectionId} is not found in ${connectionFile}`);
    }
  }
  if (connectionFile) {
    if (!fs.existsSync(connectionFile)) {
      throw new Error(`Connection file ${opts.connectionFile} does not exist`);
    }
    const json = load(fs.readFileSync(connectionFile, "utf-8"));
    const connectionDefinition = ConnectionDefinition.parse(json);
    const channel = getDestinationChannel(connectionDefinition.package, () => {});
    const describeMessage: DescribeStreamsMessage = {
      type: "describe-streams",
      payload: {
        credentials: connectionDefinition.credentials,
      },
    };

    return { channel, describeMessage };
  } else {
    const packageType = opts.packageType || "docker";
    const channel = getDestinationChannel(
      {
        type: packageType,
        image: packageType === "docker" ? opts.package : undefined,
        dir: packageType === "npm" ? opts.package : undefined,
      },
      () => {}
    );
    assert(opts.credentials, "If connection file is not set, credentials must be provided");
    const describeMessage: DescribeStreamsMessage = {
      type: "describe-streams",
      payload: {
        credentials: JSON5.parse(opts.credentials),
      },
    };
    return { channel, describeMessage };
  }
}

export async function streams(
  args: string,
  opts: CommonOpts &
    PackageOpts & {
      credentials?: string;
      connectionFile?: string;
    }
) {
  console.debug("Calling streams", args, opts);
  rewriteSeverityLevel("INFO", "DEBUG");
  configureEnvVars(["."], opts.env || []);
  const { channel, describeMessage } = getChannelAndMessage({ ...opts, connectionId: args });

  const streams = await channel.streams(describeMessage);
  console.debug("Streams", JSON.stringify(streams, null, 2));

  out(
    `${fmt.bold(opts.connectionFile || opts.package || args)} declares the ${fmt.bold(streams.payload.streams.length)} streams:`
  );
  for (let i = 0; i < streams.payload.streams.length; i++) {
    const stream = streams.payload.streams[i];
    if (!stream.rowType.$schema) {
      throw new Error(`Stream ${stream.name} does not have a valid JSON schema`);
    }
    const jsonSchema = stream.rowType as SchemaObject;
    out(
      `${fmt.gray(i > 9 ? `${i + 1}.` : `0${i + 1}. `)}${fmt.bold(fmt.cyan(stream.name))}${stream.rowType.description ? `: ${fmt.gray(stream.rowType.description)}` : ""}`
    );
    displayProperties(jsonSchema, 4);
  }
}
