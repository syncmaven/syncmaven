import { CommonOpts, PackageOpts } from "./index";
import fs from "fs";
import JSON5 from "json5";
import { ConnectionDefinition } from "../types/objects";
import { getDestinationChannelFromDefinition, getDestinationChannelFromPackage } from "./sync";
import assert from "assert";
import { DescribeStreamsMessage } from "@syncmaven/protocol";
import { configureEnvVars } from "../lib/project";
import { fmt, rewriteSeverityLevel } from "../log";
import { SchemaObject } from "ajv/dist/types";
import { displayProperties } from "./destination";

function getChannelAndMessage(opts: CommonOpts & PackageOpts & { credentials?: string; connectionFile?: string }) {
  if (opts.connectionFile) {
    if (!fs.existsSync(opts.connectionFile)) {
      throw new Error(`Connection file ${opts.connectionFile} does not exist`);
    }
    const json = JSON5.parse(fs.readFileSync(opts.connectionFile, "utf-8"));
    const connectionDefinition = ConnectionDefinition.parse(json);
    const channel = getDestinationChannelFromDefinition(connectionDefinition, () => {});
    const describeMessage: DescribeStreamsMessage = {
      type: "describe-streams",
      payload: {
        credentials: JSON5.parse(connectionDefinition.credentials),
      },
    };

    return { channel, describeMessage };
  } else {
    const channel = getDestinationChannelFromPackage(opts, () => {});
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
  opts: CommonOpts &
    PackageOpts & {
      credentials?: string;
      connectionFile?: string;
    }
) {
  rewriteSeverityLevel("INFO", "DEBUG")
  configureEnvVars(["."], opts.env || []);
  const { channel, describeMessage } = getChannelAndMessage(opts);

  const streams = await channel.streams(describeMessage);

  const output: string[] = [];
  output.push(`${fmt.bold(opts.connectionFile || opts.package)} declares the ${fmt.bold(streams.payload.streams.length)} streams:`);
  output.push("");
  for (let i = 0; i < streams.payload.streams.length; i++){
    const stream = streams.payload.streams[i];
    if (!stream.rowType.$schema) {
      throw new Error(`Stream ${stream.name} does not have a valid JSON schema`);
    }
    const jsonSchema = stream.rowType as SchemaObject;
    const requiredProperties = jsonSchema.required || [];
    const optionalProperties = Object.keys(jsonSchema.properties || {}).filter(p => !requiredProperties.includes(p));
    output.push(`${fmt.gray(i > 9 ? `${i+1}.` : `0${i+1}. `)}${fmt.cyan(stream.name)}, contains ${requiredProperties.length + optionalProperties.length} total properties, optional ${optionalProperties.length}`);
    displayProperties(jsonSchema, output, 5);
    process.stdout.write(output.join("\n") + "\n");
  }
}
