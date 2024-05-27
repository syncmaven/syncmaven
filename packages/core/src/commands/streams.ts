import { CommonOpts, PackageOpts } from "./index";
import fs from "fs";
import JSON5 from "json5";
import { ConnectionDefinition } from "../types/objects";
import { getDestinationChannelFromDefinition, getDestinationChannelFromPackage } from "./sync";
import assert from "assert";
import { DescribeStreamsMessage } from "@syncmaven/protocol";
import { configureEnvVars } from "../lib/project";

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
  configureEnvVars(["."], opts.env || []);
  const { channel, describeMessage } = getChannelAndMessage(opts);

  const streams = await channel.streams(describeMessage);

  console.log("Streams described", streams);
}
