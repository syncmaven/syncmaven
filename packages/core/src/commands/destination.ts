import { CommonOpts, PackageOpts } from "./index";
import { SchemaObject } from "ajv/dist/types";
import { DockerChannel } from "../docker/docker-channel";
import { fmt, rewriteSeverityLevel } from "../log";
import { codeHighlight } from "../lib/code-highlighter";
import path from "path";
import fs from "fs";
import { getDestinationChannelFromPackage } from "./sync";

function describeProp(prop: any) {
  const facts: string[] = [];
  if (prop.type) {
    facts.push(Array.isArray(prop.type) ? prop.type.filter(t => t !== "null").join(" or ") : prop.type);
  }
  if (prop.default) {
    facts.push(`Default: ${prop.default}`);
  }
  if (prop.description) {
    facts.push(prop.description);
  }
  return facts.join(". ");
}

export async function describeDestination(opts: CommonOpts & PackageOpts & { json?: boolean }) {
  rewriteSeverityLevel("INFO", "DEBUG");
  const { package: pkg, packageType = "docker" } = opts;
  const channel = getDestinationChannelFromPackage(opts, () => {});
  const description = await channel.describe();
  const output: string[] = [];
  const credentials = description.payload.connectionCredentials;
  if (!credentials.$schema) {
    throw new Error("Unsupported connection spec format. Credentials should be a JSON schema");
  }
  const credentialsSchema = credentials as SchemaObject;
  if (opts.json) {
    output.push(
      `${packageType === "docker" ? "🐳" : ""}${fmt.bold(fmt.cyan(pkg))} has following JSON schema for credentials`
    );
    output.push(``);
    output.push(codeHighlight(JSON.stringify(credentialsSchema, null, 2), "json"));
    process.stdout.write(output.join("\n") + "\n");
    return;
  }
  output.push(`${packageType === "docker" ? "🐳" : ""}${fmt.bold(fmt.cyan(pkg))} has following credential properties`);
  output.push(``);
  const requiredMark = " (required)";
  const maxPropWidth = Math.max(...Object.keys(credentialsSchema.properties).map(k => k.length)) + requiredMark.length;
  const props = Object.entries(credentialsSchema.properties).sort(([a], [b]) => {
    const aRequired = credentialsSchema.required?.includes(a);
    const bRequired = credentialsSchema.required?.includes(b);
    if (aRequired && !bRequired) {
      return -1;
    } else if (!aRequired && bRequired) {
      return 1;
    } else {
      return a.localeCompare(b);
    }
  });
  for (const [key, prop] of props) {
    const required = credentialsSchema.required?.includes(key);
    output.push(
      `   ${fmt.bold(key)}${required ? fmt.gray(requiredMark) : ""} ${" ".repeat(Math.max(maxPropWidth - key.length - (required ? requiredMark.length : 0), 0))} - ${describeProp(prop)}`
    );
  }
  output.push(``);
  output.push(`📌 To see a full JSON schema run the command with --json flag`);

  process.stdout.write(output.join("\n") + "\n");
}
