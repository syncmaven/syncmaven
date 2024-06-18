import { CommonOpts, PackageOpts } from "./index";
import { SchemaObject } from "ajv/dist/types";
import { fmt, out, rewriteSeverityLevel } from "../log";
import { codeHighlight } from "../lib/code-highlighter";
import { getDestinationChannel } from "./sync";

function describeProp(prop: any) {
  const facts: string[] = [];
  if (prop.type) {
    facts.push(fmt.bold(Array.isArray(prop.type) ? prop.type.filter(t => t !== "null").join(" or ") : prop.type));
  } else {
    facts.push(fmt.bold("string"));
  }
  if (prop.default) {
    facts.push(`(default: ${prop.default})`);
  }
  if (prop.description) {
    facts.push(prop.description);
  }
  return facts.join(". ");
}

export function displayProperties(credentialsSchema: SchemaObject, indent = 3) {
  const requiredMark = " (required)";
  const optionalMark = " (optional)";
  const maxPropWidth = Math.max(...Object.keys(credentialsSchema.properties).map(k => k.length));
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
    out(
      `${" ".repeat(indent)}${fmt.magenta("➔")} ${fmt.bold(key) + " ".repeat(maxPropWidth - key.length)}${required ? fmt.red(requiredMark) : fmt.gray(optionalMark)} - ${describeProp(prop)}`
    );
  }
}

export async function describeDestination(opts: CommonOpts & PackageOpts & { json?: boolean }) {
  rewriteSeverityLevel("INFO", "DEBUG");
  const { package: pkg, packageType = "docker" } = opts;
  const channel = getDestinationChannel(
    {
      type: packageType,
      image: packageType === "docker" ? opts.package : undefined,
      dir: packageType === "npm" ? opts.package : undefined,
    },
    () => {}
  );
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
  displayProperties(credentialsSchema);
  output.push(``);
  output.push(`📌 To see a full JSON schema run the command with --json flag`);

  process.stdout.write(output.join("\n") + "\n");
}
