import { DestinationProvider, InMemoryStore } from "./index";
import { TestContext } from "node:test";
import { ZodError, ZodIssue } from "zod";
import assert from "assert";

function strinfigyZodIssue(e: ZodIssue) {
  if (e.code === "invalid_type") {
    return `${e.code} - expected ${e.expected} but got ${e.received}`;
  } else if (e.code === "unrecognized_keys") {
    return `${e.code} - found keys ${e.keys.join(", ")}`;
  }

  return `${JSON.stringify(e)}`;
}

function stringifyZodError(error: any): string {
  if (!(error instanceof ZodError)) {
    return error?.message || "Unknown error";
  }
  return `${error.errors.length} errors found: ${error.errors.map((e, idx) => `#${idx + 1} - at path \$.${e.path.join(".")} - ${strinfigyZodIssue(e)}`).join(", ")}`;
}

export async function testProvider<Creds extends any>(opts: {
  provider: DestinationProvider;
  testData: Record<string, any[]>;
  envVarName: string;
  streamOptions?: Record<string, any>;
  textContext: TestContext;
  before?: (c: Creds) => Promise<void>;
  after?: (c: Creds) => Promise<void>;
  validate?: (c: Creds) => Promise<void>;
}) {
  const envCreds = process.env[opts.envVarName];
  if (!envCreds) {
    opts.textContext.skip(`${opts.envVarName} is not set.`);
    return;
  }
  const credentials = opts.provider.credentialsType.parse(JSON.parse(envCreds));
  if (opts.before) {
    console.log("Preparing external resources...");
    await opts.before(credentials);
  }

  for (const stream of opts.provider.streams) {
    const testDataStream = opts.testData[stream.name];
    if (testDataStream) {
      const ctx = {
        store: new InMemoryStore(),
      };
      console.log(`Creating output stream for ${stream.name}`);
      const outputStream = await stream.createOutputStream(
        {
          streamId: `test-${stream.name}`,
          options: opts.streamOptions?.[stream.name] || {},
          credentials,
          syncId: `test-${stream.name}-sync`,
        },
        ctx
      );
      for (const row of testDataStream) {
        console.log(`Handling row ${JSON.stringify(row)}`);
        let distilledRow: any;
        try {
          distilledRow = stream.rowType.parse(row);
        } catch (e) {
          throw new Error(`Failed to parse row ${JSON.stringify(row)}: ${stringifyZodError(e)}`);
        }

        await outputStream.handleRow(distilledRow, ctx);
      }
      if (outputStream.finish) {
        await outputStream.finish(ctx);
      }
    } else {
      console.log(`Skipping stream ${stream.name}. No test data for the stream is provided.`);
    }
  }
  try {
    if (opts.validate) {
      console.log("Validating results");
      await opts.validate(credentials);
    }
  } finally {
    if (opts.after) {
      console.log("Cleaning up external resources...");
      try {
        await opts.after(credentials);
      } catch (e) {
        console.debug("Error cleaning up external resources", e);
      }
    }
  }
}

export function tableToJsonArray(table: any[][]): Record<string, any>[] {
  assert(table.length > 0, "Table must have at least one row - header");
  const header = table[0];
  const rows = table.slice(1);
  return rows.map((row, idx) => {
    assert(
      row.length === header.length,
      `All rows must have the same number of columns (${header.length}). Row #${idx + 1} has ${row.length} columns`
    );
    return row.reduce((acc, cell, idx) => {
      acc[header[idx] + ""] = cell;
      return acc;
    }, {});
  });
}
