import { describe, test, TestContext } from "node:test";
import * as JSON5 from "json5";
import { hubspotProvider } from "../src";
import { DestinationProvider, InMemoryStore, disableStdProtocol } from "@syncmaven/node-cdk";
import { ZodError, ZodIssue } from "zod";

disableStdProtocol();

function strinfigyZodIssue(e: ZodIssue) {
  if (e.code === "invalid_type") {
    return `${e.code} - expected ${e.expected} but got ${e.received}`;
  } else if (e.code === "unrecognized_keys") {
    return `${e.code} - found keys ${e.keys.join(", ")}`;
  }

  return `${JSON.stringify(e)}`;
}

export function stringifyZodError(error: any): string {
  if (!(error instanceof ZodError)) {
    return error?.message || "Unknown error";
  }
  return `${error.errors.length} errors found: ${error.errors.map((e, idx) => `#${idx + 1} - at path \$.${e.path.join(".")} - ${strinfigyZodIssue(e)}`).join(", ")}`;
}

async function testProvider<Creds extends any>(opts: {
  provider: DestinationProvider;
  testData: Record<string, any[]>;
  envVarName: string;
  streamOptions?: Record<string, any>;
  textContext: TestContext;
  before?: (c: Creds) => void;
  after?: (c: Creds) => void;
  validate?: (c: Creds) => void;
}) {
  const envCreds = process.env[opts.envVarName];
  if (!envCreds) {
    opts.textContext.skip(`${opts.envVarName} is not set.`);
    return;
  }
  const credentials = JSON5.parse(envCreds);
  for (const stream of opts.provider.streams) {
    const testDataStream = opts.testData[stream.name];
    if (testDataStream) {
      console.log(`Testing stream ${stream.name}`);
      const ctx = {
        store: new InMemoryStore(),
      };
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
}

describe("Hubspot Test", () => {
  test("Hubspot Provider", async t => {
    await testProvider({
      provider: hubspotProvider,
      streamOptions: {
        contacts: {
          customAttributesPolicy: "skip-unknown",
        },
        companies: {
          customAttributesPolicy: "skip-unknown",
        },
      },
      testData: {
        contacts: [
          {
            id: "1",
            email: "john.doe@horns-and-hoofs.com",
            name: "John Doe",
            phone: "+1234567890",
            contact_custom_field1: "custom field value",
            company_ids: 1,
          },
          {
            id: "2",
            email: "john.do2e@another.com",
            name: "John",
            lastname: "Doe2",
            phone: "+71234567890",
            company_ids: [1, 2],
          },
          {
            id: "3",
            email: "john.do3e@another.com",
            name: "John Doe3",
            phone: "+81234567890",
            company_ids: 1,
          },
        ],
        companies: [
          {
            id: 1,
            name: "Horns and Hoofs",
            custom_field1: "custom field value",
            plan: "free",
          },
          {
            id: 2,
            name: "Another company",
            plan: "free",
          },
        ],
      },
      textContext: t,
      envVarName: "HUBSPOT_TEST_CREDENTIALS",
      after: () => {
        disableStdProtocol();
      },
      before: () => {},
    });
  });
});
