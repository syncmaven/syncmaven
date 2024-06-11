import { describe, test, TestContext } from "node:test";
import JSON5 from "json5";
import { intercomProvider } from "../src";
import { DestinationProvider, InMemoryStore } from "@syncmaven/node-cdk";
import { ZodError, ZodIssue } from "zod";

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
  textContext: TestContext;
  before?: (c: Creds) => void;
  after?: (c: Creds) => void;
}) {
  const envCreds = process.env[opts.envVarName];
  if (!envCreds) {
    opts.textContext.skip(`${opts.envVarName} is set.`);
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
          options: {},
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

describe("Intercom Test", () => {
  test("Intercom Provider", async t => {
    await testProvider({
      provider: intercomProvider,
      testData: {
        contacts: [
          {
            contact_id: 1,
            email: "john.doe@horns-and-hoofs.com",
            name: "John Doe",
            phone: "+1234567890",
            custom_field1: "custom field value",
            plan_type: "free",
          },
        ],
        companies: [
          {
            company_id: 1,
            name: "Horns and Hoofs",
            custom_field1: "custom field value",
            plan_type: "free",
          },
        ],
      },
      textContext: t,
      envVarName: "INTERCOM_TEST_CREDENTIALS",
      before: () => {},
    });
  });
});
