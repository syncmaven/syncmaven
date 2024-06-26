import { describe, test, TestContext } from "node:test";
import * as JSON5 from "json5";
import { intercomProvider } from "../src";
import { DestinationProvider, InMemoryStore, disableStdProtocol, testProvider } from "@syncmaven/node-cdk";
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

describe("Intercom Test", () => {
  test("Intercom Provider", async t => {
    await testProvider({
      provider: intercomProvider,
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
            external_id: "1",
            email: "john.doe@horns-and-hoofs.com",
            name: "John Doe",
            phone: "+1234567890",
            contact_custom_field1: "custom field value",
            company_ids: 1,
          },
          {
            external_id: "2",
            email: "john.do2e@another.com",
            name: "John Doe2",
            phone: "+71234567890",
            company_ids: 2,
          },
          {
            external_id: "3",
            email: "john.do3e@another.com",
            name: "John Doe.3",
            phone: "+81234567890",
            company_ids: 1,
          },
        ],
        companies: [
          {
            company_id: 1,
            remote_created_at: new Date(2024, 1, 1, 12, 0, 0, 0),
            size: 10,
            name: "Horns and Hoofs",
            custom_field1: "custom field value",
            plan: "free",
          },
          {
            company_id: 2,
            remote_created_at: new Date(2024, 1, 1, 12, 0, 0, 0),
            size: 10,
            name: "Another company",
            plan: "free",
          },
        ],
      },
      textContext: t,
      envVarName: "INTERCOM_TEST_CREDENTIALS",
      after: () => {
        disableStdProtocol();
      },
      before: () => {},
    });
  });
});
