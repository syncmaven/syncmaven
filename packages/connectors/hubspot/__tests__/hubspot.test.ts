import { describe, test } from "node:test";
import { HubspotCredentials, hubspotProvider } from "../src";
import { disableStdProtocol, tableToJsonArray, testProvider } from "@syncmaven/node-cdk";
import { Client } from "@hubspot/api-client";

import assert from "assert";
import { omit } from "lodash";

disableStdProtocol();

async function forEachContact(accessToken: string, cb: (contact) => any | Promise<any>) {
  const client = new Client({ accessToken });
  let nextPage: string | undefined = undefined;
  do {
    const contactsResponse = await client.crm.contacts.basicApi.getPage(100, nextPage);
    for (const contact of contactsResponse.results) {
      await cb(contact);
    }
    nextPage = contactsResponse.paging?.next?.after;
    if (nextPage) {
      console.debug(`Will load next page of contacts`);
    }
  } while (nextPage);
}

async function forEachCompany(accessToken: string, cb: (company) => any | Promise<any>) {
  const client = new Client({ accessToken });
  let nextPage: string | undefined = undefined;
  do {
    const response = await client.crm.companies.basicApi.getPage(100, nextPage);
    for (const company of response.results) {
      await cb(company);
    }
    nextPage = response.paging?.next?.after;
    if (nextPage) {
      console.debug(`Will load next page of companies`);
    }
  } while (nextPage);
}

// Function to remove all contacts and companies from an account using accessToken
async function cleanHubspotAccount(accessToken: string) {
  const client = new Client({ accessToken });
  console.log("Removing all contacts from test account...");
  await forEachContact(accessToken, async contact => await client.crm.contacts.basicApi.archive(contact.id));
  console.log("Removing all companies from test account...");
  await forEachCompany(accessToken, async company => await client.crm.companies.basicApi.archive(company.id));
}

const contacts = [
  ["id", "email", "name", "phone", "contact_custom_field1", "company_ids"],
  ["1", "john.doe@horns-and-hoofs.com", "John Doe", "+1234567890", "custom field value", 1],
  ["2", "john.do2e@another.com", "John", "+71234567890", null, [1, 2]],
  ["3", "", "John Doe3", "+81234567890", undefined, 1],
];

const companies = [
  ["id", "name", "custom_field1", "plan"],
  [1, "Horns and Hoofs", "custom field value", "free"],
  [2, "Another company", undefined, "free"],
];

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
        contacts: tableToJsonArray(contacts),
        companies: tableToJsonArray(companies),
      },
      textContext: t,
      envVarName: "HUBSPOT_TEST_CREDENTIALS",
      validate: async (c: HubspotCredentials) => {
        const contacts: any[] = [];
        const companies: any[] = [];
        console.log(`Got ${contacts.length} contacts and ${companies.length} companies`);
        await forEachContact(c.accessToken, c => contacts.push(c));
        await forEachCompany(c.accessToken, c => companies.push(c));
        console.info("Contacts:");
        console.table(contacts.map(c => ({ ...omit(c, "properties"), ...omit(c.properties, "lastmodifieddate") })));
        console.info("Companies:");
        console.table(companies.map(c => ({ ...omit(c, "properties"), ...omit(c.properties, "lastmodifieddate") })));
        assert.equal(companies.length, 2);
        assert.equal(contacts.length, 3);
      },
      before: async (c: HubspotCredentials) => {
        await cleanHubspotAccount(c.accessToken);
      },
      after: async (c: HubspotCredentials) => {
        await cleanHubspotAccount(c.accessToken);
      },
    });
  });
});
