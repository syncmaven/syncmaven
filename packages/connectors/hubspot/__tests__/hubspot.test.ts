import { describe, test } from "node:test";
import { HubspotCredentials, hubspotProvider } from "../src";
import { disableStdProtocol, testProvider } from "@syncmaven/node-cdk";
import { Client } from "@hubspot/api-client";
//import assert from "assert";

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

// Example usage with your access token
const accessToken = "your_access_token_here";

// Uncomment and use the function as needed
// removeAllContactsAndCompanies(accessToken).then(() => {
//     console.log('All contacts and companies removed.');
// }).catch((error) => {
//     console.error('Error removing contacts and companies:', error);
// });

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
      validate: async (c: HubspotCredentials) => {
        const contacts: any[] = [];
        const companies: any[] = [];
        console.log(`Got ${contacts.length} contacts and ${companies.length} companies`);
        await forEachContact(c.accessToken, c => contacts.push(c));
        await forEachCompany(c.accessToken, c => companies.push(c));
        // assert.equal(companies.length, 30)
        // assert.equal(contacts.length, 30)
        throw new Error("Not implemented");
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
