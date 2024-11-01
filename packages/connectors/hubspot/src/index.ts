import {
  BaseOutputStream,
  DestinationProvider,
  DestinationStream,
  OutputStreamConfiguration,
  stdProtocol,
} from "@syncmaven/node-cdk";
import { z } from "zod";
import { ExecutionContext } from "@syncmaven/protocol";
import { AxiosError } from "axios";
import { omit, pick } from "lodash";
import { AssociationTypes, Client } from "@hubspot/api-client";
import { PropertyCreateFieldTypeEnum, PropertyCreateTypeEnum } from "@hubspot/api-client/lib/codegen/crm/properties";

import { FilterOperatorEnum } from "@hubspot/api-client/lib/codegen/crm/companies";
import { AssociationSpecAssociationCategoryEnum } from "@hubspot/api-client/lib/codegen/crm/associations/v4/models/AssociationSpec";

export const HubspotCredentials = z.object({
  accessToken: z.string().describe("Hubspot API access token"),
});

export const CompanyRowType = z
  .object({
    id: z.union([z.string(), z.number()]).describe("Unique id of the company (in your system)"),
    name: z.string().describe("Name of the company"),
  })
  .catchall(z.any())
  .describe(
    "Company record. Must include id and name fields. Can also include other fields too, that will be treated as custom Hubspot attributes"
  );

export type CompanyRowType = z.infer<typeof CompanyRowType>;

export const ContactRowType = z
  .object({
    id: z.union([z.string(), z.number()]).describe("Unique id of the contact (in your system)"),
    name: z.string().optional().describe("Contact name"),
    email: z.string(),
    company_ids: z
      .union([z.union([z.string(), z.number()]), z.array(z.union([z.string(), z.number()]))])
      .optional()
      .describe("Company id(s) this contact is associated with. First id will be a primary company"),
  })
  .catchall(z.any())
  .describe(
    "Contact record. Must include id and name fields. Can also include other fields too, that will be treated as custom Hubspot attributes"
  );
export type ContactRowType = z.infer<typeof ContactRowType>;

export type HubspotCredentials = z.infer<typeof HubspotCredentials>;

export const customAttributesPolicies = ["skip-unknown", "create-unknown", "fail-on-unknown"] as const;
export type CustomAttributesPolicy = (typeof customAttributesPolicies)[number];
export type Model = "contacts" | "company";
const propertiesGroup: Record<Model, string> = {
  contacts: "contactinformation",
  company: "companyinformation",
};

function createClient(creds: HubspotCredentials) {
  const client = new Client({ accessToken: creds.accessToken });
  client.init();
  return client;
}

function splitName(fullName: string): [string | undefined, string | undefined] {
  if (!fullName) {
    return [undefined, undefined];
  }

  const nameParts = fullName.trim().split(/\s+/);
  if (nameParts.length === 1) {
    return [nameParts[0], undefined];
  } else if (nameParts.length === 2) {
    return [nameParts[0], nameParts[1]];
  } else {
    const lastName = nameParts.pop();
    const firstName = nameParts.join(" ");
    return [firstName, lastName || undefined];
  }
}

abstract class BaseHubspotStream<RowT extends Record<string, any>> extends BaseOutputStream<RowT, HubspotCredentials> {
  protected client: Client;
  protected model: Model;
  protected knownCustomAttributes: Record<string, any> = {};
  protected customAttributesPolicy: CustomAttributesPolicy;

  protected constructor(config: OutputStreamConfiguration<HubspotCredentials>, ctx: ExecutionContext, model: Model) {
    super(config, ctx);
    this.model = model;
    this.customAttributesPolicy = this.config.options.customAttributesPolicy || "create-unknown";
    if (!customAttributesPolicies.includes(this.customAttributesPolicy)) {
      throw new Error(
        `Invalid customAttributesPolicy ${this.customAttributesPolicy}. Valid values are ${customAttributesPolicies.join(", ")}`
      );
    }
    this.client = createClient(config.credentials);
  }

  public async init(ctx: ExecutionContext) {
    await this.refreshCustomAttributes();
    await this.ensureCustomAttribute("external_id");

    return this;
  }

  protected async searchByField(model: Model, fieldName: string, fieldValue: string): Promise<string | undefined> {
    const filterGroup = {
      filters: [
        {
          propertyName: fieldName,
          operator: FilterOperatorEnum.Eq,
          value: fieldValue,
        },
      ],
    };
    const searchResults = await this.client.crm[model === "company" ? "companies" : model].searchApi.doSearch({
      filterGroups: [filterGroup],
      limit: 10,
      after: "0",
      properties: [],
      sorts: [],
    });

    return searchResults?.results?.[0]?.id;
  }

  protected async handleCustomAttributes(customFields: Record<string, any>) {
    for (const key in customFields) {
      if (!this.knownCustomAttributes[key]) {
        if (this.customAttributesPolicy === "skip-unknown") {
          delete customFields[key];
        } else if (this.customAttributesPolicy === "fail-on-unknown") {
          throw new Error(
            `Unknown custom attribute ${key}. Please create in manually, or set customAttributesPolicy to "create-unknown" to automatically create it`
          );
        } else {
          await this.ensureCustomAttribute(key);
        }
      }
    }
  }

  protected async ensureCustomAttribute(key: string) {
    if (!this.knownCustomAttributes[key]) {
      console.log(`Creating custom attribute ${key}`);
      await this.client.crm.properties.coreApi.create(this.model, {
        name: key,
        label: key,
        type: PropertyCreateTypeEnum.String,
        fieldType: PropertyCreateFieldTypeEnum.Text,
        groupName: propertiesGroup[this.model],
        description: `Custom property, created by Syncmaven`,
      });
      this.knownCustomAttributes[key] = key;
    }
  }

  protected async refreshCustomAttributes() {
    const properties = await this.client.crm.properties.coreApi.getAll(this.model);
    this.knownCustomAttributes = properties.results.reduce((acc: any, attr: any) => {
      acc[attr.name] = attr.name;
      return acc;
    }, {});
  }
}

class ContactsOutputStream extends BaseHubspotStream<ContactRowType> {
  private companiesMap: Record<string, any> = {};
  private contactsMap: Record<string, any> = {};

  constructor(config: OutputStreamConfiguration<HubspotCredentials>, ctx: ExecutionContext) {
    super(config, ctx, "contacts");
  }

  async init(ctx: ExecutionContext) {
    await super.init(ctx);
    let entries = await ctx.store.list(["syncId=" + this.config.syncId, "companiesMap"]);
    for (const entry of entries) {
      const k = entry.key as string[];
      this.companiesMap[k[k.length - 1]] = entry.value;
    }
    entries = await ctx.store.list(["syncId=" + this.config.syncId, "contactsMap"]);
    for (const entry of entries) {
      const k = entry.key as string[];
      this.contactsMap[k[k.length - 1]] = entry.value;
    }
    return this;
  }

  private async addContact(contactObj: any, ctx: ExecutionContext) {
    try {
      const res = await this.client.crm.contacts.basicApi.create({
        ...contactObj,
        associations: [],
      });
      console.log(`Contact created: ${JSON.stringify(res)}`);
      const contactHubspotId = res.id;
      this.contactsMap[contactObj.properties.external_id] = contactHubspotId;
      await ctx.store.set(
        ["syncId=" + this.config.syncId, "contactsMap", contactObj.properties.external_id],
        contactHubspotId
      );
      return contactHubspotId;
    } catch (e: any) {
      throw toAPIError(e);
    }
  }

  private async updateContact(contactHubspotId: string, contactObj: any, ctx: ExecutionContext) {
    try {
      const res = await this.client.crm.contacts.basicApi.update(contactHubspotId, contactObj);
      console.log(`Contact updated: ${JSON.stringify(res)}`);
      if (this.contactsMap[contactObj.properties.external_id] !== contactHubspotId) {
        this.contactsMap[contactObj.properties.external_id] = contactHubspotId;
        await ctx.store.set(
          ["syncId=" + this.config.syncId, "contactsMap", contactObj.properties.external_id],
          contactHubspotId
        );
      }
      return contactHubspotId;
    } catch (e) {
      if (e instanceof AxiosError && e.response?.status === 404) {
        return this.addContact(contactObj, ctx);
      }
      throw toAPIError(e);
    }
  }

  protected async associateContactWithCompany(contactId: string, companyId: string): Promise<void> {
    await this.client.crm.associations.v4.basicApi.create("companies", companyId, "contacts", contactId, [
      {
        associationCategory: AssociationSpecAssociationCategoryEnum.HubspotDefined,
        associationTypeId: AssociationTypes.companyToContact,
      },
    ]);
  }

  async handleRow(row: ContactRowType, ctx: ExecutionContext) {
    const { id, company_ids, name, ...rest } = row;
    const knownFields = pick(rest, Object.keys(ContactRowType.shape));
    const customFields = omit(rest, Object.keys(ContactRowType.shape));
    const [firstname, lastname] = name ? splitName(name) : [undefined, undefined];
    await this.handleCustomAttributes(customFields);
    console.log(`Processing contact: ${JSON.stringify(row)}`);
    const contactObj = {
      properties: {
        external_id: id.toString(),
        firstname,
        lastname,
        ...knownFields,
        ...customFields,
      } as { [key: string]: string },
    };
    let contactHubspotId: string | undefined = undefined;
    let companyHubspotIds: any[] = [];
    // try to find companyHubspotIds for this contact
    if (company_ids) {
      const ids = Array.isArray(company_ids) ? company_ids : [company_ids];
      for (const id of ids) {
        let companyHubspotId = this.companiesMap[id.toString()];
        if (!companyHubspotId) {
          try {
            const hid = await this.searchByField("company", "external_id", id.toString());
            if (hid) {
              companyHubspotIds.push(hid);
              this.companiesMap[id.toString()] = hid;
              await ctx.store.set(["syncId=" + this.config.syncId, "companiesMap", id.toString()], hid);
            } else {
              console.warn(`Not found company with external_id=${id}`);
            }
          } catch (e: any) {
            console.error(`Failed to search company by external_id=${id}: ${e.message}`);
            throw toAPIError(e);
          }
        } else {
          companyHubspotIds.push(companyHubspotId);
        }
      }
    }
    try {
      contactHubspotId = this.contactsMap[id.toString()];
      if (!contactHubspotId) {
        contactHubspotId = await this.searchByField("contacts", "external_id", id.toString());
      }
      if (!contactHubspotId) {
        contactHubspotId = await this.addContact(contactObj, ctx);
      } else {
        contactHubspotId = await this.updateContact(contactHubspotId, contactObj, ctx);
      }
    } catch (e: any) {
      console.error(`Failed to create/update contact: ${e.message}`);
      throw toAPIError(e);
    }
    for (const companyHubspotId of companyHubspotIds) {
      try {
        await this.associateContactWithCompany(contactHubspotId, companyHubspotId);
        console.log(`Contact linked to company: ${contactHubspotId} -> ${companyHubspotId}`);
      } catch (e) {
        throw toAPIError(e, { request: { id: companyHubspotId } });
      }
    }
  }
}

function toAPIError(e: any, opts: { request?: any } = {}): Error {
  return e;
}

class CompaniesOutputStream extends BaseHubspotStream<CompanyRowType> {
  private companiesMap: Record<string, any> = {};

  constructor(config: OutputStreamConfiguration<HubspotCredentials>, ctx: ExecutionContext) {
    super(config, ctx, "company");
  }

  async init(ctx: ExecutionContext) {
    await super.init(ctx);
    let entries = await ctx.store.list(["syncId=" + this.config.syncId, "companiesMap"]);
    for (const entry of entries) {
      const k = entry.key as string[];
      this.companiesMap[k[k.length - 1]] = entry.value;
    }
    return this;
  }

  async handleRow(row: CompanyRowType, ctx: ExecutionContext) {
    const { id, ...rest } = row;
    const knownFields = pick(rest, Object.keys(CompanyRowType.shape));
    const customFields = omit(rest, Object.keys(CompanyRowType.shape));
    await this.handleCustomAttributes(customFields);
    const companyProperties = {
      properties: {
        external_id: id.toString(),
        ...knownFields,
        ...customFields,
      },
    };
    let companyHubspotId: string | undefined = undefined;
    try {
      companyHubspotId = this.companiesMap[id.toString()];
      if (!companyHubspotId) {
        companyHubspotId = await this.searchByField("company", "external_id", id.toString());
      }
      if (!companyHubspotId) {
        // Contact does not exist, create it
        const res = await this.client.crm.companies.basicApi.create({
          ...companyProperties,
          associations: [],
        });
        console.log(`Company created: ${JSON.stringify(res)}`);
        companyHubspotId = res.id;
        this.companiesMap[companyProperties.properties.external_id] = companyHubspotId;
        await ctx.store.set(
          ["syncId=" + this.config.syncId, "companiesMap", companyProperties.properties.external_id],
          companyHubspotId
        );
      } else {
        const res = await this.client.crm.companies.basicApi.update(companyHubspotId, companyProperties);
        console.log(`Company updated: ${JSON.stringify(res)}`);
      }
    } catch (e) {
      throw toAPIError(e);
    }
  }
}

export const companiesStream: DestinationStream<HubspotCredentials, CompanyRowType> = {
  name: "companies",
  rowType: CompanyRowType,
  createOutputStream: async (cred, ctx) => await new CompaniesOutputStream(cred, ctx).init(ctx),
};

export const contactsStream: DestinationStream<HubspotCredentials, ContactRowType> = {
  name: "contacts",
  rowType: ContactRowType,
  createOutputStream: async (cred, ctx) => await new ContactsOutputStream(cred, ctx).init(ctx),
};

export const hubspotProvider: DestinationProvider<HubspotCredentials> = {
  name: "hubspot",
  credentialsType: HubspotCredentials,
  streams: [contactsStream, companiesStream],
  defaultStream: "contacts",
};

stdProtocol(hubspotProvider);
