import {
  BaseRateLimitedOutputStream,
  DestinationProvider,
  DestinationStream,
  OutputStreamConfiguration,
  stdProtocol,
} from "@syncmaven/node-cdk";
import { z } from "zod";
import { ExecutionContext } from "@syncmaven/protocol";
import axios, { AxiosError, AxiosInstance } from "axios";
import { omit, pick } from "lodash";

export const IntercomCredentials = z.object({
  accessToken: z.string(),
});

const alreadyExistsPattern = /(An archived)?.*contact matching those details already exists with id=(\w+)/;

export const CompanyRowType = z
  .object({
    name: z.string(),
    company_id: z.union([z.string(), z.number()]),
    plan: z.string().optional(),
    size: z.number().optional(),
    website: z.string().optional(),
    industry: z.string().optional(),
    remote_created_at: z.coerce.date().optional(),
    monthly_spend: z.number().optional(),
  })
  .catchall(z.any());

export type CompanyRowType = z.infer<typeof CompanyRowType>;

export const ContactRowType = z
  .object({
    name: z.string().optional(),
    role: z.string().optional(),
    email: z.string(),
    external_id: z.union([z.string(), z.number()]),
    owner_id: z.string().optional(),
    phone: z.string().optional(),
    avatar: z.string().optional(),
    company_ids: z.union([z.union([z.string(), z.number()]), z.array(z.union([z.string(), z.number()]))]).optional(),
    signed_up_at: z.coerce.date().optional(),
    last_seen_at: z.coerce.date().optional(),
    unsubscribed_from_emails: z.boolean().optional(),
  })
  .catchall(z.any());

export type ContactRowType = z.infer<typeof ContactRowType>;

export type IntercomCredentials = z.infer<typeof IntercomCredentials>;

export const customAttributesPolicies = ["skip-unknown", "create-unknown", "fail-on-unknown"] as const;
export type CustomAttributesPolicy = (typeof customAttributesPolicies)[number];
export type Model = "contact" | "company";

function createClient(creds: IntercomCredentials) {
  return axios.create({
    baseURL: `https://api.intercom.io/`,
    headers: {
      Authorization: `Bearer ${creds.accessToken}`,
      "Intercom-Version": "2.11",
      "Content-Type": "application/json",
    },
  });
}

abstract class BaseIntercomStream<RowT extends Record<string, any>> extends BaseRateLimitedOutputStream<
  RowT,
  IntercomCredentials
> {
  protected client: AxiosInstance;
  protected model: Model;
  protected knownCustomAttributes: Record<string, any> = {};
  protected customAttributesPolicy: CustomAttributesPolicy;

  protected constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext, model: Model) {
    super(config, ctx, 1000 / 60);
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
    return this;
  }

  protected async handleCustomAttributes(customFields: Record<string, any>) {
    let added = false;
    for (const key in customFields) {
      if (!this.knownCustomAttributes[key]) {
        if (this.customAttributesPolicy === "skip-unknown") {
          delete customFields[key];
        } else if (this.customAttributesPolicy === "fail-on-unknown") {
          throw new Error(
            `Unknown custom attribute ${key}. Please create in manually, or set customAttributesPolicy to "create-unknown" to automatically create it`
          );
        } else {
          let type: string = typeof customFields[key];
          if (type === "number") {
            type = "float";
          } else if (type !== "string" && type !== "boolean") {
            type = "string";
            customFields[key] = customFields[key] + "";
          }
          console.log(`Creating custom attribute ${key} of type ${type}`);
          const createAttributeRequest = {
            name: key,
            model: this.model,
            data_type: type,
          };
          await this.client.post(`/data_attributes`, createAttributeRequest);
          added = true;
        }
      }
    }
    if (added) {
      await this.refreshCustomAttributes();
    }
  }

  protected async refreshCustomAttributes() {
    const customAttributesArray = await this.client.get(`/data_attributes?model=${this.model}`);
    this.knownCustomAttributes = customAttributesArray.data.data.reduce((acc: any, attr: any) => {
      acc[attr.name] = attr.id;
      return acc;
    }, {});
  }
}

class ContactsOutputStream extends BaseIntercomStream<ContactRowType> {
  private companiesMap: Record<string, any> = {};
  private contactsMap: Record<string, any> = {};

  constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext) {
    super(config, ctx, "contact");
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
      const res = await this.client.post(`/contacts`, contactObj);
      console.log(`Contact created: ${JSON.stringify(res.data)}`);
      const contactIntercomId = res.data.id as string;
      this.contactsMap[contactObj.external_id] = contactIntercomId;
      await ctx.store.set(["syncId=" + this.config.syncId, "contactsMap", contactObj.external_id], contactIntercomId);
      return contactIntercomId;
    } catch (e: any) {
      if (e instanceof AxiosError && Array.isArray(e.response?.data.errors)) {
        for (const error of e.response.data.errors) {
          if (error.code === "conflict") {
            console.warn(error.message);
            const match = alreadyExistsPattern.exec(error.message);
            if (match) {
              if (match[1]) {
                await this.unarchiveContact(match[2]);
              }
              return this.updateContact(match[2], contactObj, ctx);
            }
          }
        }
      }
      throw toAPIError(e);
    }
  }

  private async updateContact(contactIntercomId: string, contactObj: any, ctx: ExecutionContext) {
    try {
      const res = await this.client.put(`/contacts/${contactIntercomId}`, contactObj);
      console.log(`Contact updated: ${JSON.stringify(res.data)}`);
      if (this.contactsMap[contactObj.external_id] !== contactIntercomId) {
        this.contactsMap[contactObj.external_id] = contactIntercomId;
        await ctx.store.set(["syncId=" + this.config.syncId, "contactsMap", contactObj.external_id], contactIntercomId);
      }
      return contactIntercomId;
    } catch (e) {
      if (e instanceof AxiosError && e.response?.status === 404) {
        return this.addContact(contactObj, ctx);
      }
      throw toAPIError(e);
    }
  }

  private async unarchiveContact(contactIntercomId: string) {
    try {
      const res = await this.client.post(`/contacts/${contactIntercomId}/unarchive`);
      console.log(`Contact unarchived: ${JSON.stringify(res.data)}`);
    } catch (e) {
      throw toAPIError(e);
    }
  }

  // https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Contacts/CreateContact/
  protected async handleRowRateLimited(row: ContactRowType, ctx: ExecutionContext) {
    const { external_id, company_ids, signed_up_at, last_seen_at, ...rest } = row;
    const knownFields = pick(rest, Object.keys(ContactRowType.shape));
    const signedUpAt = signed_up_at;
    const lastSeenAt = last_seen_at;
    const customFields = omit(rest, Object.keys(ContactRowType.shape));
    await this.handleCustomAttributes(customFields);
    const contactObj = {
      ...knownFields,
      external_id: external_id.toString(),
      signed_up_at: signedUpAt ? signedUpAt.getTime() / 1000 : undefined,
      last_seen_at: lastSeenAt ? lastSeenAt.getTime() / 1000 : undefined,
      custom_attributes: Object.keys(customFields).length > 0 ? customFields : undefined,
    };
    let contactIntercomId: string | undefined;
    let companyIntercomIds: any[] = [];
    // try to find companyIntercomIds for this contact
    if (company_ids) {
      const ids = Array.isArray(company_ids) ? company_ids : [company_ids];
      for (const id of ids) {
        let companyIntercomId = this.companiesMap[id.toString()];
        if (!companyIntercomId) {
          try {
            const res = await this.client.get(`/companies?company_id=${id}`);
            if (res.data && res.data.id) {
              companyIntercomIds.push(res.data.id);
              this.companiesMap[id.toString()] = res.data.id;
              await ctx.store.set(["syncId=" + this.config.syncId, "companiesMap", id.toString()], res.data.id);
            } else {
              console.warn(`Company with company_id=${id} not found`);
            }
          } catch (e) {
            throw toAPIError(e);
          }
        } else {
          companyIntercomIds.push(companyIntercomId);
        }
      }
    }
    try {
      contactIntercomId = this.contactsMap[external_id.toString()];
      if (!contactIntercomId) {
        let res = await this.client.post(`/contacts/search`, {
          query: {
            operator: "AND",
            value: [
              {
                field: "external_id",
                operator: "=",
                value: external_id,
              },
            ],
          },
        });
        if (res.data.total_count === 1) {
          contactIntercomId = res.data.data[0].id;
          this.contactsMap[external_id.toString()] = contactIntercomId;
          console.log(`Contact found by external_id: ${external_id}: ${contactIntercomId}`);
          await ctx.store.set(
            ["syncId=" + this.config.syncId, "contactsMap", external_id.toString()],
            contactIntercomId
          );
        } else {
          console.log(`Contact not found by external_id: ${external_id}: ${JSON.stringify(res.data)}`);
        }
      }
      if (!contactIntercomId) {
        contactIntercomId = await this.addContact(contactObj, ctx);
      } else {
        contactIntercomId = await this.updateContact(contactIntercomId, contactObj, ctx);
      }
    } catch (e) {
      throw toAPIError(e);
    }
    for (const companyIntercomId of companyIntercomIds) {
      try {
        const res = await this.client.post(`/contacts/${contactIntercomId}/companies`, {
          id: companyIntercomId,
        });
        console.log(`Contact linked to company: ${JSON.stringify(res.data)}`);
      } catch (e) {
        throw toAPIError(e, { request: { id: companyIntercomId } });
      }
    }
  }
}

function jsonify(param: any) {
  if (typeof param === "string") {
    try {
      return JSON.parse(param);
    } catch (e) {
      return param;
    }
  }
  return param;
}

function toAPIError(e: any, opts: { request?: any } = {}): Error {
  if (e instanceof AxiosError) {
    const requestBody = e.config?.data;
    const baseMessage = `Failed to call ${e.request.method} ${e.request.path} with status ${e.response?.status}`;
    console.debug(
      baseMessage,
      JSON.stringify({
        request: jsonify(opts.request || requestBody),
        error: e.response?.data,
      })
    );
    const err = new Error(baseMessage);
    err["code"] = e.response?.status;
    return err;
  } else {
    return e;
  }
}

class CompaniesOutputStream extends BaseIntercomStream<CompanyRowType> {
  constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext) {
    super(config, ctx, "company");
  }

  // https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Companies/company/
  protected async handleRowRateLimited(row: CompanyRowType, ctx: ExecutionContext) {
    const { company_id, remote_created_at, ...rest } = row;
    const knownFields = pick(rest, Object.keys(CompanyRowType.shape));
    const createdAt = remote_created_at;
    const customFields = omit(rest, Object.keys(CompanyRowType.shape));
    await this.handleCustomAttributes(customFields);
    const companyObj = {
      company_id: company_id.toString(),
      ...knownFields,
      remote_created_at: createdAt ? createdAt.getTime() / 1000 : undefined,
      custom_attributes: Object.keys(customFields).length > 0 ? customFields : undefined,
    };
    try {
      const res = await this.client.post(`/companies`, companyObj);
      console.log(`Company created: ${JSON.stringify(res.data)}`);
    } catch (e) {
      throw toAPIError(e);
    }
  }
}

export const companiesStream: DestinationStream<IntercomCredentials, CompanyRowType> = {
  name: "companies",
  rowType: CompanyRowType,
  createOutputStream: async (cred, ctx) => await new CompaniesOutputStream(cred, ctx).init(ctx),
};

export const contactsStream: DestinationStream<IntercomCredentials, ContactRowType> = {
  name: "contacts",
  rowType: ContactRowType,
  createOutputStream: async (cred, ctx) => await new ContactsOutputStream(cred, ctx).init(ctx),
};

export const intercomProvider: DestinationProvider<IntercomCredentials> = {
  name: "intercom",
  credentialsType: IntercomCredentials,
  streams: [contactsStream, companiesStream],
  defaultStream: "contacts",
};

stdProtocol(intercomProvider);
