import {
  BaseOutputStream,
  DestinationProvider,
  DestinationStream,
  OutputStreamConfiguration,
} from "@syncmaven/node-cdk";
import { z } from "zod";
import { ExecutionContext } from "@syncmaven/protocol";
import axios, { AxiosError, AxiosInstance } from "axios";
import { omit, pick } from "lodash";

export const IntercomCredentials = z.object({
  accessToken: z.string(),
  appId: z.string(),
});

export const CompanyRowType = z
  .object({
    name: z.string(),
    company_id: z.coerce.string(),
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
    external_id: z.coerce.string(),
    owner_id: z.string().optional(),
    phone: z.string().optional(),
    avatar: z.string().optional(),
    company_id: z.coerce.string(),
    signed_up_at: z.coerce.date().optional(),
    last_seen_at: z.coerce.date().optional(),
    unsubscribed_from_emails: z.boolean().optional(),
  })
  .catchall(z.any());

export type ContactRowType = z.infer<typeof ContactRowType>;

export type IntercomCredentials = z.infer<typeof IntercomCredentials>;

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

abstract class BaseIntercomStream<RowT extends Record<string, any>> extends BaseOutputStream<
  RowT,
  IntercomCredentials
> {
  protected client: AxiosInstance;
  protected model: Model;
  protected knownCustomAttributes: Record<string, any> = {};
  protected customAttributesPolicy: CustomAttributesPolicy;

  protected constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext, model: Model) {
    super(config, ctx);
    this.model = model;
    this.customAttributesPolicy = this.config.options.customAttributesPolicy || "create-unknown";
    if (!customAttributesPolicies.includes(this.customAttributesPolicy)) {
      throw new Error(
        `Invalid customAttributesPolicy ${this.customAttributesPolicy}. Valid values are ${customAttributesPolicies.join(", ")}`
      );
    }
    this.client = createClient(config.credentials);
    this.client.interceptors.response.use(
      response => response,
      e => Promise.reject(rethrowAxiosError(e))
    );
  }

  public async init(): Promise<this> {
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
  private companiesCache: Record<string, any> = {};

  constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext) {
    super(config, ctx, "contact");
  }

  // https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Contacts/CreateContact/
  async handleRow(row: ContactRowType, ctx: ExecutionContext) {
    const { company_id, signed_up_at, last_seen_at, ...rest } = row;
    const knownFields = pick(rest, Object.keys(ContactRowType.shape));
    const signedUpAt = signed_up_at;
    const lastSeenAt = last_seen_at;
    const customFields = omit(rest, Object.keys(ContactRowType.shape));
    await this.handleCustomAttributes(customFields);
    const contactObj = {
      ...knownFields,
      signed_up_at: signedUpAt ? signedUpAt.getTime() / 1000 : undefined,
      last_seen_at: lastSeenAt ? lastSeenAt.getTime() / 1000 : undefined,
      custom_attributes: Object.keys(customFields).length > 0 ? customFields : undefined,
    };
    let contactId: string | undefined;
    let company: any;
    // try to find company for this contact
    if (company_id) {
      company = this.companiesCache[company_id];
      if (!company) {
        try {
          const res = await this.client.get(`/companies?company_id=${company_id}`);
          if (res.data && res.data.id) {
            company = res.data;
            this.companiesCache[company_id] = company;
          } else {
            console.warn(`Company with company_id=${company_id} not found`);
          }
        } catch (e) {
          throw rethrowAxiosError(e);
        }
      }
    }
    try {
      //search existing contact by external_id
      let res = await this.client.post(`/contacts/search`, {
        query: {
          operator: "AND",
          value: [
            {
              field: "external_id",
              operator: "=",
              value: knownFields.external_id,
            },
          ],
        },
      });
      if (res.data.total_count === 1) {
        contactId = res.data.data[0].id;
        console.log(`Contact found by external_id: ${contactId}`);
      } else {
        console.log(`Contact not found by external_id: ${knownFields.external_id}: ${JSON.stringify(res.data)}`);
      }
      if (!contactId) {
        res = await this.client.post(`/contacts`, contactObj);
        console.log(`Contact created: ${JSON.stringify(res.data)}`);
        contactId = res.data.id;
      } else {
        res = await this.client.put(`/contacts/${contactId}`, contactObj);
        console.log(`Contact updated: ${JSON.stringify(res.data)}`);
      }
    } catch (e) {
      throw rethrowAxiosError(e);
    }
    if (company) {
      try {
        const res = await this.client.post(`/contacts/${contactId}/companies`, {
          id: company.id,
        });
        console.log(`Contact linked to company: ${JSON.stringify(res.data)}`);
      } catch (e) {
        throw rethrowAxiosError(e, { request: { id: company.id } });
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

function rethrowAxiosError(e: any, opts: { request?: any } = {}): Error {
  if (e instanceof AxiosError) {
    const requestBody = e.config?.data;
    const baseMessage = `Failed to call ${e.request.method} ${e.request.path} with status ${e.response?.status}`;
    console.debug(
      baseMessage,
      JSON.stringify(
        {
          request: jsonify(opts.request || requestBody),
          error: e.response?.data,
        },
        null,
        2
      )
    );
    return new Error(baseMessage);
  } else {
    return e;
  }
}

export const customAttributesPolicies = ["skip-unknown", "create-unknown", "fail-on-unknown"] as const;
export type CustomAttributesPolicy = (typeof customAttributesPolicies)[number];
export type Model = "contact" | "company";

class CompaniesOutputStream extends BaseIntercomStream<CompanyRowType> {
  constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext) {
    super(config, ctx, "company");
  }

  // https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Companies/company/
  async handleRow(row: CompanyRowType, ctx: ExecutionContext) {
    const { remote_created_at, ...rest } = row;
    const knownFields = pick(rest, Object.keys(CompanyRowType.shape));
    const createdAt = remote_created_at;
    const customFields = omit(rest, Object.keys(CompanyRowType.shape));
    await this.handleCustomAttributes(customFields);
    const companyObj = {
      ...knownFields,
      remote_created_at: createdAt ? createdAt.getTime() / 1000 : undefined,
      custom_attributes: Object.keys(customFields).length > 0 ? customFields : undefined,
    };
    try {
      const res = await this.client.post(`/companies`, companyObj);
      console.log(`Company created: ${JSON.stringify(res.data)}`);
    } catch (e) {
      throw rethrowAxiosError(e);
    }
  }
}

export const companiesStream: DestinationStream<IntercomCredentials, CompanyRowType> = {
  name: "companies",
  rowType: CompanyRowType,
  createOutputStream: async (cred, ctx) => await new CompaniesOutputStream(cred, ctx).init(),
};

export const contactsStream: DestinationStream<IntercomCredentials, ContactRowType> = {
  name: "contacts",
  rowType: ContactRowType,
  createOutputStream: async (cred, ctx) => await new ContactsOutputStream(cred, ctx).init(),
};

export const intercomProvider: DestinationProvider<IntercomCredentials> = {
  name: "intercom",
  credentialsType: IntercomCredentials,
  streams: [contactsStream, companiesStream],
  defaultStream: "audience",
};
