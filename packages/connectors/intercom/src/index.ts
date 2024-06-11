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
    plan_type: z.string().optional(),
    plan_id: z.string().optional(),
    plan_name: z.string().optional(),
    size: z.number().optional(),
    website: z.string().optional(),
    industry: z.string().optional(),
    monthly_spend: z.number().optional(),
  })
  .catchall(z.any());

export type CompanyRowType = z.infer<typeof CompanyRowType>;

export const ContactRowType = z.object({});

export type ContactRowType = z.infer<typeof ContactRowType>;

export type IntercomCredentials = z.infer<typeof IntercomCredentials>;

function createClient(creds: IntercomCredentials) {
  return axios.create({
    baseURL: `https://api.intercom.io/`,
    headers: {
      Authorization: `Bearer ${creds.accessToken}`,
      "Content-Type": "application/json",
    },
  });
}

class ContactsOutputStream extends BaseOutputStream<ContactsOutputStream, IntercomCredentials> {
  constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext) {
    super(config, ctx);
  }

  async handleRow(row: ContactsOutputStream, ctx: ExecutionContext) {}

  async init(): Promise<this> {
    return this;
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

class CompaniesOutputStream extends BaseOutputStream<CompanyRowType, IntercomCredentials> {
  private client: AxiosInstance;
  private customAttributes: Record<string, any> = {};
  private customAttributesPolicy: CustomAttributesPolicy;

  constructor(config: OutputStreamConfiguration<IntercomCredentials>, ctx: ExecutionContext) {
    super(config, ctx);
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

  async handleRow(row: CompanyRowType, ctx: ExecutionContext) {
    /**
     * https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Companies/company/
     */
    const { plan_id, plan_name, plan_type, ...rest } = row;
    const plan = undefined;
    // {
    //   id: plan_id,
    //   name: plan_name,
    //   type: plan_type,
    // };
    const knownFields = pick(rest, Object.keys(CompanyRowType.shape));
    const customFields = omit(rest, Object.keys(CompanyRowType.shape));
    for (const key in customFields) {
      if (!this.customAttributes[key]) {
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
            model: "company",
            type,
          };
          await this.client.post(`/data_attributes`, createAttributeRequest);
          await this.refreshCustomAttributes();
        }
      }
    }

    const companyObj = {
      plan: plan && Object.keys(plan).length > 0 ? plan : undefined,
      ...knownFields,
      custom_attributes: customFields,
    };
    try {
      await this.client.post(`/companies`, companyObj);
    } catch (e) {
      throw rethrowAxiosError(e);
    }
  }

  async init(): Promise<this> {
    await this.refreshCustomAttributes();
    return this;
  }

  private async refreshCustomAttributes() {
    const customAttributesArray = await this.client.get(`/data_attributes?model=company`);
    this.customAttributes = customAttributesArray.data.data.reduce((acc: any, attr: any) => {
      acc[attr.name] = attr.id;
      return acc;
    }, {});
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
