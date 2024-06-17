import { z } from "zod";
import {
  BaseRateLimitedOutputStream,
  DestinationProvider,
  normalizeEmail,
  OutputStreamConfiguration,
  splitName,
  stdProtocol,
} from "@syncmaven/node-cdk";
import { ExecutionContext } from "@syncmaven/protocol";
import { Resend } from "resend";
import assert from "assert";

export const ResendCredentials = z.object({
  apiKey: z.string().describe("Resend API key. Can be found here: https://resend.com/api-keys"),
});

export type ResendCredentials = z.infer<typeof ResendCredentials>;

const ResendRow = z.object({
  email: z.string(),
  name: z.string().optional(),
  unsubscribed: z.boolean().optional(),
});

type ResendRow = z.infer<typeof ResendRow>;

class ResendStream extends BaseRateLimitedOutputStream<ResendRow, ResendCredentials> {
  private resend: Resend;
  private audienceId: string = "";

  constructor(config: OutputStreamConfiguration<ResendCredentials>, ctx: ExecutionContext) {
    super(config, ctx, 1000);
    this.resend = new Resend(config.credentials.apiKey);
  }

  async init(ctx: ExecutionContext) {
    const audienceName =
      this.config.options.audienceName || `AudienceSync: ${this.config.syncId}, stream=${this.config.streamId}`;
    const audiences = await this.resend.audiences.list();
    if (audiences.error) {
      throw new Error(`Error getting audiences ${audiences.error.message}`);
    }
    assert(audiences.data);

    const audience = audiences.data.data.find(a => a.name === audienceName);
    if (!audience) {
      console.log(`Audience with name ${audienceName} not found, creating...`);
      const newAudience = await this.resend.audiences.create({
        name: audienceName,
      });
      if (newAudience.error) {
        throw new Error(`Error creating audience: ${newAudience.error}`);
      }
      assert(newAudience.data);
      this.audienceId = newAudience.data.id;
    } else {
      this.audienceId = audience.id;
    }
    return this;
  }

  protected async handleRowRateLimited(row: ResendRow, ctx: ExecutionContext) {
    let retry = false;
    do {
      const email = normalizeEmail(row.email);
      const { first, last } = row.name ? splitName(row.name) : { first: email.split("@")[0], last: "" };
      const createPayload = {
        email: email,
        firstName: first,
        lastName: last,
        unsubscribed: !!row.unsubscribed,
        audienceId: this.audienceId,
      };
      const creationResult = await this.resend.contacts.create(createPayload);
      if (creationResult.error) {
        console.error(`Error creating contact ${email}: ${creationResult.error.message}`);
        const rpsMatch = creationResult.error.message.match(/(\d+) requests per second/);
        if (rpsMatch) {
          this.rateLimit = parseInt(rpsMatch[1]);
          console.warn(`Rate limit set to ${this.rateLimit} rps.`);
          throw { code: 429 };
        }
      }
    } while (retry);
  }
}

export const resendProvider: DestinationProvider<ResendCredentials> = {
  name: "resend",
  credentialsType: ResendCredentials,
  streams: [
    {
      name: "audience",
      rowType: ResendRow,
      createOutputStream: (config, ctx) => new ResendStream(config, ctx).init(ctx),
    },
  ],
  defaultStream: "audience",
};

stdProtocol(resendProvider);
