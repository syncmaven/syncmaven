import { z } from "zod";
import {
  BaseOutputStream,
  CredentialsCanBeEmpty,
  RateLimitError,
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
  name: z.string().optional(),
  email: z.string(),
  unsubscribed: z.boolean().or(z.null()).optional(),
});

type ResendRow = z.infer<typeof ResendRow>;

class ResendAudienceStream extends BaseOutputStream<ResendRow, CredentialsCanBeEmpty<ResendCredentials>> {
  private resend: Resend;
  private audienceId: string = "";

  constructor(config: OutputStreamConfiguration<CredentialsCanBeEmpty<ResendCredentials>>, ctx: ExecutionContext) {
    super(config, ctx);
    if (config.credentials.$empty) {
      throw new Error("Resend credentials must be set");
    }
    this.resend = new Resend(config.credentials.apiKey);
  }

  async init(ctx: ExecutionContext) {
    const audienceName =
      this.config.options.audienceName || `Syncmaven: ${this.config.syncId}, stream=${this.config.streamId}`;
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

  async handleRow(row: ResendRow, ctx: ExecutionContext) {
    let retry = false;
    do {
      const email = normalizeEmail(row.email);
      const { first, last } = row.name ? splitName(row.name) : { first: email.split("@")[0], last: "" };
      const createPayload = {
        email: email,
        firstName: first,
        lastName: last,
        //undefined means that we keep original value in resend
        unsubscribed: row.unsubscribed || undefined,
        audienceId: this.audienceId,
      };
      const creationResult = await this.resend.contacts.create(createPayload);
      if (creationResult.error) {
        const rpsMatch = creationResult.error.message.match(/(\d+) requests per second/);
        if (rpsMatch) {
          console.error(
            `Error creating contact ${email} - rate limit exceeded, will notify the runner to retry in 1s. Original message: ${creationResult?.error?.message}`
          );
          throw new RateLimitError(`Rate limit exceeded: ${rpsMatch?.[1]} requests per second`, {
            //this is a random number, we should get it from the error message / headers
            retryAfterMs: 1000,
          });
        } else {
          throw new Error(`Error creating contact ${email}: ${creationResult.error.message}`);
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
      createOutputStream: (config, ctx) => new ResendAudienceStream(config, ctx).init(ctx),
    },
  ],
  defaultStream: "audience",
};

stdProtocol(resendProvider);
