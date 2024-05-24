import { z } from "zod";
import {
  BaseOutputStream,
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
  apiKey: z.string(),
});

export type ResendCredentials = z.infer<typeof ResendCredentials>;

const ResendRow = z.object({
  email: z.string(),
  name: z.string().optional(),
  unsubscribed: z.boolean().optional(),
});

type ResendRow = z.infer<typeof ResendRow>;

class ResendStream extends BaseOutputStream<ResendRow, ResendCredentials> {
  private resend: Resend;
  private audienceId: string = "";
  private lastCallTime = Date.now();
  // rateLimitRps
  private rateLimit = 1000;

  constructor(
    config: OutputStreamConfiguration<ResendCredentials>,
    ctx: ExecutionContext,
  ) {
    super(config, ctx);
    this.resend = new Resend(config.credentials.apiKey);
  }

  async init() {
    const audienceName =
      this.config.options.audienceName ||
      `AudienceSync: ${this.config.syncId}, stream=${this.config.streamId}`;
    const audiences = await this.resend.audiences.list();
    if (audiences.error) {
      throw new Error(`Error getting audiences ${audiences.error.message}`);
    }
    assert(audiences.data);

    const audience = audiences.data.data.find((a) => a.name === audienceName);
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

  public async handleRow(row: ResendRow, ctx: ExecutionContext) {
    let retry = false;
    do {
      const email = normalizeEmail(row.email);
      const { first, last } = row.name
        ? splitName(row.name)
        : { first: email.split("@")[0], last: "" };
      const createPayload = {
        email: email,
        firstName: first,
        lastName: last,
        unsubscribed: !!row.unsubscribed,
        audienceId: this.audienceId,
      };
      const creationResult = await this.resend.contacts.create(createPayload);
      if (creationResult.error) {
        console.error(
          `Error creating contact ${email}: ${creationResult.error.message}`,
        );
        const rpsMatch = creationResult.error.message.match(
          /(\d+) requests per second/,
        );
        if (rpsMatch) {
          this.rateLimit = parseInt(rpsMatch[1]);
          retry = !retry;
          console.warn(
            `Rate limit set to ${this.rateLimit} rps. Retrying: ${retry}`,
          );
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
      await this.rateLimitDelay();
    } while (retry);
  }

  private async rateLimitDelay() {
    const delay = 1000 / this.rateLimit;
    const dt = Date.now();
    const elapsed = dt - this.lastCallTime;
    this.lastCallTime = dt;
    if (elapsed < delay) {
      await new Promise((resolve) => setTimeout(resolve, delay - elapsed));
    }
  }
}

export const resendProvider: DestinationProvider<ResendCredentials> = {
  name: "resend",
  credentialsType: ResendCredentials,
  streams: [
    {
      name: "audience",
      rowType: ResendRow,
      createOutputStream: (config, ctx) => new ResendStream(config, ctx).init(),
    },
  ],
  defaultStream: "audience",
};

stdProtocol(resendProvider);
