import { z } from "zod";
import {
  BatchingOutputStream,
  DestinationProvider,
  normalizeEmail,
  OutputStreamConfiguration,
  splitName,
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

class ResendStream extends BatchingOutputStream<ResendRow, ResendCredentials> {
  private resend: Resend;
  private audienceId: string = "";

  constructor(config: OutputStreamConfiguration<ResendCredentials>, ctx: ExecutionContext, maxBatchSize: number) {
    super(config, ctx, maxBatchSize);
    this.resend = new Resend(config.credentials.apiKey);
  }

  async init() {
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
      const newAudience = await this.resend.audiences.create({ name: audienceName });
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

  protected async processBatch(currentBatch: ResendRow[], ctx: ExecutionContext) {
    for (const row of currentBatch) {
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
        console.log(`Error creating contact ${email}: ${creationResult.error.message}`);
      }
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
      createOutputStream: (config, ctx) => new ResendStream(config, ctx, 1000).init(),
    },
  ],
  defaultStream: "audience",
};
