import { z } from "zod";
import { BatchingOutputStream, DestinationProvider, ExecutionContext, OutputStreamConfiguration } from "../types";
import { GoogleAdsApi } from "google-ads-api";

export const GoogleAdsCredentials = z.object({
  developerToken: z.string(),
  clientSecret: z.string(),
  clientId: z.string(),

  refreshToken: z.string(),
  customerId: z.string(),
});

export type GoogleAdsCredentials = z.infer<typeof GoogleAdsCredentials>;

const AudienceRowType = z.object({
  email: z.string(),
});

type AudienceRowType = z.infer<typeof AudienceRowType>;

class GoogleAdsAudienceOutputStream extends BatchingOutputStream<AudienceRowType, GoogleAdsCredentials> {
  constructor(config: OutputStreamConfiguration<GoogleAdsCredentials>, ctx: ExecutionContext) {
    super(config, ctx);
  }

  async init(): Promise<this> {
    const audienceName =
      this.config.options?.audienceName ||
      `audience-sync?syncId=${this.config.syncId}&streamId=${this.config.streamId}`;
    const client = new GoogleAdsApi({
      client_id: this.config.credentials.clientId,
      client_secret: this.config.credentials.clientSecret,
      developer_token: this.config.credentials.developerToken,
    });
    const customer = client.Customer({
      customer_id: this.config.credentials.customerId,
      refresh_token: this.config.credentials.refreshToken,
    });
    const audiences = await customer.query(`SELECT custom_audience.id, custom_audience.name FROM custom_audience`);
    if (!audiences.includes(audienceName)) {
      console.log(`Audience with ${audienceName} not found, creating...`);
      const newAudience = await customer.audiences.create([
        {
          name: audienceName,
          description: `This audience is created by Jitsu for stream ${this.config.streamId} with syncId ${this.config.syncId}. Don't change it's name!`,
        },
      ]);
      console.log("Created audience", newAudience);
    }

    return this;
  }

  protected processBatch(currentBatch: AudienceRowType[], ctx: ExecutionContext): Promise<void> | void {}
}

export const googleAdsProvider: DestinationProvider = {
  credentialsType: GoogleAdsCredentials,
  defaultStream: "audience",
  name: "google-ads",
  streams: [
    {
      name: "audience",
      rowType: AudienceRowType,
      createOutputStream: (config, ctx) => new GoogleAdsAudienceOutputStream(config, ctx).init(),
    },
  ],
};
