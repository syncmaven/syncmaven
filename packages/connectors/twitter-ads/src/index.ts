import { z } from "zod";
import { BatchingOutputStream, DestinationProvider, stdProtocol } from "@syncmaven/node-cdk";
import TwitterAdsAPI from "twitter-ads";
import { emailHash, normalizeEmail } from "@syncmaven/node-cdk";
import request from "request";

export const TwitterCredentials = z.object({
  consumerKey: z.string(),
  consumerSecret: z.string(),
  accessToken: z.string(),
  accessTokenSecret: z.string(),
  accountId: z.string(),
});

const AudienceRowType = z.object({
  email: z.string(),
});

type AudienceRowType = z.infer<typeof AudienceRowType>;

export type TwitterCredentials = z.infer<typeof TwitterCredentials>;

class TwitterOutputStream extends BatchingOutputStream<AudienceRowType, TwitterCredentials> {
  private bearerToken: string = "";
  private api: any;
  private rowsCacheKey: string[];
  private audienceId: string = "";
  private twitterApiVersion = 12;

  constructor(config, ctx) {
    super(config, ctx);
    this.rowsCacheKey = [`syncId=${config.syncId}`, `stream=${config.streamId}`, "last-synced-rows"];
  }

  async init() {
    this.api = new TwitterAdsAPI({
      consumer_key: this.config.credentials.consumerKey,
      consumer_secret: this.config.credentials.consumerSecret,
      access_token: this.config.credentials.accessToken,
      access_token_secret: this.config.credentials.accessTokenSecret,
      sandbox: false,
      api_version: "12",
    });

    const audienceName =
      this.config.options.audienceName || `AudienceSync: ${this.config.syncId}, stream=${this.config.streamId}`;
    const audiences = await this.get(`/accounts/${this.config.credentials.accountId}/custom_audiences`);
    console.debug(`Twitter audiences ${typeof audiences}. Will look for '${audienceName}'`, audiences.data);
    for (const audience of audiences.data) {
      if (audience.name === audienceName) {
        this.audienceId = audience.id;
        console.debug(`Found audience with name '${audienceName}' and id ${this.audienceId}`);
        break;
      }
    }
    if (!this.audienceId) {
      console.log(`Audience with name ${audienceName} not found, creating...`);
      const newAudience = await this.post(`/accounts/${this.config.credentials.accountId}/custom_audiences`, {
        name: audienceName,
        description: `Audience created by AudienceSync for stream ${this.config.streamId} with syncId ${this.config.syncId}. Don't change it's name!`,
      });
      console.debug(`Created audience`, newAudience);
      this.audienceId = newAudience.data.id;

      console.log(`Created audience with id ${this.audienceId}`);
    }
    if (!this.config.options.doNotClearAudience) {
      const size = await this.ctx.store.size(this.rowsCacheKey);
      console.log(`Cleaning twitter audience = ${this.audienceId}. Previously synced audience size is ${size}`);
      await this.ctx.store.streamBatch(
        this.rowsCacheKey,
        async batch => {
          console.log(`Deleting ${batch.length} rows from audience`);
          await this.post(
            `/accounts/${this.config.credentials.accountId}/custom_audiences/${this.audienceId}/users`,
            [
              {
                operation_type: "Delete",
                users: batch.map(row => AudienceRowType.parse(row.value)).map(r => emailHash(normalizeEmail(r.email))),
              },
            ],
            { forceJson: true }
          );
        },
        this.maxBatchSize
      );
      await this.ctx.store.deleteByPrefix(this.rowsCacheKey);
    }
    return this;
  }

  private async get(url: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this.api.get(url, {}, (error, resp, data) => {
        if (error) {
          reject(error);
        } else {
          resolve(data);
        }
      });
    });
  }

  private async delete(url: string, body?: any): Promise<any> {
    return new Promise((resolve, reject) => {
      this.api.delete(url, {}, (error, resp, data) => {
        if (error) {
          reject(error);
        } else {
          resolve(data);
        }
      });
    });
  }

  private async post(url: string, body: any, opts: { forceJson?: boolean } = {}): Promise<any> {
    if (opts.forceJson) {
      return new Promise((resolve, reject) => {
        request(
          {
            baseUrl: `https://ads-api.twitter.com/${this.twitterApiVersion}`,
            url: url,
            method: "POST",
            json: true,
            body: body,
            oauth: {
              consumer_key: this.config.credentials.consumerKey,
              consumer_secret: this.config.credentials.consumerSecret,
              token: this.config.credentials.accessToken,
              token_secret: this.config.credentials.accessTokenSecret,
            },
          },
          function (err, resp, body) {
            if (err) {
              reject(err);
            } else {
              resolve(body);
            }
          }
        );
      });
    } else {
      return new Promise((resolve, reject) => {
        this.api.post(url, body, (error, resp, data) => {
          if (error) {
            reject(error);
          } else {
            resolve(data);
          }
        });
      });
    }
  }

  protected async processBatch(currentBatch: AudienceRowType[]) {
    function fixISO(iso: string) {
      //apparently, twitter has its own idea of what a valid ISO date is
      const parts = iso.split(".");
      return parts[0] + "Z";
    }

    const response = await this.post(
      `/accounts/${this.config.credentials.accountId}/custom_audiences/${this.audienceId}/users`,
      [
        {
          operation_type: "Update",
          params: {
            effective_at: fixISO(new Date().toISOString()),
            expires_at: fixISO(new Date(new Date().getTime() + 1000 * 60 * 60 * 24 * 30).toISOString()), // 30 days
            users: currentBatch.map(row => ({
              email: [emailHash(normalizeEmail(row.email))],
            })),
          },
        },
      ],
      { forceJson: true }
    );
    console.log(`Sent ${currentBatch.length} users to twitter API. Response:`, JSON.stringify(response, null, 2));
    console.log(`Saving ${currentBatch.length} rows to cache`);
    for (const row of currentBatch) {
      const key = [...this.rowsCacheKey, row.email];
      await this.ctx.store.set(key, row);
    }
  }
}

export const twitterAdsProvider: DestinationProvider<TwitterCredentials> = {
  credentialsType: TwitterCredentials,
  defaultStream: "audience",
  name: "Twitter Ads",
  streams: [
    {
      name: "audience",
      rowType: AudienceRowType,
      createOutputStream: (config, ctx) => new TwitterOutputStream(config, ctx).init(),
    },
  ],
};

stdProtocol(twitterAdsProvider);
