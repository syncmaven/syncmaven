import { z } from "zod";
import {
  BaseOutputStream,
  DestinationProvider,
  DestinationStream,
  ExecutionContext,
  OutputStreamConfiguration,
} from "../types";
import { createRpcClient, rpc, RpcError, RpcFunc } from "../../lib/rpc";
import crypto from "crypto";

export const FacebookAdsCredentials = z.object({
  accessToken: z.string(),
  accountId: z.string(),
});

export type FacebookAdsCredentials = z.infer<typeof FacebookAdsCredentials>;

const AudienceRowType = z.object({
  email: z.string(),
});

type AudienceRowType = z.infer<typeof AudienceRowType>;
const maxBatchSize = 1000;

class FacebookAudienceOutputStream extends BaseOutputStream<AudienceRowType, FacebookAdsCredentials> {
  private accountId: string;
  private currentBatch: AudienceRowType[] = [];
  private sessionId: number = Math.round(Math.random() * 100_000_000_000);
  private batchSequence = 1;
  private audienceId: string = "";
  private apiVersion = "v18.0";
  private client: RpcFunc;
  private rowsKey: string[];

  constructor(config: OutputStreamConfiguration<FacebookAdsCredentials>, ctx: ExecutionContext) {
    super(config, ctx);
    this.accountId = config.credentials.accountId.startsWith("act_")
      ? config.credentials.accountId
      : "act_" + config.credentials.accountId;
    this.client = createRpcClient({
      headers: {
        Authorization: `Bearer ${config.credentials.accessToken}`,
        "Content-Type": "application/json",
      },
      urlBase: `https://graph.facebook.com/${this.apiVersion}/${this.accountId}`,
    });
    this.rowsKey = [`sync=${config.syncId}`, `stream=${config.streamId}`, "last-synced-rows"];
  }

  async init() {
    const { streamId, syncId } = this.config;
    const currentAudiences = await this.client(`/customaudiences?fields=id,name,description`);
    const audienceName = this.config.options?.audienceName || `audience-sync?syncId=${syncId}&streamId=` + streamId;
    const description = `This audience is created by Jitsu for stream ${streamId} with syncId ${syncId}. Don't change it's name!`;
    this.audienceId = currentAudiences.data.find(a => a.name === audienceName)?.id;
    if (!this.audienceId) {
      console.log(`Audience with ${audienceName} not found, creating...`);
      const audience = await this.client(`/customaudiences`, {
        method: "POST",
        body: {
          name: audienceName,
          description,
          subtype: "CUSTOM",
          customer_file_source: "USER_PROVIDED_ONLY",
          retention_days: 30,
        },
      });
      this.audienceId = audience.id;
    }

    console.log(`Using audience ${this.audienceId}`);
    if (!this.config.options.doNotClearAudience) {
      const toRemoveBatch: AudienceRowType[] = [];

      const removeBatch = async (toRemoveBatch: AudienceRowType[]) => {
        if (toRemoveBatch.length === 0) {
          return;
        }
        console.log(`Removing batch of ${toRemoveBatch.length} users`);
        const payload = {
          schema: ["EMAIL_SHA256"],
          data: toRemoveBatch.map(r => crypto.createHash("sha256").update(r.email.toLowerCase()).digest("hex")),
        };
        await rpc(`https://graph.facebook.com/${this.apiVersion}/${this.audienceId}/users`, {
          method: "DELETE",
          headers: {
            Authorization: `Bearer ${this.config.credentials.accessToken}`,
            "Content-Type": "application/json",
          },
          body: { payload },
        });
        toRemoveBatch.length = 0;
      };

      await this.ctx.store.stream(this.rowsKey, async ({ key, value }) => {
        let row: AudienceRowType;
        try {
          row = AudienceRowType.parse(value);
        } catch (e) {
          console.warn(`Can't parse value at key ${key}`, value, e);
          return;
        }
        toRemoveBatch.push(row);
        if (toRemoveBatch.length >= maxBatchSize) {
          await removeBatch(toRemoveBatch);
        }
      });
      await removeBatch(toRemoveBatch);
      await this.ctx.store.deleteByPrefix(this.rowsKey);
    }

    return this;
  }

  async handleRow(row: AudienceRowType, ctx: ExecutionContext) {
    this.currentBatch.push(row);
    if (this.currentBatch.length >= maxBatchSize) {
      await this.flushBatch();
    }
  }

  async finish(ctx: ExecutionContext): Promise<void> {
    if (this.currentBatch.length > 0) {
      await this.flushBatch();
    }
  }

  private async flushBatch(lastBatch: boolean = false) {
    console.log(`Flushing batch of ${this.currentBatch.length} users`);

    function sha256(email: string) {
      return crypto.createHash("sha256").update(email.toLowerCase()).digest("hex");
    }

    const payload = {
      schema: ["EMAIL_SHA256"],
      data: this.currentBatch.map(r => [sha256(r.email)]),
    };

    const body = {
      session: {
        session_id: this.sessionId,
        batch_seq: this.batchSequence++,
        last_batch_flag: lastBatch,
        //estimated_num_total: 10000,
      },
      payload,
    };

    try {
      //console.log(`Batch payload`, body);
      const batchFlushResponse = await rpc(`https://graph.facebook.com/${this.apiVersion}/${this.audienceId}/users`, {
        headers: { Authorization: `Bearer ${this.config.credentials.accessToken}`, "Content-Type": "application/json" },
        method: "POST",
        body: body,
      });
      console.log(`Batch of ${this.currentBatch.length} users flushed. Response`, batchFlushResponse);
    } catch (e) {
      if (e instanceof RpcError) {
        console.error(
          `Error flushing batch of ${this.currentBatch.length} users. Code ${e.statusCode}. Response`,
          e.response
        );
      }
    } finally {
      this.currentBatch.length = 0;
    }
  }
}

const audienceStream: DestinationStream<FacebookAdsCredentials, AudienceRowType> = {
  name: "audience",
  rowType: AudienceRowType,
  createOutputStream: async (config, ctx) => await new FacebookAudienceOutputStream(config, ctx).init(),
};

export const facebookAdsProvider: DestinationProvider<FacebookAdsCredentials> = {
  name: "facebook-ads",
  credentialsType: FacebookAdsCredentials,
  streams: [audienceStream],
  defaultStream: "audience",
};
