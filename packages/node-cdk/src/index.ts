import { ZodType } from "zod";
import type { ExecutionContext } from "@syncmaven/protocol";
import crypto from "crypto";
import { RateLimitError } from "./rate-limit";

type AnyRow = Record<string, any>;
type AnyCredentials = any;

export type DestinationStream<
  Cred extends AnyCredentials = AnyCredentials,
  RowType extends AnyRow = AnyRow,
  StreamOptions extends AnyCredentials = AnyCredentials,
> = {
  name: string;
  rowType: ZodType<RowType>;
  streamOptions?: ZodType<StreamOptions>;
  createOutputStream: (
    config: OutputStreamConfiguration<Cred>,
    ctx: ExecutionContext
  ) => OutputStream<RowType> | Promise<OutputStream<RowType>>;
};
/**
 * Some destinations have constant stream settings and do not require credentials to be set.
 * This helper type should be used to indicate that credentials can be empty.
 */
export type CredentialsCanBeEmpty<T> = (T & { $empty?: never }) | ({ $empty: true } & { [K in keyof T]?: never });

export type OutputStreamConfiguration<T extends AnyCredentials = AnyCredentials, O = any> = {
  streamId: string;
  options: O;
  credentials: T;
  syncId: string;
  fullRefresh?: boolean;
};

export type OutputStream<RowType extends AnyRow = AnyRow> = {
  handleRow: (row: RowType, ctx: ExecutionContext) => Promise<void> | void;
  finish?: (ctx: ExecutionContext) => Promise<void>;
};

export type DestinationProvider<T extends AnyCredentials = AnyCredentials> = {
  credentialsType: ZodType<T>;
  name: string;
  streams: DestinationStream<T, any>[];
  defaultStream: string;
};

export type EnrichmentConfig<Cred extends AnyCredentials = AnyCredentials, Opts = any> = {
  credentials: Cred;
  options: Opts;
};

type EnrichmentResult<T> = T[] | T | undefined | void;

export type StreamEnrichment<RowType extends AnyRow = AnyRow> = {
  enrichRow: (row: RowType, ctx: ExecutionContext) => Promise<EnrichmentResult<RowType>> | EnrichmentResult<RowType>;
};

export type EnrichmentProvider<Cred extends AnyCredentials = AnyCredentials, RowType extends AnyRow = AnyRow> = {
  credentialsType: ZodType<Cred>;
  name: string;
  createEnrichment: (
    config: EnrichmentConfig<Cred>,
    ctx: ExecutionContext
  ) => StreamEnrichment<RowType> | Promise<StreamEnrichment<RowType>>;
};

export abstract class BaseOutputStream<RowT extends Record<string, any>, ConfigT> implements OutputStream<RowT> {
  protected readonly config: OutputStreamConfiguration<ConfigT>;
  protected ctx: ExecutionContext;

  protected constructor(config: OutputStreamConfiguration<ConfigT>, ctx: ExecutionContext) {
    this.config = config;
    this.ctx = ctx;
  }

  /**
   * Constructors can't be async, so we need to have an init method
   */
  abstract init(ctx: ExecutionContext): this | Promise<this>;

  abstract handleRow(row: RowT, ctx: ExecutionContext): Promise<void> | void;
}

export abstract class BatchingOutputStream<RowT extends Record<string, any>, ConfigT> implements OutputStream<RowT> {
  protected readonly config: OutputStreamConfiguration<ConfigT>;
  protected ctx: ExecutionContext;
  protected currentBatch: RowT[] = [];
  protected maxBatchSize: number;

  protected constructor(
    config: OutputStreamConfiguration<ConfigT>,
    ctx: ExecutionContext,
    maxBatchSize: number = 1000
  ) {
    this.config = config;
    this.ctx = ctx;
    this.maxBatchSize = maxBatchSize;
  }

  /**
   * Constructors can't be async, so we need to have an init method
   */
  abstract init(ctx: ExecutionContext): this | Promise<this>;

  async handleRow(row: RowT, ctx: ExecutionContext) {
    this.currentBatch.push(row);
    if (this.currentBatch.length >= this.maxBatchSize) {
      await this.flushBatch(ctx);
    }
  }

  async finish(ctx) {
    if (this.currentBatch.length > 0) {
      await this.flushBatch(ctx);
    }
  }

  private async flushBatch(ctx: ExecutionContext) {
    const batch = this.currentBatch;
    this.currentBatch = [];
    console.log(`Flushing batch of ${batch.length} rows....`);
    const start = Date.now();
    await this.processBatch(batch, ctx);
    console.log(`Batch of ${batch.length} rows processed in ${Date.now() - start}ms`);
  }

  protected abstract processBatch(currentBatch: RowT[], ctx: ExecutionContext): Promise<void> | void;
}

export function splitName(name: string): { first: string; last: string } {
  const [first, ...rest] = name.split(" ");
  return {
    first,
    last: rest.join(" "),
  };
}

export function emailHash(email: string): string {
  return crypto.createHash("sha256").update(email).digest("hex");
}

export function normalizeEmail(email: string): string {
  return email.toLowerCase().trim();
}

export * from "./rpc";
export * from "./std";
export * from "./inmem-store";
export * from "./test-kit";
export * from "./rate-limit";
