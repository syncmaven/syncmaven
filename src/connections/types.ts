import { ZodType } from "zod";

type AnyRow = Record<string, any>;
type AnyCredentials = any;

export type DestinationStream<Cred extends AnyCredentials = AnyCredentials, RowType extends AnyRow = AnyRow> = {
  name: string;
  rowType: ZodType<RowType>;
  createOutputStream: (
    config: OutputStreamConfiguration<Cred>,
    ctx: ExecutionContext
  ) => OutputStream<RowType> | Promise<OutputStream<RowType>>;
};

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

export type StorageKey = string | string[];

export type Entry = {
  key: StorageKey;
  value: any;
};

export interface StreamPersistenceStore {
  get(key: StorageKey): Promise<any>;

  set(key: StorageKey, value: any): Promise<void>;

  del(key: StorageKey): Promise<void>;

  list(prefix: StorageKey): Promise<Entry[]>;

  stream(prefix: StorageKey, cb: (entry: Entry) => Promise<void> | void): Promise<any>;

  streamBatch(prefix: StorageKey, cb: (batch: Entry[]) => Promise<void> | void, maxBatchSize: number): Promise<any>;

  deleteByPrefix(rowsKey: StorageKey): Promise<void>;

  size(rowsCacheKey: StorageKey): Promise<number>;
}

export type ExecutionContext = {
  store: StreamPersistenceStore;
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
  abstract init(): this | Promise<this>;

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
  abstract init(): this | Promise<this>;

  async handleRow(row: RowT, ctx: ExecutionContext) {
    this.currentBatch.push(row);
    if (this.currentBatch.length >= 1000) {
      await this.flushBatch(ctx);
    }
  }

  async finish(ctx) {
    if (this.currentBatch.length > 0) {
      await this.flushBatch(ctx);
    }
  }

  private async flushBatch(ctx: ExecutionContext) {
    console.log(`Flushing batch of ${this.currentBatch.length} rows....`);
    const start = Date.now();
    await this.processBatch(this.currentBatch, ctx);
    console.log(`Batch of ${this.currentBatch.length} rows processed in ${Date.now() - start}ms`);
    this.currentBatch = [];
  }

  protected abstract processBatch(currentBatch: RowT[], ctx: ExecutionContext): Promise<void> | void;
}
