import { Entry, StorageKey, StreamPersistenceStore } from "@syncmaven/protocol";
import fs from "fs";
import Sqlite, { Database } from "better-sqlite3";
import { Client } from "pg";

function validateKey(key: StorageKey) {
  const segments = Array.isArray(key) ? key : [key];
  for (const segment of segments) {
    if (segment.indexOf("::") >= 0) {
      throw new Error(`Invalid key segment: '${segment}'. Key segments cannot contain '::'`);
    }
  }
}

function stringifyKey(prefix: StorageKey) {
  validateKey(prefix);
  const prefixArr = Array.isArray(prefix) ? prefix : [prefix];
  return prefixArr.join("::");
}

function getParameterFromConnectionString(url: string, param: string): string | undefined {
  const urlObj = new URL(url);
  return urlObj.searchParams.get(param) || undefined;
}

export class PostgresStore implements StreamPersistenceStore {
  private client: Client;
  private schema: string;

  constructor(url: string) {
    this.client = new Client({
      connectionString: url,
    });
    this.schema = getParameterFromConnectionString(url, "schema") || "syncmaven";
  }

  async init(): Promise<void> {
    await this.client.connect();
    await this.client.query(`SET search_path TO ${this.schema}`);
    await this.client.query(`create schema if not exists ${this.schema}`);
    await this.client.query(`create table if not exists syncmaven_store
                             (
                                 key   TEXT primary key,
                                 value TEXT
                             )`);
  }

  async get(key: StorageKey) {
    const res = await this.client.query(`SELECT value FROM syncmaven_store WHERE key = $1`, [stringifyKey(key)]);
    if (res.rowCount === 0) {
      return undefined;
    }
    return JSON.parse(res.rows[0].value);
  }

  async set(key: StorageKey, value: any) {
    await this.client.query(
      `INSERT INTO syncmaven_store (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2`,
      [stringifyKey(key), JSON.stringify(value)]
    );
  }

  async del(key: StorageKey) {
    await this.client.query(`DELETE FROM syncmaven_store WHERE key = $1`, [stringifyKey(key)]);
  }

  async list(prefix: StorageKey) {
    const res: Entry[] = [];
    await this.stream(prefix, entry => {
      res.push(entry);
    });
    return res;
  }

  async stream(prefix: StorageKey, cb: (entry: Entry) => void | Promise<void>) {
    const key = stringifyKey(prefix);
    const keyPattern = `${key}::%`;
    const res = await this.client.query(
      `SELECT key, value FROM syncmaven_store WHERE key LIKE $1 or key = $2 ORDER BY key ASC`,
      [keyPattern, key]
    );
    for (const row of res.rows) {
      await cb({
        key: (row as any).key.split("::"),
        value: JSON.parse((row as any).value),
      });
    }
  }

  async streamBatch(prefix: StorageKey, cb: (batch: Entry[]) => void | Promise<void>, maxBatchSize: number) {
    const batch: Entry[] = [];
    const flushBatch = async () => {
      if (batch.length > 0) {
        console.debug(`Going to flush batch len of ` + batch.length);
        await cb(batch);
        batch.length = 0;
      }
      console.debug(`Batch flushed and cleaned. New len: ` + batch.length);
    };

    await this.stream(prefix, async ({ key, value }) => {
      batch.push({ key, value });
      if (batch.length >= maxBatchSize) {
        console.debug(`Batch full, flushing...`);
        await flushBatch();
      }
    });
    await flushBatch();
  }

  async deleteByPrefix(prefix: StorageKey) {
    const key = stringifyKey(prefix);
    const keyPattern = `${key}::%`;
    await this.client.query(`DELETE FROM syncmaven_store WHERE key LIKE $1 OR key = $2`, [keyPattern, key]);
  }

  async size(prefix: StorageKey) {
    const key = stringifyKey(prefix);
    const keyPattern = `${key}::%`;
    const res = await this.client.query(`SELECT count(*) as count FROM syncmaven_store WHERE key LIKE $1 OR key = $2`, [
      keyPattern,
      key,
    ]);
    if (res.rowCount === 0) {
      return 0;
    }
    return res.rows[0].count as number;
  }
}

export class SqliteStore implements StreamPersistenceStore {
  private database: Database;

  constructor(dir: string) {
    if (!fs.existsSync(dir)) {
      console.log(`Creating directory ${dir} to store state`);
      fs.mkdirSync(dir, { recursive: true });
    }
    this.database = new Sqlite(`${dir}/store.db`);
    this.database.exec(`create table if not exists store
                        (
                            key   TEXT primary key,
                            value TEXT
                        )`);
    this.database.exec(`create index if not exists key_index on store (key)`);
  }

  init(): Promise<void> {
    return Promise.resolve();
  }

  del(key: StorageKey): Promise<void> {
    this.database.prepare(`DELETE FROM store WHERE key = ?`).run(stringifyKey(key));
    return Promise.resolve();
  }

  get(key: StorageKey): Promise<any> {
    const strVal = this.database
      .prepare<any, any>(`SELECT value FROM store WHERE key = ?`)
      .get(stringifyKey(key))?.value;
    return Promise.resolve(strVal ? JSON.parse(strVal) : undefined);
  }

  async list(prefix: StorageKey): Promise<Entry[]> {
    const res: Entry[] = [];
    await this.stream(prefix, entry => {
      res.push(entry);
    });
    return res;
  }

  set(key: StorageKey, value: any): Promise<void> {
    this.database
      .prepare(`insert or replace into store (key, value) values (?, ?)`)
      .run(stringifyKey(key), JSON.stringify(value));
    return Promise.resolve();
  }

  async streamBatch(
    prefix: StorageKey,
    cb: (batch: Entry[]) => Promise<void> | void,
    maxBatchSize: number
  ): Promise<any> {
    const batch: Entry[] = [];
    const flushBatch = async () => {
      if (batch.length > 0) {
        console.debug(`Going to flush batch len of ` + batch.length);
        await cb(batch);
        batch.length = 0;
      }
      console.debug(`Batch flushed and cleaned. New len: ` + batch.length);
    };

    await this.stream(prefix, async ({ key, value }) => {
      batch.push({ key, value });
      if (batch.length >= maxBatchSize) {
        console.debug(`Batch full, flushing...`);
        await flushBatch();
      }
    });

    await flushBatch();
  }

  async stream(prefix: StorageKey, cb: (entry: Entry) => Promise<void> | void): Promise<any> {
    const key = stringifyKey(prefix);
    const keyPattern = `${key}::%`;
    const stmt = this.database.prepare(`SELECT key, value FROM store WHERE key LIKE ? or key = ? ORDER BY key ASC`);
    const stream = stmt.iterate(keyPattern, key);
    for (const row of stream) {
      await cb({
        key: (row as any).key.split("::"),
        value: JSON.parse((row as any).value),
      });
    }
  }

  size(prefix: StorageKey): Promise<number> {
    // const result = this.database.prepare(`SELECT count(*) as count FROM store WHERE key LIKE ? OR key = ?`).get(`${key}::%`, key);

    const stmt = this.database.prepare(`SELECT count(*) as count FROM store WHERE key LIKE ? OR key = ?`);
    const key = stringifyKey(prefix);
    const keyPattern = `${key}::%`;
    const result = stmt.get(keyPattern, key);
    return (result as any).count;
  }

  deleteByPrefix(prefix: StorageKey): Promise<void> {
    const key = stringifyKey(prefix);
    const keyPattern = `${key}::%`;
    this.database.prepare(`DELETE FROM store WHERE key LIKE ? OR key = ?`).run(keyPattern, key);
    return Promise.resolve();
  }
}
