import { Entry, StorageKey, StreamPersistenceStore } from "../connections/types";
import fs from "fs";
import Sqlite, { Database } from "better-sqlite3";

function stringifyKey(prefix: StorageKey) {
  const prefixArr = Array.isArray(prefix) ? prefix : [prefix];
  return prefixArr.join("::");
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
    this.database.exec(`create table if not exists store
                        (
                            key   TEXT primary key,
                            value TEXT
                        )`);
    this.database.exec(`create index if not exists key_index on store (key)`);
  }

  del(key: StorageKey): Promise<void> {
    this.validateKey(key);
    this.database.prepare(`DELETE FROM store WHERE key = ?`).run(key);
    return Promise.resolve();
  }

  get(key: StorageKey): Promise<any> {
    const strVal = this.database.prepare<any, any>(`SELECT value FROM store WHERE key = ?`).get(typeof key === "string" ? key : key.join("::"))?.value;
    return Promise.resolve(strVal ? JSON.parse(strVal) : undefined);
  }

  async list(prefix: StorageKey): Promise<Entry[]> {
    const res: Entry[] = [];
    await this.stream(prefix, (entry) => {
      res.push(entry);
    });
    return res;
  }

  set(key: StorageKey, value: any): Promise<void> {
    this.validateKey(key);
    this.database.prepare(`insert or replace into store (key, value) values (?, ?)`).run(stringifyKey(key), JSON.stringify(value));
    return Promise.resolve();
  }

  async streamBatch(prefix: StorageKey, cb: (batch: Entry[]) => (Promise<void> | void), maxBatchSize: number): Promise<any> {
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
  }

  async stream(prefix: StorageKey, cb: (entry: Entry) => (Promise<void> | void)): Promise<any> {
    const keyString = stringifyKey(prefix);
    const stmt = this.database.prepare(`SELECT key, value FROM store WHERE key LIKE ? or key = ? ORDER BY key ASC`);
    const stream = stmt.iterate(`${keyString}::%`, keyString);
    for (const row of stream) {
      await cb({ key: (row as any).key.split("::"), value: JSON.parse((row as any).value) });
    }
  }


  size(prefix: StorageKey): Promise<number> {
    // const result = this.database.prepare(`SELECT count(*) as count FROM store WHERE key LIKE ? OR key = ?`).get(`${key}::%`, key);

    const stmt = this.database.prepare(`SELECT count(*) as count FROM store WHERE key LIKE ? OR key = ?`);
    const prefixArr = Array.isArray(prefix) ? prefix : [prefix];
    const key = prefixArr.join('::');
    const keyPattern = `${key}::%`;
    const result = stmt.get(keyPattern, key);
    return (result as any).count;
  }



  deleteByPrefix(prefix: StorageKey): Promise<void> {
    const prefixArr = Array.isArray(prefix) ? prefix : [prefix];
    const keyString = prefixArr.join("::");
    this.database.prepare(`DELETE FROM store WHERE key LIKE ? OR key = ?`).run(`${keyString}::%`, keyString);
    return Promise.resolve();
  }

  private validateKey(key: StorageKey) {
    const segments = Array.isArray(key) ? key : [key];
    for (const segment of segments) {
      if (segment.indexOf("::") >= 0) {
        throw new Error(`Invalid key segment: '${segment}'. Key segments cannot contain '::'`);
      }
    }
  }
}