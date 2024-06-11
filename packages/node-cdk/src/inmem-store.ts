import { Entry, StorageKey, StreamPersistenceStore } from "@syncmaven/protocol";

export class InMemoryStore implements StreamPersistenceStore {
  private store: Map<string, any>;

  constructor() {
    this.store = new Map();
  }

  async init(): Promise<void> {
    // Initialize the in-memory store if necessary
  }

  async get(key: StorageKey): Promise<any> {
    return this.store.get(this.getKeyString(key));
  }

  async set(key: StorageKey, value: any): Promise<void> {
    this.store.set(this.getKeyString(key), value);
  }

  async del(key: StorageKey): Promise<void> {
    this.store.delete(this.getKeyString(key));
  }

  async list(prefix: StorageKey): Promise<Entry[]> {
    const entries: Entry[] = [];
    for (const [key, value] of this.store.entries()) {
      if (key.startsWith(this.getKeyString(prefix))) {
        entries.push({ key: key.split("::"), value });
      }
    }
    return entries;
  }

  async stream(prefix: StorageKey, cb: (entry: Entry) => Promise<void> | void): Promise<any> {
    (await this.list(prefix)).forEach(cb);
  }

  async streamBatch(prefix: StorageKey, cb: (batch: Entry[]) => Promise<void> | void, maxBatchSize: number): Promise<any> {
    const all = await this.list(prefix);
    cb(all);
  }

  async deleteByPrefix(prefix: StorageKey): Promise<void> {
    for (const key of this.store.keys()) {
      if (key.startsWith(this.getKeyString(prefix))) {
        this.store.delete(key);
      }
    }
  }

  async size(prefix: StorageKey): Promise<number> {
    let count = 0;
    for (const key of this.store.keys()) {
      if (key.startsWith(this.getKeyString(prefix))) {
        count++;
      }
    }
    return count;
  }

  private getKeyString(key: StorageKey): string {
    return Array.isArray(key) ? key.join("::") : key;
  }
}