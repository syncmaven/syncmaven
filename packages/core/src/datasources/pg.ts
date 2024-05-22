import { Client, type QueryResult } from "pg";
import { maskPassword } from "../lib/util";
import Cursor from "pg-cursor";
import { ModelDefinition } from "../types/objects";
import { DataSource, StreamingHandler } from "./index";

export async function newPostgresDatasource(
  modelDefinition: ModelDefinition,
): Promise<DataSource> {
  const dsn = modelDefinition.datasource;
  if (typeof dsn !== "string") {
    throw new Error(`Invalid datasource: ${dsn}`);
  }
  if (!dsn.startsWith("postgresql://")) {
    throw new Error(`Invalid postgresql datasource: ${dsn}`);
  }
  const client = new Client({
    connectionString: dsn,
  });
  console.debug(`Connecting`);

  await client.connect();

  console.debug(`Connected to database ${maskPassword(dsn)}.`);

  const id = modelDefinition.id || modelDefinition.name || "postgres";

  return {
    id: () => id,
    executeQuery: async (param: {
      handler: StreamingHandler;
      query: string;
    }) => {
      console.debug(`[${id}] Executing query: ${param.query}`);

      const cursor = client.query(new Cursor(param.query));
      let cursorBatchSize = 100;
      let result = await read(cursor, cursorBatchSize);
      await param.handler.header({
        columns: result.fields.map((f) => ({ name: f.name, type: f.format })),
      });
      while (result?.rows && result.rows.length > 0) {
        for (const row of result.rows) {
          await param.handler.row(row);
        }
        result = await read(cursor, cursorBatchSize);
      }
      await param.handler.finalize();
    },
    close: async () => {
      await client.end();
    },
  };
}

async function read(cursor: Cursor, batchSize: number): Promise<QueryResult> {
  return new Promise((resolve, reject) => {
    cursor.read(batchSize, (err, _, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}
