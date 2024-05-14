import { Client, type QueryResult } from "pg";
import { maskPassword } from "./util";
import Cursor from "pg-cursor";

type TableColumn = {
  name: string;
  type: string;
};

type TableHeader = {
  columns: TableColumn[];
};

type SQLValue = any;

type StreamingHandler = {
  header: (header: TableHeader) => void | Promise<void>;
  row: (row: Record<string, SQLValue>) => void | Promise<void>;
  finalize: () => void | Promise<void>;
};

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

export async function executeQuery(param: {
  handler: StreamingHandler;
  datasource: string;
  query: string;
}): Promise<void> {
  const [protocol] = param.datasource.split("://");
  if (protocol !== "postgresql") {
    throw new Error(`Unsupported protocol ${protocol} in datasource ${param.datasource}`);
  }

  const client = new Client({
    connectionString: param.datasource,
  });
  console.debug(`Connecting`);

  await client.connect();
  console.debug(`Connected to database ${maskPassword(param.datasource)}. Executing query: ${param.query}`);
  const cursor = client.query(new Cursor(param.query));
  let cursorBatchSize = 100;
  let result = await read(cursor, cursorBatchSize);
  await param.handler.header({ columns: result.fields.map(f => ({ name: f.name, type: f.format })) });
  while (result?.rows && result.rows.length > 0) {
    for (const row of result.rows) {
      await param.handler.row(row);
    }
    result = await read(cursor, cursorBatchSize);
  }
  await param.handler.finalize();
}
