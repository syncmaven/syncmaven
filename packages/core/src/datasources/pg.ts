import { Client, FieldDef, type QueryResult } from "pg";
import { maskPassword } from "../lib/util";
import Cursor from "pg-cursor";
import { ModelDefinition } from "../types/objects";
import { DataSource, genericToQueryParameter, SQLValue, StreamingHandler } from "./index";
import { ColumnType, GenericColumnType } from "./types";
import { ExpressionValue } from "node-sql-parser";

async function getDataTypesTable(client: Client): Promise<Record<number, string>> {
  return (await client.query("SELECT oid, typname FROM pg_type")).rows.reduce((acc, row) => {
    acc[row.oid] = row.typname;
    return acc;
  }, {});
}

function getGenericType(typeName: string): GenericColumnType {
  if (typeName.toLowerCase() === "text") {
    return "string";
  } else if (typeName.toLowerCase().startsWith("int")) {
    return "integer";
  } else if (typeName.toLowerCase().startsWith("bool")) {
    return "boolean";
  } else if (typeName.toLowerCase().startsWith("timestamp")) {
    return "date";
  } else if (typeName.toLowerCase().startsWith("float")) {
    return "float";
  } else {
    return "string";
  }
}

export async function newPostgresDatasource(modelDefinition: ModelDefinition): Promise<DataSource> {
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
  console.debug(`Connecting to database ${maskPassword(dsn)}.`);

  await client.connect();

  console.debug(`Connected to database ${maskPassword(dsn)}.`);
  const dataTypes: Record<number, string> = await getDataTypesTable(client);

  const id = modelDefinition.id || modelDefinition.name || "postgres";

  function parseType(f: FieldDef): ColumnType {
    const typeName = dataTypes[f.dataTypeID];
    if (!typeName) {
      throw new Error(`Column ${f.name} has unknown data type ${f.dataTypeID}`);
    }
    return {
      nativeType: typeName,
      genericType: getGenericType(typeName),
    };
  }

  return {
    type: () => "postgres",
    id: () => id,
    toQueryParameter(paramVal: SQLValue): ExpressionValue {
      return genericToQueryParameter(paramVal, "TIMESTAMP WITH TIME ZONE");
    },
    executeQuery: async (param: { handler: StreamingHandler; query: string }) => {
      console.info(`[${id}] Executing query: ${param.query}`);

      const cursor = client.query(new Cursor(param.query));
      let cursorBatchSize = 100;
      let result = await read(cursor, cursorBatchSize);
      await param.handler.header({
        columns: result.fields.map(f => ({ name: f.name, type: parseType(f) })),
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
