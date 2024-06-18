import snowflake from "snowflake-sdk";
import { ModelDefinition } from "../types/objects";
import { DataSource, genericToQueryParameter, SQLValue, StreamingHandler } from "./index";
import { z } from "zod";
import { GenericColumnType } from "./types";
import { ExpressionValue } from "node-sql-parser";

export const SnowflakeCredentials = z.object({
  account: z.string(),
  database: z.string(),
  warehouse: z.string(),
  username: z.string(),
  password: z.string(),
  schema: z.string().optional().default("PUBLIC"),
  application: z.string().optional().default("syncmaven"),
});

export async function connectSnowflake(cred: z.infer<typeof SnowflakeCredentials>): Promise<snowflake.Connection> {
  const connection = snowflake.createConnection(cred);
  await new Promise((resolve, reject) => {
    connection.connect(function (err, conn) {
      if (err) {
        reject(err);
      } else {
        resolve(conn);
      }
    });
  });
  return connection;
}

export async function snowflakeQuery(connection: snowflake.Connection, query: string) {
  await new Promise<any[] | undefined>((resolve, reject) => {
    connection.execute({
      sqlText: query,
      complete: async function (err, stmt, rows) {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      },
    });
  });
}

export async function newSnowflakeDatasource(modelDefinition: ModelDefinition): Promise<DataSource> {
  const ds = modelDefinition.datasource;
  if (typeof ds !== "object") {
    throw new Error(`Invalid datasource: ${ds}`);
  }
  if (ds.type !== "snowflake") {
    throw new Error(`Invalid snowflake datasource: ${JSON.stringify(ds)}`);
  }
  const cred = SnowflakeCredentials.parse(ds.credentials);

  const connection = await connectSnowflake(cred);

  const id = modelDefinition.id || modelDefinition.name || "snowflake";

  return {
    type: () => "snowflake",
    id: () => id,
    toQueryParameter(paramVal: SQLValue): ExpressionValue {
      return genericToQueryParameter(paramVal, "TIMESTAMP_TZ");
    },
    executeQuery: async (param: { handler: StreamingHandler; query: string }) => {
      console.info(`[${id}] Executing query: ${param.query}`);
      const query = param.query;
      const handler = param.handler;

      await new Promise<void>((resolve, reject) => {
        connection.execute({
          sqlText: query,
          streamResult: true,
          complete: async function (err, stmt) {
            if (err) {
              reject(err);
              return;
            }
            if (handler.header) {
              await handler.header({
                columns:
                  stmt.getColumns().map(col => ({
                    name: col.getName().toLowerCase(),
                    type: {
                      nativeType: col.getType(),
                      genericType: getGenericType(col),
                    },
                  })) || [],
              });
            }

            const stream = stmt.streamRows();
            // Read data from the stream when it is available
            stream
              .on("readable", async function (row: any) {
                while ((row = stream.read()) !== null) {
                  row = Object.fromEntries(
                    Object.entries(row).map(([k, v]) => {
                      const className = v?.constructor?.name;
                      if (className === "Date") {
                        v = new Date((v as any).getTime());
                        (v as any).setMilliseconds((v as any).getMilliseconds());
                      }
                      return [k.toLowerCase(), v];
                    })
                  );
                  handler.row(row);
                }
              })
              .on("end", async function () {
                if (handler.finalize) {
                  await handler.finalize();
                }
                resolve();
              })
              .on("error", function (err) {
                reject(err);
              });
          },
        });
      });
    },
    close: async () => {
      connection.destroy(function (err, conn) {
        if (err) {
          console.debug("Unable to disconnect: " + err.message);
        } else {
          console.debug("Disconnected connection with id: " + connection.getId());
        }
      });
    },
  };
}

function getGenericType(col: snowflake.Column): GenericColumnType {
  if (col.isString()) {
    return "string";
  } else if (
    col.isDate() ||
    col.isTime() ||
    col.isTimestamp() ||
    col.isTimestampTz() ||
    col.isTimestampLtz() ||
    col.isTimestampNtz()
  ) {
    return "date";
  } else if (col.isNumber()) {
    if (col.getScale() > 0) {
      return "float";
    } else {
      return "integer";
    }
  } else if (col.isBoolean()) {
    return "boolean";
  } else {
    return "string";
  }
}
