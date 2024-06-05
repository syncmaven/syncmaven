import { BigQuery } from "@google-cloud/bigquery";
import { ModelDefinition } from "../types/objects";
import { DataSource, genericToQueryParameter, SQLValue, StreamingHandler } from "./index";
import { z } from "zod";
import { ExpressionValue } from "node-sql-parser";
import { GenericColumnType } from "./types";

export const BigQueryCredentials = z.object({
  projectId: z.string(),
  location: z.string().optional(),
  key: z.union([z.string(), z.record(z.any())]),
});

export async function newBigQueryDatasource(modelDefinition: ModelDefinition): Promise<DataSource> {
  const ds = modelDefinition.datasource;
  if (typeof ds !== "object") {
    throw new Error(`Invalid datasource: ${ds}`);
  }
  if (ds.type !== "bigquery") {
    throw new Error(`Invalid bigquery datasource: ${JSON.stringify(ds)}`);
  }
  const cred = BigQueryCredentials.parse(ds.credentials);
  const location = cred.location || "US";
  const bigQuery = new BigQuery({
    credentials: typeof cred.key === "string" ? JSON.parse(ds.credentials.key) : cred.key,
    projectId: cred.projectId,
  });

  console.debug(`Connecting`);

  const id = modelDefinition.id || modelDefinition.name || "bigquery";

  return {
    type: () => "bigquery",
    id: () => id,
    toQueryParameter(paramVal: SQLValue): ExpressionValue {
      return genericToQueryParameter(paramVal, "TIMESTAMP");
    },
    executeQuery: async (param: { handler: StreamingHandler; query: string }) => {
      console.info(`[${id}] Executing query: ${param.query}`);
      const query = param.query;
      const handler = param.handler;
      const options = {
        query,
        location,
      };
      const [job] = await bigQuery.createQueryJob(options);
      let pageToken: string | undefined = undefined;
      do {
        const jobResult = await job.getQueryResults({
          pageToken,
          autoPaginate: false,
        });
        const rows = jobResult[0];
        const meta = jobResult[2];
        if (!pageToken) {
          //send header only on the first page
          await handler.header({
            columns:
              meta?.schema?.fields?.map(f => ({
                name: f.name || "",
                type: {
                  nativeType: f.type || "",
                  genericType: getGenericType(f.type),
                },
              })) || [],
          });
        }
        for (const row of rows) {
          Object.entries(row).forEach(([k, v]) => {
            const className = v?.constructor?.name;
            if (className === "BigQueryDate" || className === "BigQueryTimestamp" || className === "BigQueryTime") {
              row[k] = new Date((v as any).value);
            }
          });
          await handler.row(row);
        }
        pageToken = meta.pageToken;
        if (pageToken) {
          console.debug(`[${id}] Fetching next page.`);
        }
      } while (pageToken);
      await handler.finalize();
    },
    close: async () => {},
  };
}

function getGenericType(type: string): GenericColumnType {
  switch (type) {
    case "STRING":
      return "string";
    case "INT64":
      return "integer";
    case "NUMERIC":
    case "FLOAT64":
      return "float";
    case "BOOL":
      return "boolean";
    case "DATE":
    case "DATETIME":
    case "TIMESTAMP":
      return "date";
    default:
      return "string";
  }
}
