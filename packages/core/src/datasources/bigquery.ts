import { BigQuery } from "@google-cloud/bigquery";
import { ModelDefinition } from "../types/objects";
import { DataSource, StreamingHandler } from "./index";

export async function newBigQueryDatasource(modelDefinition: ModelDefinition): Promise<DataSource> {
  const ds = modelDefinition.datasource;
  if (typeof ds !== "object") {
    throw new Error(`Invalid datasource: ${ds}`);
  }
  if (ds.type !== "bigquery") {
    throw new Error(`Invalid bigquery datasource: ${JSON.stringify(ds)}`);
  }
  const location = ds.credentials?.location || "US";
  const bigQuery = new BigQuery({
    credentials: typeof ds.credentials?.key === "string" ? JSON.parse(ds.credentials.key) : ds.credentials?.key,
    projectId: ds.credentials?.projectId,
  });

  console.debug(`Connecting`);

  const id = modelDefinition.id || modelDefinition.name || "postgres";

  return {
    type: () => "bigquery",
    id: () => id,
    executeQuery: async (param: { handler: StreamingHandler; query: string }) => {
      console.debug(`[${id}] Executing query: ${param.query}`);
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
                type: f.type || "",
              })) || [],
          });
        }
        for (const row of rows) {
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
