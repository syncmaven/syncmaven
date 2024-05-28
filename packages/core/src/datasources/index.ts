import { ModelDefinition } from "../types/objects";
import { newPostgresDatasource } from "./pg";
import { newBigQueryDatasource } from "./bigquery";
import { ColumnType } from "./types";

export type TableColumn = {
  name: string;
  type: ColumnType;
};

export type TableHeader = {
  columns: TableColumn[];
};

export type SQLValue = any;

export type StreamingHandler = {
  header: (header: TableHeader) => void | Promise<void>;
  row: (row: Record<string, SQLValue>) => void | Promise<void>;
  finalize: () => void | Promise<void>;
};

export interface DataSource {
  id(): string;
  type(): "postgres" | "bigquery";
  executeQuery(param: { handler: StreamingHandler; query: string }): Promise<void>;
  close(): Promise<void>;
}

export async function createDatasource(modelDefinition: ModelDefinition): Promise<DataSource> {
  if (typeof modelDefinition.datasource === "string") {
    const dsn = modelDefinition.datasource;
    const protocol = dsn.split("://")[0];
    switch (protocol) {
      case "postgresql":
        return newPostgresDatasource(modelDefinition);
      default:
        throw new Error(`Unsupported protocol: ${protocol}`);
    }
  } else if (typeof modelDefinition.datasource === "object" && !Array.isArray(modelDefinition.datasource)) {
    const { type } = modelDefinition.datasource;
    switch (type) {
      case "bigquery":
        return newBigQueryDatasource(modelDefinition);
      default:
        throw new Error(`Unsupported datasource type: ${type}`);
    }
  } else {
    throw new Error(`Invalid datasource type: ${modelDefinition.datasource}`);
  }
}
