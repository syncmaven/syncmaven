import { ModelDefinition } from "../types/objects";
import { newPostgresDatasource } from "./pg";
import { newBigQueryDatasource } from "./bigquery";
import { ColumnType } from "./types";
import { newSnowflakeDatasource } from "./snowlake";
import { ExpressionValue } from "node-sql-parser";

export type TableColumn = {
  name: string;
  type: ColumnType;
};

export type TableHeader = {
  columns: TableColumn[];
};

export type SQLValue = any;

export type StreamingHandler = {
  header?: (header: TableHeader) => void | Promise<void>;
  /**
   * If function returns true, the streaming will stop
   */
  row: (row: Record<string, SQLValue>) => (void | true) | Promise<void | true>;
  finalize?: () => void | Promise<void>;
};

export interface DataSource {
  id(): string;
  type(): "postgres" | "bigquery" | "snowflake";
  toQueryParameter(value: SQLValue): ExpressionValue;
  executeQuery(param: { handler: StreamingHandler; query: string }): Promise<void>;
  close(): Promise<void>;
}

export type QueryParamFunction = {
  (paramVal: SQLValue): ExpressionValue;
};

export function genericToQueryParameter(paramVal: SQLValue, timestampType: string): ExpressionValue {
  let node = {} as any;
  if (typeof paramVal === "string") {
    node.type = "single_quote_string";
    node.value = paramVal;
  } else if (typeof paramVal === "number") {
    node.type = "number";
    node.value = paramVal;
  } else if (paramVal instanceof Date) {
    node.type = "cast";
    node.keyword = "cast";
    node.expr = {
      type: "single_quote_string",
      value: paramVal.toISOString(),
    };
    node.symbol = "as";
    node.target = {
      dataType: timestampType,
    };
    delete node.value;
  } else if (paramVal === null) {
    node.type = "null";
    node.value = null;
  } else {
    throw new Error(`Unsupported parameter type: ${typeof paramVal}`);
  }
  return node as ExpressionValue;
}

export async function createDatasource(modelDefinition: ModelDefinition): Promise<DataSource> {
  if (typeof modelDefinition.datasource === "string") {
    const dsn = modelDefinition.datasource;
    const protocol = dsn.split("://")[0];
    switch (protocol) {
      case "postgresql":
      case "postgres":
        return newPostgresDatasource(modelDefinition);
      default:
        throw new Error(`Unsupported protocol: ${protocol}`);
    }
  } else if (typeof modelDefinition.datasource === "object" && !Array.isArray(modelDefinition.datasource)) {
    const { type } = modelDefinition.datasource;
    switch (type) {
      case "bigquery":
        return newBigQueryDatasource(modelDefinition);
      case "snowflake":
        return newSnowflakeDatasource(modelDefinition);
      default:
        throw new Error(`Unsupported datasource type: ${type}`);
    }
  } else {
    throw new Error(`Invalid datasource type: ${modelDefinition.datasource}`);
  }
}
