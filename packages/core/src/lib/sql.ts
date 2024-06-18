import { AST, Parser, Select } from "node-sql-parser";
import { cloneDeep } from "lodash";
import { DataSource, QueryParamFunction } from "../datasources";

type SqlDialect = "postgres" | "bigquery" | "snowflake";

const dialectsMapping = {
  postgres: "PostgresQL",
  bigquery: "BigQuery",
  snowflake: "Snowflake",
};

export class SqlQuery {
  private query: string;
  private select: Select;
  private namedParams: string[] = [];
  private queryParamFunction: QueryParamFunction;
  private dialect: SqlDialect;

  constructor(query: string, dialect: SqlDialect, queryParamFunction: QueryParamFunction) {
    this.query = query;
    this.queryParamFunction = queryParamFunction;
    const parser = new Parser();
    let ast: AST[] | AST;
    this.dialect = dialect;
    try {
      ast = parser.astify(query, {
        //see dialects here https://github.com/taozhi8833998/node-sql-parser?tab=readme-ov-file#supported-database-sql-syntax
        database: SqlQuery.dialectToDatabaseType(this.dialect),
      });
    } catch (e: any) {
      throw new Error(`SQL query contains syntax error: ${e?.message}`);
    }
    if (Array.isArray(ast)) {
      if (ast.length > 1) {
        throw new Error(`SQL query contains multiple (${ast.length}) statements`);
      }
      ast = ast[0];
    }
    if (ast.type !== "select") {
      throw new Error(`SQL query must be a SELECT statement`);
    }
    this.select = ast;
    const params: Set<string> = new Set();
    treeWalker(this.select, (node: any) => {
      if (node.type === "param") {
        params.add(node.value);
      }
    });
    this.namedParams = [...params];
  }

  private static dialectToDatabaseType(dialect: SqlDialect) {
    return dialectsMapping[dialect] || "PostgresQL";
  }

  public getNamedParameters(): string[] {
    return this.namedParams;
  }

  public compile(namedParams: Record<string, any>): string {
    const ast = cloneDeep(this.select);
    treeWalker(ast, (node: any) => {
      if (node.type === "param") {
        const paramName = node.value;
        const paramVal = namedParams[paramName];
        if (paramVal === undefined) {
          throw new Error(`Missing parameter value for :${paramName}`);
        }
        try {
          const newNode = this.queryParamFunction(paramVal);
          for (const member in node) delete node[member];
          Object.entries(newNode).forEach(([key, value]) => {
            node[key] = value;
          });
        } catch (e: any) {
          throw new Error(`Error setting parameter ${paramName}: ${e?.message}`);
        }
      }
    });
    return new Parser().sqlify(ast, {
      database: SqlQuery.dialectToDatabaseType(this.dialect),
    });
  }
}

function treeWalker(
  ast: AST,
  cb: {
    (node: AST): void;
  }
) {
  const visitor = (node: any) => {
    if (!node) {
      return;
    }
    if (typeof node?.type === "string") {
      cb(node);
    } else if (Array.isArray(node)) {
      node.forEach(visitor);
    }
    if (node && typeof node === "object") {
      Object.values(node).filter(Boolean).forEach(visitor);
    }
  };
  visitor(ast);
}
