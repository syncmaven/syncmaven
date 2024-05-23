import { AST, Parser, Select } from "node-sql-parser";
import { cloneDeep } from "lodash";

function treeWalker(ast: AST, cb: {
  (node: AST): void;
}) {
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

type SqlDialect = "postgres" | "bigquery";

export class SqlQuery {
  private query: string;
  private select: Select;
  private namedParams: string[] = [];
  private dialect: SqlDialect;

  constructor(query: string, dialect: SqlDialect) {
    this.query = query;
    const parser = new Parser();
    let ast: AST[] | AST;
    this.dialect = dialect;
    try {
      ast = parser.astify(query, {
        //see dialects here https://github.com/taozhi8833998/node-sql-parser?tab=readme-ov-file#supported-database-sql-syntax
        database: SqlQuery.dialectToDatabaseType(dialect),
      });
    } catch (e: any) {
      throw new Error(`SQL query contains syntax error: ${e?.message}`);
    }
    if (Array.isArray(ast)) {
      throw new Error(`SQL query contains multiple statements`);
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

  private static dialectToDatabaseType(dialect: "postgres" | "bigquery") {
    return dialect === "postgres" ? "PostgresQL" : "PostgresQL";
  }

  public getUsedNamedParameters(): string[] {
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
            "type": "single_quote_string",
            value: paramVal.toISOString(),
          };
          node.as = null;
          node.symbol = "::";
          node.target = {
            dataType: "TIMESTAMP WITH TIMEZONE",
          };
          node.arrows = [];
          node.properties = [];
          delete node.value;
        } else if (paramVal === null) {
          node.type = "null";
          node.value = null;
        } else {
          throw new Error(`Unsupported '${paramName}' parameter type: ${typeof paramVal}`);
        }
      }
    });
    return new Parser().sqlify(ast, {
      database: SqlQuery.dialectToDatabaseType(this.dialect),
    });
  }
}