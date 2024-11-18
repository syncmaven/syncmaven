import { z } from "zod";
import {
  BaseOutputStream,
  BatchingOutputStream,
  DestinationProvider,
  OutputStreamConfiguration,
  stdProtocol,
} from "@syncmaven/node-cdk";
import { ExecutionContext } from "@syncmaven/protocol";

export const HttpCredentials = z.object({
  url: z.string().describe("URL of HTTP endpoint"),
  method: z
    .enum(["GET", "POST", "PUT", "DELETE"])
    .default("POST")
    .optional()
    .describe("HTTP method. Can be GET, POST, PUT, DELETE"),
  headers: z.array(z.string()).optional().describe("List of headers in format `key: value`"),
  format: z
    .enum(["ndjson", "json", "array"])
    .default("json")
    .optional()
    .describe(
      "Payload format. Can be 'ndjson', 'array' - array of JSON objects, 'json' - single JSON object (can nest array of JSON objects with help of 'template')"
    ),
  body: z
    .union([z.string(), z.object({}).passthrough()])
    .optional()
    .describe(
      'Template for request body in \'json\' format. Must be a valid JSON string. Support the following macros: "{{ result.rows }}","{{ result.row }}","{{ result.length }}" or any "{{ env.VAR }}"'
    ),
  batchSize: z
    .number()
    .default(1)
    .optional()
    .describe("Batch size for 'array', 'ndjson' or 'json' with nested \"{{ result.rows }}\" formats"),
  timeout: z.number().default(10000).optional().describe("Request timeout in milliseconds. Default is 10000"),
});

export type HttpCredentials = z.infer<typeof HttpCredentials>;

const HttpRow = z.object({}).passthrough();

type HttpRow = z.infer<typeof HttpRow>;

function processTemplate(template: any, row?: HttpRow, rows?: HttpRow[]) {
  if (typeof template !== "object") {
    return;
  }
  if (Array.isArray(template)) {
    for (const value of template) {
      processTemplate(value, row, rows);
    }
    return;
  }
  for (const [key, value] of Object.entries(template)) {
    // check if value is a macro
    if (typeof value === "string") {
      if (value === "[RESULT_ROWS]") {
        if (rows) {
          template[key] = rows;
        } else {
          template[key] = [row];
        }
      } else if (value === "[RESULT_ROW]") {
        if (rows) {
          throw new Error(
            "Array provided for 'result.row' macro. 'result.row' macro may be used only with batch size = 1"
          );
        }
        template[key] = row;
      } else if (value === "[RESULT_LENGTH]") {
        if (rows) {
          template[key] = rows.length;
        } else {
          template[key] = row ? 1 : 0;
        }
      }
    } else if (typeof value === "object") {
      processTemplate(value, row, rows);
    }
  }
}

class HttpSingleStream extends BaseOutputStream<HttpRow, HttpCredentials> {
  constructor(config: OutputStreamConfiguration<HttpCredentials>, ctx: ExecutionContext) {
    super(config, ctx);
  }

  async init(ctx: ExecutionContext) {
    return this;
  }

  private preparePayload(row: HttpRow) {
    const config = this.config.credentials;
    switch (config.format || "json") {
      case "json":
        let template: any = {};
        if (typeof config.body === "string") {
          template = JSON.parse(config.body);
        } else {
          template = config.body;
        }
        processTemplate(template, row);
        return JSON.stringify(template);
      case "array":
        return JSON.stringify(row);
      case "ndjson":
        return JSON.stringify(row) + "\n";
      default:
        throw new Error(`Unsupported format: ${config.format}. Supported formats: 'json', 'array', 'ndjson'`);
    }
  }

  public async handleRow(row: HttpRow, ctx: ExecutionContext) {
    const url = this.config.credentials.url;
    const method = this.config.credentials.method || "POST";
    const headers = this.config.credentials.headers || [];
    const body = this.preparePayload(row);
    const timeout = this.config.credentials.timeout || 10000;
    const abortController = new AbortController();
    const timeoutId = setTimeout(() => abortController.abort(), timeout);
    try {
      const response = await fetch(url, {
        method,
        headers: {
          "Content-Type": this.config.credentials.format === "ndjson" ? "application/x-ndjson" : "application/json",
          ...Object.fromEntries(headers.map(h => h.split(":").map(s => s.trim()))),
        },
        body,
        signal: abortController.signal,
      });
      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
      }
      console.log(`Row sent. Response:`, JSON.stringify(await response.json(), null, 2));
    } catch (e: any) {
      if (e.name === "AbortError") {
        throw new Error("Request timeout");
      }
      throw e;
    } finally {
      clearTimeout(timeoutId);
    }
  }
}

class HttpBatchStream extends BatchingOutputStream<HttpRow, HttpCredentials> {
  constructor(config: OutputStreamConfiguration<HttpCredentials>, ctx: ExecutionContext) {
    super(config, ctx, config.credentials.batchSize);
  }

  async init(ctx: ExecutionContext) {
    return this;
  }

  private preparePayload(rows: HttpRow[]) {
    const config = this.config.credentials;
    switch (config.format || "json") {
      case "json":
        let template: any = {};
        if (typeof config.body === "string") {
          template = JSON.parse(config.body);
        } else {
          template = config.body;
        }
        processTemplate(template, undefined, rows);
        return JSON.stringify(template);
      case "array":
        return JSON.stringify(rows);
      case "ndjson":
        return rows.map(r => JSON.stringify(r)).join("\n");
      default:
        throw new Error(`Unsupported format: ${config.format}. Supported formats: 'json', 'array', 'ndjson'`);
    }
  }

  public async processBatch(rows: HttpRow[], ctx: ExecutionContext) {
    const url = this.config.credentials.url;
    const method = this.config.credentials.method || "POST";
    const headers = this.config.credentials.headers || [];
    const body = this.preparePayload(rows);
    const timeout = this.config.credentials.timeout || 10000;
    const abortController = new AbortController();
    const timeoutId = setTimeout(() => abortController.abort(), timeout);
    try {
      const response = await fetch(url, {
        method,
        headers: {
          "Content-Type": this.config.credentials.format === "ndjson" ? "application/x-ndjson" : "application/json",
          ...Object.fromEntries(headers.map(h => h.split(":").map(s => s.trim()))),
        },
        body,
        signal: abortController.signal,
      });
      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
      }
      console.log(`Sent ${rows.length} rows. Response:`, JSON.stringify(await response.json(), null, 2));
    } catch (e: any) {
      if (e.name === "AbortError") {
        throw new Error("Request timeout");
      }
      throw e;
    } finally {
      clearTimeout(timeoutId);
    }
  }
}

export const httpProvider: DestinationProvider<HttpCredentials> = {
  name: "http",
  credentialsType: HttpCredentials,
  streams: [
    {
      name: "default",
      rowType: HttpRow,
      createOutputStream: (config, ctx) => {
        if (config.credentials.batchSize && config.credentials.batchSize > 1) {
          return new HttpBatchStream(config, ctx).init(ctx);
        } else {
          return new HttpSingleStream(config, ctx).init(ctx);
        }
      },
    },
  ],
  defaultStream: "default",
};

stdProtocol(httpProvider);
