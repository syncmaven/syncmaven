import { ZodError } from "zod";
import { stringifyZodError } from "./zod";

const Ajv = require("ajv");
const addFormats = require("ajv-formats");

type ParseResult<T = any> =
  | { success: true; data: T; error?: never }
  | { success: false; data?: never; error: Error };
export type SchemaBasedParser<T = any> = {
  parse: (data: any) => T;
  safeParse: (data: any) => ParseResult<T>;
  /**
   * Serialize schema. May not be supported by all parsers
   */
  schema(): any;
};

function isZodSchema(obj: any): boolean {
  return obj && obj._def && obj._def.typeName;
}

function isJsonSchema(obj: any): boolean {
  return obj && obj.$schema;
}

export function stringifyParseError(error: any): string {
  if (error instanceof ZodError) {
    return stringifyZodError(error);
  } else {
    return error.message || "Unknown error";
  }
}

/**
 * Depending on the schema, creates either JSON schema or Zod schema parser
 *
 * Parser is more like a validator, it should throw an error if the data is invalid. Also
 * it may (or may not) remove non-schema keys from the data.
 * @param schema
 */
export function createParser<T>(schema: any): SchemaBasedParser<T> {
  if (isZodSchema(schema)) {
    return {
      ...schema,
      schema(): any {
        return { properties: schema._def.shape };
      },
    };
  } else if (isJsonSchema(schema)) {
    const ajv = new Ajv();
    addFormats(ajv);
    const validator = ajv.compile({ ...schema, additionalProperties: true });
    const safeParse: (data: any) => ParseResult = (data) => {
      // stringify-parse to convert dates to ISO strings
      const valid = validator(JSON.parse(JSON.stringify(data)));
      if (valid) {
        return { success: true, data: data as T };
      } else {
        return {
          success: false,
          error: new Error(ajv.errorsText(validator.errors)),
        };
      }
    };
    return {
      schema(): any {
        return schema;
      },
      parse: (data: any) => {
        const res = safeParse(data);
        if (!res.success) {
          throw res.error;
        } else {
          return res.data as T;
        }
      },
      safeParse,
    };
  } else {
    throw new Error("Unsupported schema type");
  }
}
