import { ZodError, ZodIssue } from "zod";

function strinfigyZodIssue(e: ZodIssue) {
  if (e.code === "invalid_type") {
    return `${e.code} - expected ${e.expected} but got ${e.received}`;
  } else if (e.code === "unrecognized_keys") {
    return `${e.code} - found keys ${e.keys.join(", ")}`;
  }

  return `${JSON.stringify(e)}`;
}

export function stringifyZodError(error: any): string {
  if (!(error instanceof ZodError)) {
    return error?.message || "Unknown error";
  }
  return `${error.errors.length} errors found: ${error.errors.map((e, idx) => `#${idx + 1} - at path \$.${e.path.join(".")} - ${strinfigyZodIssue(e)}`).join(", ")}`;
}

export class ZodErrorWrapper extends Error {
  public readonly zodErrors: ZodIssue[];
  constructor(error: ZodError) {
    super(stringifyZodError(error));
    this.zodErrors = error.errors;
  }
}

export function wrap(zodError: any): Error {
  if (zodError instanceof ZodError) {
    return new ZodErrorWrapper(zodError);
  } else {
    return zodError;
  }
}
