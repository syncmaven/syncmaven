export function tryJson(res: any) {
  if (typeof res === "string") {
    try {
      return JSON.parse(res);
    } catch (e) {
      return res;
    }
  }
  return res;
}

async function parseJsonResponse(result: Response, method: string, url: string) {
  const text = await result.text();
  try {
    return JSON.parse(text);
  } catch (e: any) {
    const message = `Error parsing JSON (len=${text.length}) from ${method} ${url} ${e?.message ? `: ${e.message}` : ""}`;
    throw new Error(`${message}`);
  }
}

export function urlWithQueryString(url: string, query: Record<string, any>, opts: { filterUndefined?: boolean } = {}) {
  return `${url}?${Object.entries(query)
    .filter(([, v]) => (opts.filterUndefined ? v !== undefined : true))
    .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
    .join("&")}`;
}

function notEmpty(param): boolean {
  return param !== undefined && param !== null && Object.keys(param).length > 0;
}

export type RpcParams<Query = Record<string, any>, Payload = any> = Omit<RequestInit, "method" | "body"> & {
  method?: string;
  body?: Payload;
  query?: Query;
};

export interface RpcFunc<Result = any, Query = Record<string, any>, Payload = any> {
  (url: string, params?: RpcParams<Query, Payload>): Promise<Result>;
}

function extractString(obj: any): string | undefined {
  return typeof obj === "string" ? obj : undefined;
}

export function createRpcClient<Result = any, Query = Record<string, any>, Payload = any>(
  params: Pick<RequestInit, "headers"> & { urlBase?: string }
): RpcFunc<Result, Query, Payload> {
  return async (url, { headers, ...rest } = {}) => {
    return rpc(params.urlBase ? `${params.urlBase}${url}` : url, {
      headers: { ...params.headers, ...headers },
      ...rest,
    } as any);
  };
}

function toRecord(headers: Headers) {
  const result: Record<string, string> = {};
  headers.forEach((value, key) => {
    result[key] = value;
  });
  return result;
}

export class RpcError extends Error {
  public readonly statusCode: number;
  public readonly headers: Record<string, string>;
  public readonly url: string;
  public response: any;

  constructor(
    message: string,
    opts: {
      url: string;
      statusCode: number;
      headers: Record<string, string>;
      response: any;
    }
  ) {
    super(message);
    this.headers = opts.headers;
    this.statusCode = opts.statusCode;
    this.url = opts.url;
    if (typeof opts.response === "string") {
      try {
        this.response = JSON.parse(opts.response);
      } catch (e) {
        this.response = opts.response;
      }
    } else {
      this.response = opts.response;
    }
  }
}

export const rpc: RpcFunc = async (url, { body, ...rest } = {}) => {
  const urlWithQuery = notEmpty(rest?.query) ? urlWithQueryString(url, rest?.query || {}) : url;

  const method = rest.method || (body ? "POST" : "GET");
  let result: Response;
  const requestParams = {
    method: method,
    body: body ? JSON.stringify(body) : undefined,
    ...rest,
  };
  const fetchImpl = fetch;
  try {
    result = await fetchImpl(urlWithQuery, requestParams);
  } catch (e: any) {
    throw new RpcError(`Error calling ${method} ${url}: ${e?.message ? `: ${e.message}` : ""}`, {
      statusCode: -1,
      url: urlWithQuery,
      headers: {},
      response: undefined,
    });
  }

  const getErrorText = async (result: Response) => {
    try {
      return await result.text();
    } catch (e) {
      return "Unknown error";
    }
  };

  if (!result.ok) {
    let errorText = await getErrorText(result);
    const errorJson = tryJson(errorText);
    const defaultErrorMessage = `Error ${result.status} on ${method} ${url}`;
    //console.error(defaultErrorMessage, errorJson);

    //Try to extract meaningful error message from response. We don't need to include a full message since it will be visible
    //in the logs. On the other hand, error message could be displayed in UI
    const errorMessage =
      extractString(errorJson.message) ||
      extractString(errorJson.error) ||
      extractString(errorJson.error?.error) ||
      `${result.status} ${result.statusText}`;
    throw new RpcError(errorMessage, {
      statusCode: result.status,
      headers: toRecord(result.headers),
      url: urlWithQuery,
      response: errorText,
    });
    // throw new ApiResponseError(errorMessage, typeof errorJson === "string" ? undefined : errorJson, {
    //   url: urlWithQuery,
    //   ...requestParams,
    //   body: body || undefined,
    // });
  }
  if ((result.headers.get("Content-Type") ?? "").startsWith("application/json")) {
    return await parseJsonResponse(result, method, url);
  } else if ((result.headers.get("Content-Type") ?? "").startsWith("application/x-ndjson")) {
    const text = await result.text();
    return text
      .split("\n")
      .filter(x => x)
      .map(x => JSON.parse(x));
  } else {
    return await result.text();
  }
};
