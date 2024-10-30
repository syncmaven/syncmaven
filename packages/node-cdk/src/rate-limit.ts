export class RateLimitError extends Error {
  private headers: Record<string, string>;
  private retryAfterMs: number | undefined;

  constructor(message, opts: { headers?: Record<string, string>; retryAfterMs?: number }) {
    super(message);
    this.headers = opts.headers || {};
    this.retryAfterMs = opts.retryAfterMs;
  }

  public getRetryAfterMs(): number | undefined {
    if (this.retryAfterMs) {
      return this.retryAfterMs;
    } else if (this.headers["retry-after"]) {
      return parseInt(this.headers["retry-after"]) * 1000;
    } else {
      return undefined;
    }
  }
}
