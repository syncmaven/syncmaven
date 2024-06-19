import cols from "picocolors";
import { isTruish } from "./lib/util";

type LoggingMethod = (message?: any, ...optionalParams: any[]) => void;

const severityLevels = ["INFO", "WARN", "ERROR", "DEBUG"] as const;
type SeverityLevel = (typeof severityLevels)[number];
const maxSeverityLevelLength = Math.max(...severityLevels.map(s => s.length));

const RESET = "\x1b[0m";
let debugLoginEnabled = isTruish(process.env.DEBUG);
if (debugLoginEnabled) {
  console.debug("Debug logging enabled via DEBUG env var");
}

const rewrites: Record<SeverityLevel, SeverityLevel | undefined> = {
  INFO: undefined,
  WARN: undefined,
  ERROR: undefined,
  DEBUG: undefined,
};

export const fmt = cols;

function wrap(fmt: string, str: any): string {
  return (
    (str + "")
      .split(RESET)
      .map(s => `${fmt}${s}`)
      .join(RESET) + RESET
  );
}

function intercept(stream: typeof process.stdout | typeof process.stderr, cb: () => void): string {
  const originalWrite = stream.write.bind(process.stdout);
  try {
    const output: string[] = [];
    stream.write = chunk => {
      const str = chunk.toString();
      output.push(str);
      return true;
    };
    cb();
    return output.join("");
  } finally {
    stream.write = originalWrite;
  }
}

const colors: Record<SeverityLevel, keyof typeof cols> = {
  INFO: "green",
  WARN: "yellow",
  ERROR: "red",
  DEBUG: "blue",
};

function prefixConsoleLoggingMethod(method: LoggingMethod, _severity: SeverityLevel): LoggingMethod {
  return (message?: any, ...optionalParams: any[]) => {
    const severity = rewrites[_severity] || _severity;
    const timestamp = new Date().toISOString();
    const color = colors[severity];
    if (severity === "DEBUG" && !debugLoginEnabled) {
      return;
    }
    const content = intercept(severity === "DEBUG" || severity === "INFO" ? process.stdout : process.stderr, () => {
      method(`${timestamp} [${severity.padEnd(maxSeverityLevelLength)}]`, message, ...optionalParams);
    });
    process.stdout.write((cols[color] as any)(content));
  };
}

export function rewriteSeverityLevel(severity: SeverityLevel, newSeverity: SeverityLevel) {
  rewrites[severity] = newSeverity;
}
export function setEnabledDebugLogging(enabled: boolean) {
  debugLoginEnabled = enabled;
}
export function initializeConsoleLogging(c: typeof console = console) {
  if ((c as any).__pathched) {
    return;
  }

  c.log = prefixConsoleLoggingMethod(c.log, "INFO");
  c.info = prefixConsoleLoggingMethod(c.info, "INFO");
  c.warn = prefixConsoleLoggingMethod(c.warn, "WARN");
  c.error = prefixConsoleLoggingMethod(c.error, "ERROR");
  c.debug = prefixConsoleLoggingMethod(c.debug, "DEBUG");

  (c as any).__pathched = true;
}

/**
 * Inlike console.* methods, this function should be used to output messages
 * in a response to CLI commands
 * @param message
 */
export function out(message?: any) {
  if (message === undefined) {
    process.stdout.write("\n");
    return;
  }
  if (Array.isArray(message)) {
    message = message.join("\n");
  }
  process.stdout.write(message + "\n");
}

export function startLoading(proc: string): () => void {
  const symbols = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
  if (process.stdout.isTTY) {
    let cnt = 0;
    const interval = setInterval(() => {
      process.stdout.cursorTo(0);
      process.stdout.write(fmt.green(symbols[cnt++ % symbols.length]) + " " + proc);
    }, 100);
    return () => {
      clearInterval(interval);
      process.stdout.write("\x1b[0G");
      process.stdout.write("\x1b[2K");
      process.stdout.write("\n");
      process.stdout.write(fmt.green("✔") + " " + proc + "\n");
    };
  } else {
    out(`${proc.trim()}...`);
    return () => {};
  }
}
