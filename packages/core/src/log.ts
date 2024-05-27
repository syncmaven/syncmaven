import { isTruish } from "./lib/util";

type LoggingMethod = (message?: any, ...optionalParams: any[]) => void;

const severityLevels = ["INFO", "WARN", "ERROR", "DEBUG"] as const;
type SeverityLevel = (typeof severityLevels)[number];
const maxSeverityLevelLength = Math.max(...severityLevels.map(s => s.length));

const RESET = "\x1b[0m";
let debugLoginEnabled = true;

const rewrites: Record<SeverityLevel, SeverityLevel | undefined> = {
  INFO: undefined,
  WARN: undefined,
  ERROR: undefined,
  DEBUG: undefined,
};

export const fmt = {
  bold: (x: any) => wrap("\x1b[1m", x),
  italic: (x: any) => wrap("\x1b[3m", x),
  underline: (x: any) => wrap("\x1b[4m", x),
  red: (x: any) => wrap("\x1b[31m", x),
  green: (x: any) => wrap("\x1b[32m", x),
  yellow: (x: any) => wrap("\x1b[33m", x),
  blue: (x: any) => wrap("\x1b[34m", x),
  cyan: (x: any) => wrap("\x1b[36m", x),
  gray: (x: any) => wrap("\x1b[90m", x),
  brown: (x: any) => wrap("\x1b[33m", x),
  violet: (x: any) => wrap("\x1b[35m", x),
} as const;

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

const colors: Record<SeverityLevel, keyof typeof fmt> = {
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
    process.stdout.write(fmt[color](content));
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
