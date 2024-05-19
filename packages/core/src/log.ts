import { isTruish } from "./lib/util";

type LoggingMethod = (message?: any, ...optionalParams: any[]) => void;

const severityLevels = ["INFO", "WARN", "ERROR", "DEBUG"] as const;
type SeverityLevel = (typeof severityLevels)[number];
const maxSeverityLevelLength = Math.max(...severityLevels.map(s => s.length));

const RESET = "\x1b[0m";

const streamFormatStacks: Record<number, string[]> = {
  [process.stdout.fd]: [],
  [process.stderr.fd]: [],
};

export const fmt = {
  bold: (x: any) => wrap("\x1b[1m", x),
  italic: (x: any) => wrap("\x1b[3m", x),
  underline: (x: any) => wrap("\x1b[4m", x),
} as const;

function wrap(fmt: string, str: any): string {
  const stream = process.stdout;
  const stack = streamFormatStacks[stream.fd]!;
  return `${fmt}${str}\x1b[0m${stack.length}${stack.join("")}`;
}

function withFmt(fmt: string, cb: () => void) {
  const stream = process.stdout;
  const stack = streamFormatStacks[stream.fd]!;
  const symbols = [...stack];
  const supportsColor = stream.isTTY && !isTruish(process.env.DISABLE_TTY_COLOR);
  if (supportsColor) {
    stream.write(fmt);
    stack.push(fmt);
    try {
      cb();
    } finally {
      stream.write(RESET);
      stack.pop();
      symbols.forEach(s => stream.write(s));
    }
  } else {
    cb();
  }
}

const colors: Record<SeverityLevel, string> = {
  INFO: "\x1b[32m",
  WARN: "\x1b[33m",
  ERROR: "\x1b[31m",
  DEBUG: "\x1b[34m",
};

function prefixConsoleLoggingMethod(method: LoggingMethod, severity: SeverityLevel): LoggingMethod {
  return (message?: any, ...optionalParams: any[]) => {
    const timestamp = new Date().toISOString();
    withFmt(colors[severity], () => {
      method(`${timestamp} [${severity.padEnd(maxSeverityLevelLength)}]`, message, ...optionalParams);
    });
  };
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
