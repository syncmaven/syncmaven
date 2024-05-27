import test from "node:test";
import { fmt, initializeConsoleLogging, rewriteSeverityLevel, setEnabledDebugLogging } from "../../src/log";

initializeConsoleLogging();

function writeLogs() {
  console.log(`Message ${fmt.bold("bold")} not-bold`, { a: 1 });
  console.warn(`Message ${fmt.bold("bold")} not-bold`, { a: 1 });
  console.error(`Message ${fmt.bold("bold")} not-bold`, { a: 1 });
  console.debug(`Message ${fmt.bold("bold")} not-bold`, { a: 1 });
}

test("test-sql-query-parser", () => {
  writeLogs();
  rewriteSeverityLevel("INFO", "DEBUG");
  setEnabledDebugLogging(false);
  writeLogs();
});
