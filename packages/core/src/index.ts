import { initializeConsoleLogging, setEnabledDebugLogging } from "./log";
import { checkNewVersion, initCli } from "./commands";

initializeConsoleLogging();

export async function main(argv: string[] = process.argv) {
  const program = await initCli();
  const debug = argv.includes("--debug");
  if (debug) {
    setEnabledDebugLogging(true);
    console.debug("Debug logging enabled");
  } else {
    setEnabledDebugLogging(false);
  }
  try {
    await checkNewVersion();
    await program.parseAsync(argv);
    process.exit(0);
  } catch (e: any) {
    if (debug) {
      console.error(e);
    } else {
      console.error(`Failed: ${e?.message || "Unknown error"}`);
    }
    process.exit(1);
  }
}

main();
