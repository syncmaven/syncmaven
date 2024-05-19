import { Command } from "commander";
import { initializeConsoleLogging } from "./log";
import assert from "assert";
import { OAuth2Client } from "google-auth-library";
import { sync } from "./commands/sync";
import { triggerOauthFlow } from "./commands/auth-helper";
import { initCli } from "./commands";

initializeConsoleLogging();

export async function main(argv: string[] = process.argv) {
  const program = initCli();

  const debug = argv.includes("--debug");
  try {
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
