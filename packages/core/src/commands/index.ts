import { Command } from "commander";
import { sync } from "./sync";
import { defaultOauthRedirectURIPort, triggerOauthFlow } from "./auth-helper";
import { init } from "./init";

export function initCli(): Command {
  const program = new Command();
  program.name("syncmaven").description("Synchronize data from your database to external services.");

  program
    .command("sync")
    .description("Run all or selected syncs of a given project")
    .option(
      "-e, --env <file...>",
      "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options"
    )
    .option(
      "-d, --project-dir <project-directory>",
      "Which directory to look in for the project. If not specified, a current directory will be used"
    )
    .argument("[project-dir]", "Alternative way to specify project directory")
    .option("-t, --state <state-directory>", "Where to store state of syncs. Default is <project-directory>/.state")
    .option(
      "-s, --select <syncs>",
      "Which syncs to run. Can be a sync id or list of sync ids separated by comma. If not provided, all syncs will be run"
    )
    .option("--debug", "Enable extra logging for debugging purposes")
    .option(
      "-f, --full-refresh",
      "If sync supports incremental mode, this option will force full refresh. Will apply to all selected syncs"
    )
    .action(sync);

  program
    .command("auth-helper")
    .description("Trigger an oauth flow for a given connection to generate credentials")
    .option(
      "-e, --env <file...>",
      "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options"
    )
    .option("-d, --project-dir <project-directory>", "Which directory to look in for the project")
    .argument("[project-dir]", "Alternative way to specify project directory")
    .option("-c, --connection <connectionId>", "Connection id")
    .option("-p, --port <port>", `Port where to run server (${defaultOauthRedirectURIPort}) to default`)
    .option("--debug", "Enable extra logging for debugging purposes")
    .action(triggerOauthFlow);

  program
    .command("init")
    .description("Initialize a new syncmaven project")
    .option("-d, --project-dir <project-directory>", "Which directory to look in for the project")
    .option("--debug", "Enable extra logging for debugging purposes")
    .argument("[project-dir]", "Alternative way to specify project directory")
    .action(init);

  program.helpOption("-h --help", "display help for command");

  return program;
}
