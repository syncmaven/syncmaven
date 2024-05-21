import { Command } from "commander";
import { sync } from "./sync";
import { defaultOauthRedirectURIPort, triggerOauthFlow } from "./auth-helper";
import { init } from "./init";
import { connectorDev } from "./connector-dev";

const commonOptions = {
  state: {
    flag: "-t, --state <state-directory>",
    description: "Where Syncmaven should store it's state. Default is <project-directory>/.state. You can either provide a local directory, or an URL. Learn more about state management at https://syncmaven.sh/state",
  },
  env: {
    flag: "-e, --env <file...>",
    description: "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options"
  },
  debug: {
    flag: "--debug",
    description: "Enable extra logging for debugging purposes"
  }
} as const;

export function initCli(): Command {
  const program = new Command();
  program.name("syncmaven").description("Synchronize data from your database to external services.");

  program
    .command("sync")
    .description("Run all or selected syncs of a given project")
    .option(
      commonOptions.env.flag,
      commonOptions.env.description
    )
    .option(
      "-d, --project-dir <project-directory>",
      "Which directory to look in for the project. If not specified, a current directory will be used"
    )
    .argument("[project-dir]", "Alternative way to specify project directory")
    .option(commonOptions.state.flag, commonOptions.state.description)
    .option(
      "-s, --select <syncs>",
      "Which syncs to run. Can be a sync id or list of sync ids separated by comma. If not provided, all syncs will be run"
    )
    .option(commonOptions.debug.flag, commonOptions.debug.description)
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
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .argument("[project-dir]", "Alternative way to specify project directory")
    .action(init);

  program
    .command("connector-dev")
    .description("Runs a NPM based connector in development mode")
    .option(
      commonOptions.env.flag,
      commonOptions.env.description
    )
    .option(commonOptions.state.flag, commonOptions.state.description)
    .option("-c, --connector-dir <connector-directory>", "Directory where the connector is based. If not set, current process directory will be used")
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .requiredOption("-f, --connection-file <connection-file>", "File with a definition of the connection. Package section of this file will be ignored")
    .requiredOption("-m, --model-file <model-file>", "Model file that are used as a source for the test sync")
    .option("-s, --sync-file <sync-file-or-id>", "Optional. You can specify either sync file if the sync requires additional options, or id for a sync. If id is not provided, it will be generated from model and connection ids. Id is required for saving state accross runs")
    .argument("[connector-directory]", "You can also specify connector directory as a positional an argument")
    .action(connectorDev);


  program.helpOption("-h --help", "display help for command");

  return program;
}
