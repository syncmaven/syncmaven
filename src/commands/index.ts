import { Command } from "commander";
import { sync } from "./sync";
import { defaultOauthRedirectURIPort, triggerOauthFlow } from "./auth-helper";
import { init } from "./init";

const commands: Record<string, Command> = {};

function addCommand(command: Command) {
  commands[command.name()] = command;
}

type Action = (...args: any[]) => void | Promise<void>;

function withHelp(command: Command, action: Action) {
  return async (...args: any[]) => {
    if (command.opts().help && args[0].help) {
      command.help();
    } else {
      await action(...args);
    }
  };
}

export function initCli(): Command {
  const program = new Command();
  program.name("syncmaven").description("Synchronize data from your database to external services.");
  const syncCommand = program.command("sync");
  syncCommand
    .description("Run all or selected syncs of a given project")
    .option(
      "-e, --env <file...>",
      "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options"
    )
    .option("-h, --help", "Display this help message")
    .option(
      "-d, --project-dir <project-directory>",
      "Which directory to look in for the project. If not specified, a current directory will be used"
    )
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
    .action(withHelp(syncCommand, sync));
  addCommand(syncCommand);

  const authHelper = program.command("auth-helper");

  authHelper
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
    .action(withHelp(authHelper, triggerOauthFlow));
  addCommand(authHelper);

  const helpCommand = program.command("help");
  helpCommand
    .description("Display help for a given command")
    .argument("[command]", "Command to display help for", "")
    .action((command: string) => {
      if (!command) {
        program.help();
      } else {
        const cmd = commands[command];
        if (cmd) {
          cmd.help();
        } else {
          console.error(`Command ${command} not found`);
        }
      }
    });

  const initCommand = program
    .command("init")
    .description("Initialize a new syncmaven project")
    .option("-d, --project-dir <project-directory>", "Which directory to look in for the project")
    .argument("[project-dir]", "Alternative way to specify project directory")
    .option("--debug", "Enable extra logging for debugging purposes")
    .action(withHelp(authHelper, init));
  addCommand(initCommand);
  return program;
}
