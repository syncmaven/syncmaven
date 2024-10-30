import { Command } from "commander";
import { sync } from "./sync";
import { init } from "./init";
import { connectorDev } from "./connector-dev";
import { streams } from "./streams";
import { describeDestination } from "./destination";
import { getLatestVersionFromRegistry, syncmavenVersion, syncmavenVersionTag } from "../lib/version";
import { fmt } from "../log";
import { isTruish } from "../lib/util";
import { add } from "./add";
import { preview } from "./preview";
import { link } from "./link";

/**
 * Options of every command
 */
export type CommonOpts = {
  env?: string[];
  debug?: boolean;
};

export type ProjectDirOpt = {
  projectDir?: string;
};

export type StateOpt = {
  state?: string;
};

export type PackageOpts = { package: string; packageType?: string };

export type FullRefreshOpt = {
  fullRefresh?: boolean;
};

const commonOptions = {
  state: {
    flag: "--state <state-directory>",
    description:
      "Where Syncmaven should store it's state. Default is <project-directory>/.state. You can either provide a local directory, or an URL. Learn more about state management at https://syncmaven.sh/state",
  },
  env: {
    flag: "-e, --env <file...>",
    description:
      "Read environment variables from <file> in addition to <project-dir>/.env and <project-dir>/.env.local. Supports multiple files by providing multiple --env options",
  },
  debug: {
    flag: "--debug",
    description: "Enable extra logging for debugging purposes",
  },
  fullRefresh: {
    flag: "-f, --full-refresh",
    description:
      "If sync supports incremental mode, this option will force full refresh. Will apply to all selected syncs",
  },
  packageName: {
    flag: "-p, --package <package-name>",
    description:
      "Name of the package. Either docker image for -t docker, or directory of npm package for -t npm. The directory should contain package.json file, the path should be absolute.",
  },
  packageType: {
    flag: "-t, --package-type <package-name>",
    description: "Type of the package - `docker` or `npm`. `docker` is default",
  },
  projectDir: {
    flag: "-d, --project-dir <project-directory>",
    description: "Which directory to look in for the project. If not specified, a current directory will be used",
  },
} as const;

export async function checkNewVersion() {
  const out: string[] = [];
  if (syncmavenVersionTag !== "dev") {
    console.debug("Checking for new version...");
    const latestVersion = await getLatestVersionFromRegistry("syncmaven", syncmavenVersionTag);
    if (latestVersion && latestVersion !== syncmavenVersion) {
      out.push(`${fmt.cyan("│")} ${fmt.cyan("Update available")}: ${syncmavenVersion} -> ${latestVersion}`);
      out.push(`${fmt.cyan("│")} Run the following to update`);
      if (isTruish(process.env.IN_DOCKER)) {
        out.push(fmt.cyan("│") + fmt.bold(`       docker pull syncmaven/syncmaven:${syncmavenVersionTag}`));
      } else {
        out.push(fmt.cyan("│") + fmt.bold(`      npm install -g syncmaven@${syncmavenVersionTag}`));
      }
    }
    out.push("");
  }
  if (out.length > 0) {
    process.stdout.write(out.join("\n") + "\n");
  }
}

export async function initCli(): Promise<Command> {
  const program = new Command();
  program.name("syncmaven").description("Synchronize data from your database to external services.");

  program
    .command("sync")
    .description("Run all or selected syncs of a given project")
    .option(commonOptions.env.flag, commonOptions.env.description)
    .option(commonOptions.projectDir.flag, commonOptions.projectDir.description)
    .argument("[project-dir]", "Alternative way to specify project directory")
    .option(commonOptions.state.flag, commonOptions.state.description)
    .option(
      "-s, --select <syncs>",
      "Which syncs to run. Can be a sync id or list of sync ids separated by comma. If not provided, all syncs will be run"
    )
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .option(commonOptions.fullRefresh.flag, commonOptions.fullRefresh.description)
    .option(
      commonOptions.packageName.flag,
      commonOptions.packageName.description +
        ". Must be used with -m and -c for running adhoc sync without setting up the project"
    )
    .option(
      commonOptions.packageType.flag,
      commonOptions.packageType.description +
        ". Must be used with -m and -c for running adhoc sync without setting up the project"
    )
    .option(
      "-m, --model <model-or-sql-query>",
      "Specify a model for ad-hoc runs - either as JSON, or @/path/to/a/file.json or yaml"
    )
    .option(
      "-c, --credentials <credentials>",
      "Specify destination credentials for adhoc runs, either as JSON or @/path/to/a/file.json or yaml"
    )
    .option(
      "--datasource <credentials>",
      "Specify datasource credentials for adhoc runs, either as JSON or @/path/to/a/file.json or yaml"
    )
    .option("--stream", "Optionally, you can specify a stream name")
    .option("-o, --stream-options", "Optionally, you can specify a stream options")
    .option("--checkpoint-every <n>", "Optionally, you can specify a checkpoint every N rows")
    .option(
      "--sync-id <id>",
      "Id of the sync. Needed for saving state across runs if database is shared. If not set, `sync` will be used"
    )
    .action(sync);

  program
    .command("init")
    .description("Initialize a new syncmaven project")
    .option("-d, --project-dir <project-directory>", "Which directory to look in for the project")
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .argument("[project-dir]", "Alternative way to specify project directory")
    .action(init);

  program
    .command("streams")
    .description("Describes streams available in the connection")
    .option(
      "-f, --connection-file <connection-file>",
      "File where connection is defined. If not provided, you can specify package and credentials. Or project dir and connection id"
    )
    .option(commonOptions.packageName.flag, commonOptions.packageName.description)
    .option(commonOptions.packageType.flag, commonOptions.packageType.description)
    .option(
      "-c, --credentials <credentials>",
      "If connection file is not provided, you can specify package (see above) and credentials json"
    )
    .option(commonOptions.env.flag, commonOptions.env.description)
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .option(commonOptions.projectDir.flag, commonOptions.projectDir.description)
    .argument(
      "[args]",
      "If not specifying connection file, you can provide package and credentials as arguments",
      undefined
    )
    .action(streams);

  program
    .command("destination")
    .description("Describes destination parameters")
    .requiredOption(commonOptions.packageName.flag, commonOptions.packageName.description)
    .option(commonOptions.packageType.flag, commonOptions.packageType.description)
    .option(commonOptions.env.flag, commonOptions.env.description)
    .option("--json", "Output a JSON schema instead of a human readable format")
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .action(describeDestination);

  program
    .command("connector-dev")
    .description("Runs a NPM based connector in development mode")
    .option(commonOptions.env.flag, commonOptions.env.description)
    .option(commonOptions.state.flag, commonOptions.state.description)
    .option(
      "-c, --connector-dir <connector-directory>",
      "Directory where the connector is based. If not set, current process directory will be used"
    )
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .requiredOption(
      "-f, --connection-file <connection-file>",
      "File with a definition of the connection. Package section of this file will be ignored"
    )
    .requiredOption("-m, --model-file <model-file>", "Model file that are used as a source for the test sync")
    .option(
      "-s, --sync <sync-file-or-id>",
      "Optional. You can specify either sync file if the sync requires additional options, or id for a sync. If id is not provided, it will be generated from model and connection ids. Id is required for saving state across runs"
    )
    .argument("[connector-directory]", "You can also specify connector directory as a positional an argument")
    .action(connectorDev);

  program
    .command("add")
    .description("Adds object (model, sync, connection) to the project")
    .option(commonOptions.projectDir.flag, commonOptions.projectDir.description)
    .option(commonOptions.packageType.flag, commonOptions.packageType.description)
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .argument("<args...>")
    .action(add);

  program
    .command("preview")
    .description("Preview a model in the project")
    .option(commonOptions.projectDir.flag, commonOptions.projectDir.description)
    .option(commonOptions.packageType.flag, commonOptions.packageType.description)
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .argument("<model...>")
    .action(preview);

  program
    .command("link")
    .description("Creates a sync: links a model to a connection")
    .option(commonOptions.projectDir.flag, commonOptions.projectDir.description)
    .option(commonOptions.packageType.flag, commonOptions.packageType.description)
    .option(commonOptions.debug.flag, commonOptions.debug.description)
    .requiredOption("-c, --connection <connection>", "Connection id")
    .requiredOption("-m, --model <model>", "Model id")
    .option("-s, --stream <stream>", "Stream name. Should be set if connection supports multiple streams")
    .action(link);

  program.helpOption("-h --help", "display help for command");
  program.version(
    syncmavenVersionTag === "dev" ? "LOCAL.DEV.VERSION" : syncmavenVersion,
    "-v, --version",
    "output the current version"
  );

  return program;
}
