import path from "path";
import fs from "fs";
import { ConfigurationObject, Factory, Project, RawProject } from "../types/project";
import { compileProject, configureEnvVars, getObjectId, readConfigObjectFromFile } from "../lib/project";
import { ConnectionDefinition, ModelDefinition, SyncDefinition } from "../types/objects";
import assert from "assert";
import { createStore, runSync } from "./sync";

function looksLikePath(syncFileOrId: string) {
  return fs.existsSync(syncFileOrId);
}

export async function connectorDev(
  connectorDirectory: string | undefined,
  opts: {
    connectorDir?: string;
    modelFile: string;
    sync: string;
    connectionFile: string;
    env?: string[];
    state?: string;
    fullRefresh?: boolean;
  }
) {
  console.log("Running connector dev", connectorDirectory, opts);
  const dir = path.resolve(path.join(connectorDirectory || opts.connectorDir || process.cwd()));
  configureEnvVars(dir, opts.env || []);
  const packageJsonPath = path.join(dir, "package.json");
  if (!fs.existsSync(packageJsonPath)) {
    throw new Error(`Connector directory ${dir} does not contain package.json: ${packageJsonPath}`);
  }
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8"));
  if (!packageJson.main) {
    throw new Error(`Connector package.json does not contain main field`);
  }

  const connection = readConfigObjectFromFile(opts.connectionFile, "connection");
  const model = readConfigObjectFromFile(opts.modelFile, "model");

  const sync: ConfigurationObject = looksLikePath(opts.sync)
    ? readConfigObjectFromFile(opts.sync, "sync")
    : {
        type: "sync",
        relativeFileName: opts.sync,
        fileId: opts.sync,
        content: {
          id: "sync",
          model: getObjectId(model),
          destination: getObjectId(connection),
        },
      };
  const rawProject: RawProject = {
    models: [model],
    connections: [connection],
    syncs: [sync],
  };
  const project = compileProject(rawProject);
  console.log(
    `Running sync ${getObjectId(sync)} with model ${getObjectId(model)} and connection ${getObjectId(connection)}`,
    project
  );
  const storeCfg = opts.state || path.join(dir, ".state");

  await runSync({
    project,
    syncId: getObjectId(sync),
    fullRefresh: opts.fullRefresh,
    store: await createStore(storeCfg),
  });
}
