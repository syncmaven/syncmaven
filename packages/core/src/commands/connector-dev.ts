import path from "path";
import fs from "fs";
import { Factory, Project } from "../types/project";
import { configureEnvVars, readProjectObjectFromFile } from "../lib/project";
import { ConnectionDefinition, ModelDefinition, SyncDefinition } from "../types/objects";
import assert from "assert";
import { createStore, runSync } from "./sync";

function looksLikePath(syncFileOrId: string) {
  return fs.existsSync(syncFileOrId);
}

export async function connectorDev(
  connectorDirectory: string | undefined,
  opts: {
    connectorDir?: string; modelFile: string; syncFileOrId: string; connectionFile: string;
    env?: string[]
    state?: string
    fullRefresh?: boolean
  },
) {
  console.log("Running connector dev", connectorDirectory, opts);
  const dir = path.resolve(path.join(connectorDirectory || opts.connectorDir || process.cwd()));
  configureEnvVars([".env", ".env.local"], dir, opts.env || []);
  const packageJsonPath = path.join(dir, "package.json");
  if (!fs.existsSync(packageJsonPath)) {
    throw new Error(`Connector directory ${dir} does not contain package.json: ${packageJsonPath}`);
  }
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8"));
  if (!packageJson.main) {
    throw new Error(`Connector package.json does not contain main field`);
  }

  const connection = readProjectObjectFromFile(opts.connectionFile, ConnectionDefinition, { ignoreDisabled: true });
  const model = readProjectObjectFromFile(opts.modelFile, ModelDefinition, { ignoreDisabled: true })!;
  assert(model, `Can't read model from ${opts.modelFile}`);
  assert(connection, `Can't read connection from ${opts.connectionFile}`);

  const sync: { id: string; factory: Factory<SyncDefinition> } = looksLikePath(opts.syncFileOrId)
    ? readProjectObjectFromFile(opts.syncFileOrId, SyncDefinition, { ignoreDisabled: true })!
    : {
      id: opts.syncFileOrId || model.id + "-" + connection.id,
      factory: () => ({
        model: model.id,
        destination: connection.id,
      }),
    };
  const project: Project = {
    models: {
      [model.id]: model.factory,
    },
    syncs: {
      [sync.id]: sync.factory
    },
    connection: {
      [connection.id]: () => {
        return {
          ...connection.factory(),
          package: {
            type: "sub-process",
            command: process.execPath + " " + packageJson.main,
            commandDir: dir
          },
        };
      },
    },
  };
  console.log(`Running sync ${sync.id} with model ${model.id} and connection ${connection.id}`, project);

  await runSync({
    project,
    syncId: sync.id,
    fullRefresh: opts.fullRefresh,
    store: await createStore(dir, opts.state)
  });
}