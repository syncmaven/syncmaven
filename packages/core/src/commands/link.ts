import { CommonOpts, ProjectDirOpt } from "./index";
import path from "path";
import { configureEnvVars, readProject, untildify } from "../lib/project";
import { fmt, out, rewriteSeverityLevel } from "../log";
import assert from "assert";
import { Project } from "../types/project";
import { SyncDefinition } from "../types/objects";
import fs from "fs";
import { dump } from "js-yaml";

function pickSyncId(project: Project, basName: string) {
  let syncId = basName;
  let i = 0;
  while (project.syncs[syncId]) {
    syncId = `${basName}-${++i}`;
  }
  return syncId;
}

export async function link(opts: ProjectDirOpt & CommonOpts & { connection: string; model: string; stream?: string }) {
  console.debug(opts);
  const projectDir = path.resolve(untildify(opts?.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd()));
  configureEnvVars([projectDir, "."], opts.env || []);
  rewriteSeverityLevel("INFO", "DEBUG");
  const project = readProject(projectDir);
  const modelFactory = project.models[opts.model];
  assert(modelFactory, `Model ${opts.model} not found in ${projectDir}`);
  const connectionFactory = project.connection[opts.connection];
  console.assert(connectionFactory, `Connection ${opts.connection} not found in ${projectDir}`);
  const syncId = pickSyncId(project, `${opts.connection}-${opts.model}${opts.stream ? `-${opts.stream}` : ""}`);
  const syncFile = path.join(projectDir, "syncs", `${syncId}.yml`);
  const sync: SyncDefinition = {
    id: syncId,
    destination: opts.connection,
    model: opts.model,
    stream: opts.stream,
  };
  fs.writeFileSync(syncFile, dump(sync));
  out(`${fmt.green("✔")} Sync ${fmt.bold(syncId)} created in ${fmt.bold(syncFile)}`);
}
