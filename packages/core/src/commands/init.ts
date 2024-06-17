import { untildify } from "../lib/project";
import fs from "fs";
import { CommonOpts, ProjectDirOpt, StateOpt } from "./index";
import { fmt, out } from "../log";
import path from "path";
import { tryGitInit } from "../lib/git";

function isEmptyDir(projectDir: string) {
  return fs.readdirSync(projectDir).length === 0;
}

export async function init(projectDir: string, opts: CommonOpts & StateOpt & ProjectDirOpt) {
  projectDir = path.resolve(
    untildify(projectDir || opts?.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd())
  );
  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir, { recursive: true });
    out(`${fmt.green("✔")} Directory will be created ${fmt.bold(projectDir)}`);
  }
  if (!isEmptyDir(projectDir)) {
    out(
      `${fmt.red("✘")} Directory is not empty: ${fmt.bold(projectDir)}. Please create a new project in an empty directory.`
    );
    return;
  }
  fs.mkdirSync(`${projectDir}/models`);
  fs.mkdirSync(`${projectDir}/syncs`);
  fs.mkdirSync(`${projectDir}/connections`);
  fs.writeFileSync(
    `${projectDir}/.env`,
    [`# Keep secrets such as database connection strings or API keys here`, `SECRET_KEY=supersecret`].join("\n")
  );
  out(`${fmt.green("✔")} Created Syncmaven project at ${fmt.bold(projectDir)}`);
  const disableGit = false;
  if (!disableGit) {
    fs.writeFileSync(`${projectDir}/.gitignore`, [".env", ".state"].join("\n"));
    fs.writeFileSync(`${projectDir}/models/.gitkeep`, "");
    fs.writeFileSync(`${projectDir}/syncs/.gitkeep`, "");
    fs.writeFileSync(`${projectDir}/connections/.gitkeep`, "");
    if (tryGitInit(projectDir)) {
      out(`${fmt.green("✔")} Initialized git repository`);
    }
  }
}
