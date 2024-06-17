/* eslint-disable import/no-extraneous-dependencies */
import { execSync } from "child_process";
import path from "path";
import fs from "fs";

function isInGitRepository(dir: string): boolean {
  try {
    execSync("git rev-parse --is-inside-work-tree", { stdio: "ignore", cwd: dir });
    return true;
  } catch (e) {
    //console.debug(`Error running command isInGitRepository()`, e)
  }
  return false;
}

function isDefaultBranchSet(): boolean {
  try {
    execSync("git config init.defaultBranch", { stdio: "ignore" });
    return true;
  } catch (_) {}
  return false;
}

export function tryGitInit(root: string): boolean {
  let didInit = false;
  try {
    execSync("git --version", { stdio: "ignore" });
    if (isInGitRepository(root)) {
      console.debug(`Already in a git repository: ${root}`);
      return false;
    }

    execSync("git init", { stdio: "ignore", cwd: root });
    didInit = true;

    if (!isDefaultBranchSet()) {
      execSync("git checkout -b main", { stdio: "ignore", cwd: root });
    }

    execSync("git add -A", { stdio: "ignore", cwd: root });
    // execSync('git commit -m "Initial commit from Syncmaven"', {
    //   stdio: 'ignore',
    // })
    return true;
  } catch (e) {
    console.debug(`Error initializing git. Did init = ${didInit}`, e);
    if (didInit) {
      try {
        fs.rmSync(path.join(root, ".git"), { recursive: true, force: true });
      } catch (_) {}
    }
    return false;
  }
}
