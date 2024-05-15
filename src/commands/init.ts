import { untildify } from "../lib/project";
import fs from "fs";

export async function init(projectDir: string,  opts: {
  projectDir?: string;
  state?: string;
  select?: string;
  fullRefresh?: boolean;
  env?: string[];
}) {
  projectDir = untildify(projectDir || opts?.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd());
  console.debug("Initializing project in", projectDir);
  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir, { recursive: true });
    console.log(`Project dir has been created ${projectDir}`);
  }
  fs.mkdirSync(`${projectDir}/models`);
  fs.mkdirSync(`${projectDir}/syncs`);
  fs.mkdirSync(`${projectDir}/connections`);
}
