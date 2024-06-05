import path from "path";
import fs from "fs";
import JSON5 from "json5";

const packageJson = getPackageJson();

export const syncmavenVersion = process.env.SYNCMAVEN_VERSION || packageJson.version as string;
export const syncmavenVersionTag = getVersionTag(syncmavenVersion);

function getVersionTag(syncmavenVersion: string) {
  if (syncmavenVersion === "0.0.0") {
    return "dev";
  } else if (syncmavenVersion.indexOf("canary") >= 0) {
    return "canary";
  }
  return "latest";
}

export async function getLatestVersionFromRegistry(packageName: string, tag: string): Promise<string | undefined> {
  // don't delay the process if the registry is not available
  const timeout = 300;
  const url = `https://registry.npmjs.org/${packageName}`;
  const abortController = new AbortController();
  const abortTimeout = setTimeout(() => abortController.abort(), timeout * 1000);

  try {
    const response = await fetch(url, {signal: abortController.signal});
    if (!response.ok) {
      console.warn(
        `Failed to fetch latest version of ${packageName}@${tag} from npm registry: ${response.status} ${response.statusText}`
      );
      return;
    }
    clearTimeout(abortTimeout);

    const data = await response.json();
    const version = data["dist-tags"][tag];

    if (!version) {
      console.warn(`Failed to fetch latest version of ${packageName}@${tag} from npm registry: tag not found`);
      return;
    }

    return version;
  } catch (error: any) {
    console.debug(
      `Failed to fetch latest version of ${packageName}@${tag} from npm registry: ${error.message || "unknown error"}`
    );
  }
}


export function getPackageJson() {
  const maxDepth = 5;
  let depth = 0;
  let currentDir = path.resolve(__dirname);
  while (depth < maxDepth && currentDir !== "/" && currentDir) {
    const packageJson = path.join(currentDir, "package.json");
    if (fs.existsSync(packageJson)) {
      return JSON5.parse(fs.readFileSync(packageJson, "utf8"));
    }
    currentDir = path.resolve(currentDir, "..");
    depth++;
  }
}
