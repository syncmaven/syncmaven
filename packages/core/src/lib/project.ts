import os from "node:os";
import path from "path";
import { ZodSchema } from "zod";
import fs from "fs";
import nunjucks from "nunjucks";
import { merge, omit, set } from "lodash";
import { load } from "js-yaml";
import { stringifyZodError } from "./zod";
import type { Project } from "../types/project";
import { ConnectionDefinition, ModelDefinition, SyncDefinition } from "../types/objects";

type WithId = { id: string };
type MakeRequired<T, K extends keyof T> = Omit<T, K> & Required<Pick<T, K>>;

type CallbackFunction = (varName: string, defaultVal?: string) => string;

function splitFileName(fileName: string): [string, string | undefined] {
  const parts = fileName.split(".");
  if (parts.length === 1) {
    return [parts[0], undefined];
  }
  return [parts.slice(0, -1).join("."), parts[parts.length - 1]];
}

function replaceExpressions(obj: any, callback: CallbackFunction): any {
  // Regular expression to find ${varName[:defaultVal]} patterns
  // This regex allows varName to include letters, numbers, underscores, dashes, and dots
  const regex = /\$\{([a-zA-Z0-9_.-]+)(?::([^}]*))?\}/g;

  // Function to recursively process an object
  function processValue(value: any): any {
    if (Array.isArray(value)) {
      // Process each item in the array
      return value.map(item => processValue(item));
    } else if (value !== null && typeof value === "object") {
      // Process each property in the object
      const processedObject: Record<string, any> = {};
      for (const key of Object.keys(value)) {
        processedObject[key] = processValue(value[key]);
      }
      return processedObject;
    } else if (typeof value === "string") {
      // Replace expressions in the string
      return value.replace(regex, (match, varName, defaultVal) => {
        return callback(varName.trim(), defaultVal?.trim());
      });
    } else {
      // Return other types unchanged
      return value;
    }
  }

  return processValue(obj);
}

export function untildify(filePath: string): string {
  const home = process.env.HOME || os.homedir() || "/";
  if (filePath.startsWith("~")) {
    return path.join(home, filePath.slice(1));
  }
  return filePath;
}

function makeFactory<T extends WithId>(
  obj: any,
  file: { fullPath: string; idFromName: string },
  zodSchema: ZodSchema<T>
): () => MakeRequired<T, "id"> {
  let cached: MakeRequired<T, "id"> | undefined = undefined;

  return () => {
    if (cached) {
      return cached;
    }
    obj = replaceExpressions(obj, (varName, defaultVal) => {
      const [prefix, ...rest] = varName.split(".");
      if (prefix === "env") {
        const value = process.env[rest.join(".")];
        if (value === undefined) {
          if (defaultVal === undefined) {
            throw new Error(`Environment variable ${varName} is not set. It's used in ${file.fullPath}`);
          } else {
            return defaultVal;
          }
        }
        return value;
      } else {
        throw new Error(
          `Unsupported placeholder \${${varName}} in ${file.fullPath}. Only \${env.NAME} placeholders are supported. Did you mean \${env.${varName}}?`
        );
      }
    });
    const { success, error, data } = zodSchema.safeParse(obj);
    if (!success) {
      throw new Error(`Error parsing ${file.fullPath}: ${stringifyZodError(error)}`);
    }
    return (cached = { id: data.id || file.idFromName, ...omit(data, "id") });
  };
}

export function readObjectsFromDirector<T>(dir: string, zodSchema: ZodSchema<T>): Record<string, () => T> {
  const result: Record<string, () => T> = {};
  for (const child of fs.readdirSync(dir)) {
    const fullPath = path.join(dir, child);
    const pathStat = fs.statSync(fullPath);
    if (pathStat.isDirectory()) {
      console.warn(`Only files are supported in ${dir}. Skipping directry ${child}`);
    }
    const [baseName, extension] = splitFileName(child);
    if (extension === undefined) {
      console.warn(`Only files with extensions are supported in ${dir}. Skipping file ${child}`);
    } else if (extension === "sql") {
      const content = fs.readFileSync(fullPath, "utf-8");
      const templateEngine = new nunjucks.Environment();
      let config: any = {};
      templateEngine.addGlobal("config", function (arg1, arg2) {
        if (arg2 === undefined && typeof arg1 === "object") {
          config = merge(config, arg1);
        }
        if (typeof arg1 === "string" && typeof arg2 === "string") {
          set(config, arg1, arg2);
        } else {
          throw new Error(`Unsupported config() call with arguments ${typeof arg1}, ${typeof arg2}`);
        }
      });
      try {
        config.query = templateEngine.renderString(content, { env: process.env });
      } catch (e: any) {
        const message = (e?.message || "unknown error").replace("(unknown path)", fullPath);

        throw new Error(`Unable to parse template expression in ${child} model file: ${message}`, { cause: e });
      }

      if (!config.disabled) {
        const id = config.id || baseName;
        result[id] = makeFactory<any>(config, { fullPath, idFromName: baseName }, zodSchema) as any;
      }
    } else if (extension === "yaml" || extension === "yml") {
      const content = fs.readFileSync(fullPath, "utf-8");
      const yamlRaw = load(content, { filename: fullPath, json: true }) as any;
      if (!content || !yamlRaw) {
        throw new Error(`Error parsing ${dir}/${child}. File seems to be empty or invalid`);
      }
      if (!yamlRaw.disabled) {
        const id = yamlRaw.id || baseName;
        result[id] = makeFactory<any>(yamlRaw, { fullPath, idFromName: baseName }, zodSchema) as any;
      }
    } else if (extension === "ts") {
      console.warn(`TypeScript models are not supported yet. Skipping ${dir}/${child}`);
    } else {
      console.warn(`Unsupported file extension ${extension} in ${dir}/${child}. Skipping file`);
    }
  }
  return result;
}

export function readProject(dir: string): Project {
  console.debug("Reading project from", dir);
  if (!fs.existsSync(dir)) {
    throw new Error(`Project directory ${dir} does not exist`);
  }
  const modelDir = path.join(dir, "models");
  const syncDir = path.join(dir, "syncs");
  const connectionsDir = path.join(dir, "connections");
  if (!fs.existsSync(modelDir)) {
    throw new Error(`Model directory ./model does not exist in the project directory ${dir}`);
  }
  if (!fs.existsSync(syncDir)) {
    throw new Error(`Model directory ./sync does not exist in the project directory ${dir}`);
  }
  if (!fs.existsSync(connectionsDir)) {
    throw new Error(`Model directory ./destinations does not exist in the project directory ${dir}`);
  }
  return {
    syncs: readObjectsFromDirector<SyncDefinition>(syncDir, SyncDefinition),
    models: readObjectsFromDirector<ModelDefinition>(modelDir, ModelDefinition),
    connection: readObjectsFromDirector<ConnectionDefinition>(connectionsDir, ConnectionDefinition),
  };
}
