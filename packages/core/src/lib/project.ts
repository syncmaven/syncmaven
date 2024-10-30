import os from "node:os";
import path from "path";
import { ZodSchema, ZodType } from "zod";
import fs from "fs";
import Handlebars from "handlebars";
import { merge, omit, set } from "lodash";
import { load } from "js-yaml";
import { stringifyZodError } from "./zod";
import type { Factory, Project } from "../types/project";
import { ConnectionDefinition, ModelDefinition, SyncDefinition } from "../types/objects";
import dotenv from "dotenv";

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

export function configureEnvVars(dirs: (string | undefined) | (string | undefined)[], envFiles: string[]) {
  const envFileNames = [".env", ".env.local"];
  if (process.env.NODE_ENV) {
    envFileNames.push(`.env.${process.env.NODE_ENV}`);
  }
  const dirsArr = Array.isArray(dirs) ? dirs : [dirs];

  const paths: string[] = [];
  for (const dir of dirsArr) {
    if (dir) {
      for (const envFileName of envFileNames) {
        paths.push(path.join(dir, envFileName));
      }
    }
  }
  paths.push(...envFiles);
  dotenv.config({
    path: paths.map(untildify),
  });
}

export function readProjectObjectFromFile<T>(
  filePath: string,
  zodSchema: ZodType<T>,
  opts: { ignoreDisabled?: boolean } = {}
): { id: string; factory: Factory<T> } | undefined {
  const { name, dir, base, ext } = path.parse(filePath);
  if (ext === undefined || ext === "") {
    console.debug(`Only files with extensions are supported in ${dir}. Skipping file ${base}`);
  } else if (ext === ".sql") {
    const content = fs.readFileSync(filePath, "utf-8");
    let config: any = {};
    Handlebars.registerHelper("config", function (arg1, arg2) {
      if (arg2 === undefined && typeof arg1 === "object") {
        config = merge(config, arg1);
      }
      if (typeof arg1 === "string" && typeof arg2 === "string") {
        set(config, arg1, arg2);
      } else {
        throw new Error(`Unsupported config() call for '${arg1}' with arguments ${typeof arg1}, ${typeof arg2}`);
      }
    });
    try {
      const template = Handlebars.compile(content);
      config.query = template({ env: process.env });
    } catch (e: any) {
      const message = (e?.message || "unknown error").replace("(unknown path)", filePath);

      throw new Error(`Unable to parse template expression in ${filePath} model file: ${message}`, { cause: e });
    }

    if (!config.disabled || opts.ignoreDisabled) {
      const id = config.id || name;
      return {
        id,
        factory: makeFactory<any>(config, { fullPath: filePath, idFromName: name }, zodSchema) as any as any,
      };
    }
  } else if (ext === ".yaml" || ext === ".yml") {
    const content = fs.readFileSync(filePath, "utf-8");
    const yamlRaw = load(content, { filename: filePath, json: true }) as any;
    if (!content || !yamlRaw) {
      throw new Error(`Error parsing ${path}. File seems to be empty or invalid`);
    }
    if (!yamlRaw.disabled || opts.ignoreDisabled) {
      const id = yamlRaw.id || name;

      return {
        id,
        factory: makeFactory<any>(yamlRaw, { fullPath: filePath, idFromName: name }, zodSchema) as any,
      };
    }
  } else if (ext === ".json") {
    const content = fs.readFileSync(filePath, "utf-8");
    const jsonRaw = JSON.parse(content);
    if (!content || !jsonRaw) {
      throw new Error(`Error parsing ${path}. File seems to be empty or invalid`);
    }
    if (!jsonRaw.disabled || opts.ignoreDisabled) {
      const id = jsonRaw.id || name;

      return {
        id,
        factory: makeFactory<any>(jsonRaw, { fullPath: filePath, idFromName: name }, zodSchema) as any,
      };
    }
  } else if (ext === ".ts") {
    console.warn(`TypeScript models are not supported yet. Skipping ${filePath}`);
  } else {
    console.warn(`Unsupported file extension ${ext} in ${filePath}. Skipping file`);
  }
}

export function readObjectsFromDirectory<T>(dir: string, zodSchema: ZodSchema<T>): Record<string, () => T> {
  const result: Record<string, () => T> = {};
  for (const child of fs.readdirSync(dir)) {
    const fullPath = path.join(dir, child);
    const pathStat = fs.statSync(fullPath);
    if (pathStat.isDirectory()) {
      console.warn(`Only files are supported in ${dir}. Skipping directory ${child}`);
    }
    const projectFileParsed = readProjectObjectFromFile(fullPath, zodSchema);
    if (projectFileParsed) {
      result[projectFileParsed.id] = projectFileParsed.factory;
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
    throw new Error(`Model directory ./models does not exist in the project directory ${dir}`);
  }
  if (!fs.existsSync(syncDir)) {
    throw new Error(`Model directory ./syncs does not exist in the project directory ${dir}`);
  }
  if (!fs.existsSync(connectionsDir)) {
    throw new Error(`Model directory ./destinations does not exist in the project directory ${dir}`);
  }
  return {
    syncs: readObjectsFromDirectory<SyncDefinition>(syncDir, SyncDefinition),
    models: readObjectsFromDirectory<ModelDefinition>(modelDir, ModelDefinition),
    connection: readObjectsFromDirectory<ConnectionDefinition>(connectionsDir, ConnectionDefinition),
  };
}
