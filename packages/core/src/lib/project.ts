import os from "node:os";
import path from "path";
import { ZodSchema } from "zod";
import fs from "fs";
import { omit, set } from "lodash";
import { load } from "js-yaml";
import { stringifyZodError } from "./zod";
import { ConfigurationObject, Project, RawProject } from "../types/project";
import { ConnectionDefinition, ModelDefinition, SyncDefinition } from "../types/objects";
import dotenv from "dotenv";
import JSON5 from "json5";
import { HandlebarsTemplateEngine } from "./template";

type WithId = { id: string };
type MakeRequired<T, K extends keyof T> = Omit<T, K> & Required<Pick<T, K>>;

function splitFileName(fileName: string): [string, string | undefined] {
  const parts = fileName.split(".");
  if (parts.length === 1) {
    return [parts[0], undefined];
  }
  return [parts.slice(0, -1).join("."), parts[parts.length - 1]];
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
    console.debug(`Compiling object from ${file.fullPath}. Raw data: ${JSON.stringify(obj, null, 2)}`);
    obj = new HandlebarsTemplateEngine().compile(obj, { fileName: file.fullPath })({ env: process.env });
    console.debug(`Compiled object: ${JSON.stringify(obj, null, 2)}`);
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

function getFileNameFromFullPath(fullPath: string): string {
  return path.basename(fullPath).split(".")[0];
}

export function readConfigObjectFromFile(
  filePath: string,
  type: "model" | "sync" | "connection",
  { relativeDir = undefined }: { relativeDir?: string } = {}
): ConfigurationObject {
  let content: string | Record<string, any> = {};
  if (filePath.endsWith(".sql")) {
    content = fs.readFileSync(filePath, "utf-8");
  } else if (filePath.endsWith(".yaml") || filePath.endsWith(".yml")) {
    content = load(fs.readFileSync(filePath, "utf-8"), { filename: filePath, json: true }) as any;
  } else if (filePath.endsWith(".json")) {
    content = JSON5.parse(fs.readFileSync(filePath, "utf-8"));
  } else {
    console.warn(`Unsupported file extension in ${filePath}. Skipping file`);
  }
  return {
    type,
    content,
    fileId: getFileNameFromFullPath(filePath),
    relativeFileName: relativeDir ? path.relative(relativeDir, filePath) : filePath,
  };
}

export function readObjectsFromDirectory<T>(dir: string, type: ConfigurationObject["type"]): ConfigurationObject[] {
  const result: ConfigurationObject[] = [];

  for (const child of fs.readdirSync(dir)) {
    const fullPath = path.join(dir, child);

    const pathStat = fs.statSync(fullPath);
    if (pathStat.isDirectory()) {
      console.warn(`Only files are supported in ${dir}. Skipping directory ${child}`);
      continue;
    }
    const item = readConfigObjectFromFile(fullPath, type, { relativeDir: dir });

    result.push(item);
  }
  return result;
}

function pickId(startingName: string, has: (id: string) => boolean): string {
  let id = startingName;
  let i = 1;
  while (has(id)) {
    id = `${startingName}_${i}`;
    i++;
  }
  return id;
}

export function readProject(dir: string): RawProject {
  console.debug("Reading project from", dir);
  if (!fs.existsSync(dir)) {
    throw new Error(`Project directory ${dir} does not exist`);
  }
  const modelDir = path.join(dir, "models");
  const syncDir = path.join(dir, "syncs");
  const connectionsDir = path.join(dir, "connections");
  if (!fs.existsSync(syncDir)) {
    throw new Error(`Model directory ./syncs does not exist in the project directory ${dir}`);
  }
  const project = {
    syncs: fs.existsSync(modelDir) ? readObjectsFromDirectory(syncDir, "sync") : [],
    models: readObjectsFromDirectory(modelDir, "model"),
    connections: fs.existsSync(connectionsDir) ? readObjectsFromDirectory(connectionsDir, "connection") : [],
  };
  unfoldSyncs(project);
  return project;
}

export function getObjectId(obj: ConfigurationObject): string {
  if (typeof obj.content === "string") {
    //we need to handle this situation better in case of id is defined withing file as a template
    if (!obj.fileId) {
      throw new Error(`Object ${obj.relativeFileName} does not have an id`);
    }
    return obj.fileId;
  } else {
    //id defined in the object takes precedence
    return obj.content.id || obj.fileId;
  }
}

/**
 * We allow to define models and connections right in the syncs for simplicity. This
 * function moves them to a connection/model section and updates syncs to reference them.
 */
export function unfoldSyncs(project: RawProject): void {
  const usedConnectionIds = new Set<string>(project.connections.map(getObjectId));
  for (const sync of project.syncs) {
    const syncContent = sync.content as SyncDefinition;
    if (typeof syncContent.destination !== "string") {
      //we need to move the connection to the connection section
      const connectionId = pickId(syncContent.id || sync.fileId, (id: string) => usedConnectionIds.has(id));
      usedConnectionIds.add(connectionId);
      project.connections.push({
        type: "connection",
        content: syncContent.destination,
        fileId: connectionId,
        relativeFileName: sync.relativeFileName,
      });
      syncContent.destination = connectionId;
    }
  }
}

/**
 * Compile project by placeholding env variables and doing other post-processing.
 */
export function compileProject(raw: RawProject): Project {
  const models: Record<string, ModelDefinition> = {};
  const syncs: Record<string, SyncDefinition> = {};
  const connections: Record<string, ConnectionDefinition> = {};

  const templateEngine = new HandlebarsTemplateEngine();
  for (const model of raw.models) {
    if (typeof model.content === "string") {
      let obj: any = {};
      obj["query"] = templateEngine.compile(model.content, { fileName: model.relativeFileName })({
        env: process.env,
        config: (path, val) => {
          const newVar = set(obj, path, val);
          return "";
        },
      });
      //we need to run template engine again to do replacement in config calls
      obj = templateEngine.compile(obj, { fileName: model.relativeFileName })({ env: process.env });
      let parsedModel: ModelDefinition;
      try {
        parsedModel = ModelDefinition.parse(obj);
      } catch (e) {
        throw new Error(`Error parsing model ${model.relativeFileName}: ${stringifyZodError(e)}`);
      }
      models[parsedModel.id || model.fileId] = parsedModel;
    } else {
      let parsedModel: ModelDefinition;
      try {
        parsedModel = ModelDefinition.parse(model.content);
      } catch (e) {
        throw new Error(`Error parsing model ${model.relativeFileName}: ${stringifyZodError(e)}`);
      }
      models[model.content.id || model.fileId] = templateEngine.compile(parsedModel, {
        fileName: model.relativeFileName,
      })({ env: process.env });
    }
  }

  for (const conn of raw.connections) {
    if (typeof conn.content === "string") {
      throw new Error(`Connection ${conn.relativeFileName} should be defined in YML or JSON format`);
    }
    let parsedConnection: ConnectionDefinition;
    try {
      parsedConnection = ConnectionDefinition.parse(conn.content);
    } catch (e) {
      console.debug(
        `Failed to parse connection ${conn.relativeFileName} (error will follow): ${JSON.stringify(conn.content, null, 2)}`
      );
      throw new Error(`Error parsing connection ${conn.relativeFileName}: ${stringifyZodError(e)}`);
    }

    connections[conn.content.id || conn.fileId] = templateEngine.compile(parsedConnection, {
      fileName: conn.relativeFileName,
    })({
      env: process.env,
      result: {
        rows: "[RESULT_ROWS]",
        row: "[RESULT_ROW]",
        length: "[RESULT_LENGTH]",
      },
    });
  }

  for (const sync of raw.syncs) {
    if (typeof sync.content === "string") {
      throw new Error(`Connection ${sync.relativeFileName} should be defined in YML or JSON format`);
    }
    let parsedSync: SyncDefinition;
    try {
      parsedSync = SyncDefinition.parse(sync.content);
    } catch (e) {
      console.debug(
        `Failed to parse sync ${sync.relativeFileName} (error will follow): ${JSON.stringify(sync, null, 2)}`
      );
      throw new Error(`Error parsing sync ${sync.relativeFileName}: ${stringifyZodError(e)}`);
    }

    syncs[sync.content.id || sync.fileId] = templateEngine.compile(parsedSync, {
      fileName: sync.relativeFileName,
    })({ env: process.env });
  }

  return {
    models,
    syncs,
    connections,
  };
}
