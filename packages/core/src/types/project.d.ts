import type { ConnectionDefinition, ModelDefinition } from "./objects.ts";
import type { SyncDefinition } from "./sync";

/**
 * Creates a project object by placeholding env variables and doing other post-processing.
 * We need this since we read project before we have all env variables and other data
 * for post-processing.
 */
export interface Factory<T> {
  (): T;

  /**
   * Contains raw object without post-processing
   */
  raw: T;
}

/**
 * Configuration object of a project
 */
export type ConfigurationObject = {
  type: "model" | "sync" | "connection";
  /**
   * If project is read from file, this will contain the relative file path
   */
  relativeFileName: string;
  /**
   * ID from file name
   */
  fileId: string;

  content: string | Record<string, any>;
};

export type RawProject = {
  models: ConfigurationObject[];
  syncs: ConfigurationObject[];
  connections: ConfigurationObject[];
};

export type Project = {
  models: Record<string, ModelDefinition>;
  syncs: Record<string, SyncDefinition>;
  connections: Record<string, ConnectionDefinition>;
};
