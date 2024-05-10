import type { ConnectionDefinition, ModelDefinition } from "./objects.ts";
import type { SyncDefinition } from "./sync";

export type Factory<T> = () => T;
export type Project = {
  models: Record<string, Factory<ModelDefinition>>;
  syncs: Record<string, Factory<SyncDefinition>>;
  connection: Record<string, Factory<ConnectionDefinition>>;
};
