import type { Simplify } from "type-fest";
import { z } from "zod";

export const ModelDefinition = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  description: z.string().optional(),
  query: z.string(),
  cursor: z.string().optional(),
  deleteField: z.string().optional(),
  pageSize: z.number().optional(),
  pauseBetweenPagesMs: z.number().optional(),
  datasource: z.union([
    z.string(),
    z.object({
      type: z.string(),
      credentials: z.any(),
    }),
  ]),
});

export type ModelDefinition = Simplify<z.infer<typeof ModelDefinition>>;

export const EnrichmentSettings = z.object({
  connection: z.string(),
  options: z.any(),
});

export type EnrichmentSettings = Simplify<z.infer<typeof EnrichmentSettings>>;

export const ConnectionDefinition = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  package: z.object({
    type: z.string().default("docker").optional(),
    image: z.string().optional(),
    package: z.string().optional(),
    command: z.string().optional(),
    dir: z.string().optional(),
  }),
  credentials: z.any(),
});

export const SyncDefinition = z.object({
  disabled: z.boolean().optional(),
  id: z.string().optional(),
  name: z.string().optional(),
  description: z.string().optional(),
  model: z.string(),
  destination: z.union([z.string(), ConnectionDefinition]),
  stream: z.string().optional(),
  mapping: z.any(),
  enrichment: EnrichmentSettings.optional(),
  enrichments: z.array(EnrichmentSettings).optional(),
  checkpointEvery: z.number().describe("End stream and continue with a new one every N rows.").optional(),
  options: z.any(),
});

function getEnrichments(sync: SyncDefinition): EnrichmentSettings[] {
  return sync.enrichments || (sync.enrichment ? [sync.enrichment] : []);
}

export type SyncDefinition = Simplify<z.infer<typeof SyncDefinition>>;

export type ConnectionDefinition = Simplify<z.infer<typeof ConnectionDefinition>>;
