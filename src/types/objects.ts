import type {Simplify} from "type-fest";
import {z} from "zod";


export const ModelDefinition = z.object({
    id: z.string().optional(),
    name: z.string().optional(),
    description: z.string().optional(),
    query: z.string(),
    datasource: z.string(),
})

export type ModelDefinition = Simplify<z.infer<typeof ModelDefinition>>;

export const EnrichmentSettings = z.object({
    connection: z.string(),
    options: z.any(),
});

export type EnrichmentSettings = Simplify<z.infer<typeof EnrichmentSettings>>;

export const SyncDefinition = z.object({
    disabled: z.boolean().optional(),
    id: z.string().optional(),
    name: z.string().optional(),
    description: z.string().optional(),
    model: z.string(),
    destination: z.string(),
    stream: z.string().optional(),
    mapping: z.any(),
    enrichment: EnrichmentSettings.optional(),
    enrichments: z.array(EnrichmentSettings).optional(),
    options: z.any(),
})

function getEnrichments(sync: SyncDefinition): EnrichmentSettings[] {
    return sync.enrichments || (sync.enrichment ? [sync.enrichment] : []);
}

export type SyncDefinition = Simplify<z.infer<typeof SyncDefinition>>;

export type ConnectionDefinition = Simplify<z.infer<typeof ConnectionDefinition>>;


export const ConnectionDefinition = z.object({
    id: z.string().optional(),
    name: z.string().optional(),
    kind: z.string(),
    credentials: z.any(),
})
