import { z } from "zod";
import {
  EnrichmentProvider,
  rpc,
  RpcError,
  stdProtocol,
} from "@syncmaven/node-cdk";
import { emailHash, normalizeEmail } from "@syncmaven/node-cdk";

export const PeopleDataLabsCredentials = z.object({
  apiKey: z.string(),
});

export type PeopleDataLabsCredentials = z.infer<
  typeof PeopleDataLabsCredentials
>;

export function isTruish(x: any): boolean {
  return x === "true" || x === "1";
}

export const pdlEnrichmentProvider: EnrichmentProvider = {
  name: "pdl",
  credentialsType: PeopleDataLabsCredentials,

  createEnrichment: (config, ctx) => {
    let rateLimitExpires: undefined | Date;
    let usageLimitExceeded = false;
    return {
      enrichRow: async (row) => {
        const { apiKey } = config.credentials;
        const cacheNamespace =
          config.options.cacheNamespace || emailHash(apiKey);
        const cacheTTLHours = config.options.cacheTTLHours || 24 * 30;
        const email = normalizeEmail(row.email);
        const cacheKey = ["pdl", `namespace=${cacheNamespace}`, email];
        const cached = await ctx.store.get(cacheKey);
        let response;
        if (cached) {
          const { expire } = cached;
          if (new Date(expire) > new Date()) {
            response = cached.response;
          } else {
            console.debug(
              `Record for ${email} expired at ${expire}, cache key=${cacheKey.join("::")}`,
            );
          }
        }
        if (!response && !isTruish(config.options.cacheOnly)) {
          try {
            if (rateLimitExpires && rateLimitExpires > new Date()) {
              //console.debug(`Not enriching ${email}, since PDL rate limit is still active`);
              return [row];
              //rate limit is still active,
            } else if (usageLimitExceeded) {
              //console.debug(`Not enriching ${email}, since PDL rate usage limit is exceeded`);
              return [row];
            } else {
              response = await rpc(
                `https://api.peopledatalabs.com/v5/person/enrich?email=${email}&api_key=${apiKey}`,
              );
            }
          } catch (e: any) {
            if (e instanceof RpcError) {
              if (e.statusCode === 404) {
                console.debug(`Email ${email} is not known to PDL`);
                response = { data: { emails: [] } };
              } else if (e.statusCode === 429) {
                // Rate limit exceeded, this thing usually replies with x-ratelimit-limit': '{"minute": 100},
                //so let's add a minute
                console.warn(
                  `Rate limit exceeded while fetching PDL data for ${email}. See headers`,
                  e.headers,
                );
                rateLimitExpires = new Date(new Date().getTime() + 1000 * 60);
                return [row];
              } else if (e.statusCode === 402) {
                console.warn(
                  `Error fetching PDL data for ${email}. Usage limit exceeded. No further requests will be made.`,
                );
                usageLimitExceeded = true;
                return [row];
              } else {
                console.error(
                  `Error fetching PDL data for ${email}. Status code ${e.statusCode}: ${e.message}`,
                );
                return [row];
              }
            } else {
              console.error(
                `Error fetching PDL data for ${email}: ${e.message}`,
              );
              return [row];
            }
          }
          await ctx.store.set(cacheKey, {
            response,
            expire: new Date(
              new Date().getTime() + 1000 * 60 * 60 * cacheTTLHours,
            ),
          });
        }
        const emails: string[] = (response?.data?.emails || [])
          .map((email: any) => email.address as string)
          .filter(Boolean)
          .map(normalizeEmail)
          .filter((e) => e !== email);
        const additionalRecords = emails.map((email) => ({
          ...row,
          email,
        }));
        return [row, ...additionalRecords];
      },
    };
  },
};
