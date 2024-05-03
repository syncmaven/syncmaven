import { DestinationProvider, EnrichmentProvider } from "./types";
import { facebookAdsProvider } from "./destinations/facebook";
import { twitterAdsProvider } from "./destinations/twitter";
import { pdlEnrichmentProvider } from "./enrichment/pdl";
import { resendProvider } from "./destinations/resend";
import { googleAdsProvider } from "./destinations/google-ads";

export function getDestinationProvider(kind: string): DestinationProvider | undefined {
  if (kind === "facebook") {
    return facebookAdsProvider;
  } else if (kind === "twitter") {
    return twitterAdsProvider;
  } else if (kind === "resend") {
    return resendProvider;
  } else if (kind === "google-ads") {
    return googleAdsProvider;
  }
}

export function getEnrichmentProvider(kind: string): EnrichmentProvider | undefined {
  if (kind === "people-data-labs") {
    return pdlEnrichmentProvider;
  }
}

