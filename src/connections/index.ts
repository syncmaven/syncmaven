import { DestinationProvider, EnrichmentProvider } from "./types";
import { facebookAdsProvider } from "./destinations/facebook";
import { twitterAdsProvider } from "./destinations/twitter";
import { pdlEnrichmentProvider } from "./enrichment/pdl";
import { resendProvider } from "./destinations/resend";
import { googleAdsProvider } from "./destinations/google-ads";
import { ComponentChannel, createDestinationChannel, createEnrichmentChannel } from "../types/protocol";

export function getDestinationChannel(kind: string): ComponentChannel | undefined {
  if (kind === "facebook") {
    return createDestinationChannel(facebookAdsProvider);
  } else if (kind === "twitter") {
    return createDestinationChannel(twitterAdsProvider);
  } else if (kind === "resend") {
    return createDestinationChannel(resendProvider);
  } else if (kind === "google-ads") {
    return createDestinationChannel(googleAdsProvider);
  }
}

export function getEnrichmentProvider(kind: string): ComponentChannel | undefined {
  if (kind === "people-data-labs") {
    return createEnrichmentChannel(pdlEnrichmentProvider);
  }
}
