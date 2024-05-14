import { DestinationProvider, EnrichmentProvider } from "./types";
import { facebookAdsProvider } from "./destinations/facebook";
import { twitterAdsProvider } from "./destinations/twitter";
import { pdlEnrichmentProvider } from "./enrichment/pdl";
import { resendProvider } from "./destinations/resend";
import { googleAdsProvider } from "./destinations/google-ads";
import { ComponentChannel, createDestinationChannel, createEnrichmentChannel } from "../types/protocol";
import { ConnectionDefinition } from "../types/objects";
import assert from "assert";
import { DockerChannel } from "../docker/docker-channel";

export function getDestinationChannel(destination: ConnectionDefinition): ComponentChannel | undefined {
  if (destination.package) {
    if (destination.package.type === "npm") {
      throw new Error("NPM-based destination packages are not yet supported");
    }
    const image = destination.package.image;
    assert(image, "Docker image is required if package type is docker");
    return new DockerChannel(image);
  }
  const kind = destination.kind;
  assert(kind, "Destination kind is required if package is not provided");
  if (kind === "facebook") {
    return createDestinationChannel(facebookAdsProvider);
  } else if (kind === "twitter") {
    return createDestinationChannel(twitterAdsProvider);
  } else if (kind === "resend") {
    return createDestinationChannel(resendProvider);
  } else if (kind === "google-ads") {
    throw new Error(
      "Google Ads destination is not yet supported, although the code exists. It's still in development."
    );
    //return createDestinationChannel(googleAdsProvider);
  }
}

export function getEnrichmentProvider(en: ConnectionDefinition): ComponentChannel | undefined {
  if (en.package) {
    throw new Error("Package-based enrichments are not yet supported");
  }
  const kind = en.kind;
  assert(kind, "Enrichment kind is required if package is not provided");
  if (kind === "people-data-labs") {
    return createEnrichmentChannel(pdlEnrichmentProvider);
  }
}
