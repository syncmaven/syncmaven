import { DestinationChannel, EnrichmentChannel, MessageHandler } from "../types/protocol";
import { ConnectionDefinition } from "../types/objects";
import assert from "assert";
import { DockerChannel } from "../docker/docker-channel";

export function getDestinationChannel(destination: ConnectionDefinition, messagesHandler: MessageHandler): DestinationChannel | undefined {
  if (destination.package) {
    if (destination.package.type === "npm") {
      throw new Error("NPM-based destination packages are not yet supported");
    }
    const image = destination.package.image;
    assert(image, "Docker image is required if package type is docker");
    return new DockerChannel(image, messagesHandler);
  } else {
    throw new Error("Only NPM or Docker packages are supported as destinations.");
  }

}

export function getEnrichmentProvider(en: ConnectionDefinition, messagesHandler: MessageHandler): EnrichmentChannel | undefined {
    throw new Error("Package-based enrichments are not yet supported");
}
