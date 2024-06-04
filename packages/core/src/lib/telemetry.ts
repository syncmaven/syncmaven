import { isTruish } from "./util";
import { jitsuAnalytics } from "@jitsu/js";
import { syncmavenVersion, syncmavenVersionTag } from "./version";

const key = "t4RKFKQzF1ye0ddiWk9ZwXPI4szQGL7X:zKHhK6v3QZ0SWELmeV4pJs2q3BziW1Xc";
const host = "https://clx0w1kqg00003b6rg67nqa18.d.jitsu.com";
const telemetryAvailable = !isTruish(process.env.SYNCMAVEN_TELEMETRY_DISABLED) && syncmavenVersionTag !== "dev"

const jitsu = jitsuAnalytics({
  host: process.env.SYNCMAVEN_TELEMETRY_HOST || host,
  writeKey: process.env.SYNCMAVEN_TELEMETRY_KEY || key,
  debug: isTruish(process.env.SYNCMAVEN_TELEMETRY_DEBUG),
});

export const trackEvent: typeof jitsu.track = async (eventName: string, props = {}) => {
  if (telemetryAvailable) {
    try {
      await jitsu.track(eventName, {
        ...props,
        version: syncmavenVersion,
        tag: syncmavenVersionTag,
        os: process.platform,
        arch: process.arch,
        inDocker: isTruish(process.env.IN_DOCKER),
      });
    } catch (error: any) {
      console.debug(`Failed to send telemetry event '${eventName}': ${error.message || "unknown error"}`, props);
    }
  }
};
