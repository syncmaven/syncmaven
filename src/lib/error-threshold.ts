export type ErrorThreshold = {
  success(): void;
  /**
   * @returns true if the error should consider as a failure
   */
  fail(): boolean;

  summary(): string;
};

export function createErrorThreshold({
  maxRatio = 0.2,
  minTotal = 100,
}: { maxRatio?: number; minTotal?: number } = {}): ErrorThreshold {
  let errors = 0;
  let success = 0;
  return {
    success() {
      success++;
    },
    fail() {
      const total = errors + success;
      errors++;
      return total >= minTotal && errors / total >= maxRatio;
    },
    summary() {
      const total = errors + success;
      return `${errors}/${total} - ${((errors / total) * 100).toFixed(2)}%`;
    },
  };
}
