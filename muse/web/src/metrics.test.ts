import { describe, expect, it } from "vitest";

import type { MetricsSnapshot } from "./api";
import { isvcMetrics, metricsQueryRangeSeconds, metricsRangeLabel } from "./metrics";

const snapshot: MetricsSnapshot = {
  updatedAt: 4_700,
  clusters: {
    test: {
      request_rate: {
        data: {
          resultType: "matrix",
          result: [
            {
              metric: { inferenceservice: "model" },
              values: [
                [1_000, "1"],
                [4_600, "2"],
              ],
            },
          ],
        },
      },
    },
    delayed: {
      request_rate: {
        data: {
          resultType: "matrix",
          result: [
            {
              metric: { inferenceservice: "other-model" },
              values: [
                [1_100, "1"],
                [4_700, "2"],
              ],
            },
          ],
        },
      },
    },
  },
};

describe("metrics range", () => {
  it("derives dashboard labels from returned timestamps", () => {
    expect(metricsQueryRangeSeconds(snapshot, "request_rate")).toBe(3_600);
    expect(metricsRangeLabel(metricsQueryRangeSeconds(snapshot, "request_rate"))).toBe("1h");
    expect(isvcMetrics(snapshot, "test", "model").rangeSeconds).toBe(3_600);
  });

  it("stays neutral when no range samples are present", () => {
    expect(metricsQueryRangeSeconds(null, "request_rate")).toBeNull();
    expect(metricsRangeLabel(null)).toBeNull();
  });
});
