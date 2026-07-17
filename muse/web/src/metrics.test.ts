import { describe, expect, it } from "vitest";

import type { MetricsSnapshot } from "./api";
import type { IsvcMetrics } from "./metrics";
import {
  fleetTotals,
  isvcMetrics,
  latestAlignedRatio,
  metricsQueryRangeSeconds,
  metricsRangeLabel,
  perfRows,
  ratioSeries,
  sliceIsvcMetrics,
} from "./metrics";

const snapshot: MetricsSnapshot = {
  updatedAt: 4_700,
  clusters: {
    test: {
      request_rate: {
        data: {
          resultType: "matrix",
          result: [
            {
              metric: { namespace: "team-a", inferenceservice: "model" },
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
              metric: { namespace: "team-a", inferenceservice: "other-model" },
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
    const metrics = isvcMetrics(snapshot, "test", "team-a", "model", true);
    expect(metrics.rangeSeconds).toBe(3_600);
    expect(metrics.by.request_rate.series).toEqual([
      { at: 1_000, value: 1 },
      { at: 4_600, value: 2 },
    ]);
  });

  it("stays neutral when no range samples are present", () => {
    expect(metricsQueryRangeSeconds(null, "request_rate")).toBeNull();
    expect(metricsRangeLabel(null)).toBeNull();
  });

  it("selects an exact namespace even when inventory identity is ambiguous", () => {
    const metrics = isvcMetrics(snapshot, "test", "team-a", "model", false);
    expect(metrics.present).toBe(true);
    expect(metrics.identityScoped).toBe(true);
  });

  it("refuses a legacy name-only series when deployment identity is ambiguous", () => {
    const legacy: MetricsSnapshot = {
      updatedAt: snapshot.updatedAt,
      clusters: {
        test: {
          request_rate: {
            data: {
              resultType: "matrix",
              result: [
                {
                  metric: { inferenceservice: "model" },
                  values: [[4_600, "2"]],
                },
                {
                  metric: { namespace: "team-b", inferenceservice: "other-model" },
                  values: [[4_600, "3"]],
                },
              ],
            },
          },
        },
      },
    };
    const metrics = isvcMetrics(legacy, "test", "team-a", "model", false);
    expect(metrics.present).toBe(false);
    expect(metrics.identityScoped).toBe(false);
    expect(metrics.rangeSeconds).toBeNull();
    expect(metrics.by.request_rate).toEqual({ last: null, series: [] });
  });

  it("keeps same-named deployments in separate namespace rows", () => {
    const sameName = structuredClone(snapshot);
    sameName.clusters.test.request_rate.data?.result.push({
      metric: { namespace: "team-b", inferenceservice: "model" },
      values: [[4_600, "9"]],
    });

    expect(
      perfRows(sameName).map(({ cluster, namespace, isvc, qps }) => ({
        cluster,
        namespace,
        isvc,
        qps,
      })),
    ).toEqual([
      { cluster: "test", namespace: "team-b", isvc: "model", qps: 9 },
      { cluster: "test", namespace: "team-a", isvc: "model", qps: 2 },
      { cluster: "delayed", namespace: "team-a", isvc: "other-model", qps: 2 },
    ]);
  });
});

describe("timestamp-aware shaping", () => {
  it("aggregates fleet samples by timestamp rather than array index", () => {
    const totals = fleetTotals({
      updatedAt: 300,
      clusters: {
        a: {
          request_rate: {
            data: {
              resultType: "matrix",
              result: [
                {
                  metric: { inferenceservice: "a" },
                  values: [
                    [100, "1"],
                    [200, "2"],
                  ],
                },
              ],
            },
          },
        },
        b: {
          request_rate: {
            data: {
              resultType: "matrix",
              result: [
                {
                  metric: { inferenceservice: "b" },
                  values: [
                    [150, "10"],
                    [200, "20"],
                  ],
                },
              ],
            },
          },
        },
      },
    });

    expect(totals.requestSeries).toEqual([
      { at: 100, value: 1 },
      { at: 150, value: 10 },
      { at: 200, value: 22 },
    ]);
  });

  it("calculates ratios only for matching timestamps", () => {
    expect(
      ratioSeries(
        [
          { at: 100, value: 1 },
          { at: 200, value: 2 },
        ],
        [
          { at: 150, value: 10 },
          { at: 200, value: 20 },
        ],
        100,
      ),
    ).toEqual([{ at: 200, value: 10 }]);

    expect(latestAlignedRatio([{ at: 100, value: 1 }], [{ at: 100, value: 10 }], 100)).toBe(10);
    expect(latestAlignedRatio([{ at: 100, value: 1 }], [{ at: 200, value: 10 }], 100)).toBeNull();
  });

  it("slices every deployment series against one inclusive timestamp window", () => {
    const metrics: IsvcMetrics = {
      present: true,
      identityScoped: true,
      evaluatedAt: 300,
      rangeSeconds: 200,
      by: {
        request_rate: {
          last: 3,
          series: [
            { at: 100, value: 1 },
            { at: 200, value: 2 },
            { at: 300, value: 3 },
          ],
        },
        latency_p99: {
          last: 0.2,
          series: [
            { at: 100, value: 0.1 },
            { at: 150, value: 0.2 },
          ],
        },
        current_only: { last: 7, series: [] },
      },
    };

    const sliced = sliceIsvcMetrics(metrics, 100);

    expect(sliced.by.request_rate).toEqual({
      last: 3,
      series: [
        { at: 200, value: 2 },
        { at: 300, value: 3 },
      ],
    });
    expect(sliced.by.latency_p99).toEqual({ last: null, series: [] });
    expect(sliced.by.current_only).toEqual({ last: 7, series: [] });
    expect(sliced.rangeSeconds).toBe(100);
    expect(sliced.evaluatedAt).toBe(300);
    expect(sliced.present).toBe(true);
    expect(metrics.by.request_rate.series).toHaveLength(3);
  });

  it("does not treat an old deployment's final samples as the current window", () => {
    const metrics: IsvcMetrics = {
      present: true,
      identityScoped: true,
      evaluatedAt: 1_000,
      rangeSeconds: 200,
      by: {
        request_rate: {
          last: 3,
          series: [
            { at: 100, value: 1 },
            { at: 200, value: 2 },
            { at: 300, value: 3 },
          ],
        },
      },
    };

    const sliced = sliceIsvcMetrics(metrics, 100);

    expect(sliced.by.request_rate).toEqual({ last: null, series: [] });
    expect(sliced.present).toBe(false);
    expect(sliced.rangeSeconds).toBeNull();
  });
});
