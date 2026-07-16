import { describe, expect, it } from "vitest";

import type { CapacityResp, ClustersResp, MetricsSnapshot } from "./api";
import {
  loadClusterCapacity,
  metricAvailable,
  metricsHealth,
  metricsTransportState,
} from "./dataState";

const capacity = (total: number): CapacityResp => ({
  nodes: { total, ready: total, schedulable: total, acceleratorNodes: 0 },
  accelerators: [],
  byCluster: [],
  byIsland: [],
  byNvlinkDomain: [],
  byInstanceType: [],
});

describe("loadClusterCapacity", () => {
  it("retains healthy clusters when one capacity request fails", async () => {
    const clusters: ClustersResp = {
      count: 3,
      items: [
        {
          name: "ready",
          ready: true,
          status: "ready",
          nodes: 1,
          acceleratorNodes: 0,
          inferenceServices: 0,
        },
        {
          name: "syncing",
          ready: false,
          status: "syncing",
          nodes: 1,
          acceleratorNodes: 0,
          inferenceServices: 0,
        },
        {
          name: "failed",
          ready: false,
          status: "failed",
          error: "watcher stopped",
          nodes: 1,
          acceleratorNodes: 0,
          inferenceServices: 0,
        },
      ],
    };

    const result = await loadClusterCapacity(
      async () => clusters,
      async (name) => {
        if (name !== "ready") throw new Error("upstream unavailable");
        return capacity(1);
      },
    );

    expect(result[0].cap?.nodes.total).toBe(1);
    expect(result[0].capacityError).toBeUndefined();
    expect(result[1].cap).toBeUndefined();
    expect(result[1].status).toBe("syncing");
    expect(result[1].capacityError).toBe("upstream unavailable");
    expect(result[2].status).toBe("failed");
    expect(result[2].cacheError).toBe("watcher stopped");
  });
});

describe("metricsTransportState", () => {
  const snapshot: MetricsSnapshot = { updatedAt: 123, clusters: {} };

  it("keeps GET fallback failures separate from live-stream health", () => {
    expect(metricsTransportState(snapshot, "GET failed", null, true)).toEqual({
      error: null,
      loading: false,
    });
    expect(metricsTransportState({ updatedAt: 0, clusters: {} }, "GET failed", null, true)).toEqual(
      {
        error: null,
        loading: true,
      },
    );
    expect(metricsTransportState(snapshot, null, "stream disconnected", true)).toEqual({
      error: "stream disconnected",
      loading: false,
    });
    expect(metricsTransportState(null, "GET failed", null, true)).toEqual({
      error: "GET failed",
      loading: false,
    });
    expect(metricsTransportState(null, null, null, true)).toEqual({
      error: null,
      loading: true,
    });
  });
});

describe("metricsHealth", () => {
  it("distinguishes an uninitialized snapshot from a partial refresh", () => {
    expect(metricsHealth({ updatedAt: 0, clusters: {} })).toEqual({
      ready: false,
      queryCount: 0,
      failedQueryCount: 0,
      failedClusters: [],
    });

    const snapshot: MetricsSnapshot = {
      updatedAt: 123,
      clusters: {
        a: { qps: { status: "success", data: { resultType: "vector", result: [] } } },
        b: { qps: { error: "prometheus unavailable" }, p99: { error: "timeout" } },
      },
    };
    expect(metricsHealth(snapshot)).toEqual({
      ready: true,
      queryCount: 3,
      failedQueryCount: 2,
      failedClusters: ["b"],
    });
    expect(metricAvailable(snapshot, "qps")).toBe(true);
    expect(metricAvailable(snapshot, "p99")).toBe(false);
    expect(metricAvailable(snapshot, "missing")).toBe(false);
  });
});
