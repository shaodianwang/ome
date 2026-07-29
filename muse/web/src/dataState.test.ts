import { describe, expect, it } from "vitest";

import type { CapacityResp, ClustersResp, MetricsSnapshot } from "./api";
import {
  loadClusterCapacity,
  metricAvailable,
  metricsHealth,
  metricsQueryIssue,
  metricsSourceIssues,
  summarizeFleetIssues,
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

describe("metricsQueryIssue", () => {
  it("retains exact query and cluster counts", () => {
    expect(
      metricsQueryIssue(
        { ready: true, queryCount: 3, failedQueryCount: 2, failedClusters: ["a", "b"] },
        3,
      ),
    ).toEqual({
      domain: "Metrics queries",
      message: "2 of 3 queries failed across 2 clusters (a, b)",
      tone: "warning",
    });
    expect(
      metricsQueryIssue({ ready: true, queryCount: 0, failedQueryCount: 0, failedClusters: [] }, 1),
    ).toEqual({
      domain: "Metrics queries",
      message: "no query results returned across 1 scoped cluster",
      tone: "danger",
    });
    expect(
      metricsQueryIssue({ ready: true, queryCount: 0, failedQueryCount: 0, failedClusters: [] }, 0),
    ).toBeNull();
  });
});

describe("metricsSourceIssues", () => {
  it("reports each affected cluster once without inflating query failures", () => {
    expect(
      metricsSourceIssues({
        updatedAt: 123,
        clusters: { available: {}, missing: {}, forbidden: {} },
        sources: {
          available: { status: "available", target: "ome/prom:9090" },
          missing: { status: "unconfigured", target: "ome/prom:9090" },
          forbidden: { status: "unavailable", target: "ome/prom:9090" },
        },
      }),
    ).toEqual([
      {
        domain: "Metrics coverage",
        message: "serving metrics are not configured for 1 cluster (missing)",
        tone: "warning",
      },
      {
        domain: "Metrics sources",
        message: "1 configured cluster could not be reached (forbidden)",
        tone: "danger",
      },
    ]);
  });
});

describe("summarizeFleetIssues", () => {
  it("renders one punctuated warning without dropping domains", () => {
    expect(
      summarizeFleetIssues([
        { domain: "Activity stream", message: "reconnecting.", tone: "warning" },
        {
          domain: "Metrics stream",
          message: "disconnected; reconnecting. Showing the snapshot from 2m ago.",
          tone: "warning",
        },
        {
          domain: "Metrics queries",
          message: "2 of 3 queries failed across 2 clusters (a, b)",
          tone: "warning",
        },
      ]),
    ).toEqual({
      tone: "warning",
      title: "Some fleet data is degraded.",
      detail:
        "Activity stream: reconnecting; Metrics stream: disconnected; reconnecting. Showing the snapshot from 2m ago; Metrics queries: 2 of 3 queries failed across 2 clusters (a, b).",
    });
  });

  it("promotes mixed issues to danger and removes exact duplicates", () => {
    const unavailable = { domain: "Capacity", message: "unavailable", tone: "danger" as const };
    expect(summarizeFleetIssues([unavailable, unavailable])).toEqual({
      tone: "danger",
      title: "Some fleet data is stale or unavailable.",
      detail: "Capacity: unavailable.",
    });
    expect(summarizeFleetIssues([])).toBeNull();
  });
});
