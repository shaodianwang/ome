import { describe, expect, it } from "vitest";

import type { ClusterItem } from "./api";
import { clusterLagDetail, summarizeClusterCaches } from "./clusterStatus";

function cluster(status: ClusterItem["status"], error?: string): ClusterItem {
  return {
    name: status,
    ready: status === "ready",
    status,
    error,
    nodes: 0,
    acceleratorNodes: 0,
    inferenceServices: 0,
  };
}

describe("clusterLagDetail", () => {
  it("is empty when every cluster cache is ready", () => {
    expect(clusterLagDetail([cluster("ready"), cluster("ready")])).toBe("");
  });

  it("names each lagging cluster, appending the error only when one exists", () => {
    expect(clusterLagDetail([cluster("ready"), cluster("failed", "dial tcp: refused")])).toBe(
      "failed: failed (dial tcp: refused)",
    );
    expect(clusterLagDetail([cluster("syncing")])).toBe("syncing: syncing");
  });

  it("humanizes timed_out and joins multiple laggards", () => {
    expect(clusterLagDetail([cluster("timed_out"), cluster("syncing")])).toBe(
      "timed_out: timed out · syncing: syncing",
    );
  });
});

describe("summarizeClusterCaches", () => {
  it("keeps failed, timed-out, syncing, and ready states visible in the sidebar", () => {
    const summary = summarizeClusterCaches([
      cluster("failed"),
      cluster("timed_out"),
      cluster("syncing"),
      cluster("ready"),
    ]);

    expect(summary).toEqual({
      syncing: 1,
      unhealthy: 2,
      line: "1 failed · 1 timed out · 1 syncing · 1 ready",
    });
  });
});
