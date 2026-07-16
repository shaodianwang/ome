import { describe, expect, it } from "vitest";

import type { ClusterItem } from "./api";
import { summarizeClusterCaches } from "./clusterStatus";

function cluster(status: ClusterItem["status"]): ClusterItem {
  return {
    name: status,
    ready: status === "ready",
    status,
    nodes: 0,
    acceleratorNodes: 0,
    inferenceServices: 0,
  };
}

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
