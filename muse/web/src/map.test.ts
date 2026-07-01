import { describe, expect, it } from "vitest";

import type { CapacityResp, ClusterItem, EventItem, IsvcItem, ModelItem, RolloutItem } from "./api";
import { toClusterRows, toFeed, toModelCards, toRollouts, toWorkloadRows } from "./map";
import { matches } from "./util";

describe("toClusterRows", () => {
  const clusters: ClusterItem[] = [
    {
      name: "msl-gcp-usc1-1",
      ready: true,
      status: "ready",
      nodes: 1985,
      acceleratorNodes: 1729,
      inferenceServices: 0,
    },
    {
      name: "cw-use2-1",
      ready: false,
      status: "syncing",
      nodes: 2928,
      acceleratorNodes: 2900,
      inferenceServices: 8,
    },
  ];
  const capacity: CapacityResp = {
    nodes: { total: 4913, ready: 4913, schedulable: 4913, acceleratorNodes: 4629 },
    accelerators: [],
    byCluster: [
      {
        name: "msl-gcp-usc1-1",
        kind: "tpu",
        nodes: 1729,
        allocatable: 6916,
        allocated: 928,
        free: 5988,
      },
      {
        name: "cw-use2-1",
        kind: "gpu",
        nodes: 2900,
        allocatable: 11600,
        allocated: 11136,
        free: 464,
      },
    ],
    byIsland: [],
    byNvlinkDomain: [],
    byInstanceType: [],
  };

  it("infers cloud/region and derives accel + utilization per cluster", () => {
    const [gcp, cw] = toClusterRows(clusters, capacity);
    expect(gcp.cloud).toBe("gcp");
    expect(gcp.region).toBe("us-central1");
    expect(gcp.accel).toBe("6916 TPU");
    expect(gcp.nodes).toBe("1729 TPU · 1985 total");
    expect(gcp.utilPct).toBe(13); // 928 / 6916
    expect(gcp.status.tone).toBe("green");

    expect(cw.cloud).toBe("cw");
    expect(cw.accel).toBe("11600 GPU");
    expect(cw.utilPct).toBe(96);
    expect(cw.barTone).toBe("red"); // >= 90
    expect(cw.status.tone).toBe("amber"); // syncing
  });

  it("handles a cluster with no capacity entry", () => {
    const [row] = toClusterRows([clusters[0]], null);
    expect(row.accel).toBe("—");
    expect(row.utilPct).toBe(0);
  });
});

describe("toModelCards", () => {
  it("projects real metadata into ordered spec tags and a state pill", () => {
    const cluster: ModelItem[] = [
      {
        cluster: "cw",
        name: "llama-405b",
        architecture: "Llama",
        format: "safetensors",
        parameterSize: "405B",
        quantization: "fp8",
        capabilities: ["TEXT_TO_TEXT"],
        state: "Ready",
      },
    ];
    const [card] = toModelCards([], cluster);
    expect(card.scoped).toBe(true);
    expect(card.status).toEqual({ label: "Ready", tone: "green" });
    expect(card.where).toContain("cluster-scoped");
    expect(card.specs.map((s) => s.label)).toEqual([
      "Llama",
      "405B",
      "safetensors",
      "fp8",
      "TEXT_TO_TEXT",
    ]);
    expect(card.specs[0].accent).toBe(true); // architecture is accented
  });

  it("maps lifecycle state to pill tone and drops a zero param size", () => {
    const base: ModelItem[] = [
      { cluster: "cw", name: "importing", namespace: "ns", state: "Importing", parameterSize: "0" },
      { cluster: "cw", name: "unknown-state", namespace: "ns" },
    ];
    const [importing, unknown] = toModelCards(base, []);
    expect(importing.status).toEqual({ label: "Importing", tone: "amber" });
    expect(importing.scoped).toBe(false);
    expect(importing.specs.some((s) => s.label === "0")).toBe(false);
    expect(unknown.status).toEqual({ label: "Unknown", tone: "slate" });
  });
});

describe("toRollouts", () => {
  it("maps a progressing InferenceService to an in-progress card", () => {
    const items: RolloutItem[] = [
      {
        cluster: "cw",
        name: "r1",
        deploymentMode: "OMENative",
        ready: false,
        reason: "EngineNotReady",
      },
    ];
    const [r] = toRollouts(items);
    expect(r.strategy.label).toBe("OMENative");
    expect(r.gate.tone).toBe("amber");
    expect(r.note).toBe("EngineNotReady");
  });
});

describe("toWorkloadRows", () => {
  const isvcs: IsvcItem[] = [
    {
      cluster: "cw",
      name: "w1",
      namespace: "ns",
      model: "m",
      runtime: "rt",
      deploymentMode: "RawDeployment",
      ready: true,
      age: "3d",
    },
  ];

  it("marks a ready, stable workload", () => {
    const [row] = toWorkloadRows(isvcs, []);
    expect(row.ready).toBe("✓");
    expect(row.status.tone).toBe("green");
    expect(row.rollout.label).toBe("Stable");
    expect(row.mode.label).toBe("RawDeployment");
  });

  it("flags a workload that is rolling out", () => {
    const [row] = toWorkloadRows(
      [{ cluster: "cw", name: "w1", namespace: "ns", ready: false }],
      [{ cluster: "cw", name: "w1", namespace: "ns", ready: false }],
    );
    expect(row.rollout.label).toBe("Rolling out");
    expect(row.status.tone).toBe("red");
    expect(row.ready).toBe("✗");
  });
});

describe("toFeed", () => {
  const now = Math.floor(Date.now() / 1000);

  it("chooses icon/tone by kind and action, with relative time", () => {
    const [applied] = toFeed([
      { cluster: "cw", kind: "InferenceService", name: "x", action: "applied", at: now - 3600 },
    ]);
    expect(applied.icon).toBe("deploy");
    expect(applied.tone).toBe("blue");
    expect(applied.when).toBe("1h ago");

    const [deleted] = toFeed([
      { cluster: "cw", kind: "BaseModel", name: "y", action: "deleted", at: now - 30 },
    ] as EventItem[]);
    expect(deleted.icon).toBe("warning");
    expect(deleted.tone).toBe("amber");
    expect(deleted.when).toBe("30s ago");
  });
});

describe("matches", () => {
  it("is case-insensitive, skips undefined, and treats empty query as match-all", () => {
    expect(matches("", "anything")).toBe(true);
    expect(matches("LL", "llama", undefined)).toBe(true);
    expect(matches("xyz", "llama", "safetensors")).toBe(false);
    expect(matches("  ", "llama")).toBe(true); // whitespace-only = empty
  });
});
