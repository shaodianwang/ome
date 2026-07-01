import { describe, expect, it } from "vitest";

import type {
  CapacityResp,
  ClusterItem,
  ComponentItem,
  EventItem,
  IsvcItem,
  ModelItem,
  RolloutItem,
} from "./api";
import {
  loadSummary,
  modelUsage,
  replicaRollup,
  toClusterRows,
  toFeed,
  toModelCards,
  toRollouts,
  toWorkloadRows,
} from "./map";
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
    expect(card.where).toBe("cw");
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

  it("carries source, disabled, a context tag, and the live used-by count", () => {
    const base: ModelItem[] = [
      {
        cluster: "cw",
        name: "llama-405b",
        namespace: "ns",
        source: "manifold",
        maxTokens: 131072,
        disabled: true,
      },
    ];
    const usage = new Map([["llama-405b", 3]]);
    const [card] = toModelCards(base, [], usage);
    expect(card.source).toBe("manifold");
    expect(card.disabled).toBe(true);
    expect(card.usedBy).toBe(3);
    expect(card.specs.map((s) => s.label)).toContain("128k ctx");
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
  it("projects endpoint, replica rollup, load, and status for a ready workload", () => {
    const isvcs: IsvcItem[] = [
      {
        cluster: "cw",
        name: "w1",
        namespace: "ns",
        model: "m",
        runtime: "rt",
        deploymentMode: "RawDeployment",
        ready: true,
        url: "https://w1.example",
        components: [{ name: "engine", ready: true, currentReplicas: 3, desiredReplicas: 3 }],
        loadedCopies: 3,
        age: "3d",
      },
    ];
    const [row] = toWorkloadRows(isvcs);
    expect(row.status).toEqual({ label: "Ready", tone: "green" });
    expect(row.mode.label).toBe("RawDeployment");
    expect(row.endpoint).toBe("https://w1.example");
    expect(row.replicas).toEqual({ label: "3/3", tone: "green" });
    expect(row.load).toBe("3 loaded");
    expect(row.reason).toBeUndefined();
  });

  it("surfaces the reason when a workload is not ready", () => {
    const [row] = toWorkloadRows([
      { cluster: "cw", name: "w1", namespace: "ns", ready: false, reason: "BaseModelNotReady" },
    ]);
    expect(row.status.tone).toBe("red");
    expect(row.reason).toBe("BaseModelNotReady");
  });
});

describe("replicaRollup", () => {
  const comps = (cs: ComponentItem[]) => cs;
  it("sums ready/desired across components and picks a health tone", () => {
    expect(
      replicaRollup(comps([{ name: "engine", currentReplicas: 2, desiredReplicas: 2 }])),
    ).toEqual({ label: "2/2", tone: "green" });
    expect(
      replicaRollup(
        comps([
          { name: "engine", currentReplicas: 1, desiredReplicas: 2 },
          { name: "decoder", currentReplicas: 1, desiredReplicas: 1 },
        ]),
      ),
    ).toEqual({ label: "2/3", tone: "amber" });
  });

  it("returns an em dash when there are no components or no counts", () => {
    expect(replicaRollup(undefined)).toEqual({ label: "—", tone: "slate" });
    expect(replicaRollup([])).toEqual({ label: "—", tone: "slate" });
  });
});

describe("loadSummary", () => {
  it("summarizes loaded/failed copies, else a non-steady model state", () => {
    expect(loadSummary({ cluster: "c", name: "n", loadedCopies: 4, failedCopies: 1 })).toBe(
      "4 loaded · 1 failed",
    );
    expect(loadSummary({ cluster: "c", name: "n", modelState: "InProgress" })).toBe("InProgress");
    expect(loadSummary({ cluster: "c", name: "n", modelState: "UpToDate" })).toBeUndefined();
    expect(loadSummary({ cluster: "c", name: "n" })).toBeUndefined();
  });
});

describe("modelUsage", () => {
  it("counts InferenceServices per referenced model name", () => {
    const usage = modelUsage([
      { cluster: "c", name: "a", model: "llama" },
      { cluster: "c", name: "b", model: "llama" },
      { cluster: "c", name: "c", model: "mixtral" },
      { cluster: "c", name: "d" },
    ]);
    expect(usage.get("llama")).toBe(2);
    expect(usage.get("mixtral")).toBe(1);
    expect(usage.size).toBe(2);
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
