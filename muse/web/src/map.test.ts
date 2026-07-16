import { describe, expect, it } from "vitest";

import type { ComponentItem, EventItem, IsvcItem, RolloutItem } from "./api";
import {
  loadSummary,
  modelUsage,
  replicaRollup,
  runtimeUsage,
  toFeed,
  toRollouts,
  toWorkloadRows,
} from "./map";
import { resourceKey } from "./resource";
import { matches } from "./util";

describe("toRollouts", () => {
  it("maps a progressing InferenceService to an in-progress card", () => {
    const items: RolloutItem[] = [
      {
        cluster: "cw",
        name: "r1",
        namespace: "team-a",
        deploymentMode: "OMENative",
        ready: false,
        reason: "EngineNotReady",
      },
    ];
    const [r] = toRollouts(items);
    expect(r.gate.tone).toBe("amber");
    expect(r.note).toBe("EngineNotReady");
    expect(r.namespace).toBe("team-a");
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
  it("keeps duplicate model names separate by cluster, scope, and namespace", () => {
    const usage = modelUsage([
      {
        cluster: "c",
        name: "a",
        model: "llama",
        modelRef: { kind: "BaseModel", namespace: "team-a", name: "llama" },
      },
      {
        cluster: "c",
        name: "b",
        model: "llama",
        modelRef: { kind: "BaseModel", namespace: "team-b", name: "llama" },
      },
      {
        cluster: "c",
        name: "c",
        model: "llama",
        modelRef: { kind: "ClusterBaseModel", name: "llama" },
      },
      {
        cluster: "other",
        name: "d",
        model: "llama",
        modelRef: { kind: "ClusterBaseModel", name: "llama" },
      },
    ]);
    expect(
      usage.get(
        resourceKey({ cluster: "c", kind: "BaseModel", namespace: "team-a", name: "llama" }),
      ),
    ).toBe(1);
    expect(
      usage.get(
        resourceKey({ cluster: "c", kind: "BaseModel", namespace: "team-b", name: "llama" }),
      ),
    ).toBe(1);
    expect(usage.get(resourceKey({ cluster: "c", kind: "ClusterBaseModel", name: "llama" }))).toBe(
      1,
    );
    expect(usage.size).toBe(4);
  });
});

describe("runtimeUsage", () => {
  it("keeps namespaced and cluster-scoped runtimes with the same name separate", () => {
    const usage = runtimeUsage([
      {
        cluster: "c",
        namespace: "team-a",
        name: "a",
        runtime: "vllm",
        runtimeRef: { kind: "ServingRuntime", namespace: "team-a", name: "vllm" },
      },
      {
        cluster: "c",
        namespace: "team-b",
        name: "b",
        runtime: "vllm",
        runtimeRef: { kind: "ServingRuntime", namespace: "team-b", name: "vllm" },
      },
      {
        cluster: "c",
        name: "c",
        runtime: "vllm",
        runtimeRef: { kind: "ClusterServingRuntime", name: "vllm" },
      },
    ]);

    expect(
      usage.get(
        resourceKey({
          cluster: "c",
          kind: "ServingRuntime",
          namespace: "team-b",
          name: "vllm",
        }),
      ),
    ).toBe(1);
    expect(
      usage.get(resourceKey({ cluster: "c", kind: "ClusterServingRuntime", name: "vllm" })),
    ).toBe(1);
    expect(usage.size).toBe(3);
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
