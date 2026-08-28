import { afterEach, describe, expect, it, vi } from "vitest";

import type { ComponentItem, EventItem, IsvcItem, RolloutItem } from "./api";
import {
  canaryNote,
  componentCells,
  componentTone,
  componentTransition,
  conditionRank,
  isRolling,
  loadSummary,
  modelUsage,
  replicaRollup,
  rolloutProgress,
  runtimeReference,
  runtimeUsage,
  shortRevision,
  sortConditions,
  toFeed,
  toRollouts,
  toWorkloadRows,
  transitionLabel,
} from "./map";
import { resourceKey } from "./resource";
import { matches } from "./util";

describe("toRollouts", () => {
  it("fuses real replica progress and component state from the matching deployment", () => {
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
    const isvcs: IsvcItem[] = [
      {
        cluster: "cw",
        name: "r1",
        namespace: "team-a",
        components: [
          {
            name: "engine",
            readyReplicas: 3,
            totalReplicas: 4,
            rolloutPhase: "Progressing",
            revision: "r1-engine-rev-abc",
          },
          { name: "router", readyReplicas: 1, totalReplicas: 1 },
        ],
      },
    ];
    const [r] = toRollouts(items, isvcs);
    expect(r.gate.tone).toBe("amber");
    expect(r.note).toBe("EngineNotReady");
    expect(r.namespace).toBe("team-a");
    expect(r.barPct).toBe(80); // 4/5 replicas ready — real progress, not a constant
    expect(r.sel).toEqual({
      cluster: "cw",
      kind: "InferenceService",
      name: "r1",
      namespace: "team-a",
    });
    expect(r.components?.map((c) => c.label)).toEqual(["engine 3/4", "router 1/1"]);
  });

  it("degrades to zero progress when the deployment row is missing", () => {
    const [r] = toRollouts([{ cluster: "cw", name: "gone", ready: false }], []);
    expect(r.barPct).toBe(0);
    expect(r.components).toEqual([]);
  });

  it("carries the in-flight canary through the fusion", () => {
    const [r] = toRollouts(
      [{ cluster: "cw", name: "c1", ready: false }],
      [{ cluster: "cw", name: "c1", canary: { currentStep: 2, trafficWeight: 25 } }],
    );
    expect(r.canary).toEqual({ currentStep: 2, trafficWeight: 25 });
  });
});

describe("rollout fusion helpers", () => {
  it("computes progress from lifecycle counts only", () => {
    expect(rolloutProgress(undefined)).toBeNull();
    expect(rolloutProgress([{ name: "e" }])).toBeNull();
    expect(
      rolloutProgress([
        { name: "e", readyReplicas: 240, totalReplicas: 251 },
        { name: "d", readyReplicas: 61, totalReplicas: 65 },
      ]),
    ).toBe(95);
  });

  it("marks scale-ups in flight on the component chip", () => {
    const [cell] = componentCells([
      { name: "engine", readyReplicas: 8, totalReplicas: 8, desiredReplicas: 16 },
    ]);
    expect(cell.label).toBe("engine 8/8→16");
  });

  it("detects a rollout from any non-Stable component phase", () => {
    expect(isRolling([{ name: "e", rolloutPhase: "Stable" }])).toBe(false);
    expect(isRolling([{ name: "e", rolloutPhase: "Progressing" }])).toBe(true);
    expect(isRolling([{ name: "e" }])).toBe(false);
  });

  it("counts only target-revision Instances while a component converges", () => {
    // Surge: all 5 old-revision pods are still ready, but only 1 Instance is
    // ready on the target revision — the bar must not read 100%.
    expect(
      rolloutProgress([
        {
          name: "e",
          readyReplicas: 5,
          totalReplicas: 5,
          currentRevision: "w-engine-old1",
          updateRevision: "w-engine-new2",
          updatedReplicas: 2,
          updatedReadyReplicas: 1,
        },
      ]),
    ).toBe(20);
    // Missing updated counts fall back to ready — never zero out the bar.
    expect(
      rolloutProgress([
        {
          name: "e",
          readyReplicas: 4,
          totalReplicas: 5,
          currentRevision: "w-engine-old1",
          updateRevision: "w-engine-new2",
        },
      ]),
    ).toBe(80);
  });

  it("exposes the revision transition only mid-rollout", () => {
    expect(
      componentTransition({
        name: "engine",
        currentRevision: "w-engine-old1",
        updateRevision: "w-engine-new2",
      }),
    ).toEqual({ from: "w-engine-old1", to: "w-engine-new2" });
    expect(
      componentTransition({
        name: "engine",
        currentRevision: "w-engine-new2",
        updateRevision: "w-engine-new2",
      }),
    ).toBeUndefined();
    expect(componentTransition({ name: "engine" })).toBeUndefined();
    // Initial rollout: nothing runs yet, so there is no `from`.
    expect(componentTransition({ name: "engine", updateRevision: "w-engine-new2" })).toEqual({
      from: undefined,
      to: "w-engine-new2",
    });

    const [cell] = componentCells([
      {
        name: "engine",
        readyReplicas: 5,
        totalReplicas: 5,
        currentRevision: "w-engine-old1",
        updateRevision: "w-engine-new2",
      },
    ]);
    expect(cell.transition).toEqual({ from: "w-engine-old1", to: "w-engine-new2" });
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

  it("prefers lifecycle ready/total over the autoscaler scale count", () => {
    // 38 scaled, 8 still initializing: readyReplicas (30) is the numerator,
    // not currentReplicas (38).
    expect(
      replicaRollup(
        comps([
          {
            name: "engine",
            readyReplicas: 30,
            totalReplicas: 38,
            currentReplicas: 38,
            desiredReplicas: 38,
          },
        ]),
      ),
    ).toEqual({ label: "30/38", tone: "amber" });
  });

  it("returns an em dash when there are no components or no counts", () => {
    expect(replicaRollup(undefined)).toEqual({ label: "—", tone: "slate" });
    expect(replicaRollup([])).toEqual({ label: "—", tone: "slate" });
  });
});

describe("condition and component tiles", () => {
  it("orders condition tiles failing first, disabled-by-spec last", () => {
    const conds = [
      { type: "IngressReady", status: "False", reason: "IngressDisabled" },
      { type: "PredictorReady", status: "True" },
      { type: "Ready", status: "False", reason: "EngineNotReady" },
    ];
    expect(sortConditions(conds).map((c) => c.type)).toEqual([
      "Ready",
      "PredictorReady",
      "IngressReady",
    ]);
    expect(conditionRank(conds[0])).toBe("disabled");
    expect(conditionRank(conds[1])).toBe("normal");
    expect(conditionRank(conds[2])).toBe("bad");
  });

  it("tones a component by how far live replicas are from the target", () => {
    expect(componentTone({ name: "e", ready: true, currentReplicas: 2, desiredReplicas: 2 })).toBe(
      "ok",
    );
    expect(componentTone({ name: "e", ready: false, currentReplicas: 1, desiredReplicas: 2 })).toBe(
      "warn",
    );
    expect(componentTone({ name: "e", ready: false, currentReplicas: 0, desiredReplicas: 2 })).toBe(
      "bad",
    );
  });

  it("tones from lifecycle readiness, not the autoscaler scale count", () => {
    // readyReplicas (30) < totalReplicas (38) → warn, even though the
    // autoscaler reports currentReplicas == desiredReplicas == 38.
    expect(
      componentTone({
        name: "engine",
        ready: true,
        readyReplicas: 30,
        totalReplicas: 38,
        currentReplicas: 38,
        desiredReplicas: 38,
      }),
    ).toBe("warn");
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

  it("counts under the operator-resolved identity, not the declared one", () => {
    // The spec declares ClusterServingRuntime, but a namespaced ServingRuntime
    // with the same name shadows it — the server's resolved ref wins.
    const usage = runtimeUsage([
      {
        cluster: "c",
        namespace: "team-a",
        name: "a",
        runtime: "vllm",
        runtimeRef: { kind: "ClusterServingRuntime", name: "vllm" },
        resolvedRuntimeRef: {
          kind: "ServingRuntime",
          namespace: "team-a",
          name: "vllm",
          source: "spec",
        },
      },
    ]);
    expect(
      usage.get(
        resourceKey({ cluster: "c", kind: "ServingRuntime", namespace: "team-a", name: "vllm" }),
      ),
    ).toBe(1);
    expect(usage.size).toBe(1);
  });
});

describe("runtimeReference", () => {
  it("prefers the resolved ref, then the declared ref, then the legacy name", () => {
    const resolved = runtimeReference({
      cluster: "c",
      name: "a",
      runtimeRef: { kind: "ClusterServingRuntime", name: "vllm" },
      resolvedRuntimeRef: {
        kind: "ServingRuntime",
        namespace: "team-a",
        name: "vllm",
        source: "spec",
      },
    });
    expect(resolved).toMatchObject({ kind: "ServingRuntime", namespace: "team-a", name: "vllm" });

    const declared = runtimeReference({
      cluster: "c",
      name: "b",
      runtimeRef: { kind: "ClusterServingRuntime", name: "vllm" },
    });
    expect(declared).toMatchObject({ kind: "ClusterServingRuntime", name: "vllm" });

    const legacy = runtimeReference({ cluster: "c", name: "c", runtime: "vllm" });
    expect(legacy).toMatchObject({ kind: "ClusterServingRuntime", name: "vllm" });
  });

  it("links an auto-selected runtime resolved from pod labels", () => {
    // No spec.runtime: the detail response carries only the pods-sourced ref.
    const ref = runtimeReference({
      cluster: "c",
      namespace: "team-a",
      name: "a",
      resolvedRuntimeRef: { kind: "ClusterServingRuntime", name: "sglang", source: "pods" },
    });
    expect(ref).toMatchObject({ cluster: "c", kind: "ClusterServingRuntime", name: "sglang" });
  });
});

describe("toFeed", () => {
  afterEach(() => vi.restoreAllMocks());

  it("links every kind with a detail surface, and nothing that is gone", () => {
    const at = Math.floor(Date.now() / 1000);
    const [isvc, model, runtime, ftw, deleted] = toFeed([
      {
        cluster: "cw",
        kind: "InferenceService",
        name: "a",
        namespace: "ns",
        action: "applied",
        at,
      },
      { cluster: "cw", kind: "ClusterBaseModel", name: "b", action: "applied", at },
      { cluster: "cw", kind: "ServingRuntime", name: "c", namespace: "ns", action: "applied", at },
      { cluster: "cw", kind: "FineTunedWeight", name: "d", action: "applied", at },
      { cluster: "cw", kind: "ClusterBaseModel", name: "e", action: "deleted", at },
    ] as EventItem[]);
    expect(isvc.sel).toEqual({
      cluster: "cw",
      kind: "InferenceService",
      name: "a",
      namespace: "ns",
    });
    expect(model.sel).toEqual({ cluster: "cw", kind: "ClusterBaseModel", name: "b" });
    expect(runtime.sel).toEqual({
      cluster: "cw",
      kind: "ServingRuntime",
      name: "c",
      namespace: "ns",
    });
    expect(ftw.sel).toBeUndefined(); // no detail panel to land on
    expect(deleted.sel).toBeUndefined();
  });

  it("chooses icon/tone by kind and action, with relative time", () => {
    const now = Math.floor(Date.now() / 1000);
    vi.spyOn(Date, "now").mockReturnValue(now * 1000);

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

describe("transitionLabel", () => {
  it("shows only the differing revision tails", () => {
    expect(transitionLabel({ from: "w-engine-old1", to: "w-engine-new2" })).toBe("old1 → new2");
    expect(transitionLabel({ to: "w-engine-new2" })).toBe("→ new2");
    expect(transitionLabel({ from: "old", to: "new" })).toBe("old → new");
  });
});

describe("canaryNote", () => {
  it("summarizes step, traffic, and failing checks", () => {
    expect(canaryNote({ currentStep: 3, trafficWeight: 25 })).toBe("canary step 3 · 25% traffic");
    expect(canaryNote({ currentStep: 1, trafficWeight: 5, failedChecks: 2 })).toBe(
      "canary step 1 · 5% traffic · 2 checks failing",
    );
    expect(canaryNote({ currentStep: 1, trafficWeight: 5, failedChecks: 1 })).toBe(
      "canary step 1 · 5% traffic · 1 check failing",
    );
  });

  it("includes how long the canary has sat in its step", () => {
    expect(canaryNote({ currentStep: 2, trafficWeight: 25, stepAge: "3h", failedChecks: 1 })).toBe(
      "canary step 2 · 25% traffic · in step 3h · 1 check failing",
    );
  });
});

describe("shortRevision", () => {
  it("keeps the distinguishing rev suffix and falls back to the full name", () => {
    expect(shortRevision("avocado-v7p2-ckpt312-engine-rev-b54cd954")).toBe("rev-b54cd954");
    expect(shortRevision("rev-b54cd954")).toBe("rev-b54cd954");
    expect(shortRevision("no-suffix-here")).toBe("no-suffix-here");
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
