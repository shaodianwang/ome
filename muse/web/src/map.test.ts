import { describe, expect, it } from "vitest";

import type { ComponentItem, EventItem, IsvcItem, RolloutItem } from "./api";
import { loadSummary, modelUsage, replicaRollup, toFeed, toRollouts, toWorkloadRows } from "./map";
import { matches } from "./util";

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
