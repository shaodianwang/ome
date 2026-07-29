import { describe, expect, it } from "vitest";

import type { ComponentStatus, SystemComponentItem } from "./api";
import {
  fleetHealth,
  groupSummaries,
  statusTone,
  summarizeComponents,
  worstStatus,
} from "./systemStatus";

function row(
  over: Partial<SystemComponentItem> & { cluster: string; status: ComponentStatus },
): SystemComponentItem {
  return {
    name: "ome-controller-manager",
    group: "control-plane",
    kind: "Deployment",
    namespace: "ome",
    desired: 2,
    ready: over.status === "healthy" ? 2 : 0,
    ...over,
  };
}

describe("statusTone", () => {
  it("maps down/absent to red, degraded to amber, healthy to green", () => {
    expect(statusTone("healthy")).toBe("green");
    expect(statusTone("degraded")).toBe("amber");
    expect(statusTone("unavailable")).toBe("red");
    expect(statusTone("missing")).toBe("red");
    expect(statusTone("unknown")).toBe("slate");
  });
});

describe("worstStatus", () => {
  it("is worst-wins and order-independent", () => {
    expect(worstStatus([{ status: "healthy" }, { status: "missing" }])).toBe("missing");
    expect(worstStatus([{ status: "missing" }, { status: "healthy" }])).toBe("missing");
    expect(worstStatus([{ status: "degraded" }, { status: "unavailable" }])).toBe("unavailable");
    expect(worstStatus([{ status: "healthy" }, { status: "unknown" }])).toBe("unknown");
    expect(worstStatus([])).toBe("unknown");
  });
});

describe("summarizeComponents", () => {
  it("rolls up one component across clusters, worst-wins, sorted by cluster", () => {
    const summaries = summarizeComponents([
      row({ cluster: "b", status: "healthy" }),
      row({ cluster: "a", status: "unavailable" }),
    ]);
    expect(summaries).toHaveLength(1);
    const [s] = summaries;
    expect(s.name).toBe("ome-controller-manager");
    expect(s.worst).toBe("unavailable");
    expect(s.healthy).toBe(1);
    expect(s.total).toBe(2);
    // Clusters are sorted by name so the card is stable across refreshes.
    expect(s.clusters.map((c) => c.cluster)).toEqual(["a", "b"]);
  });

  it("orders components worst-first so problems float to the top", () => {
    const summaries = summarizeComponents([
      row({ cluster: "a", status: "healthy", name: "healthy-comp" }),
      row({ cluster: "a", status: "missing", name: "broken-comp" }),
    ]);
    expect(summaries.map((s) => s.name)).toEqual(["broken-comp", "healthy-comp"]);
  });
});

describe("groupSummaries + fleetHealth", () => {
  it("groups by group and reports fleet-level healthy count", () => {
    const summaries = summarizeComponents([
      row({ cluster: "a", status: "healthy", name: "ctrl", group: "control-plane" }),
      row({
        cluster: "a",
        status: "degraded",
        name: "agent",
        group: "data-plane",
        kind: "DaemonSet",
      }),
    ]);
    const groups = groupSummaries(summaries);
    // The group with a degraded component sorts before the all-healthy group.
    expect(groups.map((g) => g.group)).toEqual(["data-plane", "control-plane"]);

    const fleet = fleetHealth(summaries);
    expect(fleet).toEqual({ healthy: 1, total: 2, worst: "degraded" });
  });
});
