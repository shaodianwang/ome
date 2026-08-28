import { describe, expect, it } from "vitest";

import type { AccelGroup, ClusterItem, IsvcItem, RolloutItem } from "./api";
import {
  hasUnambiguousDeploymentMetricIdentity,
  scopeFleet,
  unambiguousDeploymentRef,
} from "./fleet";
import type { FleetRaw } from "./fleet";

const isvc = (cluster: string, namespace: string): IsvcItem => ({
  cluster,
  namespace,
  name: "chat",
});

describe("unambiguousDeploymentRef", () => {
  it("returns the sole full deployment identity", () => {
    expect(
      unambiguousDeploymentRef([isvc("member-a", "team-a")], "member-a", "team-a", "chat"),
    ).toEqual({
      cluster: "member-a",
      kind: "InferenceService",
      namespace: "team-a",
      name: "chat",
    });
  });

  it("uses namespace labels and rejects an ambiguous legacy row", () => {
    const deployments = [isvc("member-a", "team-a"), isvc("member-a", "team-b")];
    expect(unambiguousDeploymentRef(deployments, "member-a", "team-b", "chat")?.namespace).toBe(
      "team-b",
    );
    expect(unambiguousDeploymentRef(deployments, "member-a", undefined, "chat")).toBeUndefined();
  });
});

describe("scopeFleet", () => {
  const cluster = (name: string): ClusterItem => ({
    name,
    ready: true,
    status: "ready",
    nodes: 4,
    acceleratorNodes: 2,
    inferenceServices: 1,
  });
  const deployment = (clusterName: string, name: string, model: string): IsvcItem => ({
    cluster: clusterName,
    namespace: "team-a",
    name,
    model,
  });
  const rollout = (clusterName: string, name: string): RolloutItem => ({
    cluster: clusterName,
    namespace: "team-a",
    name,
    ready: false,
  });
  const group = (name: string, kind: string): AccelGroup => ({
    name,
    kind,
    nodes: 2,
    allocatable: 16,
    allocated: 8,
    free: 8,
  });
  const raw: FleetRaw = {
    clusters: [cluster("member-a"), cluster("member-b")],
    isvcs: [deployment("member-a", "chat", "llama"), deployment("member-b", "code", "qwen")],
    rollouts: [rollout("member-a", "chat"), rollout("member-b", "code")],
    events: [],
    metrics: null,
    capacity: {
      nodes: { total: 8, ready: 8, schedulable: 8, acceleratorNodes: 4 },
      accelerators: [group("gpu", "gpu"), group("tpu", "tpu")],
      byCluster: [group("member-a", "gpu"), group("member-b", "tpu")],
      byIsland: [group("island-a", "gpu"), group("island-b", "tpu")],
      byNvlinkDomain: [group("domain-a", "gpu"), group("domain-b", "tpu")],
      byInstanceType: [],
    },
  };
  const all = { cluster: "all", model: "all", accel: "all" };

  const cases: {
    name: string;
    filter: { cluster: string; model: string; accel: string };
    isvcs: string[];
    rollouts: string[];
    clusters: string[];
  }[] = [
    {
      name: "passes everything through unfiltered",
      filter: all,
      isvcs: ["chat", "code"],
      rollouts: ["chat", "code"],
      clusters: ["member-a", "member-b"],
    },
    {
      name: "cluster filter narrows deployments and rollouts together",
      filter: { ...all, cluster: "member-a" },
      isvcs: ["chat"],
      rollouts: ["chat"],
      clusters: ["member-a"],
    },
    {
      name: "model filter maps rollouts through the deployment's model",
      filter: { ...all, model: "qwen" },
      isvcs: ["code"],
      rollouts: ["code"],
      clusters: ["member-a", "member-b"],
    },
    {
      name: "accelerator filter keeps only clusters of that kind",
      filter: { ...all, accel: "tpu" },
      isvcs: ["code"],
      rollouts: ["code"],
      clusters: ["member-b"],
    },
  ];

  it.each(cases)("$name", ({ filter, isvcs, rollouts, clusters }) => {
    const scope = scopeFleet(filter, raw);

    expect(scope.isvcs.map((i) => i.name)).toEqual(isvcs);
    expect(scope.rollouts.map((r) => r.name)).toEqual(rollouts);
    expect(scope.clusters.map((c) => c.name)).toEqual(clusters);
  });

  it("recomputes accelerator totals from the scoped clusters", () => {
    expect(scopeFleet(all, raw).accelerators).toEqual(raw.capacity?.accelerators);
    expect(scopeFleet({ ...all, cluster: "member-a" }, raw).accelerators).toEqual([
      { name: "gpu", kind: "gpu", nodes: 2, allocatable: 16, allocated: 8, free: 8 },
    ]);
  });
});

describe("hasUnambiguousDeploymentMetricIdentity", () => {
  it("accepts one exact namespace and rejects duplicate or mismatched identities", () => {
    const teamA = isvc("member-a", "team-a");
    const selected = {
      cluster: "member-a",
      namespace: "team-a",
      name: "chat",
    };

    expect(hasUnambiguousDeploymentMetricIdentity([teamA], selected)).toBe(true);
    expect(
      hasUnambiguousDeploymentMetricIdentity([teamA, isvc("member-a", "team-b")], selected),
    ).toBe(false);
    expect(
      hasUnambiguousDeploymentMetricIdentity([teamA], { ...selected, namespace: "team-b" }),
    ).toBe(false);
  });
});
