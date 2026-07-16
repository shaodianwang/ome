import { describe, expect, it } from "vitest";

import type { IsvcItem } from "./api";
import { hasUnambiguousDeploymentMetricIdentity, unambiguousDeploymentRef } from "./fleet";

const isvc = (cluster: string, namespace: string): IsvcItem => ({
  cluster,
  namespace,
  name: "chat",
});

describe("unambiguousDeploymentRef", () => {
  it("returns the sole full deployment identity", () => {
    expect(unambiguousDeploymentRef([isvc("member-a", "team-a")], ["member-a"], "chat")).toEqual({
      cluster: "member-a",
      kind: "InferenceService",
      namespace: "team-a",
      name: "chat",
    });
  });

  it("rejects rows aggregated across clusters or namespaces", () => {
    const deployments = [isvc("member-a", "team-a"), isvc("member-a", "team-b")];
    expect(unambiguousDeploymentRef(deployments, ["member-a"], "chat")).toBeUndefined();
    expect(unambiguousDeploymentRef(deployments, ["member-a", "member-b"], "chat")).toBeUndefined();
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
