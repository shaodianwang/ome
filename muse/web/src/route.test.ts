import { describe, expect, it } from "vitest";

import { parseRoute, routePath } from "./route";

describe("resource routes", () => {
  it("round-trips a namespaced model's canonical identity", () => {
    const route = {
      panel: "models" as const,
      sel: {
        cluster: "member-a",
        kind: "BaseModel" as const,
        namespace: "team-a",
        name: "shared/model",
      },
    };

    const path = routePath(route);
    expect(path).toBe("/models/member-a/shared%2Fmodel?kind=BaseModel&namespace=team-a");
    const [pathname, query] = path.split("?");
    expect(parseRoute(pathname, `?${query}`)).toEqual(route);
  });

  it("keeps legacy model and runtime scope unspecified", () => {
    expect(parseRoute("/models/member-a/llama", "")).toEqual({
      panel: "models",
      sel: { cluster: "member-a", legacy: "model", name: "llama" },
    });
    expect(parseRoute("/runtimes/member-a/vllm", "")).toEqual({
      panel: "runtimes",
      sel: { cluster: "member-a", legacy: "runtime", name: "vllm" },
    });
    expect(parseRoute("/deployments/member-a/chat", "")).toEqual({
      panel: "workloads",
      sel: { cluster: "member-a", kind: "InferenceService", name: "chat" },
    });
  });

  it("round-trips an old kind-less URL without inventing a canonical scope", () => {
    const legacy = parseRoute("/models/member-a/llama", "");
    expect(routePath(legacy)).toBe("/models/member-a/llama");
  });

  it("rejects a resource kind that does not belong to the panel", () => {
    expect(parseRoute("/models/member-a/llama", "?kind=InferenceService")).toEqual({
      panel: "models",
    });
  });

  it("round-trips the capacity node-explorer deep link", () => {
    const route = { panel: "capacity" as const, cluster: "member-a", q: "island-7" };
    const path = routePath(route);
    expect(path).toBe("/capacity?cluster=member-a&q=island-7");
    const [pathname, query] = path.split("?");
    expect(parseRoute(pathname, `?${query}`)).toEqual(route);
  });

  it("round-trips the deployments namespace filter", () => {
    const route = { panel: "workloads" as const, ns: "team-a", q: "llama" };
    const path = routePath(route);
    expect(path).toBe("/deployments?ns=team-a&q=llama");
    const [pathname, query] = path.split("?");
    expect(parseRoute(pathname, `?${query}`)).toEqual(route);
  });
});
