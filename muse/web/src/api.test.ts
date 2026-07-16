import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "./api";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => vi.unstubAllGlobals());

describe("resource identity requests", () => {
  it("includes namespace for namespaced details and omits it for cluster-scoped details", async () => {
    const fetch = vi.fn().mockImplementation(() => Promise.resolve(jsonResponse({})));
    vi.stubGlobal("fetch", fetch);

    await api.model({
      cluster: "member-a",
      kind: "BaseModel",
      namespace: "team-a",
      name: "shared",
    });
    await api.model({ cluster: "member-a", kind: "ClusterBaseModel", name: "shared" });

    const namespaced = new URL(fetch.mock.calls[0][0], "http://muse.test");
    expect(namespaced.pathname).toBe("/v1/basemodels/shared");
    expect(namespaced.searchParams.get("cluster")).toBe("member-a");
    expect(namespaced.searchParams.get("namespace")).toBe("team-a");

    const clusterScoped = new URL(fetch.mock.calls[1][0], "http://muse.test");
    expect(clusterScoped.pathname).toBe("/v1/clusterbasemodels/shared");
    expect(clusterScoped.searchParams.has("namespace")).toBe(false);
  });

  it("sends the InferenceService namespace to chat", async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse({ choices: [] }));
    vi.stubGlobal("fetch", fetch);

    await api.chat(
      {
        cluster: "member-a",
        kind: "InferenceService",
        namespace: "team-a",
        name: "chat",
      },
      "/generate",
      "https://chat.example.test",
      { text: "hello" },
    );

    const request = new URL(fetch.mock.calls[0][0], "http://muse.test");
    expect(request.pathname).toBe("/v1/inferenceservices/chat/chat");
    expect(request.searchParams.get("cluster")).toBe("member-a");
    expect(request.searchParams.get("namespace")).toBe("team-a");
    expect(request.searchParams.get("path")).toBe("/generate");
  });

  it("falls back to the legacy namespaced model lookup", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ error: { code: "not_found", message: "not found" } }, 404),
      )
      .mockResolvedValueOnce(jsonResponse({ name: "shared", namespace: "team-a" }));
    vi.stubGlobal("fetch", fetch);

    await expect(api.legacyModel("member-a", "shared")).resolves.toMatchObject({
      namespace: "team-a",
    });

    const clusterScoped = new URL(fetch.mock.calls[0][0], "http://muse.test");
    const namespaced = new URL(fetch.mock.calls[1][0], "http://muse.test");
    expect(clusterScoped.pathname).toBe("/v1/clusterbasemodels/shared");
    expect(namespaced.pathname).toBe("/v1/basemodels/shared");
    expect(namespaced.searchParams.has("namespace")).toBe(false);
  });

  it("does not change resource scope after an operational failure", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ error: { code: "unavailable", message: "cache warming" } }, 503),
      );
    vi.stubGlobal("fetch", fetch);

    await expect(api.legacyRuntime("member-a", "vllm")).rejects.toThrow("cache warming");
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
