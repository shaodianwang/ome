import { describe, expect, it } from "vitest";

import { shouldInvalidate } from "./live";

describe("live query invalidation", () => {
  it("matches any resource in a batched invalidation", () => {
    const event = { resources: ["InferenceService", "Pod"] };

    expect(shouldInvalidate(event, ["InferenceService"])).toBe(true);
    expect(shouldInvalidate(event, ["BaseModel"])).toBe(false);
  });

  it("supports any-resource and stream-disabled queries", () => {
    const event = { resources: ["Capacity"] };

    expect(shouldInvalidate(event)).toBe(true);
    expect(shouldInvalidate(event, [])).toBe(false);
  });
});
