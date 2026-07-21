import { describe, expect, it } from "vitest";

import type { RuntimeItem } from "./api";
import { argLines, serves, servesTags, topologySummary } from "./runtime";

const rt = (overrides: Partial<RuntimeItem>): RuntimeItem => ({
  cluster: "cw",
  name: "srt",
  ...overrides,
});

describe("serves", () => {
  it("prefers architectures and summarizes long lists as first +N", () => {
    expect(serves(rt({ architectures: ["A", "B", "C"], formats: ["safetensors"] }))).toBe("A +2");
  });

  it("falls back to formats when no architecture is declared", () => {
    expect(serves(rt({ formats: ["safetensors", "gguf"] }))).toBe("safetensors, gguf");
    expect(serves(rt({}))).toBe("—");
  });
});

describe("servesTags", () => {
  it("orders formats, quantizations, then architectures", () => {
    const tags = servesTags(
      rt({ architectures: ["LlamaForCausalLM"], formats: ["safetensors"], quantizations: ["fp8"] }),
    );
    expect(tags).toEqual(["safetensors", "fp8", "LlamaForCausalLM"]);
  });

  it("deduplicates labels that appear in several lists", () => {
    expect(servesTags(rt({ formats: ["fp8"], quantizations: ["fp8"] }))).toEqual(["fp8"]);
    expect(servesTags(rt({}))).toEqual([]);
  });
});

describe("topologySummary", () => {
  it("labels each component with its worker-group size", () => {
    expect(
      topologySummary([
        { role: "engine", multiNode: true, nodes: 2 },
        { role: "decoder", multiNode: false },
      ]),
    ).toBe("engine multi-node ×2 · decoder ×1");
  });

  it("omits the count for a multi-node group without a declared size", () => {
    expect(topologySummary([{ role: "engine", multiNode: true }])).toBe("engine multi-node");
  });

  it("is null without components", () => {
    expect(topologySummary([])).toBeNull();
    expect(topologySummary(undefined)).toBeNull();
  });
});

describe("argLines", () => {
  it("keeps the executable on the first line and groups values under their flag", () => {
    expect(
      argLines(
        ["python3", "-m", "sglang.launch_server"],
        ["--model-path", "/mnt/models", "--tp-size", "8"],
      ),
    ).toBe("python3 -m sglang.launch_server\n  --model-path /mnt/models\n  --tp-size 8");
  });

  it("handles a command with no args and an empty command", () => {
    expect(argLines(["server"], undefined)).toBe("server");
    expect(argLines(undefined, undefined)).toBe("");
  });
});
