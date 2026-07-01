// Mock data transcribed from the OEP-0021 mockup. Replaced by live muse API
// calls in the follow-up; the shapes (see types.ts) stay the same.
import type {
  CellState,
  ClusterRow,
  FeedItem,
  Island,
  ModelCard,
  Pool,
  Rollout,
  Slice,
  WorkloadRow,
} from "./types";

export const clusters: ClusterRow[] = [
  { name: "msl-cw-use2-2", cloud: "cw", region: "us-east-2", nodes: "512", accel: "H100·SXM", utilPct: 81, barTone: "", status: { label: "Ready", tone: "green" } },
  { name: "gcp-tpu-us-east5", cloud: "gcp", region: "us-east5-b", nodes: "288", accel: "TPU·v5e", utilPct: 69, barTone: "green", status: { label: "Ready", tone: "green" } },
  { name: "gcp-tpu-us-cen2", cloud: "gcp", region: "us-central2-b", nodes: "160", accel: "TPU·v5p", utilPct: 88, barTone: "amber", status: { label: "Pressure", tone: "amber" } },
  { name: "oci-iad-h100-1", cloud: "oci", region: "us-ashburn-1", nodes: "384", accel: "H100·SXM", utilPct: 62, barTone: "", status: { label: "Ready", tone: "green" } },
  { name: "oci-phx-a100-2", cloud: "oci", region: "us-phoenix-1", nodes: "256", accel: "A100·80G", utilPct: 54, barTone: "", status: { label: "Ready", tone: "green" } },
  { name: "meta-rdma-pnb-3", cloud: "meta", region: "pnb / RDMA", nodes: "1,024", accel: "H100·RoCE", utilPct: 77, barTone: "", status: { label: "Ready", tone: "green" } },
  { name: "meta-rdma-eag-1", cloud: "meta", region: "eag / RDMA", nodes: "2,048", accel: "H100·RoCE", utilPct: 91, barTone: "amber", status: { label: "Pressure", tone: "amber" } },
  { name: "meta-rdma-ftw-2", cloud: "meta", region: "ftw / RDMA", nodes: "1,536", accel: "H100·RoCE", utilPct: 58, barTone: "", status: { label: "Ready", tone: "green" } },
  { name: "cw-msl-cw-use2-1", cloud: "cw", region: "us-east-2", nodes: "128", accel: "A100·80G", utilPct: 96, barTone: "red", status: { label: "Saturated", tone: "red" } },
];

export const feed: FeedItem[] = [
  { tone: "blue", icon: "deploy", strong: "Canary 25%", rest: " promoted to 50%", meta: "deepseek-v3 · gcp-tpu-us-east5 · auto (metrics-gated)", when: "2m" },
  { tone: "violet", icon: "arrow", strong: "Transfer started", rest: " Meta → GCP", meta: "llama-3.1-405b-instruct → TPU host · 4.7 TB", when: "14m" },
  { tone: "green", icon: "check", strong: "Deploy succeeded", rest: "", meta: "qwen2.5-72b-instruct · PD · oci-iad-h100-1 · 8 replicas", when: "38m" },
  { tone: "green", icon: "cube", strong: "Model ready", rest: " conversion complete", meta: "mixtral-8x22b · PyTorch → safetensors · fp8", when: "1h" },
  { tone: "amber", icon: "warning", strong: "Capacity pressure", rest: " on meta-rdma-eag-1", meta: "91% allocatable · 3 pending pods queued in Kueue", when: "1h" },
  { tone: "blue", icon: "lines", strong: "Runtime auto-selected", rest: "", meta: "sglang-h100-pd matched llama-3.1-405b (fp8, llama)", when: "2h" },
  { tone: "green", icon: "check", strong: "Blue/Green cutover", rest: " complete", meta: "llama-3.3-70b · meta-rdma-ftw-2 · green live, blue drained", when: "3h" },
];

export const rollouts: Rollout[] = [
  { name: "deepseek-v3", strategy: { label: "Canary", tone: "blue" }, kind: "steps", steps: ["done", "done", "cur", ""], note: "25% → 50% → 75% → 100% · at 50%", gate: { label: "p95 ✓ err ✓", tone: "green" }, cluster: "gcp-tpu-us-east5", started: "12m ago" },
  { name: "llama-3.1-405b-instruct", strategy: { label: "Canary", tone: "blue" }, kind: "steps", steps: ["cur", "", "", ""], note: "10% → 25% → 50% → 100% · at 10%", gate: { label: "analyzing", tone: "amber" }, cluster: "meta-rdma-pnb-3", started: "4m ago" },
  { name: "qwen2.5-vl-72b", strategy: { label: "Blue/Green", tone: "violet" }, kind: "bar", barTone: "violet", barPct: 100, note: "green warm · awaiting manual cutover", gate: { label: "paused", tone: "blue" }, cluster: "oci-iad-h100-1", started: "26m ago" },
  { name: "embed-gte-large", strategy: { label: "RollingUpdate", tone: "slate" }, kind: "bar", barTone: "", barPct: 66, note: "8 / 12 pods updated · surge 25%", gate: { label: "healthy", tone: "green" }, cluster: "oci-phx-a100-2", started: "9m ago" },
  { name: "mixtral-8x22b-instruct", strategy: { label: "Blue/Green", tone: "violet" }, kind: "bar", barTone: "violet", barPct: 42, barStriped: true, note: "green provisioning · 5 / 12 ready", gate: { label: "warming", tone: "slate" }, cluster: "meta-rdma-ftw-2", started: "3m ago" },
];

export const workloads: WorkloadRow[] = [
  {
    id: "d1", name: "llama-3.1-405b-instruct", namespace: "genai-prod", model: "llama-3.1-405b", modelSub: "fp8 · 405B",
    runtime: "sglang-h100-pd", mode: { label: "PD-Disagg", tone: "cyan" }, placement: "meta-rdma-pnb-3", ready: "16 / 16",
    rollout: { label: "Canary 10%", tone: "blue" }, age: "18d", status: { label: "Ready", tone: "green" },
    detail: {
      components: [
        { badge: { label: "Engine", tone: "cyan" }, title: "prefill", kv: [["replicas", "4 / 4 ready"], ["accelerator", "H100 ×8 / pod"], ["topology", "MultiNode (LWS)"], ["cluster", "meta-rdma-pnb-3"]] },
        { badge: { label: "Decoder", tone: "violet" }, title: "decode", kv: [["replicas", "12 / 12 ready"], ["accelerator", "H100 ×8 / pod"], ["topology", "MultiNode (LWS)"], ["cluster", "meta-rdma-pnb-3"]] },
      ],
      placement: { cols: ["Cluster", "Engine", "Decoder", "GPUs", "Status"], rows: [["meta-rdma-pnb-3", "4", "12", "128", "Ready"]] },
      canary: { steps: ["cur", "", "", ""], labels: ["10%", "25%", "50%", "100%"], kv: [["candidate", "rev-8f2a1c"], ["stable", "rev-3d9b07"], ["traffic", "10% candidate"], ["gate", "metrics-gated"], ["p95 latency", "412 ms ✓ (≤ 600)"], ["error rate", "0.02% ✓ (≤ 0.5)"]] },
      showActions: true,
    },
  },
  {
    id: "d2", name: "deepseek-v3", namespace: "genai-prod", model: "deepseek-v3", modelSub: "fp8 · 671B MoE",
    runtime: "sglang-tpu-v5e", mode: { label: "PD-Disagg", tone: "cyan" }, placement: "gcp-tpu-us-east5", ready: "20 / 20",
    rollout: { label: "Canary 50%", tone: "blue" }, age: "9d", status: { label: "Ready", tone: "green" },
    detail: {
      components: [
        { badge: { label: "Engine", tone: "cyan" }, title: "prefill", kv: [["replicas", "6 / 6 ready"], ["accelerator", "TPU v5e-16"], ["cluster", "gcp-tpu-us-east5"]] },
        { badge: { label: "Decoder", tone: "violet" }, title: "decode", kv: [["replicas", "14 / 14 ready"], ["accelerator", "TPU v5e-16"], ["cluster", "gcp-tpu-us-east5"]] },
      ],
      canary: { steps: ["done", "done", "cur", ""], labels: [], kv: [["candidate", "rev-c41e9a"], ["p95 latency", "288 ms ✓"], ["tok/s", "+4.1% vs stable"]] },
    },
  },
  { id: "w3", name: "qwen2.5-72b-instruct", namespace: "genai-prod", model: "qwen2.5-72b", modelSub: "bf16 · 72B", runtime: "vllm-h100", mode: { label: "MultiNode", tone: "slate" }, placement: "oci-iad-h100-1", ready: "8 / 8", rollout: { label: "Stable", tone: "slate" }, age: "31d", status: { label: "Ready", tone: "green" } },
  { id: "w4", name: "qwen2.5-vl-72b", namespace: "research", model: "qwen2.5-vl-72b", modelSub: "bf16 · VISION", runtime: "vllm-h100", mode: { label: "MultiNode", tone: "slate" }, placement: "oci-iad-h100-1", ready: "6 / 6", rollout: { label: "BlueGreen", tone: "violet" }, age: "5d", status: { label: "Ready", tone: "green" } },
  { id: "w5", name: "llama-3.3-70b-instruct", namespace: "genai-prod", model: "llama-3.3-70b", modelSub: "fp8 · 70B", runtime: "sglang-h100", mode: { label: "OMENative", tone: "blue" }, placement: "meta-rdma-ftw-2", ready: "24 / 24", rollout: { label: "Stable", tone: "slate" }, age: "62d", status: { label: "Ready", tone: "green" } },
  { id: "w6", name: "mixtral-8x22b-instruct", namespace: "platform", model: "mixtral-8x22b", modelSub: "fp8 · 141B MoE", runtime: "vllm-h100", mode: { label: "MultiNode", tone: "slate" }, placement: "meta-rdma-ftw-2", ready: "5 / 12", rollout: { label: "BlueGreen", tone: "violet" }, age: "12d", status: { label: "Progressing", tone: "amber" } },
  { id: "w7", name: "embed-gte-large", namespace: "platform", model: "gte-large-en", modelSub: "bf16 · EMBEDDING", runtime: "tei-a100", mode: { label: "RawDeploy", tone: "slate" }, placement: "oci-phx-a100-2", ready: "8 / 12", rollout: { label: "RollingUpdate", tone: "slate" }, age: "88d", status: { label: "Progressing", tone: "amber" } },
  { id: "w8", name: "rerank-bge-v2-m3", namespace: "platform", model: "bge-reranker-v2", modelSub: "bf16 · RERANK", runtime: "tei-a100", mode: { label: "RawDeploy", tone: "slate" }, placement: "oci-phx-a100-2", ready: "4 / 4", rollout: { label: "Stable", tone: "slate" }, age: "120d", status: { label: "Ready", tone: "green" } },
  { id: "w9", name: "llama-3.1-8b-instruct", namespace: "research", model: "llama-3.1-8b", modelSub: "bf16 · 8B", runtime: "sglang-a100", mode: { label: "RawDeploy", tone: "slate" }, placement: "cw-msl-cw-use2-1", ready: "0 / 6", rollout: { label: "Stable", tone: "slate" }, age: "2d", status: { label: "ImagePullBackOff", tone: "red" } },
  { id: "w10", name: "gemma-2-27b-it", namespace: "research", model: "gemma-2-27b", modelSub: "bf16 · 27B", runtime: "vllm-tpu-v5e", mode: { label: "MultiNode", tone: "slate" }, placement: "gcp-tpu-us-cen2", ready: "3 / 4", rollout: { label: "Stable", tone: "slate" }, age: "7d", status: { label: "Degraded", tone: "amber" } },
];

export const models: ModelCard[] = [
  {
    source: "hf", name: "mixtral-8x22b-instruct", status: { label: "Converting", tone: "amber" }, uri: "huggingface://mistralai/Mixtral-8x22B-Instruct-v0.1",
    specs: [{ label: "141B MoE" }, { label: "arch: mixtral" }, { label: "pytorch → safetensors", accent: true }, { label: "fp8" }, { label: "CHAT" }],
    pipeline: [{ label: "Imported", state: "done" }, { label: "Converting", state: "cur" }, { label: "config.json", state: "" }, { label: "Replicated", state: "" }],
    xfer: { heading: "Converting weights · shard 38 / 59", pct: 64, tone: "", stats: [["throughput", "2.4 GB/s"], ["elapsed", "11m"], ["ETA", "~6m"]] },
  },
  {
    source: "meta", name: "llama-3.1-405b-instruct", status: { label: "In Transit", tone: "violet" }, uri: "meta://manifold/genai/llama-3.1-405b-instruct",
    specs: [{ label: "405B" }, { label: "arch: llama" }, { label: "safetensors" }, { label: "fp8" }, { label: "TEXT_GENERATION" }],
    pipeline: [{ label: "Imported", state: "done" }, { label: "Converted", state: "done" }, { label: "config.json", state: "done" }, { label: "Replicating", state: "cur" }],
    xfer: { routeFrom: { cloud: "meta", label: "Meta·ftw" }, routeTo: { cloud: "gcp", label: "GCP·us-east5" }, routeNote: "(TPU hosting)", pct: 71, tone: "violet", stats: [["moved", "3.34 / 4.70 TB"], ["throughput", "9.8 GB/s"], ["ETA", "~2m 24s"]] },
  },
  {
    source: "hf", name: "deepseek-v3", status: { label: "Ready", tone: "green" }, uri: "huggingface://deepseek-ai/DeepSeek-V3",
    specs: [{ label: "671B MoE" }, { label: "arch: deepseek_v3" }, { label: "safetensors" }, { label: "fp8" }, { label: "CHAT" }],
    pipeline: [{ label: "Imported", state: "done" }, { label: "Converted", state: "done" }, { label: "config.json", state: "done" }, { label: "Replicated", state: "done" }],
    replicas: [{ cloud: "gcp", label: "GCP" }, { cloud: "oci", label: "OCI" }, { cloud: "meta", label: "Meta" }],
  },
  {
    source: "meta", name: "llama-3.3-70b-instruct", status: { label: "Ready", tone: "green" }, uri: "meta://manifold/genai/llama-3.3-70b",
    specs: [{ label: "70B" }, { label: "arch: llama" }, { label: "safetensors" }, { label: "fp8" }, { label: "TEXT_GENERATION" }],
    pipeline: [{ label: "Imported", state: "done" }, { label: "Converted", state: "done" }, { label: "config.json", state: "done" }, { label: "Replicated", state: "done" }],
    replicas: [{ cloud: "meta", label: "Meta ×3" }, { cloud: "cw", label: "CoreWeave" }],
  },
  {
    source: "s3", name: "gte-large-en-v1.5", status: { label: "Ready", tone: "green" }, uri: "s3://msl-models/embeddings/gte-large-en",
    specs: [{ label: "434M" }, { label: "arch: bert" }, { label: "safetensors" }, { label: "bf16" }, { label: "EMBEDDING" }],
    pipeline: [{ label: "Imported", state: "done" }, { label: "Converted", state: "done" }, { label: "config.json", state: "done" }, { label: "Replicated", state: "done" }],
  },
  {
    source: "oci", name: "qwen2.5-vl-72b-instruct", status: { label: "Ready", tone: "green" }, uri: "oci://iad.ocir.io/msl/qwen2.5-vl-72b",
    specs: [{ label: "72B" }, { label: "arch: qwen2_vl" }, { label: "safetensors" }, { label: "bf16" }, { label: "VISION" }, { label: "IMAGE_TEXT_TO_TEXT" }],
    pipeline: [{ label: "Imported", state: "done" }, { label: "Converted", state: "done" }, { label: "config.json", state: "done" }, { label: "Replicated", state: "done" }],
  },
];

// Expand an 8×4 island map ("u"=used "f"=free "h"=hot "d"=down) into cells.
const cells = (map: string): CellState[] =>
  [...map.replace(/\s/g, "")].map((c): CellState =>
    c === "u" ? "used" : c === "f" ? "free" : c === "h" ? "hot" : "down",
  );

export const islands: Island[] = [
  { name: "eag-isl-01", status: { label: "link ✓", tone: "green" }, cells: cells("uuuuuuuu uuuuuuuu uuuuuuff uuuuffff"), nodes: "512", util: "3,840 / 4,096" },
  { name: "eag-isl-02", status: { label: "pressure", tone: "amber" }, cells: cells("uuuuuuuu uuuuuuuu uuuuuuuu uuuuuuhf"), nodes: "512", util: "3,968 / 4,096" },
  { name: "ftw-isl-01", status: { label: "link ✓", tone: "green" }, cells: cells("uuuuufff uuuuffff uuufffff uuffffff"), nodes: "512", util: "2,304 / 4,096" },
  { name: "ftw-isl-02", status: { label: "link ✓", tone: "green" }, cells: cells("uuuuuuff uuuuufff uuuuffff uuufffff"), nodes: "512", util: "2,624 / 4,096" },
  { name: "pnb-isl-01", status: { label: "link ✓", tone: "green" }, cells: cells("uuuuuuuf uuuuuuff uuuuufff uuuuffff"), nodes: "512", util: "3,072 / 4,096" },
  { name: "pnb-isl-02", status: { label: "1 node down", tone: "red" }, cells: cells("uuuuuuuu uuuuuuuu uuuuuuff uuuuffdd"), nodes: "512", util: "3,712 / 4,096" },
];

export const slices: Slice[] = [
  { name: "v5e-256", status: { label: "healthy", tone: "green" }, topo: "topology 16×16 · us-east5-b", free: 6, total: 12, barTone: "green", barPct: 50, chips: "1,536 chips · 768 in use" },
  { name: "v5e-128", status: { label: "healthy", tone: "green" }, topo: "topology 8×16 · us-east5-b", free: 4, total: 10, barTone: "green", barPct: 60, chips: "1,280 chips · 768 in use" },
  { name: "v5p-128", status: { label: "pressure", tone: "amber" }, topo: "topology 8×8×2 · us-central2-b", free: 1, total: 8, barTone: "amber", barPct: 88, chips: "1,024 chips · 896 in use" },
  { name: "v5p-256", status: { label: "healthy", tone: "green" }, topo: "topology 8×8×4 · us-central2-b", free: 3, total: 6, barTone: "green", barPct: 50, chips: "1,536 chips · 768 in use" },
];

export const pools: Pool[] = [
  { name: "msl-cw-use2-2", cloud: "cw", accel: "H100·SXM", status: { label: "Ready", tone: "green" }, total: "4,096", inUse: "3,317", free: "779", barTone: "", barPct: 81 },
  { name: "oci-iad-h100-1", cloud: "oci", accel: "H100·SXM", status: { label: "Ready", tone: "green" }, total: "3,072", inUse: "1,905", free: "1,167", barTone: "", barPct: 62 },
  { name: "oci-phx-a100-2", cloud: "oci", accel: "A100·80G", status: { label: "Ready", tone: "green" }, total: "2,048", inUse: "1,106", free: "942", barTone: "", barPct: 54 },
  { name: "cw-msl-cw-use2-1", cloud: "cw", accel: "A100·80G", status: { label: "Saturated", tone: "red" }, total: "1,024", inUse: "983", free: "41", barTone: "red", barPct: 96 },
];
