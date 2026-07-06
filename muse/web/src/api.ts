// Typed client for the muse read plane.
//
// During `npm run dev`, Vite proxies `/v1` (and `/v1/stream`) to the muse-server
// on :8080, so these are same-origin fetches with no CORS setup. Shapes mirror
// the server's wire protocol (see crates/muse-server/src/protocol.rs).

export interface ClusterItem {
  name: string;
  ready: boolean;
  status: "ready" | "syncing";
  nodes: number;
  acceleratorNodes: number;
  inferenceServices: number;
}
export interface ClustersResp {
  count: number;
  items: ClusterItem[];
}

/** Accelerator capacity for a group, tagged with its kind ("gpu" | "tpu"). */
export interface AccelGroup {
  name: string;
  kind: string;
  nodes: number;
  allocatable: number;
  allocated: number;
  free: number;
}
export interface CapacityResp {
  nodes: { total: number; ready: number; schedulable: number; acceleratorNodes: number };
  /** Fleet totals, one entry per accelerator kind. */
  accelerators: AccelGroup[];
  byCluster: AccelGroup[];
  byIsland: AccelGroup[];
  byNvlinkDomain: AccelGroup[];
  byInstanceType: AccelGroup[];
}

export interface ListResp<T> {
  count: number;
  items: T[];
}

/** A status condition, projected (shared by ISVC + model detail). */
export interface ConditionItem {
  type: string;
  status: string;
  reason?: string;
  message?: string;
}

/** One ISVC Component (engine / decoder / router): readiness + live replicas. */
export interface ComponentItem {
  name: string;
  ready?: boolean;
  currentReplicas?: number;
  desiredReplicas?: number;
  rolloutPhase?: string;
  revision?: string;
  url?: string;
}

export interface IsvcItem {
  cluster: string;
  name: string;
  namespace?: string;
  model?: string;
  runtime?: string;
  deploymentMode?: string;
  ready?: boolean;
  url?: string;
  components?: ComponentItem[];
  modelState?: string;
  loadedCopies?: number;
  failedCopies?: number;
  reason?: string;
  message?: string;
  age?: string;
}

export interface PlacementCandidate {
  cluster: string;
  phase?: string;
}
export interface PlacementItem {
  cluster?: string;
  phase?: string;
  endpoint?: string;
  candidates?: PlacementCandidate[];
}
export interface CanaryItem {
  currentStep: number;
  trafficWeight: number;
}

/** Full ISVC detail (drawer): the overview plus conditions, placement, canary. */
export interface IsvcDetail extends IsvcItem {
  conditions?: ConditionItem[];
  placement?: PlacementItem;
  canary?: CanaryItem;
}

export interface ModelItem {
  cluster: string;
  name: string;
  namespace?: string;
  displayName?: string;
  architecture?: string;
  format?: string;
  parameterSize?: string;
  quantization?: string;
  maxTokens?: number;
  vendor?: string;
  version?: string;
  capabilities?: string[];
  /** Storage backend scheme: "manifold" | "pvc" | "hf" | "oci" | … */
  source?: string;
  storageUri?: string;
  disabled?: boolean;
  state?: string;
  lifecycle?: string;
  age?: string;
}

/** Full model detail (drawer): the overview plus conditions + config + nodes. */
export interface ModelDetail extends ModelItem {
  conditions?: ConditionItem[];
  modelConfiguration?: Record<string, unknown>;
  nodesReady?: number;
  nodesFailed?: number;
}

/** A serving-runtime component (engine / decoder / router) and its node topology. */
export interface RuntimeComponent {
  role: string;
  multiNode: boolean;
  nodes?: number;
}

/** ServingRuntime / ClusterServingRuntime overview (both share this shape). */
export interface RuntimeItem {
  cluster: string;
  name: string;
  namespace?: string;
  /** Serving-engine container image. */
  image?: string;
  architectures?: string[];
  formats?: string[];
  quantizations?: string[];
  /** Wire protocols served (openAI, cohere, …). */
  protocols?: string[];
  /** Components (engine, and when disaggregated decoder / router), each with its
   *  own node topology. */
  components?: RuntimeComponent[];
  disabled?: boolean;
  age?: string;
}

/** Fine-tuned weight (LoRA / adapter) — links back to a base model. */
export interface FtwItem {
  cluster: string;
  name: string;
  namespace?: string;
  baseModel?: string;
  baseModelNamespace?: string;
  modelType?: string;
  version?: string;
  age?: string;
}

export interface NodeItem {
  cluster: string;
  name: string;
  ready: boolean;
  schedulable: boolean;
  instanceType?: string;
  island?: string;
  nvlinkDomain?: string;
  zone?: string;
  accelerator?: { kind: string; allocatable: number; allocated: number; free: number };
  cpu?: string;
  memory?: string;
  pods?: string;
  devices?: Record<string, string>;
  kubeletVersion?: string;
  age?: string;
}

export interface EventItem {
  cluster: string;
  kind: string;
  namespace?: string;
  name: string;
  action: "applied" | "deleted";
  at: number;
}

export interface RolloutItem {
  cluster: string;
  name: string;
  namespace?: string;
  deploymentMode?: string;
  ready: boolean;
  reason?: string;
  message?: string;
  age?: string;
}

/** Feature flags from `GET /v1/config` (read once at load). */
export interface AppConfig {
  chat: boolean;
}

/** Minimal OpenAI-style chat completion response (only what the console reads). */
export interface ChatResponse {
  choices?: { message?: { role?: string; content?: string } }[];
  [k: string]: unknown;
}

// Requests are relative to the app's base path (Vite BASE_URL — "/muse/" in the
// packaged image, "/" in dev), so the console works behind a gateway that serves
// it under a sub-path (the gateway strips the prefix before muse-server).
const BASE = import.meta.env.BASE_URL;
const at = (path: string) => BASE + path.replace(/^\//, "");

/** SSE endpoint for live resource-change events (see `live.ts`). */
export const STREAM_URL = at("/v1/stream");

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(at(path), { headers: { accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`${path} → ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T;
}

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(at(path), {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const j = (await res.json()) as { error?: { message?: string } };
      if (j.error?.message) msg = j.error.message;
    } catch {
      // non-JSON error body — keep the status line
    }
    throw new Error(msg);
  }
  return (await res.json()) as T;
}

const enc = encodeURIComponent;

export const api = {
  clusters: () => getJSON<ClustersResp>("/v1/clusters"),
  capacity: () => getJSON<CapacityResp>("/v1/capacity"),
  capacityForCluster: (name: string) =>
    getJSON<CapacityResp>(`/v1/capacity?cluster=${encodeURIComponent(name)}`),
  inferenceServices: () => getJSON<ListResp<IsvcItem>>("/v1/inferenceservices"),
  baseModels: () => getJSON<ListResp<ModelItem>>("/v1/basemodels"),
  clusterBaseModels: () => getJSON<ListResp<ModelItem>>("/v1/clusterbasemodels"),
  finetunedWeights: () => getJSON<ListResp<FtwItem>>("/v1/finetunedweights"),
  servingRuntimes: () => getJSON<ListResp<RuntimeItem>>("/v1/servingruntimes"),
  clusterServingRuntimes: () => getJSON<ListResp<RuntimeItem>>("/v1/clusterservingruntimes"),
  nodes: () => getJSON<ListResp<NodeItem>>("/v1/nodes"),
  events: () => getJSON<ListResp<EventItem>>("/v1/events"),
  rollouts: () => getJSON<ListResp<RolloutItem>>("/v1/rollouts"),
  // Per-object detail for the drawers; `cluster` disambiguates same-named
  // objects across clusters (the server matches the first cluster otherwise).
  inferenceService: (cluster: string, name: string) =>
    getJSON<IsvcDetail>(`/v1/inferenceservices/${enc(name)}?cluster=${enc(cluster)}`),
  baseModel: (cluster: string, name: string) =>
    getJSON<ModelDetail>(`/v1/basemodels/${enc(name)}?cluster=${enc(cluster)}`),
  clusterBaseModel: (cluster: string, name: string) =>
    getJSON<ModelDetail>(`/v1/clusterbasemodels/${enc(name)}?cluster=${enc(cluster)}`),
  servingRuntime: (cluster: string, name: string) =>
    getJSON<RuntimeItem>(`/v1/servingruntimes/${enc(name)}?cluster=${enc(cluster)}`),
  clusterServingRuntime: (cluster: string, name: string) =>
    getJSON<RuntimeItem>(`/v1/clusterservingruntimes/${enc(name)}?cluster=${enc(cluster)}`),
  // Console feature flags + the inference test proxy (off unless the server enables it).
  config: () => getJSON<AppConfig>("/v1/config"),
  chat: (cluster: string, name: string, body: unknown) =>
    postJSON<ChatResponse>(`/v1/inferenceservices/${enc(name)}/chat?cluster=${enc(cluster)}`, body),
};
