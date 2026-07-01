// Typed client for the muse read plane.
//
// During `npm run dev`, Vite proxies `/v1` (and `/v1/stream`) to the muse-server
// on :8080, so these are same-origin fetches with no CORS setup. Shapes mirror
// the server's view DTOs (see crates/muse-server/src/{views,capacity,feed}.rs).

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

export interface IsvcItem {
  cluster: string;
  name: string;
  namespace?: string;
  model?: string;
  runtime?: string;
  deploymentMode?: string;
  ready?: boolean;
  age?: string;
}

export interface ModelItem {
  cluster: string;
  name: string;
  namespace?: string;
  architecture?: string;
  format?: string;
  parameterSize?: string;
  quantization?: string;
  vendor?: string;
  capabilities?: string[];
  state?: string;
  lifecycle?: string;
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

/** SSE endpoint for live resource-change events (see `live.ts`). */
export const STREAM_URL = "/v1/stream";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`${path} → ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T;
}

export const api = {
  clusters: () => getJSON<ClustersResp>("/v1/clusters"),
  capacity: () => getJSON<CapacityResp>("/v1/capacity"),
  capacityForCluster: (name: string) =>
    getJSON<CapacityResp>(`/v1/capacity?cluster=${encodeURIComponent(name)}`),
  inferenceServices: () => getJSON<ListResp<IsvcItem>>("/v1/inferenceservices"),
  baseModels: () => getJSON<ListResp<ModelItem>>("/v1/basemodels"),
  clusterBaseModels: () => getJSON<ListResp<ModelItem>>("/v1/clusterbasemodels"),
  nodes: () => getJSON<ListResp<NodeItem>>("/v1/nodes"),
  events: () => getJSON<ListResp<EventItem>>("/v1/events"),
  rollouts: () => getJSON<ListResp<RolloutItem>>("/v1/rollouts"),
};
