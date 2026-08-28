// Typed client for the muse read plane.
//
// During `npm run dev`, Vite proxies `/v1` (and `/v1/stream`) to the muse-server
// on :8080, so these are same-origin fetches with no CORS setup. Shapes mirror
// the server's wire protocol (see crates/muse-server/src/protocol.rs).

import { RESOURCE_PLURALS } from "./resource";
import type { ResourceRef, Selected } from "./resource";

export interface ClusterItem {
  name: string;
  ready: boolean;
  status: "syncing" | "ready" | "failed" | "timed_out";
  /** Initial informer sync failure or timeout detail. */
  error?: string;
  /** Apiserver Kubernetes version (e.g. "v1.35.6"). */
  version?: string;
  /** Cloud region from the standard `topology.kubernetes.io/region` node label. */
  region?: string;
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
  /** Unscheduled accelerator demand — Pending pods not yet placed, by kind. */
  pending?: { kind: string; accelerators: number; pods: number }[];
  /** Accelerators on NotReady/cordoned nodes — present but unschedulable, by kind. */
  unavailable?: { kind: string; accelerators: number; nodes: number }[];
  /** TPU slice occupancy by shape. */
  tpuSlices?: PlacementUnits[];
  /** TPU subblocks — one entry per physical subblock, for the heatmap. */
  tpuSubblocks?: UnitBox[];
  /** GPU node occupancy by GPUs-per-node. */
  gpuNodes?: PlacementUnits[];
  /** GPU racks — one entry per NVLink domain, for the heatmap. */
  gpuRacks?: UnitBox[];
  /** Top accelerator consumers on this cluster (by namespace), most chips first. */
  consumers?: { namespace: string; kind: string; accelerators: number; pods: number }[];
  /** Top accelerator consumers by owning InferenceService, most chips first.
   *  Pods without the ownership label (non-OME workloads) appear only in
   *  `consumers`. */
  consumersByDeployment?: DeploymentConsumer[];
}

/** A top accelerator consumer attributed to one InferenceService. */
export interface DeploymentConsumer {
  cluster: string;
  namespace: string;
  name: string;
  kind: string;
  accelerators: number;
  pods: number;
}

/** One carve-able domain (a TPU subblock or GPU NVLink rack) for the heatmap: id,
 *  cluster, and chip occupancy. The console derives state from free/chips/healthy. */
export interface UnitBox {
  id: string;
  cluster: string;
  chips: number;
  free: number;
  healthy: boolean;
}

/** One fragmented carve-able unit, named for a drill-down (a TPU slice/partition id
 *  or an NVLink-domain/node name), with how many of its chips are free. */
export interface Fragment {
  id: string;
  cluster: string;
  free: number;
  chips: number;
}

/** Occupancy breakdown of the carve-able units at one size (a TPU slice shape, a
 *  GPU node, or an NVLink domain). Every unit is in exactly one bucket, so
 *  total === free + fragmented + full + unhealthy. */
export interface PlacementUnits {
  /** Size label: a TPU shape ("2x2x2") or the unit's chip/GPU count ("8", "72"). */
  label: string;
  /** Chips (TPU) or GPUs per unit. */
  chips: number;
  total: number;
  /** Wholly free + schedulable — placeable for a job needing the whole unit. */
  free: number;
  /** Partially used (some chips busy, all nodes schedulable) — the defrag targets. */
  fragmented: number;
  full: number;
  /** ≥1 node NotReady/cordoned. */
  unhealthy: number;
  /** Free chips trapped in fragmented units (recoverable-by-defrag waste). */
  strandedChips: number;
  /** Most-stranded fragmented units, named — the operator drill-down. */
  fragments: Fragment[];
}

/** One fleet-triage finding, with click-through context. */
export interface AttentionItem {
  severity: "critical" | "warning";
  category: "cluster" | "deployment" | "rollout" | "pods" | "model";
  cluster: string;
  namespace?: string;
  /** Resource kind for click-through ("InferenceService", "BaseModel", ...). */
  kind: string;
  name: string;
  /** One line of why this is here. */
  summary: string;
  detail?: string;
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
  /** Ready Instances (lifecycle.readyReplicas). Excludes still-initializing
   *  Instances, unlike `currentReplicas`. Absent for non-OMENative components. */
  readyReplicas?: number;
  /** Total Instances (lifecycle.replicas). Absent for non-OMENative components. */
  totalReplicas?: number;
  currentReplicas?: number;
  desiredReplicas?: number;
  rolloutPhase?: string;
  revision?: string;
  url?: string;
}

/** The runtime the operator actually resolves for an ISVC: a namespaced
 *  ServingRuntime shadows a ClusterServingRuntime with the same name, and the
 *  kind declared on the spec is not consulted. `source` says where the name came
 *  from — the spec, or the labels of the service's running pods (the only record
 *  of an auto-selected runtime; supplied on detail responses only). */
export interface ResolvedRuntimeRef {
  kind: "ServingRuntime" | "ClusterServingRuntime";
  name: string;
  namespace?: string;
  source: "spec" | "pods";
}

export interface IsvcItem {
  cluster: string;
  name: string;
  namespace?: string;
  model?: string;
  modelRef?: ResourceRef;
  runtime?: string;
  runtimeRef?: ResourceRef;
  /** Operator-resolved runtime identity; preferred over `runtimeRef`, which
   *  echoes the spec declaration verbatim. */
  resolvedRuntimeRef?: ResolvedRuntimeRef;
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
  /** Accelerator chips currently held by this service's scheduled pods. */
  accelerators?: number;
}

/** One container of an InferenceService pod, for the expanded row. */
export interface IsvcPodContainer {
  name: string;
  ready: boolean;
  restarts: number;
  /** Current state: "running" | "waiting" | "terminated". */
  state?: string;
  /** Current waiting/terminated reason (`CrashLoopBackOff`, …). */
  reason?: string;
  /** Why the previous run ended (`OOMKilled`, `exit 137`). */
  lastRestart?: string;
}

/** One pod of an InferenceService (`GET /v1/inferenceservices/{name}/pods`):
 *  identity, health, and — when stuck or restarting — why. */
export interface IsvcPod {
  name: string;
  component?: string;
  phase?: string;
  ready: boolean;
  restarts: number;
  /** Why the pod is stuck: the scheduling failure (`Unschedulable`), a container
   *  waiting/terminated reason (`ImagePullBackOff`, `CrashLoopBackOff`, …), or
   *  the pod-level not-Ready reason. Absent for a healthy pod. */
  reason?: string;
  /** Why the most-restarted container's previous run ended. */
  lastRestart?: string;
  node?: string;
  accelerators?: number;
  age?: string;
  containers?: IsvcPodContainer[];
}

/** The pods response: cached pods of one InferenceService, problems first. */
export interface IsvcPodsResp {
  cluster: string;
  namespace: string;
  name: string;
  count: number;
  pods: IsvcPod[];
}

/** One Kubernetes Event about a pod — the "why" narrative (BackOff, probe
 *  failures, kills) the status fields can't carry. */
export interface PodEvent {
  type?: string;
  reason?: string;
  message?: string;
  count?: number;
  lastSeen?: string;
}

/** One container from the live pod fetch: full messages and the previous run's
 *  exit (what/when/why). */
export interface PodInspectContainer {
  name: string;
  ready: boolean;
  restarts: number;
  state?: string;
  reason?: string;
  message?: string;
  runningFor?: string;
  lastExit?: string;
  lastExitAt?: string;
  lastExitMessage?: string;
}

/** Live, per-click inspection of one pod: full condition/state messages plus
 *  its recent Events. */
export interface PodInspect {
  cluster: string;
  name: string;
  conditions?: ConditionItem[];
  containers?: PodInspectContainer[];
  events?: PodEvent[];
}

/** A bounded log tail of one pod's current or previous run. */
export interface PodLogs {
  cluster: string;
  namespace: string;
  pod: string;
  container?: string;
  previous: boolean;
  tail: number;
  text: string;
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

/** A serving-runtime container's env var. */
export interface RuntimeEnvVar {
  name: string;
  value?: string;
}
/** One serving component's container config (from the runtime detail). */
export interface RuntimeContainer {
  role: string;
  image?: string;
  command?: string[];
  args?: string[];
  env?: RuntimeEnvVar[];
  resources?: { requests?: Record<string, string>; limits?: Record<string, string> };
}
/** Full runtime detail: the overview plus each component's serving-container config. */
export interface RuntimeDetail extends RuntimeItem {
  containers?: RuntimeContainer[];
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
  /** Problems with the node (NotReady / cordoned / pressure); empty = healthy. */
  issues?: string[];
  age?: string;
}

/** Full node detail (row drill-down): the overview plus conditions, taints, and
 *  the pods currently scheduled on it. */
export interface NodeDetail extends NodeItem {
  conditions?: ConditionItem[];
  taints?: { key: string; value?: string; effect: string }[];
  podsOnNode?: {
    namespace: string;
    name: string;
    accelerators?: number;
    /** Owning InferenceService, for OME serving pods. */
    deployment?: string;
  }[];
}

export interface EventItem {
  cluster: string;
  kind: string;
  namespace?: string;
  name: string;
  action: "applied" | "deleted";
  at: number;
}

/** Compact live-query refresh hint, delivered separately from activity items. */
export interface InvalidationEvent {
  resources: string[];
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

/** One OME control-plane component's health on one cluster (`GET /v1/system`,
 *  OEP-0021.2). `status` distinguishes `missing` (expected but absent) and
 *  `unknown` (scaled to zero) from a live `degraded`/`unavailable`. */
export type ComponentStatus = "healthy" | "degraded" | "unavailable" | "missing" | "unknown";
/** One pod backing a system component (name/phase/node — a liveness glance). */
export interface SystemPod {
  name: string;
  phase: string;
  node?: string;
}
export interface SystemComponentItem {
  cluster: string;
  name: string;
  group?: string;
  kind: "Deployment" | "DaemonSet";
  namespace: string;
  status: ComponentStatus;
  desired: number;
  ready: number;
  updated?: number;
  message?: string;
  /** Matched workload object name(s) on this cluster (empty ⇒ missing). */
  objects?: string[];
  conditions?: ConditionItem[];
  pods?: SystemPod[];
}

/** Health thresholds where larger values are worse. */
export interface UpperThresholdPolicy {
  warningAt: number;
  criticalAt: number;
}

/** Health thresholds where smaller values are worse. */
export interface LowerThresholdPolicy {
  warningBelow: number;
  criticalBelow: number;
}

/** Optional dashboard health policy. Percent metrics use 0..100; cache metrics
 *  use 0..1 fractions. Missing categories render neutrally. */
export interface DashboardPolicy {
  utilization?: UpperThresholdPolicy;
  availability?: LowerThresholdPolicy;
  errorRate?: UpperThresholdPolicy;
  kvCache?: UpperThresholdPolicy;
  prefixCache?: LowerThresholdPolicy;
}

/** One operator-configured Deployment Monitoring window. */
export interface MetricsTimeRangeOption {
  id: string;
  label: string;
  seconds: number;
  stepSeconds: number;
}

/** Available Monitoring windows and their explicit initial selection. */
export interface MetricsTimeRanges {
  default: string;
  options: MetricsTimeRangeOption[];
}

/** Feature flags and display policy from `GET /v1/config` (read once at load). */
export interface AppConfig {
  chat: boolean;
  /** Server-side total budget (seconds) for a chat proxy request; the console
   *  bounds its own request a little above this so a stall can't hang the UI. */
  chatTimeoutSecs?: number;
  /** Browser-side grace beyond the server timeout, supplied by deployment config. */
  chatClientGraceSecs?: number;
  /** Whether on-demand pod inspection (live status, events, log tails) is
   *  configured; the console hides those affordances when false. */
  podInspect?: boolean;
  /** Log-tail line cap for pod inspection, when configured. */
  podLogTailLines?: number;
  /** Whether live log streaming is configured. */
  podLogStream?: boolean;
  /** Pods one stream session follows at most, when streaming is configured. */
  podLogStreamMaxPods?: number;
  /** Whether the metrics proxy is configured server-side; panels hide when false. */
  metrics?: boolean;
  /** Whether the catalog can safely attribute fleet-wide metrics. */
  fleetMetrics?: boolean;
  /** Active-view metrics polling cadence. */
  metricsRefreshSecs?: number;
  dashboardPolicy?: DashboardPolicy;
  metricsTimeRanges?: MetricsTimeRanges;
}

/** One Prometheus result series: instant queries carry `value`, range queries
 *  carry `values` (a `[unixSeconds, stringValue]` pair or list). */
export interface PromSeries {
  metric: Record<string, string>;
  value?: [number, string];
  values?: [number, string][];
}
/** A Prometheus query result, or `{ error }` when the server couldn't run it. */
export interface PromResult {
  status?: string;
  data?: { resultType: string; result: PromSeries[] };
  error?: string;
}

export interface MetricsSourceState {
  status: "available" | "unconfigured" | "unavailable";
  target: string;
  error?: {
    code: string;
    message: string;
    httpStatus?: number;
  };
}
/** A scoped metrics snapshot: per cluster, per query id, a Prometheus result. */
export interface MetricsSnapshot {
  updatedAt: number;
  /** Shared Prometheus evaluation timestamp for every query in this snapshot. */
  evaluatedAt?: number;
  clusters: Record<string, Record<string, PromResult>>;
  /** Per-cluster target discovery. Non-available sources have no query results. */
  sources?: Record<string, MetricsSourceState>;
}

export interface MetricsRequest {
  cluster?: string;
  namespace?: string;
  inferenceservice?: string;
  range?: string;
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

/** SSE endpoint for named activity and query-invalidation events (see `live.ts`). */
export const STREAM_URL = at("/v1/stream");

/** SSE endpoint following logs live across a deployment's pods (stern-style):
 *  `line` events carry `{pod, line}`, `notice` events carry `{message}`. */
export function logStreamUrl(
  ref: Selected,
  opts: { component?: string; pod?: string; grep?: string; tail?: number } = {},
): string {
  const sp = new URLSearchParams({ cluster: ref.cluster });
  if (ref.namespace) sp.set("namespace", ref.namespace);
  if (opts.component) sp.set("component", opts.component);
  if (opts.pod) sp.set("pod", opts.pod);
  if (opts.grep) sp.set("grep", opts.grep);
  if (opts.tail != null) sp.set("tail", String(opts.tail));
  return at(`/v1/inferenceservices/${enc(ref.name)}/logs/stream?${sp}`);
}

/**
 * Parse a response body as JSON, turning a non-JSON payload into a legible error
 * instead of a raw `Unexpected token '<' … is not valid JSON` from `res.json()`.
 * A proxy in front of the API (e.g. the Silfra tunnel) replaces a non-2xx muse
 * response — or a request it can't route — with its own HTML error page, often
 * served with a 2xx status, so an HTML body can reach even the success path.
 */
async function asJSON<T>(res: Response, path: string): Promise<T> {
  const body = await res.text();
  const ct = res.headers.get("content-type") ?? "";
  if (!ct.includes("application/json")) {
    throw new Error(
      `${path} returned a non-JSON response (${res.status} ${res.statusText}) — ` +
        `the API may be unreachable or a proxy intercepted the request.`,
    );
  }
  try {
    return JSON.parse(body) as T;
  } catch {
    throw new Error(`${path} returned a malformed JSON response (${res.status}).`);
  }
}

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(at(path), { headers: { accept: "application/json" } });
  if (!res.ok) {
    let code: string | undefined;
    let message = `${path} → ${res.status} ${res.statusText}`;
    try {
      const body = (await res.json()) as { error?: { code?: string; message?: string } };
      code = body.error?.code;
      if (body.error?.message) message = body.error.message;
    } catch {
      // Non-JSON error body: retain the status line.
    }
    throw new ApiRequestError(res.status, code, message);
  }
  return asJSON<T>(res, path);
}

class ApiRequestError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | undefined,
    message: string,
  ) {
    super(message);
    this.name = "ApiRequestError";
  }
}

/** True when the server definitively reported the resource as nonexistent — as
 *  opposed to a transient transport or server failure. Callers use this to show
 *  "does not exist" only when the API actually said so. */
export function isNotFoundError(error: unknown): boolean {
  return error instanceof ApiRequestError && error.status === 404 && error.code === "not_found";
}

function fallbackOnNotFound<T>(request: Promise<T>, fallback: () => Promise<T>): Promise<T> {
  return request.catch((error: unknown) => {
    if (isNotFoundError(error)) return fallback();
    throw error;
  });
}

async function postJSON<T>(path: string, body: unknown, timeoutMs?: number): Promise<T> {
  // Optional client-side deadline: abort the request if the server hasn't
  // responded in time, so a stalled upstream can't leave the UI hanging.
  const ctrl = timeoutMs != null ? new AbortController() : undefined;
  const timer = ctrl ? setTimeout(() => ctrl.abort(), timeoutMs) : undefined;
  try {
    const res = await fetch(at(path), {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify(body),
      signal: ctrl?.signal,
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
    return asJSON<T>(res, path);
  } catch (e) {
    if (ctrl?.signal.aborted) {
      const secs = Math.round((timeoutMs ?? 0) / 1000);
      throw new Error(
        `no response within ${secs}s — the endpoint may be unreachable from the console`,
      );
    }
    throw e;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

const enc = encodeURIComponent;

function resourceParams(ref: Selected): URLSearchParams {
  const sp = new URLSearchParams({ cluster: ref.cluster });
  if (ref.namespace) sp.set("namespace", ref.namespace);
  return sp;
}

function resourcePath(ref: Selected): string {
  return `/v1/${RESOURCE_PLURALS[ref.kind]}/${enc(ref.name)}?${resourceParams(ref)}`;
}

function metricsPath(request: MetricsRequest): string {
  const params = new URLSearchParams();
  if (request.cluster) params.set("cluster", request.cluster);
  if (request.namespace) params.set("namespace", request.namespace);
  if (request.inferenceservice) params.set("inferenceservice", request.inferenceservice);
  if (request.range) params.set("range", request.range);
  const query = params.toString();
  return `/v1/metrics${query ? `?${query}` : ""}`;
}

export const api = {
  clusters: () => getJSON<ClustersResp>("/v1/clusters"),
  attention: () => getJSON<ListResp<AttentionItem>>("/v1/attention"),
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
  nodesForCluster: (name: string) => getJSON<ListResp<NodeItem>>(`/v1/nodes?cluster=${enc(name)}`),
  nodeDetail: (cluster: string, name: string) =>
    getJSON<NodeDetail>(`/v1/nodes/${enc(name)}?cluster=${enc(cluster)}`),
  events: () => getJSON<ListResp<EventItem>>("/v1/events"),
  rollouts: () => getJSON<ListResp<RolloutItem>>("/v1/rollouts"),
  system: () => getJSON<ListResp<SystemComponentItem>>("/v1/system"),
  // Per-object detail for the drawers; kind + cluster + namespace + name preserve
  // full identity. Namespace remains optional for backward-compatible deep links.
  inferenceService: (ref: Selected) =>
    getJSON<IsvcDetail>(resourcePath({ ...ref, kind: "InferenceService" })),
  // The service's cached pods, for the deployment diagnosis view.
  inferenceServicePods: (ref: Selected) =>
    getJSON<IsvcPodsResp>(`/v1/inferenceservices/${enc(ref.name)}/pods?${resourceParams(ref)}`),
  // Live per-click inspection of one pod (status with full messages + Events).
  inferenceServicePodInspect: (ref: Selected, pod: string) =>
    getJSON<PodInspect>(
      `/v1/inferenceservices/${enc(ref.name)}/pods/${enc(pod)}?${resourceParams(ref)}`,
    ),
  // Bounded log tail of one pod's current or previous run.
  inferenceServicePodLogs: (
    ref: Selected,
    pod: string,
    opts: { container?: string; previous?: boolean } = {},
  ) => {
    const sp = resourceParams(ref);
    if (opts.container) sp.set("container", opts.container);
    if (opts.previous) sp.set("previous", "true");
    return getJSON<PodLogs>(`/v1/inferenceservices/${enc(ref.name)}/pods/${enc(pod)}/logs?${sp}`);
  },
  model: (ref: Selected) => getJSON<ModelDetail>(resourcePath(ref)),
  legacyModel: (cluster: string, name: string) =>
    fallbackOnNotFound(
      getJSON<ModelDetail>(resourcePath({ cluster, name, kind: "ClusterBaseModel" })),
      () => getJSON<ModelDetail>(resourcePath({ cluster, name, kind: "BaseModel" })),
    ),
  runtime: (ref: Selected) => getJSON<RuntimeDetail>(resourcePath(ref)),
  // Namespaced first, matching how the operator resolves a runtime name. Legacy
  // bookmark URLs carry only cluster + name, so the ServingRuntime probe is
  // unscoped and returns the first name match across namespaces — ambiguous when
  // unrelated namespaces reuse a runtime name. Callers that know the full
  // identity should use `runtime` instead.
  legacyRuntime: (cluster: string, name: string) =>
    fallbackOnNotFound(
      getJSON<RuntimeDetail>(resourcePath({ cluster, name, kind: "ServingRuntime" })),
      () => getJSON<RuntimeDetail>(resourcePath({ cluster, name, kind: "ClusterServingRuntime" })),
    ),
  // Console feature flags + the inference test proxy (off unless the server enables it).
  config: () => getJSON<AppConfig>("/v1/config"),
  // Query or reuse one bounded fleet/deployment metrics scope.
  metrics: (request: MetricsRequest = {}) => getJSON<MetricsSnapshot>(metricsPath(request)),
  chat: (ref: Selected, path: string, base: string, body: unknown, timeoutMs?: number) => {
    const sp = resourceParams(ref);
    sp.set("path", path);
    sp.set("base", base);
    return postJSON<ChatResponse>(
      `/v1/inferenceservices/${enc(ref.name)}/chat?${sp}`,
      body,
      timeoutMs,
    );
  },
};
