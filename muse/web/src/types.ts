// View-model types for the MUSE console.
//
// These describe what the UI renders. For now they're filled by `mock.ts`; the
// follow-up that wires the live read plane will map the muse API responses
// (/v1/clusters, /v1/capacity, /v1/inferenceservices, /v1/{base,cluster}models,
// /v1/nodes) into these same shapes, so only the data source changes.

export type Cloud = "meta" | "gcp" | "oci" | "cw";
export type Tone = "green" | "amber" | "red" | "blue" | "violet" | "cyan" | "slate";
export type BarTone = "" | "green" | "amber" | "red" | "violet";
export type CellState = "used" | "free" | "hot" | "down";
export type StepState = "done" | "cur" | "";
export type ModelSource = "hf" | "meta" | "s3" | "oci";

export const CLOUD_LABEL: Record<Cloud, string> = {
  meta: "Meta",
  gcp: "GCP",
  oci: "OCI",
  cw: "CoreWeave",
};

export interface Pill {
  label: string;
  tone: Tone;
}

/** Fleet · workload clusters table */
export interface ClusterRow {
  name: string;
  cloud: Cloud;
  region: string;
  nodes: string;
  accel: string;
  utilPct: number;
  barTone: BarTone;
  status: Pill;
}

/** Fleet · recent activity feed */
export interface FeedItem {
  tone: "green" | "blue" | "violet" | "amber";
  icon: "deploy" | "arrow" | "check" | "cube" | "warning" | "lines";
  strong: string;
  rest: string;
  meta: string;
  when: string;
}

/** Fleet · active rollouts strip */
export interface Rollout {
  name: string;
  strategy: Pill;
  /** "steps" renders the canary step dots; "bar" renders a progress bar. */
  kind: "steps" | "bar";
  steps?: StepState[];
  barTone?: BarTone;
  barPct?: number;
  barStriped?: boolean;
  note: string;
  gate: Pill;
  cluster: string;
  started: string;
}

/** Workloads · expandable detail */
export interface CompCard {
  badge: Pill;
  title: string;
  kv: [string, string][];
}
export interface CanaryInfo {
  steps: StepState[];
  labels: string[];
  kv: [string, string][];
}
export interface WorkloadDetail {
  components: CompCard[];
  placement?: { cols: string[]; rows: string[][] };
  canary?: CanaryInfo;
  showActions?: boolean;
}

/** Workloads · table row */
export interface WorkloadRow {
  id: string;
  name: string;
  namespace: string;
  model: string;
  modelSub: string;
  runtime: string;
  mode: Pill;
  placement: string;
  ready: string;
  rollout: Pill;
  age: string;
  status: Pill;
  detail?: WorkloadDetail;
}

/** Models · onboarding pipeline node */
export interface PipelineNode {
  label: string;
  state: StepState;
}
/** Models · transfer/convert progress block */
export interface Xfer {
  routeFrom?: { cloud: Cloud; label: string };
  routeTo?: { cloud: Cloud; label: string };
  routeNote?: string;
  heading?: string;
  pct: number;
  tone: BarTone;
  stats: [string, string][];
}
export interface ModelCard {
  source: ModelSource;
  name: string;
  status: Pill;
  uri: string;
  specs: { label: string; accent?: boolean }[];
  pipeline: PipelineNode[];
  /** in-flight convert/transfer progress (mutually exclusive with `replicas`) */
  xfer?: Xfer;
  /** replica placement chips for Ready models */
  replicas?: { cloud: Cloud; label: string }[];
}

/** Capacity · RDMA island */
export interface Island {
  name: string;
  status: Pill;
  cells: CellState[];
  nodes: string;
  util: string;
}
/** Capacity · TPU slice */
export interface Slice {
  name: string;
  status: Pill;
  topo: string;
  free: number;
  total: number;
  barTone: BarTone;
  barPct: number;
  chips: string;
}
/** Capacity · GPU pool */
export interface Pool {
  name: string;
  cloud: Cloud;
  accel: string;
  status: Pill;
  total: string;
  inUse: string;
  free: string;
  barTone: BarTone;
  barPct: number;
}
