// View-model types for the MUSE console — the shapes the panels render, mapped
// from the muse read-plane API (see src/map.ts and src/api.ts).

export type Cloud = "meta" | "gcp" | "oci" | "cw";
export type Tone = "green" | "amber" | "red" | "blue" | "violet" | "cyan" | "slate";
export type BarTone = "" | "green" | "amber" | "red" | "violet";

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

/** Fleet · active rollouts */
export interface Rollout {
  name: string;
  strategy: Pill;
  barTone: BarTone;
  barPct: number;
  barStriped?: boolean;
  note: string;
  gate: Pill;
  cluster: string;
  started: string;
}

/** Workloads · table row */
export interface WorkloadRow {
  id: string;
  cluster: string;
  name: string;
  namespace: string;
  model: string;
  runtime: string;
  mode: Pill;
  /** Servable endpoint (status.url), when ready. */
  endpoint?: string;
  /** Rolled-up ready/desired replicas across Components, with a health tone. */
  replicas: { label: string; tone: Tone };
  /** Model-load summary (e.g. "12 loaded", "2 failed"), when known. */
  load?: string;
  /** Why it isn't ready (coarse reason), shown only when not ready. */
  reason?: string;
  age: string;
  status: Pill;
}

/** Models · registry card (real BaseModel / ClusterBaseModel metadata) */
export interface ModelCard {
  name: string;
  cluster: string;
  namespace?: string;
  scoped: boolean;
  status: Pill;
  where: string;
  /** Storage backend scheme (manifold / pvc / hf / …). */
  source?: string;
  /** Live count of InferenceServices in the fleet referencing this model. */
  usedBy: number;
  disabled: boolean;
  specs: { label: string; accent?: boolean }[];
}
