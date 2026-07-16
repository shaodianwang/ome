// View-model types for the MUSE console — the shapes the panels render, mapped
// from the muse read-plane API (see src/map.ts and src/api.ts).

export type Tone = "green" | "amber" | "red" | "blue" | "violet" | "cyan" | "slate";
export type BarTone = "" | "green" | "amber" | "red" | "violet";

/** A cluster-scoped reference to a named resource — the shape used for detail
 *  views and cross-panel navigation. */
export interface Selected {
  cluster: string;
  name: string;
}

export interface Pill {
  label: string;
  tone: Tone;
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
