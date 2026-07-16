// View-model types for the MUSE console — the shapes the panels render, mapped
// from the muse read-plane API (see src/map.ts and src/api.ts).

import type { ResourceRef } from "./resource";
export type Tone = "green" | "amber" | "red" | "blue" | "violet" | "cyan" | "slate";
export type BarTone = "" | "green" | "amber" | "red" | "violet";

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
  namespace?: string;
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
  namespace?: string;
  model: string;
  modelRef?: ResourceRef;
  runtime: string;
  runtimeRef?: ResourceRef;
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
