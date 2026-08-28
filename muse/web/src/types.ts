// View-model types for the MUSE console — the shapes the panels render, mapped
// from the muse read-plane API (see src/map.ts and src/api.ts).

import type { ResourceRef, Selected } from "./resource";
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
  /** Full identity for click-through, when the event names a deployment that
   *  still exists (deletions have nothing to open). */
  sel?: Selected;
}

/** Fleet · active rollouts */
export interface Rollout {
  name: string;
  namespace?: string;
  barTone: BarTone;
  /** Real progress: ready/total replicas across the components mid-rollout. */
  barPct: number;
  barStriped?: boolean;
  note: string;
  gate: Pill;
  cluster: string;
  started: string;
  /** Per-component rollout state (phase + revision), for the expanded card. */
  components?: ComponentCell[];
  /** In-flight canary (step, observed traffic weight, failing checks). */
  canary?: { currentStep: number; trafficWeight: number; failedChecks?: number };
  /** Full identity for click-through to the deployment. */
  sel: Selected;
}

/** One component's replica state, rendered as a compact chip. */
export interface ComponentCell {
  name: string;
  /** Short display label, e.g. "eng 245/251" (with "→N" when scaling). */
  label: string;
  tone: "ok" | "warn" | "bad";
  phase?: string;
  revision?: string;
  /** Revision hashes mid-rollout: what the bulk runs → what it converges to.
   *  `from` is absent on an initial rollout. Unset at steady state. */
  transition?: { from?: string; to: string };
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
  /** Per-component replica chips (engine / decoder / router), when reported. */
  components: ComponentCell[];
  /** Deployment mode (OMENative / RawDeployment / …), when reported. */
  mode?: string;
  /** Accelerator chips currently held by the deployment's scheduled pods. */
  accelerators?: number;
  /** Whether any component is mid-rollout (non-Stable rollout phase). */
  rolling: boolean;
  /** Model-load summary (e.g. "12 loaded", "2 failed"), when known. */
  load?: string;
  /** Why it isn't ready (coarse reason), shown only when not ready. */
  reason?: string;
  age: string;
  status: Pill;
}
