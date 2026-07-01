// Map muse read-plane responses into the view-model shapes the panels render.
//
// The API DTOs are deliberately lean; display-only attributes the API doesn't
// carry (cloud, region) are inferred here, best-effort. Sections with no backing
// endpoint (Workloads PD/canary detail, Models onboarding pipeline) are left
// empty and the panels render a placeholder.

import type {
  CapacityResp,
  ClusterItem,
  ComponentItem,
  EventItem,
  IsvcItem,
  ModelItem,
  RolloutItem,
} from "./api";
import type {
  BarTone,
  Cloud,
  ClusterRow,
  FeedItem,
  ModelCard,
  Pill,
  Rollout,
  Tone,
  WorkloadRow,
} from "./types";

// ---- small helpers -------------------------------------------------------

/** Accelerator utilization percent (allocated / allocatable). */
function util(g: { allocatable: number; allocated: number }): number {
  return g.allocatable > 0 ? Math.round((g.allocated / g.allocatable) * 100) : 0;
}

function barToneFor(pct: number): BarTone {
  if (pct >= 90) return "red";
  if (pct >= 75) return "amber";
  return "green";
}

/** Best-effort cloud from a cluster name. */
function cloudOf(name: string): Cloud {
  const n = name.toLowerCase();
  if (n.includes("gcp") || n.includes("gke")) return "gcp";
  if (n.includes("oci")) return "oci";
  if (n.includes("meta") || n.includes("fb")) return "meta";
  return "cw";
}

/** Best-effort region from a cluster name, "—" when unknown. */
function regionOf(name: string): string {
  const n = name.toLowerCase();
  if (n.includes("usc1")) return "us-central1";
  if (n.includes("use2")) return "us-east-2";
  if (n.includes("use1")) return "us-east-1";
  if (n.includes("usw2")) return "us-west-2";
  return "—";
}

/** Relative "Ns/Nm/Nh/Nd ago" from an epoch-seconds timestamp. */
function ago(atSecs: number): string {
  const secs = Math.max(0, Math.floor(Date.now() / 1000) - atSecs);
  if (secs >= 86_400) return `${Math.floor(secs / 86_400)}d ago`;
  if (secs >= 3_600) return `${Math.floor(secs / 3_600)}h ago`;
  if (secs >= 60) return `${Math.floor(secs / 60)}m ago`;
  return `${secs}s ago`;
}

const MODE_TONE: Record<string, Tone> = {
  RawDeployment: "slate",
  MultiNode: "blue",
  PDDisaggregated: "violet",
  PDisaggregated: "violet",
  OMENative: "cyan",
  Serverless: "green",
};
function modePill(mode: string | undefined): Pill {
  return { label: mode ?? "—", tone: (mode && MODE_TONE[mode]) || "slate" };
}

// ---- Fleet ----------------------------------------------------------------

export function toClusterRows(
  clusters: ClusterItem[],
  capacity: CapacityResp | null,
): ClusterRow[] {
  const byCluster = capacity?.byCluster ?? [];
  return clusters.map((c) => {
    const groups = byCluster.filter((g) => g.name === c.name);
    const alloc = groups.reduce((s, g) => s + g.allocatable, 0);
    const used = groups.reduce((s, g) => s + g.allocated, 0);
    const pct = util({ allocatable: alloc, allocated: used });
    const kinds = [...new Set(groups.map((g) => g.kind))];
    const kindLabel = kinds.length === 1 ? kinds[0].toUpperCase() : "accel";
    return {
      name: c.name,
      cloud: cloudOf(c.name),
      region: regionOf(c.name),
      // e.g. "51 GPU · 57 total" — 51 GPU nodes of 57 (the rest are CPU/system).
      nodes: `${c.acceleratorNodes} ${kindLabel} · ${c.nodes} total`,
      accel: groups.length === 0 ? "—" : `${alloc} ${kindLabel}`,
      utilPct: pct,
      barTone: barToneFor(pct),
      status: c.ready ? { label: "Ready", tone: "green" } : { label: "Syncing", tone: "amber" },
    };
  });
}

const FEED_ICON: Record<string, FeedItem["icon"]> = {
  InferenceService: "deploy",
  BaseModel: "cube",
  ClusterBaseModel: "cube",
  ServingRuntime: "lines",
  ClusterServingRuntime: "lines",
  FineTunedWeight: "cube",
  WorkloadCluster: "lines",
};
function feedTone(event: EventItem): FeedItem["tone"] {
  if (event.action === "deleted") return "amber";
  if (event.kind === "InferenceService") return "blue";
  if (event.kind.endsWith("Model")) return "violet";
  return "green";
}

/** Plain-language noun for a CRD kind — the console never shows raw kind names. */
const HUMAN_KIND: Record<string, string> = {
  InferenceService: "deployment",
  BaseModel: "model",
  ClusterBaseModel: "model",
  ServingRuntime: "runtime",
  ClusterServingRuntime: "runtime",
  FineTunedWeight: "fine-tuned weight",
  WorkloadCluster: "cluster",
};

export function toFeed(events: EventItem[]): FeedItem[] {
  return events.map((e) => ({
    tone: feedTone(e),
    icon: e.action === "deleted" ? "warning" : (FEED_ICON[e.kind] ?? "lines"),
    strong: e.name,
    rest: ` ${HUMAN_KIND[e.kind] ?? "resource"} ${e.action === "deleted" ? "removed" : "updated"}`,
    meta: e.cluster,
    when: ago(e.at),
  }));
}

export function toRollouts(rollouts: RolloutItem[]): Rollout[] {
  return rollouts.map((r) => ({
    name: r.name,
    strategy: modePill(r.deploymentMode),
    barTone: "amber",
    barPct: r.ready ? 90 : 45,
    barStriped: true,
    note: r.message ?? r.reason ?? "rolling out",
    gate: r.ready ? { label: "Ready", tone: "green" } : { label: "In progress", tone: "amber" },
    cluster: r.cluster,
    started: r.age ?? "—",
  }));
}

// ---- Workloads ------------------------------------------------------------

/** Roll up ready/desired replicas across an ISVC's Components into one cell. */
export function replicaRollup(components?: ComponentItem[]): { label: string; tone: Tone } {
  if (!components || components.length === 0) return { label: "—", tone: "slate" };
  let current = 0;
  let desired = 0;
  let haveCurrent = false;
  let haveDesired = false;
  for (const c of components) {
    if (c.currentReplicas != null) {
      current += c.currentReplicas;
      haveCurrent = true;
    }
    if (c.desiredReplicas != null) {
      desired += c.desiredReplicas;
      haveDesired = true;
    }
  }
  if (!haveCurrent && !haveDesired) return { label: "—", tone: "slate" };
  if (!haveDesired) return { label: `${current}`, tone: "slate" };
  const tone: Tone =
    current >= desired && desired > 0 ? "green" : current < desired ? "amber" : "slate";
  return { label: `${current}/${desired}`, tone };
}

/** A short model-load summary: loaded/failed copies, else a non-steady state. */
export function loadSummary(s: IsvcItem): string | undefined {
  const parts: string[] = [];
  if (s.loadedCopies != null) parts.push(`${s.loadedCopies} loaded`);
  if (s.failedCopies) parts.push(`${s.failedCopies} failed`);
  if (parts.length) return parts.join(" · ");
  if (s.modelState && s.modelState !== "UpToDate") return s.modelState;
  return undefined;
}

export function toWorkloadRows(isvcs: IsvcItem[]): WorkloadRow[] {
  return isvcs.map((s) => {
    const status: Pill =
      s.ready === true
        ? { label: "Ready", tone: "green" }
        : s.ready === false
          ? { label: "Not ready", tone: "red" }
          : { label: "Unknown", tone: "slate" };
    return {
      id: `${s.cluster}/${s.namespace ?? ""}/${s.name}`,
      cluster: s.cluster,
      name: s.name,
      namespace: s.namespace ?? "—",
      model: s.model ?? "—",
      runtime: s.runtime ?? "—",
      mode: modePill(s.deploymentMode),
      endpoint: s.url,
      replicas: replicaRollup(s.components),
      load: loadSummary(s),
      reason: s.ready === false ? s.reason : undefined,
      age: s.age ?? "—",
      status,
    };
  });
}

/** Count of InferenceServices referencing each model name, across the fleet. */
export function modelUsage(isvcs: IsvcItem[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const s of isvcs) {
    if (!s.model) continue;
    counts.set(s.model, (counts.get(s.model) ?? 0) + 1);
  }
  return counts;
}

// ---- Models ---------------------------------------------------------------

/** Lifecycle state → pill: Ready=green, Failed=red, in-progress=amber, else slate. */
function statePill(state?: string): Pill {
  if (!state) return { label: "Unknown", tone: "slate" };
  if (state === "Ready") return { label: "Ready", tone: "green" };
  if (state === "Failed") return { label: "Failed", tone: "red" };
  return { label: state, tone: "amber" }; // Importing / Converting / In Transit / …
}

/** Humanize a max-tokens count as a context-window tag (e.g. 131072 → "128k ctx"). */
function contextTag(maxTokens?: number): string | undefined {
  if (!maxTokens || maxTokens <= 0) return undefined;
  return maxTokens >= 1024 ? `${Math.round(maxTokens / 1024)}k ctx` : `${maxTokens} ctx`;
}

export function toModelCards(
  base: ModelItem[],
  cluster: ModelItem[],
  usage?: Map<string, number>,
): ModelCard[] {
  const mk = (m: ModelItem, scoped: boolean): ModelCard => {
    const specs: { label: string; accent?: boolean }[] = [];
    if (m.architecture) specs.push({ label: m.architecture, accent: true });
    if (m.parameterSize && m.parameterSize !== "0") specs.push({ label: m.parameterSize });
    if (m.format) specs.push({ label: m.format });
    if (m.quantization) specs.push({ label: m.quantization });
    const ctx = contextTag(m.maxTokens);
    if (ctx) specs.push({ label: ctx });
    if (m.vendor) specs.push({ label: m.vendor });
    for (const cap of m.capabilities ?? []) if (cap) specs.push({ label: cap });
    return {
      name: m.name,
      cluster: m.cluster,
      namespace: m.namespace,
      scoped,
      status: statePill(m.state),
      where: m.cluster,
      source: m.source,
      usedBy: usage?.get(m.name) ?? 0,
      disabled: m.disabled ?? false,
      specs,
    };
  };
  return [...cluster.map((m) => mk(m, true)), ...base.map((m) => mk(m, false))];
}
