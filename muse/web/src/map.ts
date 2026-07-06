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
  RolloutItem,
} from "./api";
import type { Cloud, ClusterRow, FeedItem, Pill, Rollout, Tone, WorkloadRow } from "./types";
import { ago, utilPct, utilTone } from "./util";

// ---- small helpers -------------------------------------------------------

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
    const pct = utilPct({ allocatable: alloc, allocated: used });
    const kinds = [...new Set(groups.map((g) => g.kind))];
    const kindLabel = kinds.length === 1 ? kinds[0].toUpperCase() : "accel";
    return {
      name: c.name,
      cloud: cloudOf(c.name),
      region: regionOf(c.name),
      // e.g. "51 GPU · 57 total" — 51 GPU nodes of 57 (the rest are CPU/system).
      nodes: `${c.acceleratorNodes} ${kindLabel} · ${c.nodes} total`,
      accel: groups.length === 0 ? "—" : `${alloc} ${kindLabel}`,
      kind: kindLabel,
      free: alloc - used,
      utilPct: pct,
      barTone: utilTone(pct),
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

/** Fleet-wide deployment readiness rollup, plus the not-ready ones to surface. */
export interface FleetHealth {
  ready: number;
  notReady: number;
  unknown: number;
  total: number;
  attention: { name: string; cluster: string; namespace?: string; reason?: string }[];
}

export function deploymentHealth(isvcs: IsvcItem[]): FleetHealth {
  let ready = 0;
  let notReady = 0;
  let unknown = 0;
  const attention: FleetHealth["attention"] = [];
  for (const s of isvcs) {
    if (s.ready === true) {
      ready++;
    } else if (s.ready === false) {
      notReady++;
      attention.push({
        name: s.name,
        cluster: s.cluster,
        namespace: s.namespace,
        reason: s.reason,
      });
    } else {
      unknown++;
    }
  }
  return { ready, notReady, unknown, total: isvcs.length, attention };
}

// ---- Models ---------------------------------------------------------------

/** Lifecycle state → tone: Ready=green, Failed=red, in-progress=amber, else slate. */
export function stateTone(state?: string): Tone {
  if (!state) return "slate";
  if (state === "Ready") return "green";
  if (state === "Failed") return "red";
  return "amber"; // Importing / Converting / In Transit / …
}
