// Map muse read-plane responses into the view-model shapes the panels render.
//
// Sections with no backing endpoint (Workloads PD/canary detail, Models onboarding
// pipeline) are left empty and the panels render a placeholder.

import type { ComponentItem, ConditionItem, EventItem, IsvcItem, RolloutItem } from "./api";
import { resourceKey, selectResource } from "./resource";
import type { ResourceRef, Selected } from "./resource";
import type { FeedItem, Pill, Rollout, Tone, WorkloadRow } from "./types";
import { ago } from "./util";

// ---- Fleet ----------------------------------------------------------------

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
    namespace: r.namespace,
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

// ---- Deployment overview tiles ---------------------------------------------

export type ConditionRank = "bad" | "normal" | "disabled";

/** Tile rank for one condition. A `…Disabled` reason marks a capability turned
 *  off by spec (e.g. ingress) — an expected state rendered dimmed, never a
 *  failure; any other False condition is failing. */
export function conditionRank(c: ConditionItem): ConditionRank {
  if (c.reason?.endsWith("Disabled")) return "disabled";
  return c.status === "False" ? "bad" : "normal";
}

const RANK_ORDER: Record<ConditionRank, number> = { bad: 0, normal: 1, disabled: 2 };

/** Condition tiles in reading order: failing first, disabled-by-spec last;
 *  stable within a rank. */
export function sortConditions(conditions: ConditionItem[]): ConditionItem[] {
  return [...conditions].sort(
    (a, b) => RANK_ORDER[conditionRank(a)] - RANK_ORDER[conditionRank(b)],
  );
}

/** Tile tone for one component: red when nothing is live against a nonzero
 *  target, amber when short of target or reported not ready. */
export function componentTone(c: ComponentItem): "ok" | "warn" | "bad" {
  const cur = c.currentReplicas ?? 0;
  const des = c.desiredReplicas ?? 0;
  if (des > 0 && cur === 0) return "bad";
  return c.ready === false || cur < des ? "warn" : "ok";
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
      namespace: s.namespace,
      model: s.model ?? "—",
      modelRef: s.modelRef,
      runtime: s.runtime ?? "—",
      runtimeRef: runtimeRefOf(s),
      endpoint: s.url,
      replicas: replicaRollup(s.components),
      load: loadSummary(s),
      reason: s.ready === false ? s.reason : undefined,
      age: s.age ?? "—",
      status,
    };
  });
}

function referencedResource(
  isvc: IsvcItem,
  ref: ResourceRef | undefined,
  legacyName: string | undefined,
  legacyKind: "ClusterBaseModel" | "ClusterServingRuntime",
): Selected | undefined {
  const resolved = ref ?? (legacyName ? { kind: legacyKind, name: legacyName } : undefined);
  return resolved ? selectResource(isvc.cluster, resolved) : undefined;
}

export function modelReference(isvc: IsvcItem): Selected | undefined {
  return referencedResource(isvc, isvc.modelRef, isvc.model, "ClusterBaseModel");
}

/** The runtime ref to show for an ISVC: the operator-resolved identity when the
 *  server supplies it, else the spec declaration (whose kind can be wrong — the
 *  operator never consults it). */
export function runtimeRefOf(isvc: IsvcItem): ResourceRef | undefined {
  const resolved = isvc.resolvedRuntimeRef;
  if (resolved) {
    return { kind: resolved.kind, name: resolved.name, namespace: resolved.namespace };
  }
  return isvc.runtimeRef;
}

export function runtimeReference(isvc: IsvcItem): Selected | undefined {
  return referencedResource(isvc, runtimeRefOf(isvc), isvc.runtime, "ClusterServingRuntime");
}

/** Count InferenceServices by the full identity of the model they reference. */
export function modelUsage(isvcs: IsvcItem[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const s of isvcs) {
    const ref = modelReference(s);
    if (!ref) continue;
    const key = resourceKey(ref);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return counts;
}

/** Count InferenceServices by the full identity of the runtime they reference. */
export function runtimeUsage(isvcs: IsvcItem[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const s of isvcs) {
    const ref = runtimeReference(s);
    if (!ref) continue;
    const key = resourceKey(ref);
    counts.set(key, (counts.get(key) ?? 0) + 1);
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
