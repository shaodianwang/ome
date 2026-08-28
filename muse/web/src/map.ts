// Map muse read-plane responses into the view-model shapes the panels render.

import type { ComponentItem, ConditionItem, EventItem, IsvcItem, RolloutItem } from "./api";
import { isResourceKind, resourceKey, selectResource } from "./resource";
import type { ResourceRef, Selected } from "./resource";
import type { ComponentCell, FeedItem, Pill, Rollout, Tone, WorkloadRow } from "./types";
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
    // Every kind with a detail surface links to it; deleted resources have
    // nowhere to land, so those rows stay inert.
    sel:
      e.action !== "deleted" && isResourceKind(e.kind)
        ? { cluster: e.cluster, kind: e.kind, name: e.name, namespace: e.namespace }
        : undefined,
  }));
}

/** Revision change a component is converging through: what the bulk currently
 *  runs → what the rollout targets. Undefined at steady state (revisions equal
 *  or unreported). `from` is absent on an initial rollout. */
export function componentTransition(c: ComponentItem): { from?: string; to: string } | undefined {
  if (!c.updateRevision || c.updateRevision === c.currentRevision) return undefined;
  return { from: c.currentRevision, to: c.updateRevision };
}

/** Ready-vs-total replica progress across components, as a 0–100 percentage.
 *  While a component converges to a new revision, only Instances ready on that
 *  revision count — old-revision pods keep `readyReplicas` high under surge and
 *  would fake a finished bar. `null` when no component reports counts. */
export function rolloutProgress(components?: ComponentItem[]): number | null {
  let ready = 0;
  let total = 0;
  for (const c of components ?? []) {
    const r = componentReplicas(c);
    if (r.total != null) {
      const progressed =
        componentTransition(c) && c.updatedReadyReplicas != null
          ? c.updatedReadyReplicas
          : (r.ready ?? 0);
      ready += Math.min(progressed, r.total);
      total += r.total;
    }
  }
  if (total === 0) return null;
  return Math.round((100 * ready) / total);
}

/** Fuse the rollouts listing with the deployment rows it refers to, so the cards
 *  carry real replica progress and per-component rollout state instead of a
 *  fabricated bar. */
export function toRollouts(rollouts: RolloutItem[], isvcs: IsvcItem[]): Rollout[] {
  const byId = new Map(isvcs.map((s) => [`${s.cluster}/${s.namespace ?? ""}/${s.name}`, s]));
  return rollouts.map((r) => {
    const isvc = byId.get(`${r.cluster}/${r.namespace ?? ""}/${r.name}`);
    const pct = rolloutProgress(isvc?.components);
    return {
      name: r.name,
      namespace: r.namespace,
      barTone: "amber" as const,
      barPct: pct ?? 0,
      barStriped: !r.ready,
      note: r.message ?? r.reason ?? "rolling out",
      gate: r.ready
        ? { label: "Ready", tone: "green" as const }
        : { label: "In progress", tone: "amber" as const },
      cluster: r.cluster,
      started: r.age ?? "—",
      components: componentCells(isvc?.components),
      canary: isvc?.canary,
      sel: {
        cluster: r.cluster,
        kind: "InferenceService" as const,
        name: r.name,
        namespace: r.namespace,
      },
    };
  });
}

// ---- Workloads ------------------------------------------------------------

/** Ready/total counts for one Component. Prefers the lifecycle counts
 *  (which exclude still-initializing Instances); falls back to the autoscaler
 *  scale counts for non-OMENative components that report no lifecycle. */
export function componentReplicas(c: ComponentItem): {
  ready: number | null;
  total: number | null;
} {
  const ready = c.readyReplicas ?? c.currentReplicas ?? null;
  const total = c.totalReplicas ?? c.desiredReplicas ?? null;
  return { ready, total };
}

/** Roll up ready/total replicas across an ISVC's Components into one cell. */
export function replicaRollup(components?: ComponentItem[]): { label: string; tone: Tone } {
  if (!components || components.length === 0) return { label: "—", tone: "slate" };
  let ready = 0;
  let total = 0;
  let haveReady = false;
  let haveTotal = false;
  for (const c of components) {
    const r = componentReplicas(c);
    if (r.ready != null) {
      ready += r.ready;
      haveReady = true;
    }
    if (r.total != null) {
      total += r.total;
      haveTotal = true;
    }
  }
  if (!haveReady && !haveTotal) return { label: "—", tone: "slate" };
  if (!haveTotal) return { label: `${ready}`, tone: "slate" };
  const tone: Tone = ready >= total && total > 0 ? "green" : ready < total ? "amber" : "slate";
  return { label: `${ready}/${total}`, tone };
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
  const { ready, total } = componentReplicas(c);
  const r = ready ?? 0;
  const t = total ?? 0;
  if (t > 0 && r === 0) return "bad";
  return c.ready === false || r < t ? "warn" : "ok";
}

/** Component-chip tone → the `repl` color classes tables already use. */
export const COMPONENT_TONE_CLASS: Record<ComponentCell["tone"], string> = {
  ok: "green",
  warn: "amber",
  bad: "red",
};

/** Per-component replica chips: "engine 245/251" with a health tone, plus "→N"
 *  when the autoscaler wants a different total than currently exists (scale in
 *  flight). Component names render in full — abbreviations read as noise. */
export function componentCells(components?: ComponentItem[]): ComponentCell[] {
  return (components ?? []).map((c) => {
    const { ready, total } = componentReplicas(c);
    let counts = total != null ? `${ready ?? 0}/${total}` : ready != null ? `${ready}` : "—";
    if (
      c.desiredReplicas != null &&
      c.totalReplicas != null &&
      c.desiredReplicas !== c.totalReplicas
    ) {
      counts += `→${c.desiredReplicas}`;
    }
    const toneMap: Record<ReturnType<typeof componentTone>, ComponentCell["tone"]> = {
      ok: "ok",
      warn: "warn",
      bad: "bad",
    };
    return {
      name: c.name,
      label: `${c.name} ${counts}`,
      tone: toneMap[componentTone(c)],
      phase: c.rolloutPhase,
      revision: c.revision,
      transition: componentTransition(c),
    };
  });
}

/** Whether any component reports a rollout in progress (a non-Stable phase). */
export function isRolling(components?: ComponentItem[]): boolean {
  return (components ?? []).some((c) => c.rolloutPhase != null && c.rolloutPhase !== "Stable");
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
      runtime: s.resolvedRuntimeRef?.name ?? s.runtime ?? "—",
      runtimeRef: runtimeRefOf(s),
      endpoint: s.url,
      replicas: replicaRollup(s.components),
      components: componentCells(s.components),
      mode: s.deploymentMode,
      accelerators: s.accelerators,
      rolling: isRolling(s.components),
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

/** Revision names embed the deployment and component (`<isvc>-<comp>-rev-<hash>`);
 * only the trailing `rev-<hash>` distinguishes revisions, so display that and
 * keep the full name for the tooltip. End-truncation would hide exactly the
 * part that differs. */
export function shortRevision(revision: string): string {
  const i = revision.lastIndexOf("rev-");
  return i > 0 ? revision.slice(i) : revision;
}

/** Short display for a revision transition, e.g. "6b9f7d4 → 8c4e2a1". Revision
 *  names end in the revision hash (the part after the last dash), so the tail
 *  is what identifies them; the shared name prefix is noise at chip size. */
export function transitionLabel(t: { from?: string; to: string }): string {
  const tail = (s: string) => {
    const i = s.lastIndexOf("-");
    return i > 0 ? s.slice(i + 1) : s;
  };
  return t.from ? `${tail(t.from)} → ${tail(t.to)}` : `→ ${tail(t.to)}`;
}

/** One-line canary summary: "canary step 3 · 25% traffic · 2 checks failing". */
export function canaryNote(c: {
  currentStep: number;
  trafficWeight: number;
  failedChecks?: number;
}): string {
  const parts = [`canary step ${c.currentStep}`, `${c.trafficWeight}% traffic`];
  if (c.failedChecks) {
    parts.push(`${c.failedChecks} ${c.failedChecks === 1 ? "check" : "checks"} failing`);
  }
  return parts.join(" · ");
}

// ---- Models ---------------------------------------------------------------

/** Lifecycle state → tone: Ready=green, Failed=red, in-progress=amber, else slate. */
export function stateTone(state?: string): Tone {
  if (!state) return "slate";
  if (state === "Ready") return "green";
  if (state === "Failed") return "red";
  return "amber"; // Importing / Converting / In Transit / …
}
