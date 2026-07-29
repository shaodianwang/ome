// Pure helpers for the System panel (OEP-0021.2): map a component status to a
// display tone, and roll up per-(component × cluster) rows into per-component
// summaries. Kept out of the component file so the rollup logic is unit-tested
// independently of React (mirrors clusterStatus.ts).
import type { ComponentStatus, SystemComponentItem } from "./api";
import type { Tone } from "./types";

/** Component statuses worst-first, so index 0 is the most severe. This is the
 *  single source of truth for severity ordering on the client. */
export const STATUS_SEVERITY: ComponentStatus[] = [
  "missing",
  "unavailable",
  "degraded",
  "unknown",
  "healthy",
];

/** The display tone for a status dot / pill. `missing` and `unavailable` are both
 *  red (a component that is down or absent), `degraded` amber, `unknown` slate. */
export function statusTone(status: ComponentStatus): Tone {
  switch (status) {
    case "healthy":
      return "green";
    case "degraded":
      return "amber";
    case "unavailable":
    case "missing":
      return "red";
    case "unknown":
      return "slate";
  }
}

function severity(status: ComponentStatus): number {
  const i = STATUS_SEVERITY.indexOf(status);
  // An unrecognized status sorts as most severe rather than silently "healthy".
  return i === -1 ? -1 : i;
}

/** The worst (most severe) status across a set of rows. Empty ⇒ `unknown`. */
export function worstStatus(rows: { status: ComponentStatus }[]): ComponentStatus {
  let worst: ComponentStatus | null = null;
  for (const row of rows) {
    if (worst === null || severity(row.status) < severity(worst)) worst = row.status;
  }
  return worst ?? "unknown";
}

/** One component rolled up across every cluster it was evaluated on. */
export interface ComponentSummary {
  name: string;
  group: string;
  kind: string;
  /** Worst status across all clusters — worst-wins, so the headline never reads
   *  healthy while one cluster is down. */
  worst: ComponentStatus;
  /** Per-cluster rows, sorted by cluster name. */
  clusters: SystemComponentItem[];
  /** Clusters where this component is healthy, and total clusters evaluated. */
  healthy: number;
  total: number;
}

const UNGROUPED = "Components";

/** Roll the flat `GET /v1/system` rows into one summary per component, grouped by
 *  `(group, name)`. Components are ordered worst-first (so problems float up),
 *  then by name; the returned list is stable regardless of row/cluster order. */
export function summarizeComponents(rows: SystemComponentItem[]): ComponentSummary[] {
  const byKey = new Map<string, SystemComponentItem[]>();
  for (const row of rows) {
    const key = JSON.stringify([row.group ?? UNGROUPED, row.name]);
    const list = byKey.get(key);
    if (list) list.push(row);
    else byKey.set(key, [row]);
  }
  const summaries: ComponentSummary[] = [];
  for (const list of byKey.values()) {
    const clusters = [...list].sort((a, b) => a.cluster.localeCompare(b.cluster));
    summaries.push({
      name: clusters[0].name,
      group: clusters[0].group ?? UNGROUPED,
      kind: clusters[0].kind,
      worst: worstStatus(clusters),
      clusters,
      healthy: clusters.filter((c) => c.status === "healthy").length,
      total: clusters.length,
    });
  }
  return summaries.sort(
    (a, b) => severity(a.worst) - severity(b.worst) || a.name.localeCompare(b.name),
  );
}

/** Group summaries by their `group`, groups ordered by their own worst status. */
export function groupSummaries(
  summaries: ComponentSummary[],
): { group: string; worst: ComponentStatus; components: ComponentSummary[] }[] {
  const byGroup = new Map<string, ComponentSummary[]>();
  for (const summary of summaries) {
    const list = byGroup.get(summary.group);
    if (list) list.push(summary);
    else byGroup.set(summary.group, [summary]);
  }
  return [...byGroup.entries()]
    .map(([group, components]) => ({
      group,
      worst: worstStatus(components.map((c) => ({ status: c.worst }))),
      components,
    }))
    .sort((a, b) => severity(a.worst) - severity(b.worst) || a.group.localeCompare(b.group));
}

/** Fleet-level headline: how many components are healthy on every cluster. */
export function fleetHealth(summaries: ComponentSummary[]): {
  healthy: number;
  total: number;
  worst: ComponentStatus;
} {
  return {
    healthy: summaries.filter((s) => s.worst === "healthy").length,
    total: summaries.length,
    worst: worstStatus(summaries.map((s) => ({ status: s.worst }))),
  };
}
