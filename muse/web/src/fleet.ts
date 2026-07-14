// One fleet filter + a single scoping pass, so changing the filter re-scopes the
// WHOLE dashboard (metrics, deployments, rollouts, activity, capacity) as one unit
// — instead of each panel filtering (or not) on its own. Panels consume only the
// returned FleetScope; nothing filters ad hoc.

import type {
  AccelGroup,
  CapacityResp,
  ClusterItem,
  EventItem,
  IsvcItem,
  MetricsSnapshot,
  RolloutItem,
} from "./api";
import { filterSnapshot } from "./metrics";

export interface FleetFilter {
  cluster: string; // "all" | cluster name
  model: string; // "all" | model
  accel: string; // "all" | "gpu" | "tpu"
  range: string; // "6h" | "1h"
}

export interface FleetRaw {
  clusters: ClusterItem[];
  isvcs: IsvcItem[];
  rollouts: RolloutItem[];
  events: EventItem[];
  metrics: MetricsSnapshot | null;
  capacity: CapacityResp | null;
}

export interface FleetScope {
  clusters: ClusterItem[];
  isvcs: IsvcItem[];
  rollouts: RolloutItem[];
  events: EventItem[];
  metrics: MetricsSnapshot | null;
  accelerators: AccelGroup[];
  byCluster: AccelGroup[];
  byIsland: AccelGroup[];
  byNvlinkDomain: AccelGroup[];
}

/**
 * Scope every data source by one filter selection. cluster/accel gate clusters
 * and everything cluster-tagged; model gates deployments (and, via the isvc→model
 * map, their rollouts and metric series). Accelerator totals are recomputed from
 * the scoped per-cluster groups so the tiles reflect the filter.
 *
 * Known limit: capacity's island / NVLink tiers aren't cluster-tagged, so under a
 * single-cluster filter the "largest placeable job" is accel-scoped but not
 * cluster-scoped — narrowing that needs the server's per-cluster capacity.
 */
export function scopeFleet(f: FleetFilter, raw: FleetRaw): FleetScope {
  const kind = new Map((raw.capacity?.byCluster ?? []).map((g) => [g.name, g.kind]));
  const isvcModel = new Map(raw.isvcs.map((i) => [i.name, i.model ?? ""]));
  const clusterOk = (cl: string) =>
    (f.cluster === "all" || f.cluster === cl) && (f.accel === "all" || kind.get(cl) === f.accel);
  const modelOk = (m?: string) => f.model === "all" || m === f.model;
  const accelOk = (k: string) => f.accel === "all" || k === f.accel;

  const byCluster = (raw.capacity?.byCluster ?? []).filter(
    (g) => clusterOk(g.name) && accelOk(g.kind),
  );
  return {
    clusters: raw.clusters.filter((c) => clusterOk(c.name)),
    isvcs: raw.isvcs.filter((i) => clusterOk(i.cluster) && modelOk(i.model)),
    rollouts: raw.rollouts.filter((r) => clusterOk(r.cluster) && modelOk(isvcModel.get(r.name))),
    events: raw.events.filter((e) => clusterOk(e.cluster)),
    metrics: filterSnapshot(raw.metrics, {
      cluster: f.cluster,
      keepCluster: (c) => f.accel === "all" || kind.get(c) === f.accel,
      keepIsvc: (i) => f.model === "all" || isvcModel.get(i) === f.model,
    }),
    accelerators:
      f.cluster === "all" && f.accel === "all"
        ? (raw.capacity?.accelerators ?? [])
        : sumByKind(byCluster),
    byCluster,
    byIsland: (raw.capacity?.byIsland ?? []).filter((g) => accelOk(g.kind)),
    byNvlinkDomain: (raw.capacity?.byNvlinkDomain ?? []).filter((g) => accelOk(g.kind)),
  };
}

/** Re-aggregate per-cluster groups into fleet totals per accelerator kind. */
function sumByKind(groups: AccelGroup[]): AccelGroup[] {
  const m = new Map<string, AccelGroup>();
  for (const g of groups) {
    const e = m.get(g.kind);
    if (e) {
      e.nodes += g.nodes;
      e.allocatable += g.allocatable;
      e.allocated += g.allocated;
      e.free += g.free;
    } else {
      m.set(g.kind, { ...g, name: g.kind });
    }
  }
  return [...m.values()];
}
