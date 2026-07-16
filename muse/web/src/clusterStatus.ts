import type { ClusterItem } from "./api";

export interface ClusterCacheSummary {
  syncing: number;
  unhealthy: number;
  line: string;
}

/** Derive the connection indicator and detail line used by the active sidebar. */
export function summarizeClusterCaches(items: ClusterItem[]): ClusterCacheSummary {
  const ready = items.filter((cluster) => cluster.ready).length;
  const failed = items.filter((cluster) => cluster.status === "failed").length;
  const timedOut = items.filter((cluster) => cluster.status === "timed_out").length;
  const syncing = items.filter((cluster) => cluster.status === "syncing").length;
  return {
    syncing,
    unhealthy: failed + timedOut,
    line: [
      failed ? `${failed} failed` : "",
      timedOut ? `${timedOut} timed out` : "",
      syncing ? `${syncing} syncing` : "",
      ready ? `${ready} ready` : "",
    ]
      .filter(Boolean)
      .join(" · "),
  };
}
