import type { CapacityResp, ClusterItem, ClustersResp, MetricsSnapshot } from "./api";

export interface ClusterCapacityState {
  name: string;
  ready: boolean;
  status: ClusterItem["status"];
  version?: string;
  region?: string;
  cap?: CapacityResp;
  cacheError?: string;
  capacityError?: string;
}

const errorMessage = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** Fetch every cluster independently so one failed read cannot blank fleet capacity. */
export async function loadClusterCapacity(
  fetchClusters: () => Promise<ClustersResp>,
  fetchCapacity: (cluster: string) => Promise<CapacityResp>,
): Promise<ClusterCapacityState[]> {
  const clusters = await fetchClusters();
  return Promise.all(
    clusters.items.map(async (cluster): Promise<ClusterCapacityState> => {
      const identity = {
        name: cluster.name,
        ready: cluster.ready,
        status: cluster.status,
        version: cluster.version,
        region: cluster.region,
        cacheError: cluster.error,
      };
      try {
        return { ...identity, cap: await fetchCapacity(cluster.name) };
      } catch (error) {
        return { ...identity, capacityError: errorMessage(error) };
      }
    }),
  );
}

export interface MetricsHealth {
  ready: boolean;
  queryCount: number;
  failedQueryCount: number;
  failedClusters: string[];
}

/** Summarize per-query Prometheus failures carried in an otherwise valid snapshot. */
export function metricsHealth(snapshot: MetricsSnapshot | null): MetricsHealth {
  if (!snapshot || snapshot.updatedAt <= 0) {
    return { ready: false, queryCount: 0, failedQueryCount: 0, failedClusters: [] };
  }

  let queryCount = 0;
  let failedQueryCount = 0;
  const failedClusters = new Set<string>();
  for (const [cluster, queries] of Object.entries(snapshot.clusters)) {
    for (const result of Object.values(queries)) {
      queryCount++;
      if (result.error) {
        failedQueryCount++;
        failedClusters.add(cluster);
      }
    }
  }
  return {
    ready: true,
    queryCount,
    failedQueryCount,
    failedClusters: [...failedClusters].sort(),
  };
}

/** True when at least one scoped cluster returned this query without an error. */
export function metricAvailable(snapshot: MetricsSnapshot | null, queryId: string): boolean {
  if (!snapshot || snapshot.updatedAt <= 0) return false;
  return Object.values(snapshot.clusters).some((queries) => {
    const result = queries[queryId];
    return result != null && !result.error;
  });
}

/** Derive transport state without letting the GET fallback race the live stream. */
export function metricsTransportState(
  snapshot: MetricsSnapshot | null,
  fetchError: string | null,
  streamError: string | null,
  enabled: boolean,
): { error: string | null; loading: boolean } {
  if (!enabled) return { error: null, loading: false };
  const hasSnapshot = snapshot != null;
  const ready = snapshot != null && snapshot.updatedAt > 0;
  const error = streamError ?? (hasSnapshot ? null : fetchError);
  return { error, loading: !ready && error == null };
}
