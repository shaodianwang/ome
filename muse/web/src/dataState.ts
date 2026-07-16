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

export interface FleetNoticeIssue {
  domain: string;
  message: string;
  tone: "danger" | "warning";
}

export interface FleetNoticeSummary {
  detail: string;
  title: string;
  tone: FleetNoticeIssue["tone"];
}

const trimSentence = (value: string) => value.trim().replace(/[.;]+$/, "");

/** Collapse correlated source failures into one readable, accessible notice. */
export function summarizeFleetIssues(issues: FleetNoticeIssue[]): FleetNoticeSummary | null {
  const normalized = issues.map((issue) => ({
    ...issue,
    domain: issue.domain.trim(),
    message: trimSentence(issue.message),
  }));
  const unique = [
    ...new Map(
      normalized.map(
        (issue) => [`${issue.domain}\u0000${issue.message}\u0000${issue.tone}`, issue] as const,
      ),
    ).values(),
  ].filter((issue) => issue.domain && issue.message);
  if (!unique.length) return null;

  const tone = unique.some((issue) => issue.tone === "danger") ? "danger" : "warning";
  return {
    tone,
    title:
      tone === "danger"
        ? "Some fleet data is stale or unavailable."
        : "Some fleet data is degraded.",
    detail: `${unique.map((issue) => `${issue.domain}: ${issue.message}`).join("; ")}.`,
  };
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

/** Describe failed Prometheus queries without losing query or cluster counts. */
export function metricsQueryIssue(
  health: MetricsHealth,
  scopedClusterCount: number,
): FleetNoticeIssue | null {
  if (!health.ready) return null;
  if (health.queryCount === 0) {
    const noun = scopedClusterCount === 1 ? "cluster" : "clusters";
    return {
      domain: "Metrics queries",
      message: `no query results returned across ${scopedClusterCount} scoped ${noun}`,
      tone: "danger",
    };
  }
  if (health.failedQueryCount === 0) return null;

  const clusterCount = health.failedClusters.length;
  const noun = clusterCount === 1 ? "cluster" : "clusters";
  const names = clusterCount ? ` (${health.failedClusters.join(", ")})` : "";
  return {
    domain: "Metrics queries",
    message: `${health.failedQueryCount} of ${health.queryCount} queries failed across ${clusterCount} ${noun}${names}`,
    tone: health.failedQueryCount === health.queryCount ? "danger" : "warning",
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
