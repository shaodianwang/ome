import type { MetricsSnapshot, PromSeries } from "./api";

export interface ReplicaPerfRow {
  namespace: string;
  pod: string;
  component: string;
  qps: number | null;
  throughput: number | null;
  p99: number | null;
  ttft: number | null;
  running: number | null;
  queued: number | null;
  kvCache: number | null;
}

export interface ReplicaDiagnostics {
  configured: boolean;
  rows: ReplicaPerfRow[];
}

const REPLICA_QUERIES = [
  ["replica_request_rate", "qps"],
  ["replica_throughput_tokens", "throughput"],
  ["replica_latency_p99", "p99"],
  ["replica_ttft", "ttft"],
  ["replica_running_reqs", "running"],
  ["replica_queue_reqs", "queued"],
  ["replica_kv_cache_usage", "kvCache"],
] as const;

function latest(series: PromSeries): number | null {
  const values = series.values;
  const raw = series.value?.[1] ?? (values?.length ? values[values.length - 1][1] : undefined);
  if (raw == null) return null;
  const value = Number.parseFloat(raw);
  return Number.isFinite(value) ? value : null;
}

/**
 * Join the engine-only instant queries by stable scrape identity. Replica rows
 * require namespace, pod, and component labels; a partially labelled series is
 * omitted instead of being attributed to the wrong deployment or replica.
 */
export function replicaDiagnostics(
  snap: MetricsSnapshot | null,
  cluster: string,
  namespace: string | undefined,
  isvc: string,
): ReplicaDiagnostics {
  const queries = snap?.clusters?.[cluster];
  if (!queries) return { configured: false, rows: [] };

  const rows = new Map<string, ReplicaPerfRow>();
  let configured = false;
  for (const [queryId, field] of REPLICA_QUERIES) {
    if (!(queryId in queries)) continue;
    configured = true;
    for (const series of queries[queryId]?.data?.result ?? []) {
      const metric = series.metric;
      if (
        !namespace ||
        metric.namespace !== namespace ||
        metric.inferenceservice !== isvc ||
        !metric.pod ||
        !metric.component
      ) {
        continue;
      }
      const key = `${namespace}\u0000${metric.pod}\u0000${metric.component}`;
      let row = rows.get(key);
      if (!row) {
        row = {
          namespace,
          pod: metric.pod,
          component: metric.component,
          qps: null,
          throughput: null,
          p99: null,
          ttft: null,
          running: null,
          queued: null,
          kvCache: null,
        };
        rows.set(key, row);
      }
      row[field] = latest(series);
    }
  }

  return {
    configured,
    rows: [...rows.values()].sort(
      (a, b) =>
        a.component.localeCompare(b.component) ||
        (b.qps ?? 0) - (a.qps ?? 0) ||
        a.pod.localeCompare(b.pod),
    ),
  };
}
