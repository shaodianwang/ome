// Shaping for the fleet metrics snapshot: fold the per-cluster, per-query Prometheus
// results into the rows/series the panels render (per-isvc, per-deployment, fleet
// totals). Query ids (`latency_p99`, …) match the server's curated set
// (charts/muse/files/metrics-queries.json).

import type { MetricsSnapshot, PromResult, PromSeries } from "./api";

/** One finite Prometheus sample. `at` is a Unix timestamp in seconds. */
export interface MetricPoint {
  at: number;
  value: number;
}

/** One exact InferenceService deployment's serving metrics. */
export interface PerfRow {
  isvc: string;
  namespace?: string;
  cluster: string;
  /** Request rate, per second (QPS). */
  qps: number | null;
  /** p50 / p99 end-to-end latency, seconds. */
  p50: number | null;
  p99: number | null;
  /** p99 time-to-first-token, seconds. */
  ttft: number | null;
  /** Generation throughput for this deployment, tokens/s. */
  throughput: number | null;
  /** Aborted-request rate (per second) and as a % of requests. */
  errRate: number | null;
  errPct: number | null;
  /** Per-model GPU utilization — null until DCGM is in an in-cluster Prometheus. */
  gpuUtil: number | null;
}

/** The latest scalar of a series: an instant `value`, or the last range point. */
function last(s: PromSeries): number | null {
  const raw = s.value ? s.value[1] : s.values?.length ? s.values[s.values.length - 1][1] : null;
  if (raw == null) return null;
  const n = parseFloat(raw);
  return Number.isFinite(n) ? n : null;
}

/** The full finite time series, retaining Prometheus sample timestamps. */
function points(s: PromSeries): MetricPoint[] {
  const src = s.values ?? (s.value ? [s.value] : []);
  const byTime = new Map<number, number>();
  for (const [at, raw] of src) {
    const value = parseFloat(raw);
    if (Number.isFinite(at) && Number.isFinite(value)) byTime.set(at, value);
  }
  return [...byTime].map(([at, value]) => ({ at, value })).sort((a, b) => a.at - b.at);
}

/** Divide timestamp-aligned samples; unmatched or zero-denominator points are omitted. */
export function ratioSeries(
  numerator: MetricPoint[],
  denominator: MetricPoint[],
  scale: number,
): MetricPoint[] {
  const denominatorByTime = new Map(denominator.map((point) => [point.at, point.value]));
  return numerator.flatMap((point) => {
    const divisor = denominatorByTime.get(point.at);
    return divisor != null && divisor > 0
      ? [{ at: point.at, value: (point.value / divisor) * scale }]
      : [];
  });
}

/** Latest ratio only when both latest samples describe the same instant. */
export function latestAlignedRatio(
  numerator: MetricPoint[],
  denominator: MetricPoint[],
  scale: number,
): number | null {
  const top = numerator[numerator.length - 1];
  const bottom = denominator[denominator.length - 1];
  return top && bottom && top.at === bottom.at && bottom.value > 0
    ? (top.value / bottom.value) * scale
    : null;
}

function rangeSeconds(series: PromSeries[]): number | null {
  let longest: number | null = null;
  for (const item of series) {
    let first = Number.POSITIVE_INFINITY;
    let last = Number.NEGATIVE_INFINITY;
    for (const [at] of item.values ?? []) {
      if (!Number.isFinite(at)) continue;
      first = Math.min(first, at);
      last = Math.max(last, at);
    }
    if (Number.isFinite(first) && last > first) {
      longest = Math.max(longest ?? 0, last - first);
    }
  }
  return longest;
}

function results(r?: PromResult): PromSeries[] {
  return r?.data?.result ?? [];
}

/** Actual range represented by one query's returned Prometheus timestamps. */
export function metricsQueryRangeSeconds(
  snap: MetricsSnapshot | null,
  queryId: string,
): number | null {
  if (!snap?.clusters) return null;
  const series: PromSeries[] = [];
  for (const queries of Object.values(snap.clusters)) {
    series.push(...results(queries[queryId]));
  }
  return rangeSeconds(series);
}

/** Compact duration label derived from data, never from a dashboard default. */
export function metricsRangeLabel(seconds: number | null): string | null {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return null;
  const rounded = Math.round(seconds);
  if (rounded % 86_400 === 0) return `${rounded / 86_400}d`;
  if (rounded % 3_600 === 0) return `${rounded / 3_600}h`;
  if (rounded % 60 === 0) return `${rounded / 60}m`;
  return `${rounded}s`;
}

/**
 * Fold a snapshot into one row per cluster/namespace/InferenceService identity.
 * Series without an `inferenceservice` label are skipped. Legacy query sets that
 * lack `namespace` still remain isolated per cluster instead of being merged
 * fleet-wide. Rows are sorted by QPS then throughput.
 */
export function perfRows(snap: MetricsSnapshot | null): PerfRow[] {
  if (!snap?.clusters) return [];
  const byIsvc = new Map<string, PerfRow>();
  const row = (isvc: string, namespace: string | undefined, cluster: string): PerfRow => {
    const key = `${cluster}\u0000${namespace ?? ""}\u0000${isvc}`;
    let r = byIsvc.get(key);
    if (!r) {
      r = {
        isvc,
        namespace,
        cluster,
        qps: null,
        p50: null,
        p99: null,
        ttft: null,
        throughput: null,
        errRate: null,
        errPct: null,
        gpuUtil: null,
      };
      byIsvc.set(key, r);
    }
    return r;
  };
  // If a query returns multiple source series for one deployment, rates sum and
  // latencies take the worst value.
  const worst = (cur: number | null, v: number | null) => (v == null ? cur : Math.max(cur ?? 0, v));
  const add = (cur: number | null, v: number | null) => (cur ?? 0) + (v ?? 0);

  for (const [cluster, queries] of Object.entries(snap.clusters)) {
    const apply = (qid: string, f: (r: PerfRow, s: PromSeries) => void) => {
      for (const s of results(queries[qid])) {
        const isvc = s.metric?.inferenceservice;
        if (!isvc) continue;
        f(row(isvc, s.metric?.namespace, cluster), s);
      }
    };
    apply("request_rate", (r, s) => (r.qps = add(r.qps, last(s))));
    apply("latency_p50", (r, s) => (r.p50 = worst(r.p50, last(s))));
    apply("latency_p99", (r, s) => (r.p99 = worst(r.p99, last(s))));
    apply("ttft", (r, s) => (r.ttft = worst(r.ttft, last(s))));
    apply("throughput_tokens", (r, s) => (r.throughput = add(r.throughput, last(s))));
    apply("error_rate", (r, s) => (r.errRate = add(r.errRate, last(s))));
  }

  for (const r of byIsvc.values()) {
    r.errPct = r.qps && r.qps > 0 && r.errRate != null ? (r.errRate / r.qps) * 100 : null;
  }

  // Highest-traffic deployments first: order by QPS, then throughput.
  return [...byIsvc.values()].sort(
    (a, b) => (b.qps ?? 0) - (a.qps ?? 0) || (b.throughput ?? 0) - (a.throughput ?? 0),
  );
}

/** One query id's latest scalar + its range series (for a sparkline). */
export interface IsvcSeries {
  last: number | null;
  series: MetricPoint[];
}
/** Per-deployment serving metrics: every curated query id for one isvc on one
 *  cluster. Generic — new queries in metrics-queries.json surface here without
 *  code changes; the detail page maps query ids to tiles. Read one with
 *  `seriesOf` (never throws; empty if absent). */
export interface IsvcMetrics {
  /** Whether this deployment has any serving signal in the snapshot. */
  present: boolean;
  /** Whether namespace labels (or unique inventory identity) make selection safe. */
  identityScoped: boolean;
  /** Actual time span represented by the returned range samples. */
  rangeSeconds: number | null;
  /** By query id (`latency_p99`, `tpot`, `kv_transfer_latency`, …). */
  by: Record<string, IsvcSeries>;
}

const EMPTY_SERIES: IsvcSeries = { last: null, series: [] };

export function seriesOf(m: IsvcMetrics, qid: string): IsvcSeries {
  return m.by[qid] ?? EMPTY_SERIES;
}

export function isvcMetrics(
  snap: MetricsSnapshot | null,
  cluster: string,
  namespace: string | undefined,
  isvc: string,
  identityUnambiguous: boolean,
): IsvcMetrics {
  const queries = snap?.clusters?.[cluster];
  const by: Record<string, IsvcSeries> = {};
  let requestRangeSeconds: number | null = null;
  let namespaceScoped = false;
  if (queries) {
    for (const [qid, res] of Object.entries(queries)) {
      if (qid.startsWith("replica_")) continue;
      const candidates = results(res);
      if (
        candidates.some(
          (x) => x.metric?.inferenceservice === isvc && x.metric?.namespace === namespace,
        )
      ) {
        namespaceScoped = true;
      }
      const s = candidates.find(
        (x) =>
          x.metric?.inferenceservice === isvc &&
          (x.metric?.namespace != null ? x.metric.namespace === namespace : identityUnambiguous),
      );
      if (qid === "request_rate" && s) requestRangeSeconds = rangeSeconds([s]);
      by[qid] = s ? { last: last(s), series: points(s) } : EMPTY_SERIES;
    }
  }
  const has = (qid: string) => (by[qid]?.last ?? null) != null || (by[qid]?.series.length ?? 0) > 0;
  return {
    present: has("request_rate") || has("latency_p99") || has("running_reqs"),
    identityScoped: identityUnambiguous || namespaceScoped,
    rangeSeconds: requestRangeSeconds,
    by,
  };
}

/** Fleet-wide aggregates for the SLI banner: sums across all series (throughput,
 *  requests, errors, running) and the worst p99, plus element-wise-summed range
 *  series for the tile sparklines. */
export interface FleetTotals {
  throughput: number;
  throughputSeries: MetricPoint[];
  requestRate: number;
  requestSeries: MetricPoint[];
  p50: number | null;
  p99: number | null;
  /** Worst-deployment p50/p99 at each timestamp, for the latency chart. */
  p50Series: MetricPoint[];
  p99Series: MetricPoint[];
  ttft: number | null;
  errorRate: number;
  errorSeries: MetricPoint[];
  errorPct: number | null;
  running: number;
}

export function fleetTotals(snap: MetricsSnapshot | null): FleetTotals {
  const t: FleetTotals = {
    throughput: 0,
    throughputSeries: [],
    requestRate: 0,
    requestSeries: [],
    p50: null,
    p99: null,
    p50Series: [],
    p99Series: [],
    ttft: null,
    errorRate: 0,
    errorSeries: [],
    errorPct: null,
    running: 0,
  };
  if (!snap?.clusters) return t;
  // Rates sum across timestamp-aligned samples; latency takes the timestamp-local worst.
  const throughputSeries = new Map<number, number>();
  const requestSeries = new Map<number, number>();
  const errorSeries = new Map<number, number>();
  const p50Series = new Map<number, number>();
  const p99Series = new Map<number, number>();
  const addInto = (acc: Map<number, number>, s: PromSeries) => {
    for (const point of points(s)) acc.set(point.at, (acc.get(point.at) ?? 0) + point.value);
  };
  const maxInto = (acc: Map<number, number>, s: PromSeries) => {
    for (const point of points(s)) {
      acc.set(point.at, Math.max(acc.get(point.at) ?? Number.NEGATIVE_INFINITY, point.value));
    }
  };
  for (const queries of Object.values(snap.clusters)) {
    for (const s of results(queries.throughput_tokens)) {
      t.throughput += last(s) ?? 0;
      addInto(throughputSeries, s);
    }
    for (const s of results(queries.request_rate)) {
      t.requestRate += last(s) ?? 0;
      addInto(requestSeries, s);
    }
    for (const s of results(queries.error_rate)) {
      t.errorRate += last(s) ?? 0;
      addInto(errorSeries, s);
    }
    for (const s of results(queries.latency_p99)) {
      const v = last(s);
      if (v != null) t.p99 = Math.max(t.p99 ?? 0, v);
      maxInto(p99Series, s);
    }
    for (const s of results(queries.latency_p50)) {
      const v = last(s);
      if (v != null) t.p50 = Math.max(t.p50 ?? 0, v);
      maxInto(p50Series, s);
    }
    for (const s of results(queries.ttft)) {
      const v = last(s);
      if (v != null) t.ttft = Math.max(t.ttft ?? 0, v);
    }
    for (const s of results(queries.running_reqs)) {
      t.running += last(s) ?? 0;
    }
  }
  const sorted = (series: Map<number, number>): MetricPoint[] =>
    [...series].map(([at, value]) => ({ at, value })).sort((a, b) => a.at - b.at);
  t.throughputSeries = sorted(throughputSeries);
  t.requestSeries = sorted(requestSeries);
  t.errorSeries = sorted(errorSeries);
  t.p50Series = sorted(p50Series);
  t.p99Series = sorted(p99Series);
  t.errorPct = latestAlignedRatio(t.errorSeries, t.requestSeries, 100);
  return t;
}

/**
 * Scope a snapshot to a filter selection before shaping — so the SLI banner and
 * the deployment table both reflect the active filters. Drops non-matching
 * clusters, and within each kept cluster drops series whose `inferenceservice`
 * fails `keepIsvc`. Returns a new snapshot; the original is untouched.
 */
export function filterSnapshot(
  snap: MetricsSnapshot | null,
  opts: {
    cluster?: string;
    keepCluster?: (cluster: string) => boolean;
    keepIsvc?: (isvc: string, cluster: string, namespace?: string) => boolean;
  },
): MetricsSnapshot | null {
  if (!snap?.clusters) return snap;
  const out: MetricsSnapshot = { updatedAt: snap.updatedAt, clusters: {}, sources: {} };
  for (const [cluster, queries] of Object.entries(snap.clusters)) {
    if (opts.cluster && opts.cluster !== "all" && opts.cluster !== cluster) continue;
    if (opts.keepCluster && !opts.keepCluster(cluster)) continue;
    const kept: Record<string, PromResult> = {};
    for (const [qid, res] of Object.entries(queries)) {
      const result = (res?.data?.result ?? []).filter(
        (s) =>
          !opts.keepIsvc ||
          opts.keepIsvc(s.metric?.inferenceservice ?? "", cluster, s.metric?.namespace),
      );
      kept[qid] = {
        status: res?.status,
        error: res?.error,
        data: { resultType: res?.data?.resultType ?? "vector", result },
      };
    }
    out.clusters[cluster] = kept;
    const source = snap.sources?.[cluster];
    if (source && out.sources) out.sources[cluster] = source;
  }
  return out;
}
