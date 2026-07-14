// Shaping for the fleet metrics snapshot: fold the per-cluster, per-query Prometheus
// results into the rows/series the panels render (per-isvc, per-deployment, fleet
// totals). Query ids (`latency_p99`, …) match the server's curated set
// (charts/muse/files/metrics-queries.json).

import type { MetricsSnapshot, PromResult, PromSeries } from "./api";

/** One InferenceService's fleet-wide serving metrics (merged across clusters). */
export interface PerfRow {
  isvc: string;
  clusters: string[];
  /** Request rate, per second (QPS). */
  qps: number | null;
  /** p50 / p99 end-to-end latency, seconds (worst across clusters). */
  p50: number | null;
  p99: number | null;
  /** p99 time-to-first-token, seconds. */
  ttft: number | null;
  /** Generation throughput, tokens/s (summed across clusters). */
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

/** The full numeric range series (for a sparkline). */
function nums(s: PromSeries): number[] {
  const src = s.values ?? (s.value ? [s.value] : []);
  return src.map((v) => parseFloat(v[1])).filter((n) => Number.isFinite(n));
}

function results(r?: PromResult): PromSeries[] {
  return r?.data?.result ?? [];
}

/**
 * Fold a snapshot into one row per InferenceService. Counts (throughput, running,
 * queue, errors) sum across clusters an isvc is placed in; p99 takes the worst;
 * the throughput sparkline uses the longest available series. Series without an
 * `inferenceservice` label (controller/runtime internals) are skipped. Rows are
 * sorted by throughput then running, so the busiest models surface first.
 */
export function perfRows(snap: MetricsSnapshot | null): PerfRow[] {
  if (!snap?.clusters) return [];
  const byIsvc = new Map<string, PerfRow>();
  const row = (isvc: string, cluster: string): PerfRow => {
    let r = byIsvc.get(isvc);
    if (!r) {
      r = {
        isvc,
        clusters: [],
        qps: null,
        p50: null,
        p99: null,
        ttft: null,
        throughput: null,
        errRate: null,
        errPct: null,
        gpuUtil: null,
      };
      byIsvc.set(isvc, r);
    }
    if (!r.clusters.includes(cluster)) r.clusters.push(cluster);
    return r;
  };
  // Latencies take the worst across clusters; rates sum across them.
  const worst = (cur: number | null, v: number | null) => (v == null ? cur : Math.max(cur ?? 0, v));
  const add = (cur: number | null, v: number | null) => (cur ?? 0) + (v ?? 0);

  for (const [cluster, queries] of Object.entries(snap.clusters)) {
    const apply = (qid: string, f: (r: PerfRow, s: PromSeries) => void) => {
      for (const s of results(queries[qid])) {
        const isvc = s.metric?.inferenceservice;
        if (!isvc) continue;
        f(row(isvc, cluster), s);
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
    r.errPct = r.qps && r.qps > 0 ? ((r.errRate ?? 0) / r.qps) * 100 : null;
  }

  // "Top models by traffic": order by QPS, then throughput.
  return [...byIsvc.values()].sort(
    (a, b) => (b.qps ?? 0) - (a.qps ?? 0) || (b.throughput ?? 0) - (a.throughput ?? 0),
  );
}

/** One query id's latest scalar + its range series (for a sparkline). */
export interface IsvcSeries {
  last: number | null;
  series: number[];
}
/** Per-deployment serving metrics: every curated query id for one isvc on one
 *  cluster. Generic — new queries in metrics-queries.json surface here without
 *  code changes; the detail page maps query ids to tiles. Read one with
 *  `seriesOf` (never throws; empty if absent). */
export interface IsvcMetrics {
  /** Whether this deployment has any serving signal in the snapshot. */
  present: boolean;
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
  isvc: string,
): IsvcMetrics {
  const queries = snap?.clusters?.[cluster];
  const by: Record<string, IsvcSeries> = {};
  if (queries) {
    for (const [qid, res] of Object.entries(queries)) {
      const s = results(res).find((x) => x.metric?.inferenceservice === isvc);
      by[qid] = s ? { last: last(s), series: nums(s) } : EMPTY_SERIES;
    }
  }
  const has = (qid: string) => (by[qid]?.last ?? null) != null || (by[qid]?.series.length ?? 0) > 0;
  return { present: has("request_rate") || has("latency_p99") || has("running_reqs"), by };
}

/** Fleet-wide aggregates for the SLI banner: sums across all series (throughput,
 *  requests, errors, running) and the worst p99, plus element-wise-summed range
 *  series for the tile sparklines. */
export interface FleetTotals {
  throughput: number;
  throughputSeries: number[];
  requestRate: number;
  requestSeries: number[];
  p50: number | null;
  p99: number | null;
  /** Worst-deployment p50/p99 over time (element-wise max), for the latency chart. */
  p50Series: number[];
  p99Series: number[];
  ttft: number | null;
  errorRate: number;
  errorSeries: number[];
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
  // Rates sum across clusters (aligned by index); latency takes the worst.
  const addInto = (acc: number[], s: PromSeries) => {
    const series = nums(s);
    for (let i = 0; i < series.length; i++) acc[i] = (acc[i] ?? 0) + series[i];
  };
  const maxInto = (acc: number[], s: PromSeries) => {
    const series = nums(s);
    for (let i = 0; i < series.length; i++) acc[i] = Math.max(acc[i] ?? 0, series[i]);
  };
  for (const queries of Object.values(snap.clusters)) {
    for (const s of results(queries.throughput_tokens)) {
      t.throughput += last(s) ?? 0;
      addInto(t.throughputSeries, s);
    }
    for (const s of results(queries.request_rate)) {
      t.requestRate += last(s) ?? 0;
      addInto(t.requestSeries, s);
    }
    for (const s of results(queries.error_rate)) {
      t.errorRate += last(s) ?? 0;
      addInto(t.errorSeries, s);
    }
    for (const s of results(queries.latency_p99)) {
      const v = last(s);
      if (v != null) t.p99 = Math.max(t.p99 ?? 0, v);
      maxInto(t.p99Series, s);
    }
    for (const s of results(queries.latency_p50)) {
      const v = last(s);
      if (v != null) t.p50 = Math.max(t.p50 ?? 0, v);
      maxInto(t.p50Series, s);
    }
    for (const s of results(queries.ttft)) {
      const v = last(s);
      if (v != null) t.ttft = Math.max(t.ttft ?? 0, v);
    }
    for (const s of results(queries.running_reqs)) {
      t.running += last(s) ?? 0;
    }
  }
  t.errorPct = t.requestRate > 0 ? (t.errorRate / t.requestRate) * 100 : null;
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
    keepIsvc?: (isvc: string) => boolean;
  },
): MetricsSnapshot | null {
  if (!snap?.clusters) return snap;
  const out: MetricsSnapshot = { updatedAt: snap.updatedAt, clusters: {} };
  for (const [cluster, queries] of Object.entries(snap.clusters)) {
    if (opts.cluster && opts.cluster !== "all" && opts.cluster !== cluster) continue;
    if (opts.keepCluster && !opts.keepCluster(cluster)) continue;
    const kept: Record<string, PromResult> = {};
    for (const [qid, res] of Object.entries(queries)) {
      const result = (res?.data?.result ?? []).filter(
        (s) => !opts.keepIsvc || opts.keepIsvc(s.metric?.inferenceservice ?? ""),
      );
      kept[qid] = {
        status: res?.status,
        error: res?.error,
        data: { resultType: res?.data?.resultType ?? "vector", result },
      };
    }
    out.clusters[cluster] = kept;
  }
  return out;
}
