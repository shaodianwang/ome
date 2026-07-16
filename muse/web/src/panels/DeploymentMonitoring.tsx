import type { ReactNode } from "react";

import type { DashboardPolicy, MetricsSourceState } from "../api";
import { DataNotice } from "../components";
import { metricsRangeLabel, seriesOf } from "../metrics";
import type { IsvcMetrics } from "../metrics";
import { lowerThresholdTone, upperThresholdTone } from "../policy";
import type { ReplicaDiagnostics } from "../replicaMetrics";
import { ago, fmtErrPct, fmtInt, fmtLatency, fmtPct, fmtRate } from "../util";
import { Card, Distribution, Dot, GaugeCard, MetricChart, Spark, StatTile } from "../viz";

const ACCENT = "var(--accent-2)";
const RED = "var(--red)";
const CYAN = "var(--cyan)";
const AMBER = "var(--amber)";

const tokRate = (v: number | null): ReactNode => (
  <>
    {fmtRate(v)}
    <small> tok/s</small>
  </>
);
const perSec = (v: number | null): ReactNode => (
  <>
    {fmtRate(v)}
    <small> /s</small>
  </>
);

/**
 * Serving dashboard for one deployment. The visualization is chosen per data
 * shape: golden signals as threshold-colored stats; the request latency as a
 * queue→prefill→decode lifecycle bar (derived, engine-agnostic); throughput as
 * area charts; bounded saturation (KV cache, prefix hit) as gauges; rare events
 * as charts that appear only when non-zero.
 */
export function Monitoring({
  m,
  replicas,
  metricsOn,
  policy,
  loading,
  error,
  queryCount,
  failedQueryCount,
  updatedAt,
  source,
}: {
  m: IsvcMetrics;
  replicas: ReplicaDiagnostics;
  metricsOn: boolean;
  policy?: DashboardPolicy;
  loading: boolean;
  error: string | null;
  queryCount: number;
  failedQueryCount: number;
  updatedAt?: number;
  source?: MetricsSourceState;
}) {
  const hasServingData = m.present || replicas.rows.length > 0;
  if (!metricsOn) {
    return <div className="muted attn-none">Metrics aren't configured on this console.</div>;
  }
  if (source && source.status !== "available" && !hasServingData) {
    const unconfigured = source.status === "unconfigured";
    const forbidden = source.error?.code === "prometheus_service_discovery_forbidden";
    return (
      <DataNotice
        tone={unconfigured ? "warning" : "danger"}
        title={unconfigured ? "Serving metrics are not configured" : "Serving metrics unavailable"}
        detail={
          unconfigured
            ? `Prometheus Service ${source.target} is not configured in this cluster.`
            : forbidden
              ? `The cluster credential cannot read Prometheus Service ${source.target}.`
              : `Muse could not validate Prometheus Service ${source.target}.`
        }
      />
    );
  }
  if (error && !hasServingData) {
    return <DataNotice tone="danger" title="Serving metrics unavailable" detail={error} />;
  }
  if (loading && !hasServingData) {
    return <div className="muted attn-none">Loading serving metrics…</div>;
  }
  if (queryCount === 0 && !hasServingData) {
    return (
      <DataNotice
        tone="danger"
        title="Serving metrics unavailable"
        detail="No Prometheus query results were returned for this cluster."
      />
    );
  }
  if (failedQueryCount === queryCount && queryCount > 0 && !hasServingData) {
    return (
      <DataNotice
        tone="danger"
        title="Serving metrics failed"
        detail={`All ${queryCount} query results failed for this cluster.`}
      />
    );
  }
  if (!hasServingData) {
    return <div className="muted attn-none">No serving traffic in the selected window.</div>;
  }
  const g = (id: string) => seriesOf(m, id);
  const rangeLabel = metricsRangeLabel(m.rangeSeconds);

  // Golden signals.
  const qps = g("request_rate").last;
  const errRate = g("error_rate").last;
  const errPct = qps && qps > 0 ? ((errRate ?? 0) / qps) * 100 : null;
  const errSeries = g("error_rate").series.map((e, i) => {
    const r = g("request_rate").series[i] ?? 0;
    return r > 0 ? (e / r) * 100 : 0;
  });
  const avail = errPct == null ? null : Math.max(0, 100 - errPct);
  const e2e = g("latency_p99").last;
  const p50 = g("latency_p50").last;
  const ttft = g("ttft").last;

  // Request lifecycle, engine-agnostic: TTFT = queue + prefill, E2E = TTFT + decode.
  const queue = g("queue_time").last;
  const prefill = e2e != null && ttft != null ? Math.max(0, ttft - (queue ?? 0)) : null;
  const decode = e2e != null && ttft != null ? Math.max(0, e2e - ttft) : null;

  // Saturation / efficiency.
  const kv = g("kv_cache_usage").last;
  const hit = g("prefix_cache_hit").last;

  const hasPD =
    g("kv_transfer_latency").series.some((v) => v > 0) ||
    g("prefill_queue").series.some((v) => v > 0) ||
    g("decode_queue").series.length > 1;

  return (
    <>
      {error ? (
        <DataNotice
          tone="warning"
          title="Live metrics disconnected"
          detail={`${updatedAt ? `Showing the snapshot from ${ago(updatedAt)}. ` : ""}${error}`}
        />
      ) : failedQueryCount > 0 ? (
        <DataNotice
          tone="warning"
          title="Serving metrics are partial"
          detail={`${failedQueryCount} of ${queryCount} query results failed${updatedAt ? `; snapshot updated ${ago(updatedAt)}` : ""}.`}
        />
      ) : null}
      <h2 className="sec">Health</h2>
      <div className="grid g4">
        <StatTile
          label="Availability"
          value={avail == null ? "—" : `${avail.toFixed(2)}%`}
          sub={rangeLabel ? `last ${rangeLabel}` : undefined}
          tone={lowerThresholdTone(avail, policy?.availability) || undefined}
        />
        <StatTile
          label="Error rate"
          value={fmtErrPct(errPct)}
          tone={upperThresholdTone(errPct, policy?.errorRate) || undefined}
        >
          <Spark points={errSeries} color={AMBER} />
        </StatTile>
        <StatTile label="E2E p99" value={fmtLatency(e2e)} sub={`p50 ${fmtLatency(p50)}`}>
          <Spark points={g("latency_p99").series} color={RED} />
        </StatTile>
        <StatTile label="TTFT p99" value={fmtLatency(ttft)} sub="to first token">
          <Spark points={g("ttft").series} color={ACCENT} />
        </StatTile>
      </div>

      {replicas.configured ? (
        <>
          <h2 className="sec">Replica performance</h2>
          <Card
            title="Engine replicas"
            sub={`current metrics${updatedAt ? ` · updated ${ago(updatedAt)}` : ""}`}
          >
            {replicas.rows.length ? (
              <table className="place-tbl perf-tbl" aria-label="Engine replica performance">
                <thead>
                  <tr>
                    <th>Replica pod</th>
                    <th>Component</th>
                    <th>QPS</th>
                    <th>p99</th>
                    <th>TTFT</th>
                    <th>tok/s</th>
                    <th>In-flight</th>
                    <th>Queued</th>
                    <th>KV cache</th>
                  </tr>
                </thead>
                <tbody>
                  {replicas.rows.map((row) => (
                    <tr key={`${row.namespace}/${row.component}/${row.pod}`}>
                      <td className="mono">{row.pod}</td>
                      <td>{row.component}</td>
                      <td className="tnum">{fmtRate(row.qps)}</td>
                      <td className="tnum">{fmtLatency(row.p99)}</td>
                      <td className="tnum">{fmtLatency(row.ttft)}</td>
                      <td className="tnum">{fmtRate(row.throughput)}</td>
                      <td className="tnum">{fmtInt(row.running)}</td>
                      <td className="tnum">{fmtInt(row.queued)}</td>
                      <td className="tnum">
                        {row.kvCache == null ? "—" : fmtPct(row.kvCache * 100)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="muted attn-none">No engine replica metrics in this snapshot.</div>
            )}
          </Card>
        </>
      ) : null}

      {e2e != null && ttft != null ? (
        <>
          <h2 className="sec">Latency breakdown</h2>
          <div className="grid g2">
            <Card title="Where the time goes" sub={`p99 · ${fmtLatency(e2e)} end-to-end`}>
              <div className="brk-bar">
                <Distribution
                  segments={[
                    { value: queue ?? 0, tone: "slate" },
                    { value: prefill ?? 0, tone: "cyan" },
                    { value: decode ?? 0, tone: "violet" },
                  ]}
                />
              </div>
              <div className="brk-legend">
                <span>
                  <Dot tone="slate" /> Queue {fmtLatency(queue)}
                </span>
                <span>
                  <Dot tone="cyan" /> Prefill {fmtLatency(prefill)}
                </span>
                <span>
                  <Dot tone="violet" /> Decode {fmtLatency(decode)}
                </span>
              </div>
            </Card>
            <MetricChart
              title="Latency trend"
              value={
                <>
                  {fmtLatency(e2e)}
                  <small> p99 · p50 {fmtLatency(p50)}</small>
                </>
              }
              series={[
                { points: g("latency_p99").series, color: RED },
                { points: g("latency_p50").series, color: CYAN },
              ]}
            />
          </div>
        </>
      ) : null}

      <h2 className="sec">Throughput</h2>
      <div className="grid g3">
        <MetricChart
          title="Output tokens"
          value={tokRate(g("throughput_tokens").last)}
          series={[{ points: g("throughput_tokens").series, color: ACCENT, area: true }]}
        />
        <MetricChart
          title="Input tokens"
          value={tokRate(g("input_tokens").last)}
          series={[{ points: g("input_tokens").series, color: ACCENT, area: true }]}
        />
        <MetricChart
          title="Requests"
          value={perSec(qps)}
          sub="per second"
          series={[{ points: g("request_rate").series, color: ACCENT, area: true }]}
        />
      </div>

      <h2 className="sec">Saturation &amp; efficiency</h2>
      <div className="grid g2">
        <GaugeCard
          title="KV cache"
          frac={kv}
          tone={upperThresholdTone(kv, policy?.kvCache)}
          sub="utilization"
        />
        <GaugeCard
          title="Prefix cache hit"
          frac={hit}
          tone={lowerThresholdTone(hit, policy?.prefixCache)}
          sub="higher is better"
        />
      </div>
      <div className="grid g2">
        <MetricChart
          title="In-flight & queued"
          value={fmtInt(g("running_reqs").last)}
          sub={`${fmtInt(g("queue_reqs").last)} queued`}
          series={[
            { points: g("running_reqs").series, color: ACCENT },
            { points: g("queue_reqs").series, color: AMBER },
          ]}
        />
        <MetricChart
          title="Preemptions"
          value={perSec(g("preemptions").last)}
          sub="retracted under pressure"
          series={[{ points: g("preemptions").series, color: AMBER }]}
          hideIfZero
        />
      </div>

      {hasPD ? (
        <>
          <h2 className="sec">Prefill / decode (PD)</h2>
          <div className="grid g3">
            <MetricChart
              title="KV transfer p99"
              value={fmtLatency(g("kv_transfer_latency").last)}
              sub="prefill → decode"
              series={[{ points: g("kv_transfer_latency").series, color: ACCENT }]}
            />
            <MetricChart
              title="Prefill queue"
              value={fmtInt(g("prefill_queue").last)}
              sub="bootstrap + inflight"
              series={[{ points: g("prefill_queue").series, color: AMBER }]}
            />
            <MetricChart
              title="Decode queue"
              value={fmtInt(g("decode_queue").last)}
              sub="prealloc + transfer"
              series={[{ points: g("decode_queue").series, color: AMBER }]}
            />
            <MetricChart
              title="Transfer fails"
              value={perSec(g("pd_transfer_fail").last)}
              sub="bootstrap + transfer"
              series={[{ points: g("pd_transfer_fail").series, color: RED }]}
              hideIfZero
            />
          </div>
        </>
      ) : null}
    </>
  );
}
