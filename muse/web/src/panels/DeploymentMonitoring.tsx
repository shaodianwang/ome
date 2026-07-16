import type { ReactNode } from "react";

import type { DashboardPolicy, MetricsSourceState } from "../api";
import { DataNotice } from "../components";
import { latestAlignedRatio, metricsRangeLabel, ratioSeries, seriesOf } from "../metrics";
import type { IsvcMetrics, MetricPoint } from "../metrics";
import { lowerThresholdTone, upperThresholdTone } from "../policy";
import type { ReplicaDiagnostics } from "../replicaMetrics";
import { ago, fmtErrPct, fmtInt, fmtLatency, fmtPct, fmtRate } from "../util";
import { Card, Distribution, Dot, GaugeCard, MetricChart, Spark, StatTile } from "../viz";
import type { Series } from "../viz";

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
const tokenRateText = (value: number) => `${fmtRate(value)} tok/s`;
const perSecondText = (value: number) => `${fmtRate(value)}/s`;
const plot = (
  points: MetricPoint[],
  label: string,
  color: string,
  formatValue: (value: number) => string,
  area = false,
): Series => ({ points, label, color, formatValue, area });

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
  const errSeries = ratioSeries(g("error_rate").series, g("request_rate").series, 100);
  const errPct = latestAlignedRatio(g("error_rate").series, g("request_rate").series, 100);
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
    g("kv_transfer_latency").series.some((point) => point.value > 0) ||
    g("prefill_queue").series.some((point) => point.value > 0) ||
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
          <Spark points={errSeries} color={AMBER} label="Error rate" formatValue={fmtErrPct} />
        </StatTile>
        <StatTile label="E2E p99" value={fmtLatency(e2e)} sub={`p50 ${fmtLatency(p50)}`}>
          <Spark
            points={g("latency_p99").series}
            color={RED}
            label="E2E p99"
            formatValue={fmtLatency}
          />
        </StatTile>
        <StatTile label="TTFT p99" value={fmtLatency(ttft)} sub="to first token">
          <Spark
            points={g("ttft").series}
            color={ACCENT}
            label="TTFT p99"
            formatValue={fmtLatency}
          />
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
                plot(g("latency_p99").series, "p99", RED, fmtLatency),
                plot(g("latency_p50").series, "p50", CYAN, fmtLatency),
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
          series={[plot(g("throughput_tokens").series, "Output", ACCENT, tokenRateText, true)]}
        />
        <MetricChart
          title="Input tokens"
          value={tokRate(g("input_tokens").last)}
          series={[plot(g("input_tokens").series, "Input", ACCENT, tokenRateText, true)]}
        />
        <MetricChart
          title="Requests"
          value={perSec(qps)}
          sub="per second"
          series={[plot(g("request_rate").series, "Requests", ACCENT, perSecondText, true)]}
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
            plot(g("running_reqs").series, "In-flight", ACCENT, fmtInt),
            plot(g("queue_reqs").series, "Queued", AMBER, fmtInt),
          ]}
        />
        <MetricChart
          title="Preemptions"
          value={perSec(g("preemptions").last)}
          sub="retracted under pressure"
          series={[plot(g("preemptions").series, "Preemptions", AMBER, perSecondText)]}
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
              series={[
                plot(g("kv_transfer_latency").series, "KV transfer p99", ACCENT, fmtLatency),
              ]}
            />
            <MetricChart
              title="Prefill queue"
              value={fmtInt(g("prefill_queue").last)}
              sub="bootstrap + inflight"
              series={[plot(g("prefill_queue").series, "Prefill queue", AMBER, fmtInt)]}
            />
            <MetricChart
              title="Decode queue"
              value={fmtInt(g("decode_queue").last)}
              sub="prealloc + transfer"
              series={[plot(g("decode_queue").series, "Decode queue", AMBER, fmtInt)]}
            />
            <MetricChart
              title="Transfer fails"
              value={perSec(g("pd_transfer_fail").last)}
              sub="bootstrap + transfer"
              series={[plot(g("pd_transfer_fail").series, "Transfer failures", RED, perSecondText)]}
              hideIfZero
            />
          </div>
        </>
      ) : null}
    </>
  );
}
