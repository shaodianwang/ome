import { useState } from "react";

import type { DepFilter } from "../App";
import { api } from "../api";
import type { AttentionItem, IsvcItem } from "../api";
import { openAttentionItem } from "../attention";
import type { AttentionHandlers } from "../attention";
import { DataNotice } from "../components";
import { fleetNoticeState, metricAvailable, metricsHealth } from "../dataState";
import type { MetricsHealth } from "../dataState";
import { scopeFleet, unambiguousDeploymentRef } from "../fleet";
import { isvcSelected } from "../resource";
import type { Selected } from "../resource";
import {
  ATTENTION_LIVE_OPTIONS,
  ISVC_LIVE_OPTIONS,
  useEventFeed,
  useLive,
  useMetrics,
} from "../live";
import { COMPONENT_TONE_CLASS, deploymentHealth, toFeed, toRollouts } from "../map";
import {
  fleetTotals,
  metricsQueryRangeSeconds,
  metricsRangeLabel,
  perfRows,
  ratioSeries,
} from "../metrics";
import type { PerfRow } from "../metrics";
import { lowerThresholdTone, upperThresholdTone } from "../policy";
import {
  ago,
  emptyText,
  fmtErrPct,
  fmtLatency,
  fmtPct,
  fmtRate,
  num as n,
  staleEmptyText,
  UNASSIGNED_GROUP,
  utilPct,
} from "../util";
import { Card, Dot, FilterChip, Gauge, Spark, StatTile, UtilRow } from "../viz";

const kpct = (a: { allocatable: number; allocated: number }) => utilPct(a);

export function Fleet({
  openDeployments,
  openDeployment,
  openModel,
  openAttention,
  openCapacityCluster,
}: {
  openDeployments: (f: DepFilter) => void;
  openDeployment: (s: Selected) => void;
  openModel: (s: Selected) => void;
  openAttention: () => void;
  openCapacityCluster: (cluster: string) => void;
}) {
  const [fCluster, setFCluster] = useState("all");
  const [fModel, setFModel] = useState("all");
  const [fAccel, setFAccel] = useState("all");
  const clusters = useLive(api.clusters, {
    kinds: ["Cluster", "InferenceService", "Node"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const capacity = useLive(api.capacity, { kinds: ["Capacity"], intervalMs: 30_000 });
  const isvcs = useLive(api.inferenceServices, ISVC_LIVE_OPTIONS);
  const rollouts = useLive(api.rollouts, ISVC_LIVE_OPTIONS);
  const events = useEventFeed(30);
  const attention = useLive(api.attention, ATTENTION_LIVE_OPTIONS);
  const cfg = useLive(api.config, { kinds: [] });
  const metricsOn = !!cfg.data?.fleetMetrics;
  const metricsCluster = fCluster === "all" ? undefined : fCluster;
  const metricsRange = cfg.data?.metricsTimeRanges?.default;
  const metrics = useMetrics(
    () => api.metrics({ cluster: metricsCluster, range: metricsRange }),
    metricsOn,
    cfg.data?.metricsRefreshSecs,
    `${metricsCluster ?? "all"}|${metricsRange ?? ""}`,
  );

  // One scoping pass — the whole dashboard reads from `scope`, so the filter bar
  // re-scopes every panel together (deployments, rollouts, activity, capacity).
  const scope = scopeFleet(
    { cluster: fCluster, model: fModel, accel: fAccel },
    {
      clusters: clusters.data?.items ?? [],
      isvcs: isvcs.data?.items ?? [],
      rollouts: rollouts.data?.items ?? [],
      events: events.events,
      metrics: metrics.data,
      capacity: capacity.data,
    },
  );

  const health = deploymentHealth(scope.isvcs);
  // Triage findings, scoped by the cluster filter only — model/accelerator
  // filters have no meaning for cluster- or model-level findings.
  const attnItems = (attention.data?.items ?? []).filter(
    (a) => fCluster === "all" || a.cluster === fCluster,
  );
  const availPct = health.total ? (health.ready / health.total) * 100 : null;
  const policy = cfg.data?.dashboardPolicy;
  const availabilityTone = lowerThresholdTone(availPct, policy?.availability);
  const feed = toFeed(scope.events);
  const rolloutCards = toRollouts(scope.rollouts, scope.isvcs);
  const topConsumers = (capacity.data?.consumersByDeployment ?? [])
    .filter((c) => fCluster === "all" || c.cluster === fCluster)
    .slice(0, 8);
  const metricState = metricsHealth(scope.metrics);
  const hasQps = metricAvailable(scope.metrics, "request_rate");
  const hasLatency = metricAvailable(scope.metrics, "latency_p99");
  const hasErrors = hasQps && metricAvailable(scope.metrics, "error_rate");
  const { notice: fleetNotice, hasSourceIssues } = fleetNoticeState({
    sources: [
      { domain: "Clusters", error: clusters.error, hasSnapshot: clusters.data != null },
      { domain: "Capacity", error: capacity.error, hasSnapshot: capacity.data != null },
      { domain: "Deployments", error: isvcs.error, hasSnapshot: isvcs.data != null },
      { domain: "Rollouts", error: rollouts.error, hasSnapshot: rollouts.data != null },
      { domain: "Activity history", error: events.error, hasSnapshot: events.events.length > 0 },
      { domain: "Configuration", error: cfg.error, hasSnapshot: cfg.data != null },
    ],
    streamReconnecting: events.connection === "reconnecting",
    hasStreamedEvents: events.events.length > 0,
    metricsOn,
    metricsError: metrics.error,
    metrics: scope.metrics,
  });
  const retrySources = () => {
    clusters.refetch();
    capacity.refetch();
    isvcs.refetch();
    rollouts.refetch();
    events.refetch();
    cfg.refetch();
  };

  const gpu = scope.accelerators.find((a) => a.kind === "gpu");
  const tpu = scope.accelerators.find((a) => a.kind === "tpu");
  const gpuPct = gpu ? kpct(gpu) : null;
  const tpuPct = tpu ? kpct(tpu) : null;
  const gpuTone = upperThresholdTone(gpuPct, policy?.utilization);
  const tpuTone = upperThresholdTone(tpuPct, policy?.utilization);
  const gpuTight = !!gpu && gpuTone === "red";

  const perf = perfRows(scope.metrics);
  const totals = fleetTotals(scope.metrics);
  const rangeLabel = metricsRangeLabel(metricsQueryRangeSeconds(scope.metrics, "request_rate"));
  const errorTone = upperThresholdTone(totals.errorPct, policy?.errorRate);
  const errorThresholds = policy?.errorRate
    ? `warning ≥ ${fmtErrPct(policy.errorRate.warningAt)} · critical ≥ ${fmtErrPct(policy.errorRate.criticalAt)}`
    : undefined;
  const errPctSeries = ratioSeries(totals.errorSeries, totals.requestSeries, 100);
  const qpsDelta = (() => {
    const s = totals.requestSeries;
    const first = s[0]?.value;
    const last = s[s.length - 1]?.value;
    return s.length >= 2 && first != null && first > 0 && last != null
      ? ((last - first) / first) * 100
      : null;
  })();

  // Allocation-by-cluster rows (scoped).
  const clusterRows = scope.byCluster
    .map((g) => ({
      name: g.name,
      kind: g.kind,
      free: g.free,
      pct: g.allocatable ? Math.round((g.allocated / g.allocatable) * 100) : 0,
    }))
    .sort((a, b) => b.pct - a.pct);

  // Filter options come from the full (unscoped) data, so any cluster/model stays selectable.
  const allClusters = clusters.data?.items ?? [];
  const allIsvcs = isvcs.data?.items ?? [];
  const modelNames = [...new Set(allIsvcs.map((i) => i.model).filter((m): m is string => !!m))];
  const clusterOpts = [
    { value: "all", label: "All" },
    ...allClusters.map((c) => ({ value: c.name, label: c.name })),
  ];
  const modelOpts = [
    { value: "all", label: "All" },
    ...modelNames.map((m) => ({ value: m, label: m })),
  ];
  const accelOpts = [
    { value: "all", label: "All" },
    { value: "gpu", label: "GPU" },
    { value: "tpu", label: "TPU" },
  ];
  // Largest placeable job, per tier (scoped by accel; see scopeFleet note on cluster tiers).
  const maxAt = (groups: { kind: string; free: number; name: string }[]) => {
    const m = new Map<string, number>();
    for (const g of groups)
      if (g.name !== UNASSIGNED_GROUP) m.set(g.kind, Math.max(m.get(g.kind) ?? 0, g.free));
    return m;
  };
  const maxRack = maxAt(scope.byNvlinkDomain);
  const maxFabric = maxAt(scope.byIsland);
  const placeKinds = [...new Set([...maxRack.keys(), ...maxFabric.keys()])].sort();

  // Per-cluster deployment breakdown (scoped), joined with cluster inventory so
  // the table doubles as a fleet map (nodes / accel nodes / k8s version).
  const byCluster = scope.clusters
    .map((c) => {
      const items = scope.isvcs.filter((i) => i.cluster === c.name);
      return {
        name: c.name,
        nodes: c.nodes,
        acceleratorNodes: c.acceleratorNodes,
        version: c.version,
        total: items.length,
        ready: items.filter((i) => i.ready === true).length,
        rolling: scope.rollouts.filter((r) => r.cluster === c.name && !r.ready).length,
        attention: items.filter((i) => i.ready === false).length,
      };
    })
    .sort((a, b) => b.total - a.total);

  return (
    <section className="panel active">
      <div className="page-head">
        <div>
          <h1>Fleet</h1>
          <p>Serving health, performance, and accelerator allocation across clusters.</p>
        </div>
      </div>

      {fleetNotice ? (
        <DataNotice
          tone={fleetNotice.tone}
          title={fleetNotice.title}
          detail={fleetNotice.detail}
          action={hasSourceIssues ? { label: "Retry", onClick: retrySources } : undefined}
        />
      ) : null}

      {metricsOn ? (
        <div className="filters">
          <FilterChip
            label="cluster"
            value={fCluster}
            options={clusterOpts}
            onChange={setFCluster}
          />
          <FilterChip label="model" value={fModel} options={modelOpts} onChange={setFModel} />
          <FilterChip label="accel" value={fAccel} options={accelOpts} onChange={setFAccel} />
        </div>
      ) : null}

      {/* Fleet-level KPIs with current values and compact trends. */}
      <div className="tiles">
        <StatTile
          label="Availability"
          value={!isvcs.data || availPct == null ? "—" : fmtPct(availPct)}
          sub={
            isvcs.error && !isvcs.data
              ? "deployment status unavailable"
              : isvcs.loading && !isvcs.data
                ? "loading deployments"
                : health.total
                  ? `${n(health.ready)} serving / ${n(health.total)}`
                  : "no deployments in scope"
          }
          tone={availabilityTone || undefined}
        />
        <StatTile
          label="Fleet QPS"
          value={
            <>
              {hasQps ? fmtRate(totals.requestRate) : "—"}
              {hasQps ? <small>/s</small> : null}
            </>
          }
          sub={
            !hasQps || qpsDelta == null
              ? undefined
              : `${qpsDelta >= 0 ? "▲" : "▼"} ${Math.abs(qpsDelta).toFixed(0)}% vs window start`
          }
        >
          {hasQps ? (
            <Spark
              points={totals.requestSeries}
              label="Fleet QPS"
              formatValue={(value) => `${fmtRate(value)}/s`}
            />
          ) : null}
        </StatTile>
        <StatTile
          label="p99 latency"
          value={hasLatency ? fmtLatency(totals.p99) : "—"}
          sub={
            hasLatency
              ? `p50 ${fmtLatency(totals.p50)} · TTFT ${fmtLatency(totals.ttft)}`
              : undefined
          }
        >
          {hasLatency ? (
            <Spark
              points={totals.p99Series}
              color="var(--red)"
              label="p99 latency"
              formatValue={fmtLatency}
            />
          ) : null}
        </StatTile>
        <StatTile
          label="Error rate"
          value={hasErrors ? fmtErrPct(totals.errorPct) : "—"}
          sub={!hasErrors || totals.errorPct == null ? undefined : errorThresholds}
          tone={errorTone || undefined}
        >
          {hasErrors ? (
            <Spark
              points={errPctSeries}
              color="var(--amber)"
              label="Error rate"
              formatValue={fmtErrPct}
            />
          ) : null}
        </StatTile>
        <StatTile
          label="GPU allocated"
          value={gpu ? `${gpuPct}%` : "—"}
          sub={gpu ? `${n(gpu.free)} free` : undefined}
          tone={gpuTone || undefined}
        />
      </div>

      {/* Per-deployment performance detail. */}
      {metricsOn ? (
        <>
          <h2 className="sec">
            Performance
            {metricState.ready && scope.metrics ? (
              <span className="who">· updated {ago(scope.metrics.updatedAt)}</span>
            ) : null}
          </h2>
          <PerfTable
            perf={perf}
            rangeLabel={rangeLabel}
            isvcs={scope.isvcs}
            metricsError={metrics.error}
            metricsLoading={metrics.loading}
            metricState={metricState}
            openDeployment={openDeployment}
            openDeployments={openDeployments}
          />
        </>
      ) : null}

      <h2 className="sec">Capacity</h2>
      {gpuTight ? (
        <div className="panel-alert">
          <Dot tone="red" />
          <span>
            <b>GPU {gpuPct}% allocated</b> — {gpu ? n(gpu.free) : 0} free. This view reflects
            allocation because utilization telemetry is unavailable.
          </span>
        </div>
      ) : null}
      <div className="grid g3">
        <Card title="Accelerator allocation" sub="claimed / capacity">
          <div className="cap-line">
            <span className="cap-k">GPU</span>
            {gpu ? <Gauge pct={kpct(gpu)} tone={gpuTone} /> : <span className="muted">—</span>}
            <span className="cap-free">{gpu ? `${n(gpu.free)} free` : "—"}</span>
          </div>
          <div className="cap-line">
            <span className="cap-k">TPU</span>
            {tpu ? <Gauge pct={kpct(tpu)} tone={tpuTone} /> : <span className="muted">—</span>}
            <span className="cap-free">{tpu ? `${n(tpu.free)} free` : "—"}</span>
          </div>
          <p className="panel-note">Allocation only; utilization telemetry is unavailable.</p>
        </Card>
        <Card title="Allocation by cluster" sub="load · free">
          <div className="urows">
            {clusterRows.map((c) => (
              <button
                type="button"
                className="urow-open"
                key={c.name}
                title="open Capacity"
                onClick={() => openCapacityCluster(c.name)}
              >
                <UtilRow
                  name={c.name}
                  tag={c.kind}
                  pct={c.pct}
                  tone={upperThresholdTone(c.pct, policy?.utilization)}
                  value={`${n(c.free)} free`}
                />
              </button>
            ))}
            {clusterRows.length === 0 ? (
              <div className="muted urow-empty">
                {emptyText(clusters, "Loading clusters…", "No clusters")}
              </div>
            ) : null}
          </div>
        </Card>
        <Card title="Largest placeable job" sub="free now, per tier">
          <table className="place-tbl">
            <thead>
              <tr>
                <th></th>
                <th>
                  Rack <span className="muted">NVLink / ICI</span>
                </th>
                <th>
                  Fabric <span className="muted">island</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {placeKinds.map((k) => (
                <tr key={k}>
                  <td className="place-kind">{k.toUpperCase()}</td>
                  <td>
                    {maxRack.has(k) ? (
                      <b className="tnum">{n(maxRack.get(k) ?? 0)}</b>
                    ) : (
                      <span className="muted">—</span>
                    )}
                  </td>
                  <td>
                    {maxFabric.has(k) ? (
                      <b className="tnum">{n(maxFabric.get(k) ?? 0)}</b>
                    ) : (
                      <span className="muted">—</span>
                    )}
                  </td>
                </tr>
              ))}
              {placeKinds.length === 0 ? (
                <tr>
                  <td colSpan={3} className="muted">
                    —
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
          <p className="panel-note">
            Biggest tightly‑coupled job that fits in one block right now.
          </p>
        </Card>
      </div>

      <h2 className="sec">Deployments</h2>
      <div className="grid g2">
        <Card title="Top consumers" sub="accelerators held · by deployment">
          {topConsumers.length ? (
            <table className="place-tbl">
              <thead>
                <tr>
                  <th>Deployment</th>
                  <th>Cluster</th>
                  <th>Kind</th>
                  <th className="tnum">Pods</th>
                  <th className="tnum">Accels</th>
                </tr>
              </thead>
              <tbody>
                {topConsumers.map((c) => (
                  <tr
                    key={`${c.cluster}/${c.namespace}/${c.name}/${c.kind}`}
                    className="link"
                    onClick={() => openDeployment(isvcSelected(c.cluster, c.name, c.namespace))}
                  >
                    <td className="mono">
                      {c.name} <span className="muted">{c.namespace}</span>
                    </td>
                    <td className="mono muted">{c.cluster}</td>
                    <td>{c.kind.toUpperCase()}</td>
                    <td className="tnum">{c.pods}</td>
                    <td className="tnum">{c.accelerators.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="muted attn-none">
              {emptyText(capacity, "Loading…", "No deployment-attributed consumption.")}
            </div>
          )}
        </Card>
        <Card
          title="By cluster"
          actions={
            <span className="right">
              <button className="linkbtn" onClick={() => openDeployments("all")}>
                open Deployments →
              </button>
            </span>
          }
        >
          <table className="place-tbl">
            <thead>
              <tr>
                <th>Cluster</th>
                <th className="tnum">Nodes</th>
                <th className="tnum">Accel</th>
                <th className="tnum">Deploys</th>
                <th className="tnum">Ready</th>
                <th className="tnum">Rolling</th>
                <th className="tnum">Attention</th>
              </tr>
            </thead>
            <tbody>
              {byCluster.map((c) => (
                <tr
                  key={c.name}
                  className="link"
                  title={c.version ? `Kubernetes ${c.version} · open Capacity` : "open Capacity"}
                  onClick={() => openCapacityCluster(c.name)}
                >
                  <td className="mono">{c.name}</td>
                  <td className="tnum">{c.nodes.toLocaleString()}</td>
                  <td className="tnum">{c.acceleratorNodes.toLocaleString()}</td>
                  <td className="tnum">{c.total}</td>
                  <td className={c.ready ? "tnum txt-ok" : "tnum"}>{c.ready}</td>
                  <td className="tnum">{c.rolling || <span className="muted">—</span>}</td>
                  <td className={c.attention ? "tnum txt-bad" : "tnum"}>
                    {c.attention || <span className="muted">—</span>}
                  </td>
                </tr>
              ))}
              {byCluster.length === 0 ? (
                <tr>
                  <td colSpan={7} className="muted">
                    {emptyText(clusters, "Loading…", "No clusters")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </Card>
        <AttentionCard
          items={attnItems}
          status={attention}
          openAttention={openAttention}
          handlers={{ openDeployment, openModel, openCapacityCluster }}
        />
      </div>

      <h2 className="sec">Rollouts &amp; activity</h2>
      <div className="grid g2">
        <Card title={`Active rollouts · ${n(rolloutCards.length)}`} sub="progress · phase">
          {rolloutCards.length ? (
            <div className="urows">
              {rolloutCards.map((r) => (
                <button
                  type="button"
                  className="urow-open"
                  key={`${r.cluster}/${r.namespace ?? ""}/${r.name}`}
                  onClick={() => openDeployment(r.sel)}
                >
                  <UtilRow
                    name={r.name}
                    tag={r.namespace}
                    pct={r.barPct ?? 0}
                    tone={r.barTone || "amber"}
                    value={r.note}
                  />
                  {r.components?.length ? (
                    <span className="comp-cells rollout-comps">
                      {r.components.map((c) => (
                        <span
                          key={c.name}
                          className={`repl ${COMPONENT_TONE_CLASS[c.tone]}`}
                          title={c.revision}
                        >
                          {c.label}
                          {c.phase && c.phase !== "Stable" ? ` · ${c.phase}` : ""}
                        </span>
                      ))}
                    </span>
                  ) : null}
                </button>
              ))}
            </div>
          ) : (
            <div className="muted attn-none">
              {staleEmptyText(rollouts, {
                stale: "Rollout refresh failed; no active rollouts in the last snapshot.",
                unavailable: "Rollout status is unavailable.",
                loading: "Loading…",
                empty: "Nothing rolling out.",
              })}
            </div>
          )}
        </Card>
        <div className="card">
          <div className="card-head">
            <h3>Recent activity</h3>
            <span className="right">
              <span
                className={
                  events.connection === "reconnecting"
                    ? "live degraded"
                    : events.connection === "connecting"
                      ? "live connecting"
                      : "live"
                }
              >
                {events.connection === "reconnecting"
                  ? "Reconnecting"
                  : events.connection === "connecting"
                    ? "Connecting"
                    : "Live"}
              </span>
            </span>
          </div>
          <div className="card-body feed">
            {feed.map((f, i) => {
              const body = (
                <div className="feed-body">
                  <div className="row1">
                    <div className="t">
                      <b>{f.strong}</b>
                      {f.rest}
                    </div>
                    <span className="m when">{f.when}</span>
                  </div>
                  <div className="m mono">{f.meta}</div>
                </div>
              );
              return f.sel ? (
                <button
                  type="button"
                  className="feed-item urow-open"
                  key={i}
                  onClick={() => f.sel && openDeployment(f.sel)}
                >
                  {body}
                </button>
              ) : (
                <div className="feed-item" key={i}>
                  {body}
                </div>
              );
            })}
            {feed.length === 0 ? (
              <div className="muted attn-none">
                {emptyText(events, "Loading…", "No recent activity")}
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </section>
  );
}

/** The Deployments-by-traffic table: the busiest deployments with their serving
 *  KPIs, each row linking to its detail when the identity is unambiguous. */
function PerfTable({
  perf,
  rangeLabel,
  isvcs,
  metricsError,
  metricsLoading,
  metricState,
  openDeployment,
  openDeployments,
}: {
  perf: PerfRow[];
  rangeLabel: string | null;
  isvcs: IsvcItem[];
  metricsError: string | null;
  metricsLoading: boolean;
  metricState: MetricsHealth;
  openDeployment: (s: Selected) => void;
  openDeployments: (f: DepFilter) => void;
}) {
  return (
    <Card
      title="Deployments by traffic"
      actions={
        <span className="right">
          <button className="linkbtn" onClick={() => openDeployments("all")}>
            open Deployments →
          </button>
        </span>
      }
    >
      {perf.length ? (
        <>
          <table className="place-tbl perf-tbl">
            <thead>
              <tr>
                <th>Deployment</th>
                <th>Cluster</th>
                <th>QPS {rangeLabel ? <span className="muted">{rangeLabel}</span> : null}</th>
                <th>p50</th>
                <th>p99</th>
                <th>TTFT</th>
                <th>tok/s</th>
                <th>Err%</th>
                <th>GPU util</th>
              </tr>
            </thead>
            <tbody>
              {perf.slice(0, 12).map((r) => {
                const target = unambiguousDeploymentRef(isvcs, r.cluster, r.namespace, r.isvc);
                return (
                  <tr
                    key={`${r.cluster}/${r.namespace ?? ""}/${r.isvc}`}
                    className={target ? "link" : undefined}
                    onClick={() => target && openDeployment(target)}
                  >
                    <td className="mono">
                      {target ? (
                        <button
                          type="button"
                          className="row-open"
                          onClick={(event) => {
                            event.stopPropagation();
                            openDeployment(target);
                          }}
                        >
                          {r.namespace ? `${r.namespace}/${r.isvc}` : r.isvc}
                        </button>
                      ) : r.namespace ? (
                        `${r.namespace}/${r.isvc}`
                      ) : (
                        r.isvc
                      )}
                    </td>
                    <td className="muted">{r.cluster}</td>
                    <td className="tnum">{fmtRate(r.qps)}</td>
                    <td className="tnum">{fmtLatency(r.p50)}</td>
                    <td className="tnum">{fmtLatency(r.p99)}</td>
                    <td className="tnum">{fmtLatency(r.ttft)}</td>
                    <td className="tnum">{fmtRate(r.throughput)}</td>
                    <td className="tnum">{fmtPct(r.errPct)}</td>
                    <td className="tnum muted">
                      {r.gpuUtil == null ? "—" : `${Math.round(r.gpuUtil)}%`}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {perf.length > 12 ? (
            <p className="panel-note">
              Showing top 12 of {n(perf.length)} by traffic · open Deployments for all.
            </p>
          ) : null}
        </>
      ) : (
        <div className="muted attn-none">
          {metricsError && !metricState.ready
            ? "Serving metrics are unavailable."
            : metricsLoading || !metricState.ready
              ? "Loading serving metrics…"
              : metricState.queryCount === 0
                ? "No metrics query results."
                : metricState.failedQueryCount === metricState.queryCount
                  ? "All serving metric queries failed."
                  : "No serving traffic in the selected window."}
        </div>
      )}
    </Card>
  );
}

/** The Needs-attention card: the most severe triage findings in scope, each
 *  linking to the page that diagnoses it. */
function AttentionCard({
  items,
  status,
  openAttention,
  handlers,
}: {
  items: AttentionItem[];
  status: { loading: boolean; error: string | null; data: unknown };
  openAttention: () => void;
  handlers: AttentionHandlers;
}) {
  return (
    <Card
      title={`Needs attention · ${n(items.length)}`}
      actions={
        <span className="right">
          <button className="linkbtn" onClick={openAttention}>
            see all →
          </button>
        </span>
      }
    >
      {items.length ? (
        <ul className="attn-list wide">
          {items.slice(0, 8).map((a, i) => (
            <li key={i}>
              <button
                type="button"
                className="attn-open"
                onClick={() => openAttentionItem(a, handlers)}
              >
                <Dot tone={a.severity === "critical" ? "red" : "amber"} />
                <span className="mono">{a.name}</span>
                <span className="muted">{a.cluster}</span>
                <span className="attn-reason">{a.summary}</span>
              </button>
            </li>
          ))}
        </ul>
      ) : (
        <div className="muted attn-none">
          {staleEmptyText(status, {
            stale: "Status refresh failed; no active alerts in the last snapshot.",
            unavailable: "Attention data is unavailable.",
            loading: "Loading…",
            empty: "Nothing needs attention in scope.",
          })}
        </div>
      )}
    </Card>
  );
}
