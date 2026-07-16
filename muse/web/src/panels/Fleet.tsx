import { useState } from "react";

import type { DepFilter } from "../App";
import { api } from "../api";
import { scopeFleet } from "../fleet";
import type { Selected } from "../types";
import { useEventFeed, useLive, useMetrics } from "../live";
import { deploymentHealth, toFeed, toRollouts } from "../map";
import { fleetTotals, metricsQueryRangeSeconds, metricsRangeLabel, perfRows } from "../metrics";
import { lowerThresholdTone, upperThresholdTone } from "../policy";
import {
  emptyText,
  fmtErrPct,
  fmtLatency,
  fmtPct,
  fmtRate,
  num as n,
  UNASSIGNED_GROUP,
  utilPct,
} from "../util";
import { Card, Dot, FilterChip, Gauge, Spark, StatTile, UtilRow } from "../viz";

const kpct = (a: { allocatable: number; allocated: number }) => utilPct(a);

export function Fleet({
  openDeployments,
  openDeployment,
}: {
  openDeployments: (f: DepFilter) => void;
  openDeployment: (s: Selected) => void;
}) {
  const clusters = useLive(api.clusters, {
    kinds: ["Cluster", "InferenceService", "Node"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const capacity = useLive(api.capacity, { kinds: ["Capacity"], intervalMs: 30_000 });
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });
  const rollouts = useLive(api.rollouts, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });
  const events = useEventFeed(30);
  const cfg = useLive(api.config, { kinds: [] });
  const metricsOn = !!cfg.data?.metrics;
  const metrics = useMetrics(metricsOn);

  const [fCluster, setFCluster] = useState("all");
  const [fModel, setFModel] = useState("all");
  const [fAccel, setFAccel] = useState("all");

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
  const availPct = health.total ? (health.ready / health.total) * 100 : null;
  const policy = cfg.data?.dashboardPolicy;
  const availabilityTone = lowerThresholdTone(availPct, policy?.availability);
  const feed = toFeed(scope.events);
  const rolloutCards = toRollouts(scope.rollouts);

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
  const errPctSeries = totals.errorSeries.map((e, i) => {
    const r = totals.requestSeries[i] ?? 0;
    return r > 0 ? (e / r) * 100 : 0;
  });
  const qpsDelta = (() => {
    const s = totals.requestSeries;
    return s.length >= 2 && s[0] > 0 ? ((s[s.length - 1] - s[0]) / s[0]) * 100 : null;
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

  // Per-cluster deployment breakdown (scoped).
  const byCluster = scope.clusters
    .map((c) => {
      const items = scope.isvcs.filter((i) => i.cluster === c.name);
      return {
        name: c.name,
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
          value={fmtPct(availPct)}
          sub={`${n(health.ready)} serving / ${n(health.total)}`}
          tone={availabilityTone || undefined}
        />
        <StatTile
          label="Fleet QPS"
          value={
            <>
              {fmtRate(totals.requestRate)}
              <small>/s</small>
            </>
          }
          sub={
            qpsDelta == null
              ? undefined
              : `${qpsDelta >= 0 ? "▲" : "▼"} ${Math.abs(qpsDelta).toFixed(0)}% vs window start`
          }
        >
          <Spark points={totals.requestSeries} />
        </StatTile>
        <StatTile
          label="p99 latency"
          value={fmtLatency(totals.p99)}
          sub={`p50 ${fmtLatency(totals.p50)} · TTFT ${fmtLatency(totals.ttft)}`}
        >
          <Spark points={totals.p99Series} color="var(--red)" />
        </StatTile>
        <StatTile
          label="Error rate"
          value={fmtErrPct(totals.errorPct)}
          sub={totals.errorPct == null ? undefined : errorThresholds}
          tone={errorTone || undefined}
        >
          <Spark points={errPctSeries} color="var(--amber)" />
        </StatTile>
        <StatTile
          label="GPU allocated"
          value={gpu ? `${gpuPct}%` : "—"}
          sub={gpu ? `${n(gpu.free)} free` : undefined}
          tone={gpuTone || undefined}
        />
      </div>

      {/* Per-model performance detail. */}
      {metricsOn ? (
        <>
          <h2 className="sec">Performance</h2>
          <Card
            title="Models by traffic"
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
                    {perf.slice(0, 12).map((r) => (
                      <tr
                        key={r.isvc}
                        className="link"
                        onClick={() =>
                          openDeployment({ cluster: r.clusters[0] ?? "", name: r.isvc })
                        }
                      >
                        <td className="mono">
                          <button
                            type="button"
                            className="row-open"
                            onClick={(event) => {
                              event.stopPropagation();
                              openDeployment({ cluster: r.clusters[0] ?? "", name: r.isvc });
                            }}
                          >
                            {r.isvc}
                          </button>
                        </td>
                        <td className="muted">{r.clusters.join(", ")}</td>
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
                    ))}
                  </tbody>
                </table>
                {perf.length > 12 ? (
                  <p className="panel-note">
                    Showing top 12 of {n(perf.length)} by traffic · open Deployments for all.
                  </p>
                ) : null}
              </>
            ) : (
              <div className="muted attn-none">Waiting for serving metrics…</div>
            )}
          </Card>
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
              <UtilRow
                key={c.name}
                name={c.name}
                tag={c.kind}
                pct={c.pct}
                tone={upperThresholdTone(c.pct, policy?.utilization)}
                value={`${n(c.free)} free`}
              />
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
                <th>Total</th>
                <th>Ready</th>
                <th>Rolling</th>
                <th>Attention</th>
              </tr>
            </thead>
            <tbody>
              {byCluster.map((c) => (
                <tr key={c.name}>
                  <td className="mono">{c.name}</td>
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
                  <td colSpan={5} className="muted">
                    {emptyText(clusters, "Loading…", "No clusters")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </Card>
        <Card
          title={`Needs attention · ${n(health.attention.length)}`}
          actions={
            <span className="right">
              <button className="linkbtn" onClick={() => openDeployments("attention")}>
                see all →
              </button>
            </span>
          }
        >
          {health.attention.length ? (
            <ul className="attn-list wide">
              {health.attention.slice(0, 8).map((a, i) => (
                <li key={i}>
                  <button
                    type="button"
                    className="attn-open"
                    onClick={() => openDeployments("attention")}
                  >
                    <Dot tone="red" />
                    <span className="mono">{a.name}</span>
                    <span className="muted">{a.cluster}</span>
                    {a.reason ? <span className="attn-reason">{a.reason}</span> : null}
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <div className="muted attn-none">All healthy.</div>
          )}
        </Card>
      </div>

      <h2 className="sec">Rollouts &amp; activity</h2>
      <div className="grid g2">
        <Card title={`Active rollouts · ${n(rolloutCards.length)}`} sub="progress · phase">
          {rolloutCards.length ? (
            <div className="urows">
              {rolloutCards.map((r) => (
                <UtilRow
                  key={`${r.cluster}/${r.name}`}
                  name={r.name}
                  pct={r.barPct ?? 0}
                  tone={r.barTone || "amber"}
                  value={r.note}
                />
              ))}
            </div>
          ) : (
            <div className="muted attn-none">
              {rollouts.loading ? "Loading…" : "Nothing rolling out."}
            </div>
          )}
        </Card>
        <div className="card">
          <div className="card-head">
            <h3>Recent activity</h3>
            <span className="right">
              <span className="live">Live</span>
            </span>
          </div>
          <div className="card-body feed">
            {feed.map((f, i) => (
              <div className="feed-item" key={i}>
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
              </div>
            ))}
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
