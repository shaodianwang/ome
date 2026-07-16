import { useState } from "react";

import type { DepFilter } from "../App";
import { api } from "../api";
import { scopeFleet } from "../fleet";
import type { Selected } from "../types";
import { useEventFeed, useLive, useMetrics } from "../live";
import { deploymentHealth, toFeed, toRollouts } from "../map";
import { fleetTotals, perfRows } from "../metrics";
import {
  emptyText,
  fmtErrPct,
  fmtLatency,
  fmtPct,
  fmtRate,
  num as n,
  UNASSIGNED_GROUP,
  utilPct,
  utilTone,
} from "../util";
import { Card, Dot, FilterChip, Gauge, Spark, StatTile, UtilRow } from "../viz";

const kpct = (a?: { allocatable: number; allocated: number }) => (a ? utilPct(a) : 0);

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
  const [fRange, setFRange] = useState("6h");

  // One scoping pass — the whole dashboard reads from `scope`, so the filter bar
  // re-scopes every panel together (deployments, rollouts, activity, capacity).
  const scope = scopeFleet(
    { cluster: fCluster, model: fModel, accel: fAccel, range: fRange },
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
  const availPct = health.total ? Math.round((health.ready / health.total) * 100) : null;
  const feed = toFeed(scope.events);
  const rolloutCards = toRollouts(scope.rollouts);

  const gpu = scope.accelerators.find((a) => a.kind === "gpu");
  const tpu = scope.accelerators.find((a) => a.kind === "tpu");
  const gpuPct = kpct(gpu);
  const gpuTight = !!gpu && gpuPct >= 90;

  const perf = perfRows(scope.metrics);
  const totals = fleetTotals(scope.metrics);
  const rangePts = fRange === "1h" ? 12 : 72;
  const trim = (a: number[]) => a.slice(-rangePts);
  const errPctSeries = trim(
    totals.errorSeries.map((e, i) => {
      const r = totals.requestSeries[i] ?? 0;
      return r > 0 ? (e / r) * 100 : 0;
    }),
  );
  const qpsDelta = (() => {
    const s = trim(totals.requestSeries);
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
  const rangeOpts = [
    { value: "6h", label: "Last 6h" },
    { value: "1h", label: "Last 1h" },
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
          <h1>
            Fleet <small className="muted">overview · all clusters</small>
          </h1>
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
          <FilterChip value={fRange} options={rangeOpts} onChange={setFRange} accent />
        </div>
      ) : null}

      {/* SLI BANNER — the fleet-aggregate KPIs (current value + trend spark). */}
      <div className="tiles">
        <StatTile
          label="Availability"
          value={availPct == null ? "—" : `${availPct}%`}
          sub={`${n(health.ready)} serving / ${n(health.total)}`}
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
          <Spark points={trim(totals.requestSeries)} />
        </StatTile>
        <StatTile
          label="p99 latency"
          value={fmtLatency(totals.p99)}
          sub={`p50 ${fmtLatency(totals.p50)} · TTFT ${fmtLatency(totals.ttft)}`}
        >
          <Spark points={trim(totals.p99Series)} color="var(--red)" />
        </StatTile>
        <StatTile
          label="Error rate"
          value={fmtErrPct(totals.errorPct)}
          sub={
            totals.errorPct == null
              ? undefined
              : totals.errorPct < 1
                ? "within SLO (<1%)"
                : "above SLO"
          }
        >
          <Spark points={errPctSeries} color="var(--amber)" />
        </StatTile>
        <StatTile
          label="GPU allocated"
          value={
            <span className={gpuTight ? "txt-bad" : undefined}>{gpu ? `${gpuPct}%` : "—"}</span>
          }
          sub={gpu ? `${n(gpu.free)} free · util — (DCGM)` : undefined}
        />
        <StatTile label="Reclaimable GPU" value="—" sub="needs GPU utilization (DCGM)" />
      </div>

      {/* PERFORMANCE — per-model detail (what the banner can't show). */}
      {metricsOn ? (
        <>
          <h2 className="sec">
            Performance <span className="who">· per model · researchers · PMs · managers</span>
          </h2>
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
                      <th>
                        QPS <span className="muted">6h</span>
                      </th>
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
                        <td className="mono">{r.isvc}</td>
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

      {/* CAPACITY */}
      <h2 className="sec">
        Capacity <span className="who">· allocation · operators · managers</span>
      </h2>
      {gpuTight ? (
        <div className="panel-alert">
          <Dot tone="red" />
          <span>
            <b>GPU {gpuPct}% allocated</b> — {gpu ? n(gpu.free) : 0} free. Utilization (power) is
            pending DCGM into an in‑cluster Prometheus, so idle‑but‑allocated capacity isn't visible
            yet.
          </span>
        </div>
      ) : null}
      <div className="grid g3">
        <Card title="Accelerator allocation" sub="claimed / capacity">
          <div className="cap-line">
            <span className="cap-k">GPU</span>
            <Gauge pct={gpuPct} tone={utilTone(gpuPct)} />
            <span className="cap-free">{gpu ? `${n(gpu.free)} free` : "—"}</span>
          </div>
          <div className="cap-line">
            <span className="cap-k">TPU</span>
            <Gauge pct={kpct(tpu)} tone={utilTone(kpct(tpu))} />
            <span className="cap-free">{tpu ? `${n(tpu.free)} free` : "—"}</span>
          </div>
          <p className="panel-note">
            Utilization (GPU power / TPU duty cycle) pending metrics in‑cluster.
          </p>
        </Card>
        <Card title="Allocation by cluster" sub="load · free">
          <div className="urows">
            {clusterRows.map((c) => (
              <UtilRow
                key={c.name}
                name={c.name}
                tag={c.kind}
                pct={c.pct}
                tone={utilTone(c.pct)}
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

      {/* DEPLOYMENTS */}
      <h2 className="sec">
        Deployments <span className="who">· everyone</span>
      </h2>
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
                <li key={i} onClick={() => openDeployments("attention")}>
                  <Dot tone="red" />
                  <span className="mono">{a.name}</span>
                  <span className="muted">{a.cluster}</span>
                  {a.reason ? <span className="attn-reason">{a.reason}</span> : null}
                </li>
              ))}
            </ul>
          ) : (
            <div className="muted attn-none">All healthy.</div>
          )}
        </Card>
      </div>

      {/* ROLLOUTS & ACTIVITY */}
      <h2 className="sec">
        Rollouts &amp; activity <span className="who">· operators · engineers</span>
      </h2>
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
