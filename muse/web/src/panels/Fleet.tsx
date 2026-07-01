import type { PanelKey } from "../App";
import { api } from "../api";
import { Bar, CloudDot, Pill, UtilCell } from "../components";
import { ArrowRight, Check, IconModels, Lines, Plus, Refresh, Upload, Warning } from "../icons";
import { useEventFeed, useLive } from "../live";
import { toClusterRows, toFeed, toRollouts } from "../map";
import { CLOUD_LABEL, type FeedItem } from "../types";

function feedIcon(icon: FeedItem["icon"]) {
  switch (icon) {
    case "deploy":
      return <Upload />;
    case "arrow":
      return <ArrowRight />;
    case "check":
      return <Check />;
    case "cube":
      return <IconModels />;
    case "warning":
      return <Warning />;
    case "lines":
      return <Lines />;
  }
}

const n = (v: number) => v.toLocaleString();

export function Fleet({ goto }: { goto: (p: PanelKey) => void }) {
  // Counts change only on create/delete (rare), so refetch is coalesced and
  // backstopped by a slow interval; capacity isn't on the stream (interval-only);
  // the activity feed streams in place with no refetch.
  const clusters = useLive(api.clusters, {
    kinds: ["InferenceService"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const capacity = useLive(api.capacity, { kinds: [], intervalMs: 30_000 });
  const events = useEventFeed(60);
  const rollouts = useLive(api.rollouts, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });

  const clusterItems = clusters.data?.items ?? [];
  const rows = toClusterRows(clusterItems, capacity.data);
  const feed = toFeed(events.events);
  const rolloutCards = toRollouts(rollouts.data?.items ?? []);

  const isvcTotal = clusterItems.reduce((s, c) => s + c.inferenceServices, 0);
  const ready = clusterItems.filter((c) => c.ready).length;
  const accel = capacity.data?.accelerators ?? [];
  const gpu = accel.find((a) => a.kind === "gpu");
  const tpu = accel.find((a) => a.kind === "tpu");
  const accelNodes = capacity.data?.nodes.acceleratorNodes;
  const kpct = (a?: { allocatable: number; allocated: number }) =>
    a && a.allocatable > 0 ? Math.round((a.allocated / a.allocatable) * 100) : 0;

  const refresh = () => {
    clusters.refetch();
    capacity.refetch();
    rollouts.refetch();
  };

  return (
    <section className="panel active" id="panel-fleet">
      <div className="page-head">
        <div>
          <h1>Fleet overview</h1>
          <p>
            Single pane across every connected GPU and TPU cluster. The control plane reconciles
            each InferenceService and fans placement out to workload clusters.
          </p>
        </div>
        <div className="actions">
          <button className="btn ghost" onClick={refresh}>
            <Refresh />
            Refresh
          </button>
          <button className="btn primary" onClick={() => goto("deploy")}>
            <Plus />
            New deployment
          </button>
        </div>
      </div>

      <div className="stat-grid">
        <div className="stat">
          <div className="label">InferenceServices</div>
          <div className="val">{n(isvcTotal)}</div>
          <div className="meta">across {clusterItems.length} clusters</div>
          <div className="ico-bg">
            <Lines />
          </div>
        </div>
        <div className="stat">
          <div className="label">Clusters connected</div>
          <div className="val">
            {ready} <small>/ {clusterItems.length}</small>
          </div>
          <div className="meta">
            <span className={`trend ${ready === clusterItems.length ? "flat" : "up"}`}>●</span>{" "}
            {ready === clusterItems.length ? "all synced" : "syncing…"}
          </div>
        </div>
        <div className="stat">
          <div className="label">Accelerator nodes</div>
          <div className="val">{accelNodes != null ? n(accelNodes) : "—"}</div>
          <div className="meta">
            {capacity.data ? `${n(capacity.data.nodes.total)} nodes total` : "loading…"}
          </div>
        </div>
        <div className="stat">
          <div className="label">GPUs in use</div>
          <div className="val">
            {gpu ? n(gpu.allocated) : "—"}
            <small> / {gpu ? n(gpu.allocatable) : "—"}</small>
          </div>
          <div className="meta">
            {gpu ? (
              <>
                <Bar pct={kpct(gpu)} width={120} />
                <span className="mono" style={{ fontSize: 11 }}>
                  {kpct(gpu)}%
                </span>
              </>
            ) : (
              "no GPU clusters"
            )}
          </div>
        </div>
        <div className="stat">
          <div className="label">TPUs in use</div>
          <div className="val">
            {tpu ? n(tpu.allocated) : "—"}
            <small> / {tpu ? n(tpu.allocatable) : "—"}</small>
          </div>
          <div className="meta">
            {tpu ? (
              <>
                <Bar pct={kpct(tpu)} width={120} />
                <span className="mono" style={{ fontSize: 11 }}>
                  {kpct(tpu)}%
                </span>
              </>
            ) : (
              "no TPU clusters"
            )}
          </div>
        </div>
        <div className="stat">
          <div className="label">Active rollouts</div>
          <div className="val" style={{ color: "var(--accent-2)" }}>
            {rolloutCards.length}
          </div>
          <div className="meta">InferenceServices mid-rollout</div>
        </div>
      </div>

      <div className="two-col">
        <div className="card">
          <div className="card-head">
            <h3>Workload clusters</h3>
            <span className="sub">capacity read live from each cluster's informer cache</span>
            <div className="right">
              <span className="tag">{clusterItems.length} registered</span>
            </div>
          </div>
          <div className="tbl-wrap">
            <table className="tbl">
              <thead>
                <tr>
                  <th>Cluster</th>
                  <th>Cloud</th>
                  <th>Region</th>
                  <th className="right">Nodes</th>
                  <th>Accelerator</th>
                  <th>Allocatable used</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((c) => (
                  <tr key={c.name}>
                    <td className="cell-name mono">{c.name}</td>
                    <td>
                      <div className="cloud-cell">
                        <CloudDot cloud={c.cloud} />
                        {CLOUD_LABEL[c.cloud]}
                      </div>
                    </td>
                    <td className="muted">{c.region}</td>
                    <td className="num">{c.nodes}</td>
                    <td>
                      <span className="tag">{c.accel}</span>
                    </td>
                    <td>
                      <UtilCell pct={c.utilPct} tone={c.barTone} />
                    </td>
                    <td>
                      <Pill tone={c.status.tone} label={c.status.label} />
                    </td>
                  </tr>
                ))}
                {rows.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="muted empty-cell">
                      {clusters.error ?? (clusters.loading ? "Loading clusters…" : "No clusters")}
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <h3>Recent activity</h3>
            <div className="right">
              <span className="live">Live</span>
            </div>
          </div>
          <div className="feed">
            {feed.map((f, i) => (
              <div className="feed-item" key={i}>
                <div className={`feed-ic ${f.tone}`}>{feedIcon(f.icon)}</div>
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
              <div className="muted" style={{ padding: 16, fontSize: "var(--t-sm)" }}>
                {events.error ?? (events.loading ? "Loading activity…" : "No recent activity")}
              </div>
            ) : null}
          </div>
        </div>
      </div>

      <div className="card section-gap">
        <div className="card-head">
          <h3>Active rollouts</h3>
          <span className="sub">InferenceServices not yet fully rolled out</span>
        </div>
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th>InferenceService</th>
                <th>Strategy</th>
                <th>Progress</th>
                <th>Gate</th>
                <th>Cluster</th>
                <th className="right">Age</th>
              </tr>
            </thead>
            <tbody>
              {rolloutCards.map((r) => (
                <tr key={`${r.cluster}/${r.name}`}>
                  <td className="cell-name mono">{r.name}</td>
                  <td>
                    <Pill tone={r.strategy.tone} label={r.strategy.label} />
                  </td>
                  <td style={{ width: 230 }}>
                    <div className="canary-track">
                      <Bar pct={r.barPct ?? 0} tone={r.barTone} striped={r.barStriped} />
                      <span className="muted" style={{ fontSize: 11 }}>
                        {r.note}
                      </span>
                    </div>
                  </td>
                  <td>
                    <Pill tone={r.gate.tone} label={r.gate.label} />
                  </td>
                  <td className="mono muted">{r.cluster}</td>
                  <td className="right muted nowrap">{r.started}</td>
                </tr>
              ))}
              {rolloutCards.length === 0 ? (
                <tr>
                  <td colSpan={6} className="muted empty-cell">
                    {rollouts.loading ? "Loading…" : "No rollouts in progress"}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
