import type { PanelKey } from "../App";
import { api } from "../api";
import { Bar, CloudDot, EmptyRow, Pill, UtilCell } from "../components";
import { ArrowRight, Check, IconModels, Lines, Plus, Refresh, Upload, Warning } from "../icons";
import { useEventFeed, useLive } from "../live";
import { toClusterRows, toFeed, toRollouts } from "../map";
import { CLOUD_LABEL, type FeedItem } from "../types";
import { emptyText, utilPct } from "../util";

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
            One view across every connected GPU and TPU cluster — what's deployed, and how much
            accelerator capacity is in use.
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
          <div className="label">Deployments</div>
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
                <Bar pct={utilPct(gpu)} width={120} />
                <span className="mono" style={{ fontSize: 11 }}>
                  {utilPct(gpu)}%
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
                <Bar pct={utilPct(tpu)} width={120} />
                <span className="mono" style={{ fontSize: 11 }}>
                  {utilPct(tpu)}%
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
          <div className="meta">Deployments mid-rollout</div>
        </div>
      </div>

      <div className="two-col">
        <div className="card">
          <div className="card-head">
            <h3>Clusters</h3>
            <span className="sub">live accelerator capacity per cluster</span>
            <div className="right">
              <span className="tag">{clusterItems.length} connected</span>
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
                  <th>Utilization</th>
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
                  <EmptyRow colSpan={7}>
                    {emptyText(clusters, "Loading clusters…", "No clusters")}
                  </EmptyRow>
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
                {emptyText(events, "Loading activity…", "No recent activity")}
              </div>
            ) : null}
          </div>
        </div>
      </div>

      <div className="card section-gap">
        <div className="card-head">
          <h3>Active rollouts</h3>
          <span className="sub">Deployments still rolling out</span>
        </div>
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th>Deployment</th>
                <th>Progress</th>
                <th>Cluster</th>
                <th className="right">Age</th>
              </tr>
            </thead>
            <tbody>
              {rolloutCards.map((r) => (
                <tr key={`${r.cluster}/${r.name}`}>
                  <td className="cell-name mono">{r.name}</td>
                  <td style={{ width: 260 }}>
                    <div className="canary-track">
                      <Bar pct={r.barPct ?? 0} tone={r.barTone} striped={r.barStriped} />
                      <span className="muted" style={{ fontSize: 11 }}>
                        {r.note}
                      </span>
                    </div>
                  </td>
                  <td className="mono muted">{r.cluster}</td>
                  <td className="right muted nowrap">{r.started}</td>
                </tr>
              ))}
              {rolloutCards.length === 0 ? (
                <EmptyRow colSpan={4}>
                  {emptyText(rollouts, "Loading…", "No rollouts in progress")}
                </EmptyRow>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
