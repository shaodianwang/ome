import type { DepFilter } from "../App";
import { api } from "../api";
import { useEventFeed, useLive } from "../live";
import { deploymentHealth, toClusterRows, toFeed, toRollouts } from "../map";
import { emptyText, utilTone } from "../util";
import { Distribution, Dot, Gauge, UtilRow } from "../viz";

const n = (v: number) => v.toLocaleString();
const kpct = (a?: { allocatable: number; allocated: number }) =>
  a && a.allocatable > 0 ? Math.round((a.allocated / a.allocatable) * 100) : 0;

export function Fleet({ openDeployments }: { openDeployments: (f: DepFilter) => void }) {
  const clusters = useLive(api.clusters, {
    kinds: ["InferenceService"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const capacity = useLive(api.capacity, { kinds: [], intervalMs: 30_000 });
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

  const clusterItems = clusters.data?.items ?? [];
  const rows = toClusterRows(clusterItems, capacity.data).sort((a, b) => b.utilPct - a.utilPct);
  const health = deploymentHealth(isvcs.data?.items ?? []);
  const feed = toFeed(events.events);
  const rolloutCards = toRollouts(rollouts.data?.items ?? []);
  const readyClusters = clusterItems.filter((c) => c.ready).length;

  const accel = capacity.data?.accelerators ?? [];
  const gpu = accel.find((a) => a.kind === "gpu");
  const tpu = accel.find((a) => a.kind === "tpu");

  // Largest single fabric block free per kind — the biggest tightly-coupled job
  // you could place right now (a job must fit within one block).
  const maxBlock = new Map<string, number>();
  for (const g of capacity.data?.byIsland ?? []) {
    maxBlock.set(g.kind, Math.max(maxBlock.get(g.kind) ?? 0, g.free));
  }

  return (
    <section className="panel active">
      <div className="page-head">
        <div>
          <h1>Fleet</h1>
        </div>
      </div>

      <div className={`fleet-status ${health.notReady ? "bad" : "ok"}`}>
        <Dot tone={health.notReady ? "red" : "green"} />
        {health.notReady ? (
          <button className="statusbtn" onClick={() => openDeployments("attention")}>
            {n(health.notReady)} need attention
          </button>
        ) : (
          <b>All healthy</b>
        )}
        <span className="sep">·</span>
        {n(health.total)} deployments
        <span className="sep">·</span>
        {readyClusters}/{clusterItems.length} clusters ready
        <div className="fleet-status-bar">
          <Distribution
            segments={[
              { value: health.ready, tone: "green" },
              { value: health.notReady, tone: "red" },
              { value: health.unknown, tone: "slate" },
            ]}
          />
        </div>
      </div>

      {health.attention.length ? (
        <div className="card attn-card">
          <div className="card-head">
            <h3>Needs attention · {n(health.attention.length)}</h3>
            <span className="right">
              <button className="linkbtn" onClick={() => openDeployments("attention")}>
                see all →
              </button>
            </span>
          </div>
          <div className="card-body">
            <ul className="attn-list wide">
              {health.attention.slice(0, 10).map((a, i) => (
                <li key={i} onClick={() => openDeployments("attention")}>
                  <Dot tone="red" />
                  <span className="mono">{a.name}</span>
                  <span className="muted">{a.cluster}</span>
                  {a.reason ? <span className="attn-reason">{a.reason}</span> : null}
                </li>
              ))}
            </ul>
          </div>
        </div>
      ) : null}

      <div className="tri-col">
        <div className="card">
          <div className="card-head">
            <h3>Capacity</h3>
            <span className="sub">accelerator use</span>
          </div>
          <div className="card-body">
            <div className="cap-line">
              <span className="cap-k">GPU</span>
              <Gauge pct={kpct(gpu)} tone={utilTone(kpct(gpu))} />
              <span className="cap-free">{gpu ? `${n(gpu.free)} free` : "—"}</span>
            </div>
            <div className="cap-line">
              <span className="cap-k">TPU</span>
              <Gauge pct={kpct(tpu)} tone={utilTone(kpct(tpu))} />
              <span className="cap-free">{tpu ? `${n(tpu.free)} free` : "—"}</span>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <h3>Clusters</h3>
            <span className="sub">load · free</span>
          </div>
          <div className="card-body">
            <div className="urows">
              {rows.map((c) => (
                <UtilRow
                  key={c.name}
                  name={c.name}
                  tag={c.accel === "—" ? "" : c.kind}
                  pct={c.utilPct}
                  tone={c.barTone}
                  value={c.accel === "—" ? "—" : `${n(c.free)} free`}
                />
              ))}
              {rows.length === 0 ? (
                <div className="muted urow-empty">
                  {emptyText(clusters, "Loading clusters…", "No clusters")}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <h3>Largest placeable job</h3>
            <span className="sub">in one fabric block</span>
          </div>
          <div className="card-body">
            <div className="place-vals">
              <div className="place-val">
                <span className="place-num tnum">{n(maxBlock.get("gpu") ?? 0)}</span>
                <span className="place-unit">GPU</span>
              </div>
              <div className="place-val">
                <span className="place-num tnum">{n(maxBlock.get("tpu") ?? 0)}</span>
                <span className="place-unit">TPU</span>
              </div>
            </div>
            <p className="panel-note">
              The biggest tightly-coupled job schedulable now — capped by the largest single
              network-fabric block. See <b>Capacity</b> for where.
            </p>
          </div>
        </div>
      </div>

      <div className="two-col">
        <div className="card">
          <div className="card-head">
            <h3>Active rollouts</h3>
          </div>
          <div className="card-body">
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
          </div>
        </div>

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
