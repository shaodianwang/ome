import { useState } from "react";

import { api } from "../api";
import type { AccelGroup, CapacityResp } from "../api";
import { Bar } from "../components";
import { useLive } from "../live";
import { matches } from "../util";

type Dim = "cluster" | "island" | "nvlink" | "instance";

const DIMS: { key: Dim; label: string; col: string; help: string }[] = [
  {
    key: "cluster",
    label: "Cluster",
    col: "Cluster",
    help: "Accelerator capacity totalled per cluster and accelerator kind.",
  },
  {
    key: "island",
    label: "Island / block",
    col: "Island",
    help: "Outer topology domain — RDMA island (network fabric) for GPUs, physical block for TPUs. A job needing tight interconnect must fit within one.",
  },
  {
    key: "nvlink",
    label: "NVLink / ICI",
    col: "Domain",
    help: "Inner topology domain within an island — an NVLink domain for GPUs (e.g. an NVL72 rack), an ICI sub-block for TPUs.",
  },
  {
    key: "instance",
    label: "Instance type",
    col: "Instance type",
    help: "Capacity grouped by node hardware SKU.",
  },
];

interface Row {
  cluster: string;
  name: string;
  kind: string;
  nodes: number;
  total: number;
  used: number;
  free: number;
  util: number;
}

const n = (v: number) => v.toLocaleString();
const pct = (g: { allocatable: number; allocated: number }) =>
  g.allocatable > 0 ? Math.round((g.allocated / g.allocatable) * 100) : 0;
const utilTone = (p: number): "green" | "amber" | "red" =>
  p >= 90 ? "red" : p >= 75 ? "amber" : "green";

export function Capacity() {
  const [dim, setDim] = useState<Dim>("cluster");
  const [q, setQ] = useState("");

  // One small request per cluster (scoped capacity), combined client-side, so
  // island/domain rows stay correctly attributed to their cluster and kind.
  const live = useLive(
    async () => {
      const cl = await api.clusters();
      return Promise.all(
        cl.items.map((c) => api.capacityForCluster(c.name).then((cap) => ({ name: c.name, cap }))),
      );
    },
    { kinds: [], intervalMs: 30_000 },
  );

  const caps: { name: string; cap: CapacityResp }[] = live.data ?? [];

  // Fleet totals per accelerator kind (GPUs and TPUs never share a number).
  const byKind = new Map<string, { alloc: number; used: number; free: number; nodes: number }>();
  for (const { cap } of caps) {
    for (const a of cap.accelerators) {
      const e = byKind.get(a.kind) ?? { alloc: 0, used: 0, free: 0, nodes: 0 };
      e.alloc += a.allocatable;
      e.used += a.allocated;
      e.free += a.free;
      e.nodes += a.nodes;
      byKind.set(a.kind, e);
    }
  }
  const accelNodes = caps.reduce((s, { cap }) => s + cap.nodes.acceleratorNodes, 0);

  const meta = DIMS.find((d) => d.key === dim)!;
  const groupsFor = (cap: CapacityResp): AccelGroup[] =>
    dim === "cluster"
      ? cap.accelerators
      : dim === "island"
        ? cap.byIsland
        : dim === "nvlink"
          ? cap.byNvlinkDomain
          : cap.byInstanceType;

  const rows: Row[] = [];
  for (const { name: cluster, cap } of caps) {
    for (const g of groupsFor(cap)) {
      rows.push({
        cluster,
        name: dim === "cluster" ? "" : g.name,
        kind: g.kind,
        nodes: g.nodes,
        total: g.allocatable,
        used: g.allocated,
        free: g.free,
        util: pct(g),
      });
    }
  }
  const filtered = rows
    .filter((r) => matches(q, r.cluster, r.name, r.kind))
    .sort((a, b) => b.free - a.free);

  const showName = dim !== "cluster";

  return (
    <section className="panel active" id="panel-capacity">
      <div className="page-head">
        <div>
          <h1>Capacity</h1>
          <p>
            Live accelerator inventory across clusters, ranked by free capacity. GPUs and TPUs are
            counted separately.
          </p>
        </div>
      </div>

      <div className="stat-grid" style={{ gridTemplateColumns: `repeat(${2 + byKind.size},1fr)` }}>
        <div className="stat">
          <div className="label">Accelerator nodes</div>
          <div className="val">{n(accelNodes)}</div>
          <div className="meta">across {caps.length} clusters</div>
        </div>
        {[...byKind.entries()].map(([kind, t]) => (
          <div className="stat" key={kind}>
            <div className="label">Free {kind.toUpperCase()}</div>
            <div className="val" style={{ color: "var(--green)" }}>
              {n(t.free)}
            </div>
            <div className="meta">
              of {n(t.alloc)} · {t.alloc ? Math.round((t.used / t.alloc) * 100) : 0}% used
            </div>
          </div>
        ))}
        <div className="stat">
          <div className="label">Clusters</div>
          <div className="val">{caps.length || "—"}</div>
          <div className="meta">reporting capacity</div>
        </div>
      </div>

      <div className="cap-controls">
        <div className="seg">
          {DIMS.map((d) => (
            <button
              key={d.key}
              className={d.key === dim ? "active" : undefined}
              onClick={() => setDim(d.key)}
            >
              {d.label}
            </button>
          ))}
        </div>
        <div className="input grow">
          <input
            placeholder="Filter by cluster, name, or kind…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>
      <p className="cap-help">{meta.help} Sorted by free capacity, most available first.</p>

      <div className="card">
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th>Cluster</th>
                {showName ? <th>{meta.col}</th> : null}
                <th>Type</th>
                <th className="right">Nodes</th>
                <th className="right">Total</th>
                <th className="right">Used</th>
                <th className="right">Free</th>
                <th style={{ width: 190 }}>Utilization</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={`${r.cluster}/${r.name}/${r.kind}/${i}`}>
                  <td className="cell-name mono">{r.cluster}</td>
                  {showName ? <td className="mono">{r.name || "—"}</td> : null}
                  <td>
                    <span className="tag">{r.kind.toUpperCase()}</span>
                  </td>
                  <td className="num">{n(r.nodes)}</td>
                  <td className="num">{n(r.total)}</td>
                  <td className="num muted">{n(r.used)}</td>
                  <td className="num">
                    <span
                      className="free-strong"
                      style={{ color: r.free > 0 ? "var(--green)" : "var(--fg-3)" }}
                    >
                      {n(r.free)}
                    </span>
                  </td>
                  <td>
                    <div className="bar-cell">
                      <Bar pct={r.util} tone={utilTone(r.util)} width={120} />
                      <span className="mono">{r.util}%</span>
                    </div>
                  </td>
                </tr>
              ))}
              {filtered.length === 0 ? (
                <tr>
                  <td colSpan={showName ? 8 : 7} className="muted empty-cell">
                    {live.error ?? (live.loading ? "Loading capacity…" : "No capacity data")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className="list-foot">
        {filtered.length} {showName ? "rows" : "cluster/kind rows"}
        {q.trim() ? ` matching "${q}"` : ""}
      </div>
    </section>
  );
}
