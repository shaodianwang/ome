import { useEffect, useState } from "react";

import { api } from "../api";
import type { CapacityResp, NodeItem } from "../api";
import { Bar, DataNotice, EmptyRow, Pager, SearchInput } from "../components";
import { loadClusterCapacity } from "../dataState";
import type { ClusterCapacityState } from "../dataState";
import { useLive } from "../live";
import { isvcSelected } from "../resource";
import type { Selected } from "../resource";
import { upperThresholdTone } from "../policy";
import { ago, matches, num as n, utilPct } from "../util";
import { Card, Gauge, StatTile } from "../viz";
import { ClusterNodes, ClusterOverview } from "./CapacityCluster";
import { PlacementCard, PlacementTiers } from "./PlacementCapacity";

type Dim = "cluster" | "island" | "nvlink" | "instance";
type Kind = "all" | "gpu" | "tpu";

const DIMS: { key: Dim; label: string; col: string; help: string }[] = [
  { key: "cluster", label: "Cluster", col: "Cluster", help: "Totalled per cluster." },
  {
    key: "island",
    label: "Island",
    col: "Island",
    help: "Network-fabric block (RDMA island / physical block). A tightly-coupled job must fit within one.",
  },
  {
    key: "nvlink",
    label: "NVLink / ICI",
    col: "Domain",
    help: "Inner topology domain within an island (an NVLink domain for GPUs, an ICI sub-block for TPUs).",
  },
  { key: "instance", label: "Instance type", col: "Instance type", help: "Grouped by node SKU." },
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

type SortKey = keyof Row;

const kpct = (t: { alloc: number; used: number }) =>
  utilPct({ allocatable: t.alloc, allocated: t.used });
const unavailableCluster = (cluster: ClusterCapacityState): string => {
  const status =
    cluster.status === "timed_out"
      ? "timed out"
      : cluster.status === "ready"
        ? "unavailable"
        : cluster.status;
  const detail = cluster.cacheError ?? cluster.capacityError;
  return `${cluster.name} (${status}${detail ? `: ${detail}` : ""})`;
};
const sortVal = (r: Row, k: SortKey): string | number =>
  k === "cluster" || k === "name" || k === "kind" ? String(r[k]).toLowerCase() : r[k];

export function Capacity({
  openDeployment,
  initialCluster,
}: {
  openDeployment: (s: Selected) => void;
  /** Deep-linked cluster tab (from the route); "" / undefined = fleet view. */
  initialCluster?: string;
}) {
  const [kind, setKind] = useState<Kind>("all");
  const [dim, setDim] = useState<Dim>("cluster");
  const [q, setQ] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("free");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(25);
  const [tab, setTab] = useState(initialCluster ?? ""); // "" = all clusters; else a cluster name
  const [nodes, setNodes] = useState<NodeItem[]>([]);
  const [nodesLoading, setNodesLoading] = useState(false);
  const [nodesError, setNodesError] = useState<string | null>(null);
  const [nodeReload, setNodeReload] = useState(0);
  const config = useLive(api.config, { kinds: [] });
  const utilizationPolicy = config.data?.dashboardPolicy?.utilization;

  // One scoped-capacity request per cluster, combined client-side so island /
  // domain rows stay attributed to their cluster and kind.
  const live = useLive(() => loadClusterCapacity(api.clusters, api.capacityForCluster), {
    kinds: ["Capacity", "Cluster"],
    intervalMs: 30_000,
  });
  const allCaps = live.data ?? [];
  // A per-cluster tab scopes the whole page to one cluster; "" is the fleet view.
  const scopedCaps = tab ? allCaps.filter((c) => c.name === tab) : allCaps;
  const caps = scopedCaps.filter(
    (cluster): cluster is ClusterCapacityState & { cap: CapacityResp } => cluster.cap != null,
  );
  const unavailable = scopedCaps.filter(
    (cluster) =>
      cluster.status === "failed" ||
      cluster.status === "timed_out" ||
      (cluster.status === "ready" && cluster.capacityError),
  );
  const active = tab ? allCaps.find((c) => c.name === tab) : undefined;
  // A cluster still warming its informer caches reports partial data (pods sync
  // after nodes → allocated looks like 0, everything looks free). Flag it so the
  // numbers aren't mistaken for steady state.
  const syncing = scopedCaps.filter((c) => c.status === "syncing").map((c) => c.name);

  useEffect(() => {
    setPage(0);
  }, [kind, dim, q, pageSize, tab]);

  // Lazily load the selected cluster's nodes (node explorer + health breakdown).
  // A fresh capacity snapshot follows every Node/Pod change, so use its identity
  // as the refresh revision instead of opening another stream or polling here.
  useEffect(() => {
    if (!tab) {
      setNodes([]);
      setNodesError(null);
      return;
    }
    let cancelled = false;
    setNodes([]);
    setNodesError(null);
    setNodesLoading(true);
    api
      .nodesForCluster(tab)
      .then((r) => {
        if (!cancelled) setNodes(r.items);
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setNodes([]);
          setNodesError(error instanceof Error ? error.message : String(error));
        }
      })
      .finally(() => {
        if (!cancelled) setNodesLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [tab, nodeReload, live.data]);

  // Fleet totals per accelerator kind.
  const byKind = new Map<string, { alloc: number; used: number; free: number }>();
  for (const { cap } of caps) {
    for (const a of cap.accelerators) {
      const e = byKind.get(a.kind) ?? { alloc: 0, used: 0, free: 0 };
      e.alloc += a.allocatable;
      e.used += a.allocated;
      e.free += a.free;
      byKind.set(a.kind, e);
    }
  }
  const gpu = byKind.get("gpu");
  const tpu = byKind.get("tpu");
  const gpuPct = gpu ? kpct(gpu) : null;
  const tpuPct = tpu ? kpct(tpu) : null;
  const hasCapacity = caps.length > 0;
  const accelNodes = caps.reduce((s, { cap }) => s + cap.nodes.acceleratorNodes, 0);
  const schedNodes = caps.reduce((s, { cap }) => s + cap.nodes.schedulable, 0);
  const totalNodes = caps.reduce((s, { cap }) => s + cap.nodes.total, 0);
  const readyNodes = caps.reduce((s, { cap }) => s + cap.nodes.ready, 0);
  const kindsPresent = [...byKind.keys()].sort();

  // Per-cluster drill-downs (only when a single cluster tab is active).
  const consumers = tab ? (caps[0]?.cap.consumers ?? []) : [];
  // Deployment-attributed consumption works fleet-wide: every per-cluster
  // response carries its own top list; flatten and re-rank across the scope.
  const deploymentConsumers = caps
    .flatMap(({ cap }) => cap.consumersByDeployment ?? [])
    .sort((a, b) => b.accelerators - a.accelerators)
    .slice(0, 10);

  // Demand (Pending pods waiting on capacity) + lost capacity (accelerators on
  // NotReady/cordoned nodes), summed across clusters.
  let pendingAccel = 0;
  let pendingPods = 0;
  let unavailAccel = 0;
  let unavailNodes = 0;
  for (const { cap } of caps) {
    for (const p of cap.pending ?? []) {
      pendingAccel += p.accelerators;
      pendingPods += p.pods;
    }
    for (const u of cap.unavailable ?? []) {
      unavailAccel += u.accelerators;
      unavailNodes += u.nodes;
    }
  }

  // Breakdown rows for the current dimension + kind.
  const groupsFor = (cap: CapacityResp) =>
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
      if (kind !== "all" && g.kind !== kind) continue;
      rows.push({
        cluster,
        name: dim === "cluster" ? "" : g.name,
        kind: g.kind,
        nodes: g.nodes,
        total: g.allocatable,
        used: g.allocated,
        free: g.free,
        util: utilPct(g),
      });
    }
  }
  const filtered = rows.filter((r) => matches(q, r.cluster, r.name, r.kind));
  const dir = sortDir === "asc" ? 1 : -1;
  const sorted = [...filtered].sort((a, b) => {
    const av = sortVal(a, sortKey);
    const bv = sortVal(b, sortKey);
    return av < bv ? -dir : av > bv ? dir : 0;
  });
  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const clamped = Math.min(page, totalPages - 1);
  const startIdx = clamped * pageSize;
  const pageRows = sorted.slice(startIdx, startIdx + pageSize);

  const showName = dim !== "cluster";
  const meta = DIMS.find((d) => d.key === dim)!;

  const toggleSort = (k: SortKey) => {
    if (k === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(k);
      setSortDir(k === "cluster" || k === "name" || k === "kind" ? "asc" : "desc");
    }
  };
  const th = (k: SortKey, label: string, right = false) => (
    <th
      className={`sortable${right ? " right" : ""}`}
      aria-sort={sortKey === k ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
    >
      <button type="button" onClick={() => toggleSort(k)}>
        {label}
        <span className="sort-arrow" aria-hidden="true">
          {sortKey === k ? (sortDir === "asc" ? "▲" : "▼") : ""}
        </span>
      </button>
    </th>
  );

  return (
    <section className="panel active">
      <div className="page-head">
        <div>
          <h1>Capacity</h1>
          <p>Accelerator allocation and placement capacity across clusters.</p>
        </div>
      </div>

      <div className="dtabs" role="group" aria-label="Capacity scope">
        <button
          className={tab === "" ? "dtab active" : "dtab"}
          aria-pressed={tab === ""}
          onClick={() => setTab("")}
        >
          All clusters
        </button>
        {allCaps.map((c) => (
          <button
            key={c.name}
            className={tab === c.name ? "dtab active" : "dtab"}
            aria-pressed={tab === c.name}
            onClick={() => setTab(c.name)}
          >
            {c.name}
            {c.status === "failed"
              ? " · failed"
              : c.status === "timed_out"
                ? " · timed out"
                : c.status === "syncing"
                  ? " · syncing"
                  : c.capacityError
                    ? " · unavailable"
                    : ""}
          </button>
        ))}
      </div>

      {live.error ? (
        <DataNotice
          tone="danger"
          title={live.data ? "Capacity refresh failed" : "Capacity unavailable"}
          detail={
            live.data && live.lastSuccessAt
              ? `Showing the last successful snapshot from ${ago(live.lastSuccessAt)}. ${live.error}`
              : live.error
          }
          action={{ label: "Retry", onClick: live.refetch }}
        />
      ) : null}
      {config.error ? (
        <DataNotice
          tone="warning"
          title="Capacity policy unavailable"
          detail={config.error}
          action={{ label: "Retry", onClick: config.refetch }}
        />
      ) : null}
      {unavailable.length ? (
        <DataNotice
          tone="warning"
          title="Capacity is partial"
          detail={`${unavailable.map(unavailableCluster).join(", ")}; fleet totals exclude ${unavailable.length === 1 ? "that cluster" : "those clusters"}.`}
          action={{ label: "Retry", onClick: live.refetch }}
        />
      ) : null}

      {active ? (
        <div className="statstrip">
          <div className="s2">
            <span className="lbl">Cluster</span>
            <span className="val mono">{active.name}</span>
          </div>
          {active.region ? (
            <div className="s2">
              <span className="lbl">Region</span>
              <span className="val">{active.region}</span>
            </div>
          ) : null}
          {active.version ? (
            <div className="s2">
              <span className="lbl">Kubernetes</span>
              <span className="val">{active.version}</span>
            </div>
          ) : null}
          <div className="s2">
            <span className="lbl">Nodes</span>
            <span className="val">
              {active.cap
                ? `${n(totalNodes)} · ${n(readyNodes)} ready · ${n(schedNodes)} sched`
                : "Unavailable"}
            </span>
          </div>
          <div className="s2">
            <span className="lbl">Accelerator nodes</span>
            <span className="val">{active.cap ? n(accelNodes) : "Unavailable"}</span>
          </div>
        </div>
      ) : null}

      <div className="tiles cap-tiles">
        <StatTile
          label="GPU allocated"
          value={gpuPct == null ? "—" : `${gpuPct}%`}
          sub={
            gpu
              ? `${n(gpu.used)} / ${n(gpu.alloc)} · ${n(gpu.free)} free`
              : hasCapacity
                ? "no GPU capacity reported"
                : "capacity unavailable"
          }
        >
          {gpuPct == null ? null : (
            <Gauge pct={gpuPct} tone={upperThresholdTone(gpuPct, utilizationPolicy)} />
          )}
        </StatTile>
        <StatTile
          label="TPU allocated"
          value={tpuPct == null ? "—" : `${tpuPct}%`}
          sub={
            tpu
              ? `${n(tpu.used)} / ${n(tpu.alloc)} · ${n(tpu.free)} free`
              : hasCapacity
                ? "no TPU capacity reported"
                : "capacity unavailable"
          }
        >
          {tpuPct == null ? null : (
            <Gauge pct={tpuPct} tone={upperThresholdTone(tpuPct, utilizationPolicy)} />
          )}
        </StatTile>
        <StatTile
          label="Pending"
          value={hasCapacity ? (pendingAccel ? n(pendingAccel) : "0") : "—"}
          sub={
            hasCapacity
              ? pendingAccel
                ? `${n(pendingPods)} pods waiting for capacity`
                : "none reported in available clusters"
              : "capacity unavailable"
          }
          tone={hasCapacity ? (pendingAccel > 0 ? "red" : "green") : undefined}
        />
        <StatTile
          label="Unavailable"
          value={hasCapacity ? (unavailAccel ? n(unavailAccel) : "0") : "—"}
          sub={
            hasCapacity
              ? unavailAccel
                ? `${n(unavailNodes)} nodes NotReady / cordoned`
                : "none reported in available clusters"
              : "capacity unavailable"
          }
          tone={hasCapacity ? (unavailAccel > 0 ? "amber" : "green") : undefined}
        />
      </div>
      {hasCapacity ? (
        <p className="cap-help">
          {n(accelNodes)} accelerator nodes across {caps.length} cluster
          {caps.length === 1 ? "" : "s"} · {n(schedNodes)} schedulable of {n(totalNodes)}.
          Percentages are <b>allocated</b> (scheduled), not live GPU utilization.
        </p>
      ) : live.loading ? (
        <p className="cap-help">Loading capacity…</p>
      ) : null}
      {syncing.length ? (
        <p className="cap-syncing">
          ⏳ {syncing.length} cluster{syncing.length > 1 ? "s" : ""} still syncing (
          {syncing.join(", ")}) — capacity is partial until caches warm.
        </p>
      ) : null}

      {active ? (
        <ClusterOverview
          active={active}
          consumers={consumers}
          nodes={nodes}
          nodesLoading={nodesLoading}
          nodesError={nodesError}
          onRetryNodes={() => setNodeReload((value) => value + 1)}
        />
      ) : null}

      <Card title="Top deployments" sub="accelerators held · click to open">
        {deploymentConsumers.length ? (
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
              {deploymentConsumers.map((c) => (
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
          <p className="panel-note">
            {hasCapacity
              ? "No deployment-attributed accelerator consumption in scope."
              : "Waiting for capacity data."}
          </p>
        )}
      </Card>

      <PlacementTiers caps={caps} />

      <ClusterNodes
        show={!!active}
        tab={tab}
        nodes={nodes}
        nodesLoading={nodesLoading}
        nodesError={nodesError}
        pageSize={pageSize}
        onPageSize={setPageSize}
        openDeployment={openDeployment}
      />

      <div className="filterbar">
        <div className="seg" role="group" aria-label="Accelerator type">
          <button
            className={kind === "all" ? "active" : undefined}
            aria-pressed={kind === "all"}
            onClick={() => setKind("all")}
          >
            All
          </button>
          {kindsPresent.map((k) => (
            <button
              key={k}
              className={kind === k ? "active" : undefined}
              aria-pressed={kind === k}
              onClick={() => setKind(k as Kind)}
            >
              {k.toUpperCase()}
            </button>
          ))}
        </div>
        <div className="seg" role="group" aria-label="Capacity grouping">
          {DIMS.map((d) => (
            <button
              key={d.key}
              className={d.key === dim ? "active" : undefined}
              aria-pressed={d.key === dim}
              onClick={() => setDim(d.key)}
            >
              {d.label}
            </button>
          ))}
        </div>
        <SearchInput
          ariaLabel="Search capacity"
          placeholder="Filter by cluster, name, or kind…"
          value={q}
          onChange={setQ}
        />
      </div>
      <p className="cap-help">{meta.help} Sorted by free capacity, most first.</p>

      <div className="card">
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                {th("cluster", "Cluster")}
                {showName ? th("name", meta.col) : null}
                {th("kind", "Type")}
                {th("nodes", "Nodes", true)}
                {th("total", "Total", true)}
                {th("used", "Used", true)}
                {th("free", "Free", true)}
                {th("util", "Utilization")}
              </tr>
            </thead>
            <tbody>
              {pageRows.map((r, i) => (
                <tr
                  key={`${r.cluster}/${r.name}/${r.kind}/${i}`}
                  className="clickable"
                  onClick={() => {
                    setTab(r.cluster);
                    window.scrollTo({ top: 0, behavior: "smooth" });
                  }}
                >
                  <td className="cell-name mono">
                    <button
                      type="button"
                      className="row-open"
                      title={`Open ${r.cluster}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        setTab(r.cluster);
                        window.scrollTo({ top: 0, behavior: "smooth" });
                      }}
                    >
                      {r.cluster}
                    </button>
                  </td>
                  {showName ? <td className="mono">{r.name || "—"}</td> : null}
                  <td>
                    <span className="tag">{r.kind.toUpperCase()}</span>
                  </td>
                  <td className="num">{n(r.nodes)}</td>
                  <td className="num">{n(r.total)}</td>
                  <td className="num muted">{n(r.used)}</td>
                  <td className="num">
                    <span className={r.free > 0 ? "free-strong txt-ok" : "free-strong muted"}>
                      {n(r.free)}
                    </span>
                  </td>
                  <td>
                    <div className="bar-cell">
                      <Bar
                        pct={r.util}
                        tone={upperThresholdTone(r.util, utilizationPolicy)}
                        width={90}
                      />
                      <span className="mono">{r.util}%</span>
                    </div>
                  </td>
                </tr>
              ))}
              {sorted.length === 0 ? (
                <EmptyRow colSpan={showName ? 8 : 7}>
                  {q.trim()
                    ? `No rows match "${q}"`
                    : live.loading
                      ? "Loading capacity…"
                      : live.error || (unavailable.length > 0 && !hasCapacity)
                        ? "Capacity unavailable"
                        : "No capacity data"}
                </EmptyRow>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <Pager
        total={sorted.length}
        page={clamped}
        totalPages={totalPages}
        pageSize={pageSize}
        start={startIdx}
        noun={showName ? "rows" : "cluster/kind rows"}
        onPage={setPage}
        onPageSize={setPageSize}
      />

      <PlacementCard caps={caps} kindsPresent={kindsPresent} />
    </section>
  );
}
