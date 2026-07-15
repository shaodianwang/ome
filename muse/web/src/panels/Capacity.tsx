import { useEffect, useState } from "react";

import { api } from "../api";
import type { AccelGroup, CapacityResp, PlacementUnit } from "../api";
import { Bar, EmptyRow, Pager } from "../components";
import { useLive } from "../live";
import { matches, num as n, shortId, UNASSIGNED_GROUP, utilPct, utilTone } from "../util";
import { Card, Gauge, StatTile, UtilRow } from "../viz";

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

const kpct = (t?: { alloc: number; used: number }) =>
  t ? utilPct({ allocatable: t.alloc, allocated: t.used }) : 0;
const sortVal = (r: Row, k: SortKey): string | number =>
  k === "cluster" || k === "name" || k === "kind" ? String(r[k]).toLowerCase() : r[k];

type UnitAgg = { chips: number; total: number; available: number };

/** Sum a per-cluster PlacementUnit list into fleet totals per size label, biggest
 *  unit first. Shared by the TPU-slice, GPU-node, and GPU-domain sections. */
function aggUnits(
  caps: { cap: CapacityResp }[],
  pick: (c: CapacityResp) => PlacementUnit[] | undefined,
): [string, UnitAgg][] {
  const m = new Map<string, UnitAgg>();
  for (const { cap } of caps) {
    for (const u of pick(cap) ?? []) {
      // Ignore rows from a stale/mismatched backend (missing label/chips) so a
      // version skew degrades to an empty section instead of crashing the page.
      if (typeof u.label !== "string" || typeof u.chips !== "number") continue;
      const e = m.get(u.label) ?? { chips: u.chips, total: 0, available: 0 };
      e.chips = u.chips;
      e.total += u.total;
      e.available += u.available;
      m.set(u.label, e);
    }
  }
  return [...m.entries()].sort((a, b) => b[1].chips - a[1].chips);
}

/** A fragmentation card: per size, a bar of whole units free vs total. Renders
 *  nothing when there are no units (no TPUs, or no NVLink labels). */
function FragCard({
  title,
  sub,
  noun,
  units,
  note,
}: {
  title: string;
  sub: string;
  noun: string;
  units: [string, UnitAgg][];
  note: string;
}) {
  if (!units.length) return null;
  return (
    <Card title={title} sub={sub}>
      <div className="urows">
        {units.map(([label, u]) => (
          <UtilRow
            key={label}
            name={
              <>
                {label}{" "}
                <span className="muted">
                  · {n(u.chips)} {noun}
                </span>
              </>
            }
            pct={u.total > 0 ? Math.round((u.available / u.total) * 100) : 0}
            tone={u.available > 0 ? "green" : "amber"}
            value={`${n(u.available)} / ${n(u.total)} free`}
          />
        ))}
      </div>
      <p className="panel-note">{note}</p>
    </Card>
  );
}

export function Capacity() {
  const [kind, setKind] = useState<Kind>("all");
  const [dim, setDim] = useState<Dim>("cluster");
  const [q, setQ] = useState("");
  const [fit, setFit] = useState("");
  const [fitKind, setFitKind] = useState("gpu");
  const [sortKey, setSortKey] = useState<SortKey>("free");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(25);

  // One scoped-capacity request per cluster, combined client-side so island /
  // domain rows stay attributed to their cluster and kind.
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

  useEffect(() => {
    setPage(0);
  }, [kind, dim, q, pageSize]);

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
  const accelNodes = caps.reduce((s, { cap }) => s + cap.nodes.acceleratorNodes, 0);
  const schedNodes = caps.reduce((s, { cap }) => s + cap.nodes.schedulable, 0);
  const totalNodes = caps.reduce((s, { cap }) => s + cap.nodes.total, 0);
  const kindsPresent = [...byKind.keys()].sort();

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

  // Carve-able placement units — whole TPU slices, GPU nodes, and GPU NVLink
  // domains that are entirely free vs partially used, summed across clusters.
  const tpuSlices = aggUnits(caps, (c) => c.tpuSlices);
  const gpuNodes = aggUnits(caps, (c) => c.gpuNodes);
  const gpuDomains = aggUnits(caps, (c) => c.gpuDomains);

  // Placement: the largest free block per kind at each topology tier (rack-level
  // NVLink/ICI domain, and fabric-level island), with which cluster offers it and
  // roughly how many nodes' worth that is.
  interface Block {
    cluster: string;
    name: string;
    free: number;
    perNode: number;
  }
  const blocksAt = (pick: (c: CapacityResp) => AccelGroup[], k: string): Block[] =>
    caps.flatMap(({ name, cap }) =>
      pick(cap)
        .filter((g) => g.kind === k && g.name !== UNASSIGNED_GROUP)
        .map((g) => ({
          cluster: name,
          name: g.name,
          free: g.free,
          perNode: g.nodes > 0 ? g.allocatable / g.nodes : 0,
        })),
    );
  const largest = (bs: Block[]): Block | undefined =>
    bs.reduce<Block | undefined>((m, b) => (b.free > (m?.free ?? -1) ? b : m), undefined);
  const nodesWorth = (b?: Block) =>
    b && b.perNode > 0 ? Math.max(1, Math.round(b.free / b.perNode)) : 0;
  const tiers = kindsPresent.map((k) => ({
    kind: k,
    domain: largest(blocksAt((c) => c.byNvlinkDomain, k)),
    island: largest(blocksAt((c) => c.byIsland, k)),
  }));
  const topBlocks = caps
    .flatMap(({ name, cap }) =>
      cap.byIsland.map((g) => ({ cluster: name, name: g.name, kind: g.kind, free: g.free })),
    )
    .filter((b) => b.name !== UNASSIGNED_GROUP)
    .sort((a, b) => b.free - a.free)
    .slice(0, 6);
  const fitN = parseInt(fit, 10);
  const fitActive = !isNaN(fitN) && fitN > 0;
  const fitIslands = fitActive
    ? blocksAt((c) => c.byIsland, fitKind).filter((b) => b.free >= fitN)
    : [];
  const fitBest = largest(blocksAt((c) => c.byIsland, fitKind));

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
      onClick={() => toggleSort(k)}
      aria-sort={sortKey === k ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
    >
      {label}
      <span className="sort-arrow">{sortKey === k ? (sortDir === "asc" ? "▲" : "▼") : ""}</span>
    </th>
  );

  return (
    <section className="panel active">
      <div className="page-head">
        <div>
          <h1>Capacity</h1>
          <p>Live accelerator inventory across clusters. GPUs and TPUs are counted separately.</p>
        </div>
      </div>

      <div className="tiles cap-tiles">
        <StatTile
          label="GPU allocated"
          value={`${kpct(gpu)}%`}
          sub={gpu ? `${n(gpu.used)} / ${n(gpu.alloc)} · ${n(gpu.free)} free` : "no GPU clusters"}
        >
          <Gauge pct={kpct(gpu)} tone={utilTone(kpct(gpu))} />
        </StatTile>
        <StatTile
          label="TPU allocated"
          value={`${kpct(tpu)}%`}
          sub={tpu ? `${n(tpu.used)} / ${n(tpu.alloc)} · ${n(tpu.free)} free` : "no TPU clusters"}
        >
          <Gauge pct={kpct(tpu)} tone={utilTone(kpct(tpu))} />
        </StatTile>
        <StatTile
          label="Pending"
          value={pendingAccel ? n(pendingAccel) : "0"}
          sub={pendingAccel ? `${n(pendingPods)} pods waiting for capacity` : "none waiting"}
          tone={pendingAccel > 0 ? "red" : "green"}
        />
        <StatTile
          label="Unavailable"
          value={unavailAccel ? n(unavailAccel) : "0"}
          sub={unavailAccel ? `${n(unavailNodes)} nodes NotReady / cordoned` : "all nodes healthy"}
          tone={unavailAccel > 0 ? "amber" : "green"}
        />
      </div>
      <p className="cap-help">
        {n(accelNodes)} accelerator nodes across {caps.length || 0} clusters · {n(schedNodes)}{" "}
        schedulable of {n(totalNodes)}. Percentages are <b>allocated</b> (scheduled), not live GPU
        utilization.
      </p>

      <Card title="Placement" sub="biggest job you can schedule now">
        <table className="place-tbl">
          <thead>
            <tr>
              <th></th>
              <th>
                Per rack <span className="muted">NVLink / ICI</span>
              </th>
              <th>
                Per fabric block <span className="muted">island</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {tiers.map((t) => (
              <tr key={t.kind}>
                <td className="place-kind">{t.kind.toUpperCase()}</td>
                <td>
                  {t.domain ? (
                    <>
                      <b className="tnum">{n(t.domain.free)}</b>{" "}
                      <span className="muted">
                        ≈{nodesWorth(t.domain)} nodes · {t.domain.cluster}
                      </span>
                    </>
                  ) : (
                    <span className="muted">—</span>
                  )}
                </td>
                <td>
                  {t.island ? (
                    <>
                      <b className="tnum">{n(t.island.free)}</b>{" "}
                      <span className="muted">
                        ≈{nodesWorth(t.island)} nodes · {t.island.cluster}
                      </span>
                    </>
                  ) : (
                    <span className="muted">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        <div className="fit-row">
          <label className="fit-input">
            Will a job of
            <input
              type="number"
              min={1}
              placeholder="128"
              value={fit}
              onChange={(e) => setFit(e.target.value)}
            />
            <select value={fitKind} onChange={(e) => setFitKind(e.target.value)}>
              {kindsPresent.map((k) => (
                <option key={k} value={k}>
                  {k.toUpperCase()}
                </option>
              ))}
            </select>
            fit in one block?
          </label>
          {fitActive ? (
            fitIslands.length ? (
              <span className="fit-ok">
                ✓ fits — {fitIslands.length} {fitKind.toUpperCase()} block
                {fitIslands.length === 1 ? "" : "s"}, largest {n(fitBest?.free ?? 0)} on{" "}
                {fitBest?.cluster}
              </span>
            ) : (
              <span className="fit-no">
                ✗ largest {fitKind.toUpperCase()} block is {n(fitBest?.free ?? 0)}
                {fitBest ? ` (${fitBest.cluster})` : ""}
              </span>
            )
          ) : null}
        </div>

        {topBlocks.length ? (
          <div className="place-top">
            <div className="eyebrow">Most free blocks (islands)</div>
            <ul className="place-toplist">
              {topBlocks.map((b, i) => (
                <li key={i}>
                  <span className="mono">{shortId(b.name)}</span>
                  <span className="muted">{b.cluster}</span>
                  <span className="place-free tnum">
                    {n(b.free)} {b.kind.toUpperCase()}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        ) : null}

        <p className="panel-note">
          Only clusters whose nodes carry topology labels are counted. A "—" means no labeled block
          exists for that tier.
        </p>
      </Card>

      <FragCard
        title="TPU slices"
        sub="carve-able slices by shape — whole slices free now vs partially used"
        noun="chips"
        units={tpuSlices}
        note="A TPU job takes a whole slice, so a slice counts as free only when every chip is free and its nodes are schedulable. The rest are effectively blocked — free chips that can't take a job of that shape."
      />
      <FragCard
        title="GPU nodes"
        sub="whole-node availability by GPUs per node — free for a full-node job vs partially used"
        noun="GPUs"
        units={gpuNodes}
        note="Counts nodes that are entirely free and schedulable. Partially-used nodes aren't lost — they still take smaller jobs — but can't host one that needs the whole box."
      />
      <FragCard
        title="GPU racks"
        sub="whole-NVLink-domain availability by size — free for a full-rack job vs partially used"
        noun="GPUs"
        units={gpuDomains}
        note="An NVLink domain (e.g. an NVL72 rack) counts as free only when all its GPUs are free and every node is schedulable — what a large, tightly-coupled job needs. Smaller jobs still use partially-filled domains."
      />

      <div className="filterbar">
        <div className="seg">
          <button className={kind === "all" ? "active" : undefined} onClick={() => setKind("all")}>
            All
          </button>
          {kindsPresent.map((k) => (
            <button
              key={k}
              className={kind === k ? "active" : undefined}
              onClick={() => setKind(k as Kind)}
            >
              {k.toUpperCase()}
            </button>
          ))}
        </div>
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
                    <span className={r.free > 0 ? "free-strong txt-ok" : "free-strong muted"}>
                      {n(r.free)}
                    </span>
                  </td>
                  <td>
                    <div className="bar-cell">
                      <Bar pct={r.util} tone={utilTone(r.util)} width={90} />
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
    </section>
  );
}
