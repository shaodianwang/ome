import { Fragment as F, useEffect, useState } from "react";

import { api } from "../api";
import type {
  AccelGroup,
  CapacityResp,
  Fragment,
  NodeDetail,
  NodeItem,
  PlacementUnits,
  UnitBox,
} from "../api";
import type { Tone } from "../types";
import { Bar, EmptyRow, Pager } from "../components";
import { useLive } from "../live";
import { copyText, matches, num as n, shortId, UNASSIGNED_GROUP, utilPct, utilTone } from "../util";
import { Card, Distribution, Dot, Gauge, StatTile, UtilRow } from "../viz";

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

type UnitAgg = {
  chips: number;
  total: number;
  free: number;
  fragmented: number;
  full: number;
  unhealthy: number;
  stranded: number;
  fragments: Fragment[];
};

/** Sum a per-cluster PlacementUnits list into fleet occupancy per size label,
 *  biggest unit first. Shared by the TPU-slice, GPU-node, and GPU-domain sections. */
function aggUnits(
  caps: { cap: CapacityResp }[],
  pick: (c: CapacityResp) => PlacementUnits[] | undefined,
): [string, UnitAgg][] {
  const m = new Map<string, UnitAgg>();
  for (const { cap } of caps) {
    for (const u of pick(cap) ?? []) {
      // Ignore rows from a stale/mismatched backend (missing label/chips) so a
      // version skew degrades to an empty section instead of crashing the page.
      if (typeof u.label !== "string" || typeof u.chips !== "number") continue;
      const e = m.get(u.label) ?? {
        chips: u.chips,
        total: 0,
        free: 0,
        fragmented: 0,
        full: 0,
        unhealthy: 0,
        stranded: 0,
        fragments: [],
      };
      e.chips = u.chips;
      e.total += u.total;
      e.free += u.free;
      e.fragmented += u.fragmented;
      e.full += u.full;
      e.unhealthy += u.unhealthy;
      e.stranded += u.strandedChips;
      e.fragments = e.fragments.concat(u.fragments ?? []);
      m.set(u.label, e);
    }
  }
  for (const e of m.values()) e.fragments.sort((a, b) => b.free - a.free);
  return [...m.entries()].sort((a, b) => b[1].chips - a[1].chips);
}

/** The occupancy buckets a carve-able unit can be in, in bar + legend order. */
const OCCUPANCY: {
  key: "free" | "fragmented" | "full" | "unhealthy";
  tone: Tone;
  label: string;
}[] = [
  { key: "free", tone: "green", label: "free" },
  { key: "fragmented", tone: "amber", label: "fragmented" },
  { key: "full", tone: "cyan", label: "full" },
  { key: "unhealthy", tone: "red", label: "unhealthy" },
];

/** A placement tier's aggregate occupancy for the summary table. Coarse tiers
 *  (subblock / rack) carry their individual `units` for a drill-down table; fine
 *  tiers (slice / node) carry `fragments` (named examples). */
type Tier = {
  key: string;
  kind: string; // "TPU" | "GPU"
  tier: string; // "2x2x2 slice" | "subblock" | "4-GPU node" | "rack"
  noun: string; // "chips" | "GPUs"
  unit: string; // "slice" | "subblock" | "node" | "rack"
  chips: number;
  total: number;
  free: number;
  fragmented: number;
  full: number;
  unhealthy: number;
  stranded: number;
  units?: UnitBox[];
  fragments?: Fragment[];
};

/** Occupancy bucket of one domain unit; unhealthy wins so a down host isn't hidden. */
function unitState(b: UnitBox): "free" | "fragmented" | "full" | "unhealthy" {
  if (!b.healthy) return "unhealthy";
  if (b.free >= b.chips) return "free";
  if (b.free <= 0) return "full";
  return "fragmented";
}

/** Collapse a domain's individual units (subblocks / racks) into one summary tier.
 *  Nominal size = the largest observed; smaller units just have hosts missing. */
function domainTier(
  kind: string,
  tier: string,
  noun: string,
  unit: string,
  boxes: UnitBox[],
): Tier | null {
  if (!boxes.length) return null;
  const t: Tier = {
    key: kind + tier,
    kind,
    tier,
    noun,
    unit,
    chips: 0,
    total: boxes.length,
    free: 0,
    fragmented: 0,
    full: 0,
    unhealthy: 0,
    stranded: 0,
    units: boxes,
  };
  for (const b of boxes) {
    t.chips = Math.max(t.chips, b.chips);
    const s = unitState(b);
    if (s === "fragmented") {
      t.fragmented++;
      t.stranded += b.free;
    } else {
      t[s]++;
    }
  }
  return t;
}

/** A fine tier (TPU slice shape / GPU node size) from its aggregated occupancy. */
function fineTier(kind: string, tier: string, noun: string, unit: string, u: UnitAgg): Tier {
  return {
    key: kind + tier,
    kind,
    tier,
    noun,
    unit,
    chips: u.chips,
    total: u.total,
    free: u.free,
    fragmented: u.fragmented,
    full: u.full,
    unhealthy: u.unhealthy,
    stranded: u.stranded,
    fragments: u.fragments,
  };
}

/** Unit state → tone class, for the drill-down state tag. */
const STATE_TONE: Record<string, string> = {
  free: "green",
  fragmented: "amber",
  full: "cyan",
  unhealthy: "red",
};

/** The coarse-tier drill-down: a filterable, paged table of individual units
 *  (racks / subblocks). Find one by name, or scan the backend's most-free-first
 *  order; names are reduced to their distinguishing part. */
function UnitTable({ units, unit }: { units: UnitBox[]; unit: string }) {
  const [q, setQ] = useState("");
  const [page, setPage] = useState(0);
  const [size, setSize] = useState(25);
  const label = labeler(units.map((u) => u.id));
  const rows = units.filter((u) => matches(q, u.id, u.cluster));
  const pages = Math.max(1, Math.ceil(rows.length / size));
  const clamped = Math.min(page, pages - 1);
  const start = clamped * size;
  return (
    <div className="unit-table">
      <div className="filterbar">
        <div className="input grow">
          <input
            aria-label={`Filter ${unit}s`}
            placeholder={`Filter ${unit}s by name or cluster…`}
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              setPage(0);
            }}
          />
        </div>
      </div>
      <div className="tbl-wrap">
        <table className="tbl">
          <thead>
            <tr>
              <th>{unit}</th>
              <th>Cluster</th>
              <th className="right">Free / total</th>
              <th>State</th>
            </tr>
          </thead>
          <tbody>
            {rows.slice(start, start + size).map((b, i) => {
              const s = unitState(b);
              return (
                <tr key={i}>
                  <td className="cell-name mono" title={b.id}>
                    {label(b.id)}
                  </td>
                  <td className="mono muted">{b.cluster}</td>
                  <td className="num">
                    {n(b.free)}/{n(b.chips)}
                  </td>
                  <td>
                    <span className={`utag ${STATE_TONE[s]}`}>{s}</span>
                  </td>
                </tr>
              );
            })}
            {rows.length === 0 ? (
              <EmptyRow colSpan={4}>
                {q.trim() ? `No ${unit}s match "${q}"` : `No ${unit}s`}
              </EmptyRow>
            ) : null}
          </tbody>
        </table>
      </div>
      <Pager
        total={rows.length}
        page={clamped}
        totalPages={pages}
        pageSize={size}
        start={start}
        noun={`${unit}s`}
        onPage={setPage}
        onPageSize={setSize}
      />
    </div>
  );
}

/** One summary row in the Placement-capacity table: tier · occupancy counts · mix
 *  bar. Expands to the drill-down — a unit table for coarse tiers, named examples
 *  for fine tiers. */
function TierRow({ t }: { t: Tier }) {
  const [open, setOpen] = useState(false);
  const recoverable = t.chips > 0 ? Math.floor(t.stranded / t.chips) : 0;
  return (
    <>
      <tr className="clickable" onClick={() => setOpen((o) => !o)}>
        <td className="tier-name">
          <button
            type="button"
            className="row-open"
            aria-expanded={open}
            onClick={(event) => {
              event.stopPropagation();
              setOpen((o) => !o);
            }}
          >
            <span className="tier-caret" aria-hidden="true">
              {open ? "▾" : "▸"}
            </span>
            <span className="tag">{t.kind}</span> {t.tier}{" "}
            <span className="muted">
              · {n(t.chips)} {t.noun}
            </span>
          </button>
        </td>
        <td className="num txt-ok">{n(t.free)}</td>
        <td className="num">{t.fragmented ? n(t.fragmented) : "—"}</td>
        <td className="num muted">{n(t.full)}</td>
        <td className="num">{t.unhealthy ? n(t.unhealthy) : "—"}</td>
        <td className="num">{t.stranded ? n(t.stranded) : "—"}</td>
        <td className="tier-mix">
          <Distribution segments={OCCUPANCY.map((o) => ({ value: t[o.key], tone: o.tone }))} />
        </td>
      </tr>
      {open ? (
        <tr className="tier-detail-row">
          <td colSpan={7}>
            <div className="tier-detail">
              {t.stranded > 0 ? (
                <p className="frag-strand">
                  <b className="tnum">{n(t.stranded)}</b> {t.noun} stranded in fragmented {t.unit}s
                  {recoverable >= 1 ? (
                    <>
                      {" "}
                      · ≈<b className="tnum">{n(recoverable)}</b> more {t.unit}s if defragmented
                    </>
                  ) : null}
                </p>
              ) : null}
              {t.units ? (
                <UnitTable units={t.units} unit={t.unit} />
              ) : t.fragments && t.fragments.length ? (
                <div className="frag-list">
                  <span className="frag-list-lbl">most fragmented {t.unit}s</span>
                  {t.fragments.slice(0, 8).map((f, i) => (
                    <span className="frag-chip mono" key={i} title={f.id}>
                      {shortId(f.id)} <span className="muted">{f.cluster}</span>{" "}
                      <span className="tnum">
                        {n(f.chips - f.free)}/{n(f.chips)}
                      </span>
                    </span>
                  ))}
                </div>
              ) : (
                <p className="panel-note">
                  No fragmentation — every {t.unit} is fully free or fully used.
                </p>
              )}
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}

/** The unified Placement-capacity section: a summary table of every placement tier
 *  (TPU slice/subblock, GPU node/rack), each row expandable to its drill-down.
 *  Replaces the old per-kind fragmentation cards. */
function PlacementCapacity({ tiers }: { tiers: Tier[] }) {
  if (!tiers.length) return null;
  return (
    <Card title="Placement capacity" sub="placeable units by tier · free vs fragmented">
      <div className="health-legend frag-legend">
        {OCCUPANCY.map((o) => (
          <span key={o.key}>
            <Dot tone={o.tone} /> {o.label}
          </span>
        ))}
      </div>
      <div className="tbl-wrap">
        <table className="tbl tier-tbl">
          <thead>
            <tr>
              <th>Tier</th>
              <th className="right">Free</th>
              <th className="right">Frag</th>
              <th className="right">Full</th>
              <th className="right">Down</th>
              <th className="right">Stranded</th>
              <th>Mix</th>
            </tr>
          </thead>
          <tbody>
            {tiers.map((t) => (
              <TierRow key={t.key} t={t} />
            ))}
          </tbody>
        </table>
      </div>
      <p className="panel-note">
        Subblocks and racks are one row each — their varying chip count is just how many hosts are
        present, not different sizes. Slices and nodes split by real shape/size.
        &ldquo;Stranded&rdquo; is free chips trapped in fragmented units; &ldquo;down&rdquo; counts
        units with a NotReady/cordoned node.
      </p>
    </Card>
  );
}

/** A compact *distinguishing* label for a set of unit ids: strip the shared suffix
 *  (e.g. a common "-us-central-08a" region tail) at a separator boundary, and
 *  shorten anything still long (opaque hashes). Racks that differ only in the
 *  middle stay tellable apart, instead of all rendering as "dh20…-08a". */
function labeler(ids: string[]): (id: string) => string {
  let suf = ids[0] ?? "";
  for (const id of ids) while (suf && !id.endsWith(suf)) suf = suf.slice(1);
  const cut = suf.replace(/^[^-_/.]+/, ""); // trim to a separator boundary
  return (id) => {
    const core = cut.length > 1 && id.endsWith(cut) ? id.slice(0, -cut.length) : id;
    return core.length > 20 ? shortId(core, 18) : core;
  };
}

/** A one-click "copy this kubectl command" chip. muse never runs it — the operator
 *  runs it with their own (RBAC'd, audited) credentials. */
function CopyCmd({ label, cmd }: { label: string; cmd: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="copybtn"
      title={cmd}
      onClick={async () => {
        if (await copyText(cmd)) {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        }
      }}
    >
      {copied ? "✓ copied" : label}
    </button>
  );
}

// Conditions where `True` means trouble (standard k8s node pressures).
const BAD_WHEN_TRUE = ["MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"];

/** Color a condition only where its polarity is known: Ready (True=good), the
 *  standard pressures (True=bad). Custom detector conditions vary (`CWActive: True`
 *  is good, `GPUFault: True` is bad), so leave them neutral. */
function condTone(c: { type: string; status: string }): string {
  if (c.type === "Ready") return c.status === "True" ? "txt-ok" : "txt-bad";
  if (BAD_WHEN_TRUE.includes(c.type)) return c.status === "True" ? "txt-bad" : "txt-ok";
  return "";
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
  const [tab, setTab] = useState(""); // "" = all clusters; else a cluster name
  const [nodes, setNodes] = useState<NodeItem[]>([]);
  const [nodesLoading, setNodesLoading] = useState(false);
  const [nodeQ, setNodeQ] = useState("");
  const [nodePage, setNodePage] = useState(0);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [detail, setDetail] = useState<NodeDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // One scoped-capacity request per cluster, combined client-side so island /
  // domain rows stay attributed to their cluster and kind.
  const live = useLive(
    async () => {
      const cl = await api.clusters();
      return Promise.all(
        cl.items.map((c) =>
          api.capacityForCluster(c.name).then((cap) => ({
            name: c.name,
            ready: c.ready,
            version: c.version,
            region: c.region,
            cap,
          })),
        ),
      );
    },
    { kinds: ["Capacity", "Cluster"], intervalMs: 30_000 },
  );
  const allCaps: {
    name: string;
    ready: boolean;
    version?: string;
    region?: string;
    cap: CapacityResp;
  }[] = live.data ?? [];
  // A per-cluster tab scopes the whole page to one cluster; "" is the fleet view.
  const caps = tab ? allCaps.filter((c) => c.name === tab) : allCaps;
  const active = tab ? allCaps.find((c) => c.name === tab) : undefined;
  // A cluster still warming its informer caches reports partial data (pods sync
  // after nodes → allocated looks like 0, everything looks free). Flag it so the
  // numbers aren't mistaken for steady state.
  const syncing = caps.filter((c) => !c.ready).map((c) => c.name);

  useEffect(() => {
    setPage(0);
  }, [kind, dim, q, pageSize, tab]);

  // Lazily load the selected cluster's nodes (node explorer + health breakdown).
  // A fresh capacity snapshot follows every Node/Pod change, so use its identity
  // as the refresh revision instead of opening another stream or polling here.
  useEffect(() => {
    if (!tab) {
      setNodes([]);
      return;
    }
    let cancelled = false;
    setNodesLoading(true);
    api
      .nodesForCluster(tab)
      .then((r) => {
        if (!cancelled) setNodes(r.items);
      })
      .catch(() => {
        if (!cancelled) setNodes([]);
      })
      .finally(() => {
        if (!cancelled) setNodesLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [tab, live.data]);

  useEffect(() => {
    setNodePage(0);
    setExpanded(null);
  }, [tab, nodeQ]);

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
  const readyNodes = caps.reduce((s, { cap }) => s + cap.nodes.ready, 0);
  const kindsPresent = [...byKind.keys()].sort();

  // Per-cluster drill-downs (only when a single cluster tab is active).
  const consumers = tab ? (caps[0]?.cap.consumers ?? []) : [];
  const maxConsumer = Math.max(1, ...consumers.map((c) => c.accelerators));
  // Node-health breakdown, from the selected cluster's accelerator nodes.
  const issueCounts = new Map<string, number>();
  let unhealthyAccel = 0;
  for (const nd of nodes) {
    if (!nd.accelerator || !nd.issues?.length) continue;
    unhealthyAccel++;
    for (const iss of nd.issues) issueCounts.set(iss, (issueCounts.get(iss) ?? 0) + 1);
  }
  const nodeIssues = [...issueCounts.entries()].sort((a, b) => b[1] - a[1]);
  // Node explorer: filtered, unhealthy-first, paged.
  const nodeRows = nodes
    .filter((nd) => matches(nodeQ, nd.name, nd.instanceType, (nd.issues ?? []).join(" ")))
    .sort((a, b) => {
      const ai = a.issues?.length ? 1 : 0;
      const bi = b.issues?.length ? 1 : 0;
      return ai !== bi ? bi - ai : (b.accelerator?.free ?? 0) - (a.accelerator?.free ?? 0);
    });
  const nodeTotalPages = Math.max(1, Math.ceil(nodeRows.length / pageSize));
  const nodeClamped = Math.min(nodePage, nodeTotalPages - 1);
  const nodeStart = nodeClamped * pageSize;
  const nodePageRows = nodeRows.slice(nodeStart, nodeStart + pageSize);
  const toggleNode = (name: string) => {
    if (expanded === name) {
      setExpanded(null);
      return;
    }
    setExpanded(name);
    setDetail(null);
    setDetailLoading(true);
    api
      .nodeDetail(tab, name)
      .then(setDetail)
      .catch(() => setDetail(null))
      .finally(() => setDetailLoading(false));
  };

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
  // Placement tiers for the summary table: fine tiers (slice/node) split by real
  // shape/size; coarse tiers (subblock/rack) collapsed to one row over their
  // individual units (concatenated across the scoped clusters, most-free first).
  const domains = (pick: (c: CapacityResp) => UnitBox[] | undefined) =>
    caps.flatMap(({ cap }) => pick(cap) ?? []).sort((a, b) => b.free - a.free);
  const placeTiers: Tier[] = [
    ...aggUnits(caps, (c) => c.tpuSlices).map(([label, u]) =>
      fineTier("TPU", `${label} slice`, "chips", "slice", u),
    ),
    domainTier(
      "TPU",
      "subblock",
      "chips",
      "subblock",
      domains((c) => c.tpuSubblocks),
    ),
    ...aggUnits(caps, (c) => c.gpuNodes).map(([label, u]) =>
      fineTier("GPU", `${label}-GPU node`, "GPUs", "node", u),
    ),
    domainTier(
      "GPU",
      "rack",
      "GPUs",
      "rack",
      domains((c) => c.gpuRacks),
    ),
  ].filter((t): t is Tier => t !== null);

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
            {c.ready ? null : <Dot tone="amber" />}
          </button>
        ))}
      </div>

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
              {n(totalNodes)} · {n(readyNodes)} ready · {n(schedNodes)} sched
            </span>
          </div>
          <div className="s2">
            <span className="lbl">Accelerator nodes</span>
            <span className="val">{n(accelNodes)}</span>
          </div>
        </div>
      ) : null}

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
        {n(accelNodes)} accelerator nodes across {caps.length} cluster
        {caps.length === 1 ? "" : "s"} · {n(schedNodes)} schedulable of {n(totalNodes)}. Percentages
        are <b>allocated</b> (scheduled), not live GPU utilization.
      </p>
      {syncing.length ? (
        <p className="cap-syncing">
          ⏳ {syncing.length} cluster{syncing.length > 1 ? "s" : ""} still syncing (
          {syncing.join(", ")}) — capacity is partial until caches warm.
        </p>
      ) : null}

      {active ? (
        <div className="grid g2">
          <Card title="Node health" sub="why accelerator nodes are unavailable">
            {nodesLoading ? (
              <p className="panel-note">Loading nodes…</p>
            ) : nodeIssues.length ? (
              <div className="urows">
                {nodeIssues.map(([iss, count]) => (
                  <UtilRow
                    key={iss}
                    name={iss}
                    pct={unhealthyAccel > 0 ? Math.round((count / unhealthyAccel) * 100) : 0}
                    tone={iss === "cordoned" ? "amber" : "red"}
                    value={n(count)}
                    showPct={false}
                  />
                ))}
              </div>
            ) : (
              <p className="panel-note">All accelerator nodes are Ready and schedulable.</p>
            )}
          </Card>
          <Card title="Top consumers" sub="namespaces holding the most accelerators">
            {consumers.length ? (
              <div className="urows">
                {consumers.map((c, i) => (
                  <UtilRow
                    key={i}
                    name={c.namespace}
                    tag={c.kind.toUpperCase()}
                    pct={Math.round((c.accelerators / maxConsumer) * 100)}
                    tone="violet"
                    value={n(c.accelerators)}
                    showPct={false}
                  />
                ))}
              </div>
            ) : (
              <p className="panel-note">No accelerator workloads scheduled here.</p>
            )}
          </Card>
        </div>
      ) : null}

      <PlacementCapacity tiers={placeTiers} />

      {active ? (
        <Card
          title="Nodes"
          sub={`${n(nodeRows.length)} node${nodeRows.length === 1 ? "" : "s"} · unhealthy first`}
        >
          <div className="filterbar">
            <div className="input grow">
              <input
                aria-label="Search nodes"
                placeholder="Filter nodes by name, type, or issue…"
                value={nodeQ}
                onChange={(e) => setNodeQ(e.target.value)}
              />
            </div>
          </div>
          <div className="tbl-wrap">
            <table className="tbl">
              <thead>
                <tr>
                  <th>Node</th>
                  <th>Type</th>
                  <th className="right">Accel free/total</th>
                  <th>Health</th>
                  <th>Island / domain</th>
                  <th className="right">Age</th>
                </tr>
              </thead>
              <tbody>
                {nodePageRows.map((nd) => (
                  <F key={nd.name}>
                    <tr className="clickable" onClick={() => toggleNode(nd.name)}>
                      <td className="cell-name mono">
                        <button
                          type="button"
                          className="row-open"
                          aria-expanded={expanded === nd.name}
                          onClick={(event) => {
                            event.stopPropagation();
                            toggleNode(nd.name);
                          }}
                        >
                          {nd.name}
                        </button>
                      </td>
                      <td className="mono muted">{nd.instanceType ?? "—"}</td>
                      <td className="num">
                        {nd.accelerator
                          ? `${n(nd.accelerator.free)}/${n(nd.accelerator.allocatable)}`
                          : "—"}
                      </td>
                      <td>
                        {nd.issues?.length ? (
                          nd.issues.map((iss, i) => (
                            <span key={i} className="tag">
                              {iss}
                            </span>
                          ))
                        ) : (
                          <span className="txt-ok">Ready</span>
                        )}
                      </td>
                      <td className="mono muted">
                        {[nd.island, nd.nvlinkDomain]
                          .filter((x): x is string => !!x)
                          .map((x) => shortId(x))
                          .join(" / ") || "—"}
                      </td>
                      <td className="num muted">{nd.age ?? "—"}</td>
                    </tr>
                    {expanded === nd.name ? (
                      <tr className="node-detail-row">
                        <td colSpan={6}>
                          <div className="node-detail">
                            <div className="node-cmds">
                              <span className="node-cmds-lbl">run with your own creds</span>
                              <CopyCmd
                                label="describe"
                                cmd={`kubectl --context ${tab} describe node ${nd.name}`}
                              />
                              <CopyCmd
                                label="drain"
                                cmd={`kubectl --context ${tab} drain ${nd.name} --ignore-daemonsets --delete-emptydir-data`}
                              />
                              <CopyCmd
                                label="debug shell"
                                cmd={`kubectl --context ${tab} debug node/${nd.name} -it --image=busybox`}
                              />
                            </div>
                            {detailLoading ? (
                              <p className="panel-note">Loading node detail…</p>
                            ) : detail && detail.name === nd.name ? (
                              <div className="node-detail-grid">
                                <div>
                                  <div className="eyebrow">Conditions</div>
                                  {detail.conditions?.length ? (
                                    detail.conditions.map((c, i) => (
                                      <div className="cond-row" key={i}>
                                        <span>{c.type}</span>
                                        <span className={`cond-rs ${condTone(c)}`}>{c.status}</span>
                                      </div>
                                    ))
                                  ) : (
                                    <span className="muted">—</span>
                                  )}
                                </div>
                                <div>
                                  <div className="eyebrow">Taints</div>
                                  {detail.taints?.length ? (
                                    <div className="frag-list">
                                      {detail.taints.map((t, i) => (
                                        <span className="frag-chip mono" key={i}>
                                          {t.key}
                                          {t.value ? `=${t.value}` : ""}:{t.effect}
                                        </span>
                                      ))}
                                    </div>
                                  ) : (
                                    <span className="muted">none</span>
                                  )}
                                </div>
                                <div>
                                  <div className="eyebrow">
                                    Pods on node ({n(detail.podsOnNode?.length ?? 0)})
                                  </div>
                                  {detail.podsOnNode?.length ? (
                                    <ul className="node-pods">
                                      {detail.podsOnNode.slice(0, 12).map((p, i) => (
                                        <li key={i}>
                                          <span className="mono">
                                            {p.namespace}/{p.name}
                                          </span>
                                          {p.accelerators ? (
                                            <span className="node-pod-accel tnum">
                                              {n(p.accelerators)}
                                            </span>
                                          ) : null}
                                        </li>
                                      ))}
                                    </ul>
                                  ) : (
                                    <span className="muted">none</span>
                                  )}
                                </div>
                              </div>
                            ) : (
                              <p className="panel-note">Couldn't load node detail.</p>
                            )}
                          </div>
                        </td>
                      </tr>
                    ) : null}
                  </F>
                ))}
                {nodeRows.length === 0 ? (
                  <EmptyRow colSpan={6}>
                    {nodesLoading
                      ? "Loading nodes…"
                      : nodeQ.trim()
                        ? `No nodes match "${nodeQ}"`
                        : "No nodes"}
                  </EmptyRow>
                ) : null}
              </tbody>
            </table>
          </div>
          <Pager
            total={nodeRows.length}
            page={nodeClamped}
            totalPages={nodeTotalPages}
            pageSize={pageSize}
            start={nodeStart}
            noun="nodes"
            onPage={setNodePage}
            onPageSize={setPageSize}
          />
        </Card>
      ) : null}

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
        <div className="input grow">
          <input
            aria-label="Search capacity"
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
              aria-label="Job accelerator count"
              type="number"
              min={1}
              placeholder="128"
              value={fit}
              onChange={(e) => setFit(e.target.value)}
            />
            <select
              aria-label="Job accelerator type"
              value={fitKind}
              onChange={(e) => setFitKind(e.target.value)}
            >
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
    </section>
  );
}
