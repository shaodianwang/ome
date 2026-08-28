import { useState } from "react";

import type { AccelGroup, CapacityResp, Fragment, PlacementUnits, UnitBox } from "../api";
import type { Tone } from "../types";
import { EmptyRow, Pager, SearchInput } from "../components";
import { matches, num as n, shortId, UNASSIGNED_GROUP } from "../util";
import { Card, Distribution, Dot } from "../viz";

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
        <SearchInput
          ariaLabel={`Filter ${unit}s`}
          placeholder={`Filter ${unit}s by name or cluster…`}
          value={q}
          onChange={(value) => {
            setQ(value);
            setPage(0);
          }}
        />
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

/** Derives the placement tiers from the scoped clusters' capacity and renders
 *  the Placement-capacity card. */
export function PlacementTiers({ caps }: { caps: { cap: CapacityResp }[] }) {
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
  return <PlacementCapacity tiers={placeTiers} />;
}

/** The Placement card: the biggest schedulable block per topology tier and a
 *  does-my-job-fit probe over the fabric-level islands. */
export function PlacementCard({
  caps,
  kindsPresent,
}: {
  caps: { name: string; cap: CapacityResp }[];
  kindsPresent: string[];
}) {
  const [fit, setFit] = useState("");
  const [fitKind, setFitKind] = useState("gpu");

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

  return (
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
  );
}
