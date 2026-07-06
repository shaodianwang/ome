// Observability viz primitives — status dots, KPI tiles, gauges, stacked
// distributions, utilization bar-chart rows, and a capacity heat grid. All
// current-state (no time-series yet) and purely presentational; color encodes
// status only.
import type { ReactNode } from "react";

import type { BarTone, Tone } from "./types";

/** A small status dot. */
export function Dot({ tone }: { tone: Tone }) {
  return <span className={`vdot ${tone}`} />;
}

/** A KPI tile: label, big value, an optional inline viz, and a sub-line. */
export function StatTile({
  label,
  value,
  sub,
  children,
}: {
  label: ReactNode;
  value: ReactNode;
  sub?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="tile">
      <div className="tile-label">{label}</div>
      <div className="tile-val">{value}</div>
      {children ? <div className="tile-viz">{children}</div> : null}
      {sub != null ? <div className="tile-sub">{sub}</div> : null}
    </div>
  );
}

/** Horizontal utilization gauge (filled track + percent), for use inside tiles. */
export function Gauge({ pct, tone = "" }: { pct: number; tone?: BarTone }) {
  return (
    <div className={`gauge ${tone}`}>
      <div className="gauge-track">
        <i style={{ width: `${Math.min(100, Math.max(0, pct))}%` }} />
      </div>
      <span className="gauge-pct tnum">{pct}%</span>
    </div>
  );
}

/** A stacked distribution bar from tone-weighted segments (e.g. ready / down). */
export function Distribution({ segments }: { segments: { value: number; tone: Tone }[] }) {
  const total = segments.reduce((s, x) => s + x.value, 0) || 1;
  return (
    <div className="dist">
      {segments.map((s, i) =>
        s.value > 0 ? (
          <i key={i} className={`seg ${s.tone}`} style={{ width: `${(s.value / total) * 100}%` }} />
        ) : null,
      )}
    </div>
  );
}

/** One row of a bar-chart: name, optional kind tag, bar (width = `pct`), an
 *  optional percent readout, and an optional trailing value. Set `showPct` false
 *  when the bar is a relative ranking (e.g. free capacity) rather than a percent. */
export function UtilRow({
  name,
  tag,
  pct,
  tone = "",
  value,
  showPct = true,
}: {
  name: ReactNode;
  tag?: ReactNode;
  pct: number;
  tone?: BarTone;
  value?: ReactNode;
  showPct?: boolean;
}) {
  return (
    <div className="urow">
      <div className="urow-name mono">{name}</div>
      {tag ? <span className="urow-tag">{tag}</span> : null}
      <div className="urow-bar">
        <i className={tone} style={{ width: `${Math.min(100, Math.max(0, pct))}%` }} />
      </div>
      {showPct ? <div className="urow-pct tnum">{pct}%</div> : null}
      {value != null ? <div className="urow-val tnum">{value}</div> : null}
    </div>
  );
}

/** A compact heat grid: each cell tinted green by `intensity` (0..1 = free
 *  headroom), labelled, with a hover title. */
export function HeatGrid({
  cells,
}: {
  cells: { label: string; intensity: number; title?: string }[];
}) {
  return (
    <div className="heatgrid">
      {cells.map((c, i) => {
        const a = 0.14 + 0.66 * Math.max(0, Math.min(1, c.intensity));
        return (
          <div
            key={i}
            className="heatcell"
            title={c.title ?? c.label}
            style={{ background: `rgba(111, 174, 132, ${a})` }} /* --green tinted by headroom */
          >
            {c.label}
          </div>
        );
      })}
    </div>
  );
}
