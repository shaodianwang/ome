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

/** A KPI tile: label, big value, an optional inline viz, and a sub-line. `tone`
 *  colors the value for at-a-glance status (e.g. green healthy / red breached). */
export function StatTile({
  label,
  value,
  sub,
  tone,
  children,
}: {
  label: ReactNode;
  value: ReactNode;
  sub?: ReactNode;
  tone?: Tone;
  children?: ReactNode;
}) {
  return (
    <div className="tile">
      <div className="tile-label">{label}</div>
      <div className={tone ? `tile-val ${tone}` : "tile-val"}>{value}</div>
      {children ? <div className="tile-viz">{children}</div> : null}
      {sub != null ? <div className="tile-sub">{sub}</div> : null}
    </div>
  );
}

/** The single-value filled-track bar behind Gauge, Bar, and UtilRow: a colored
 *  fill sized to `pct` over a muted track. `size="md"` is taller (8px); `bordered`
 *  is the boxed table-cell variant. One source of truth for every non-stacked bar
 *  (the stacked/segmented bar is `Distribution`). */
export function Track({
  pct,
  tone = "",
  striped = false,
  bordered = false,
  size = "sm",
  width,
}: {
  pct: number;
  tone?: BarTone;
  striped?: boolean;
  bordered?: boolean;
  size?: "sm" | "md";
  width?: number | string;
}) {
  const cls = ["track", size, tone, striped ? "striped" : "", bordered ? "bordered" : ""]
    .filter(Boolean)
    .join(" ");
  return (
    <div className={cls} style={width !== undefined ? { width } : undefined}>
      <i style={{ width: `${Math.min(100, Math.max(0, pct))}%` }} />
    </div>
  );
}

/** Horizontal utilization gauge (filled track + percent), for use inside tiles. */
export function Gauge({ pct, tone = "" }: { pct: number; tone?: BarTone }) {
  return (
    <div className="gauge">
      <Track pct={pct} tone={tone} />
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
      <Track pct={pct} tone={tone} size="md" />
      {showPct ? <div className="urow-pct tnum">{pct}%</div> : null}
      {value != null ? <div className="urow-val tnum">{value}</div> : null}
    </div>
  );
}

/** A minimal sparkline for a numeric time-series — scales to its own min/max,
 *  presentational only. `color` is any CSS color/var; the stroke stays 1.5px under
 *  the non-uniform scale via `vector-effect`. Fewer than two points renders blank. */
export function Spark({
  points,
  color = "var(--accent)",
  width = 108,
  height = 24,
}: {
  points: number[];
  color?: string;
  width?: number;
  height?: number;
}) {
  if (points.length < 2) {
    return <svg className="spark" width={width} height={height} aria-hidden />;
  }
  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = max - min || 1;
  const dx = width / (points.length - 1);
  const d = points
    .map((p, i) => `${(i * dx).toFixed(1)},${(height - ((p - min) / span) * height).toFixed(1)}`)
    .join(" ");
  return (
    <svg
      className="spark"
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      aria-hidden
    >
      <polyline
        className="spark-ln"
        points={d}
        style={{ stroke: color }}
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}

/** A Grafana-style filter chip: a label + a native select styled as a pill.
 *  `accent` highlights it (used for the time-range control). */
export function FilterChip({
  label,
  value,
  options,
  onChange,
  accent = false,
}: {
  label?: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
  accent?: boolean;
}) {
  return (
    <label className={accent ? "fv range" : "fv"}>
      {label ? <span className="lbl">{label}</span> : null}
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

/** A line/area chart for one or more numeric series — grid lines + polylines
 *  scaled to the combined range, matching the mockup's `.chart` (300×96 viewBox,
 *  non-scaling stroke). Each series: `points` + a CSS `color`; `area` fills under
 *  it. Non-finite points (e.g. NaN latency during idle) are ignored. */
export function Chart({
  series,
  height = 96,
}: {
  series: { points: number[]; color: string; area?: boolean }[];
  height?: number;
}) {
  const W = 300;
  const H = height;
  const all = series.flatMap((s) => s.points).filter((v) => Number.isFinite(v));
  if (all.length < 2) {
    return (
      <svg className="chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden />
    );
  }
  const min = Math.min(...all, 0);
  const max = Math.max(...all) || 1;
  const span = max - min || 1;
  const path = (arr: number[]) =>
    arr
      .map((v, i) =>
        Number.isFinite(v)
          ? `${((i / (arr.length - 1)) * W).toFixed(1)},${(H - ((v - min) / span) * H).toFixed(1)}`
          : "",
      )
      .filter(Boolean)
      .join(" ");
  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden>
      {[0.25, 0.5, 0.75].map((f) => (
        <line key={f} className="grid-ln" x1="0" y1={H * f} x2={W} y2={H * f} />
      ))}
      {series.map((s, i) =>
        s.area && s.points.filter((v) => Number.isFinite(v)).length > 1 ? (
          <polygon
            key={`a${i}`}
            className="chart-area"
            points={`${path(s.points)} ${W},${H} 0,${H}`}
            style={{ fill: s.color }}
          />
        ) : null,
      )}
      {series.map((s, i) =>
        s.points.filter((v) => Number.isFinite(v)).length > 1 ? (
          <polyline
            key={`l${i}`}
            className="chart-ln"
            points={path(s.points)}
            style={{ stroke: s.color }}
            vectorEffect="non-scaling-stroke"
          />
        ) : null,
      )}
    </svg>
  );
}

/** One chart series: numeric points, a CSS color, and whether to fill under it. */
export interface Series {
  points: number[];
  color: string;
  area?: boolean;
}

/** A titled panel card: an optional header (title · sub · actions) over a body.
 *  The base container for dashboard panels — use it instead of hand-writing the
 *  `card`/`card-head`/`card-body` markup. */
export function Card({
  title,
  sub,
  actions,
  className,
  children,
}: {
  title?: ReactNode;
  sub?: ReactNode;
  actions?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  const head = title != null || sub != null || actions != null;
  return (
    <div className={className ? `card ${className}` : "card"}>
      {head ? (
        <div className="card-head">
          {title != null ? <h3>{title}</h3> : null}
          {sub != null ? <span className="sub">{sub}</span> : null}
          {actions}
        </div>
      ) : null}
      <div className="card-body">{children}</div>
    </div>
  );
}

/** A card plotting one or more series with a headline value. Renders nothing when
 *  there's no data; `hideIfZero` also hides an all-zero series, so rare-event
 *  panels (errors, preemptions) surface only when something actually happens. */
export function MetricChart({
  title,
  value,
  sub,
  series,
  hideIfZero = false,
}: {
  title: ReactNode;
  value: ReactNode;
  sub?: ReactNode;
  series: Series[];
  hideIfZero?: boolean;
}) {
  const plottable = series.some((s) => s.points.length > 1);
  if (!plottable || (hideIfZero && series.every((s) => s.points.every((v) => v === 0)))) {
    return null;
  }
  return (
    <Card title={title} sub={sub}>
      <div className="chart-val">{value}</div>
      <Chart series={series} />
    </Card>
  );
}

/** A card showing a bounded fraction (0..1) as a threshold gauge. */
export function GaugeCard({
  title,
  frac,
  tone,
  sub,
}: {
  title: ReactNode;
  frac: number | null;
  tone?: BarTone;
  sub?: ReactNode;
}) {
  if (frac == null) return null;
  const p = Math.round(frac * 100);
  return (
    <Card title={title} sub={sub}>
      <div className="chart-val">{p}%</div>
      <Gauge pct={p} tone={tone} />
    </Card>
  );
}
