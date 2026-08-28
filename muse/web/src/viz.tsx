// Observability viz primitives — status dots, KPI tiles, gauges, stacked
// distributions, utilization bar-chart rows, and a capacity heat grid. All
// current-state and time-series views; color encodes status or series identity.
import { useId, useState } from "react";
import type { KeyboardEvent, PointerEvent, ReactNode } from "react";

import {
  formatChartTimestamp,
  nearestTimestampIndex,
  splitTimestampGaps,
  timestampSelectionForKey,
} from "./chartMath";
import type { MetricPoint } from "./metrics";
import type { BarTone, Tone } from "./types";

/** A small status dot. */
export function Dot({ tone }: { tone: Tone }) {
  return <span className={`vdot ${tone}`} aria-hidden="true" />;
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

/** One timestamped chart series and its tooltip presentation. */
export interface Series {
  points: MetricPoint[];
  color: string;
  label: string;
  area?: boolean;
  formatValue: (value: number) => string;
}

function TimePlot({
  series,
  width,
  height,
  compact,
  grid,
  includeZero,
  ariaLabel,
}: {
  series: Series[];
  width: number;
  height: number;
  compact: boolean;
  grid: boolean;
  includeZero: boolean;
  ariaLabel: string;
}) {
  const announcementId = useId();
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const prepared = series.map((item) => {
    const points = item.points
      .filter((point) => Number.isFinite(point.at) && Number.isFinite(point.value))
      .sort((a, b) => a.at - b.at);
    return { ...item, points };
  });
  const allPoints = prepared.flatMap((item) => item.points);
  if (allPoints.length < 2) {
    return compact ? (
      <svg className="spark" width={width} height={height} aria-hidden />
    ) : (
      <svg
        className="chart"
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        aria-hidden
      />
    );
  }
  const timestamps = [...new Set(allPoints.map((point) => point.at))].sort((a, b) => a - b);
  const plotted = prepared.map((item) => ({
    ...item,
    segments: splitTimestampGaps(item.points, timestamps),
  }));
  const values = allPoints.map((point) => point.value);
  const minAt = timestamps[0];
  const maxAt = timestamps[timestamps.length - 1];
  const timeSpan = maxAt - minAt || 1;
  const minValue = includeZero ? Math.min(...values, 0) : Math.min(...values);
  const maxValue = includeZero ? Math.max(...values, 0) : Math.max(...values);
  const valueSpan = maxValue - minValue || 1;
  const x = (at: number) => ((at - minAt) / timeSpan) * width;
  const y = (value: number) => height - ((value - minValue) / valueSpan) * height;
  const coordinates = (points: MetricPoint[]) =>
    points.map((point) => `${x(point.at).toFixed(1)},${y(point.value).toFixed(1)}`).join(" ");
  const selectedIndex =
    activeIndex == null ? null : Math.min(activeIndex, Math.max(0, timestamps.length - 1));
  const activeAt = selectedIndex == null ? null : timestamps[selectedIndex];
  const activeX = activeAt == null ? null : x(activeAt);
  const activeRows =
    activeAt == null
      ? []
      : plotted.map((item) => ({
          ...item,
          point: item.points.find((point) => point.at === activeAt),
        }));
  const activeDescription =
    activeAt == null
      ? ""
      : [
          formatChartTimestamp(activeAt),
          ...activeRows.map(
            (row) => `${row.label}: ${row.point ? row.formatValue(row.point.value) : "no sample"}`,
          ),
        ].join(". ");
  const activePercent = activeX == null ? 50 : (activeX / width) * 100;
  const tooltipSide = activePercent < 25 ? "start" : activePercent > 75 ? "end" : "center";

  const indexAtClientX = (clientX: number, svg: SVGSVGElement) => {
    const bounds = svg.getBoundingClientRect();
    const fraction = Math.min(1, Math.max(0, (clientX - bounds.left) / (bounds.width || 1)));
    return nearestTimestampIndex(timestamps, minAt + fraction * timeSpan);
  };
  const handlePointer = (event: PointerEvent<SVGSVGElement>) => {
    setActiveIndex(indexAtClientX(event.clientX, event.currentTarget));
  };
  const handleKey = (event: KeyboardEvent<SVGSVGElement>) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End", "Escape"].includes(event.key)) return;
    event.preventDefault();
    setActiveIndex(timestampSelectionForKey(event.key, selectedIndex, timestamps.length));
  };

  return (
    <div
      className={compact ? "metric-plot compact" : "metric-plot"}
      style={compact ? { width, height } : undefined}
    >
      <svg
        className={compact ? "spark" : "chart"}
        width={compact ? width : undefined}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        tabIndex={0}
        aria-label={`${ariaLabel} time series. Use arrow keys to inspect values; Escape closes the tooltip.`}
        aria-describedby={announcementId}
        onBlur={() => setActiveIndex(null)}
        onKeyDown={handleKey}
        onPointerDown={(event) => {
          const next = indexAtClientX(event.clientX, event.currentTarget);
          setActiveIndex((current) =>
            event.pointerType !== "mouse" && current === next ? null : next,
          );
          event.currentTarget.focus({ preventScroll: true });
        }}
        onPointerMove={(event) => {
          if (event.pointerType === "mouse") handlePointer(event);
        }}
        onPointerCancel={() => setActiveIndex(null)}
        onPointerLeave={(event) => {
          if (event.pointerType === "mouse") setActiveIndex(null);
        }}
      >
        {grid
          ? [0.25, 0.5, 0.75].map((fraction) => (
              <line
                key={fraction}
                className="grid-ln"
                x1="0"
                y1={height * fraction}
                x2={width}
                y2={height * fraction}
              />
            ))
          : null}
        {plotted.flatMap((item, seriesIndex) =>
          item.area
            ? item.segments.map((segment, segmentIndex) => {
                if (segment.length < 2) return null;
                const firstX = x(segment[0].at).toFixed(1);
                const lastX = x(segment[segment.length - 1].at).toFixed(1);
                return (
                  <polygon
                    key={`a${seriesIndex}-${segmentIndex}`}
                    className="chart-area"
                    points={`${coordinates(segment)} ${lastX},${height} ${firstX},${height}`}
                    style={{ fill: item.color }}
                  />
                );
              })
            : [],
        )}
        {plotted.flatMap((item, seriesIndex) =>
          item.segments.map((segment, segmentIndex) =>
            segment.length > 1 ? (
              <polyline
                key={`l${seriesIndex}-${segmentIndex}`}
                className={compact ? "spark-ln" : "chart-ln"}
                points={coordinates(segment)}
                style={{ stroke: item.color }}
                vectorEffect="non-scaling-stroke"
              />
            ) : (
              <circle
                key={`l${seriesIndex}-${segmentIndex}`}
                className="chart-isolated-point"
                cx={x(segment[0].at)}
                cy={y(segment[0].value)}
                r={compact ? 1.5 : 2}
                style={{ fill: item.color }}
                vectorEffect="non-scaling-stroke"
              />
            ),
          ),
        )}
        {activeX != null ? (
          <>
            <line
              className="chart-crosshair"
              x1={activeX}
              y1="0"
              x2={activeX}
              y2={height}
              vectorEffect="non-scaling-stroke"
              aria-hidden="true"
            />
            {activeRows.map((row, index) =>
              row.point ? (
                <circle
                  key={`p${index}`}
                  className="chart-point"
                  cx={activeX}
                  cy={y(row.point.value)}
                  r={compact ? 2 : 2.5}
                  style={{ fill: row.color }}
                  vectorEffect="non-scaling-stroke"
                  aria-hidden="true"
                />
              ) : null,
            )}
          </>
        ) : null}
      </svg>
      <span id={announcementId} className="plot-announcement" aria-live="polite" aria-atomic="true">
        {activeDescription}
      </span>
      {activeAt != null ? (
        <div
          aria-hidden="true"
          className={`chart-tooltip ${compact ? "compact" : ""} ${tooltipSide}`}
          style={{ left: `${activePercent}%` }}
        >
          <time dateTime={new Date(activeAt * 1_000).toISOString()}>
            {formatChartTimestamp(activeAt)}
          </time>
          {activeRows.map((row, index) => (
            <span className="chart-tooltip-row" key={index}>
              {compact ? null : <i style={{ background: row.color }} aria-hidden="true" />}
              {compact ? null : <span>{row.label}</span>}
              <b>{row.point ? row.formatValue(row.point.value) : "—"}</b>
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

/** A compact interactive time-series, scaled to its own value range. */
export function Spark({
  points,
  color = "var(--accent)",
  width = 108,
  height = 42,
  label,
  formatValue,
}: {
  points: MetricPoint[];
  color?: string;
  width?: number;
  height?: number;
  label: string;
  formatValue: (value: number) => string;
}) {
  return (
    <TimePlot
      series={[{ points, color, label, formatValue }]}
      width={width}
      height={height}
      compact
      grid={false}
      includeZero={false}
      ariaLabel={label}
    />
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
      <select
        aria-label={label ?? "Time range"}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

/** An interactive line/area chart with timestamp-aware scaling and inspection. */
export function Chart({
  series,
  height = 96,
  ariaLabel,
}: {
  series: Series[];
  height?: number;
  ariaLabel: string;
}) {
  const W = 300;
  return (
    <TimePlot
      series={series}
      width={W}
      height={height}
      compact={false}
      grid
      includeZero
      ariaLabel={ariaLabel}
    />
  );
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
  tone,
  hideIfZero = false,
}: {
  title: ReactNode;
  value: ReactNode;
  sub?: ReactNode;
  series: Series[];
  tone?: BarTone;
  hideIfZero?: boolean;
}) {
  const plottable = series.some((s) => s.points.length > 1);
  if (
    !plottable ||
    (hideIfZero && series.every((s) => s.points.every((point) => point.value === 0)))
  ) {
    return null;
  }
  return (
    <Card title={title} sub={sub}>
      <div className={tone ? `chart-val ${tone}` : "chart-val"}>{value}</div>
      <Chart series={series} ariaLabel={typeof title === "string" ? title : "Metric"} />
    </Card>
  );
}
