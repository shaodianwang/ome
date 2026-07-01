// Small reusable presentational pieces shared across panels.
import type { CSSProperties } from "react";

import type { BarTone, Cloud, Tone } from "./types";

export function Pill({ tone, label, style }: { tone: Tone; label: string; style?: CSSProperties }) {
  return (
    <span className={`pill ${tone}`} style={style}>
      <span className="pdot" />
      {label}
    </span>
  );
}

export function Bar({
  pct,
  tone = "",
  striped = false,
  width,
}: {
  pct: number;
  tone?: BarTone;
  striped?: boolean;
  width?: number | string;
}) {
  const cls = ["bar", tone, striped ? "striped" : ""].filter(Boolean).join(" ");
  return (
    <div className={cls} style={width !== undefined ? { width } : undefined}>
      <i style={{ width: `${pct}%` }} />
    </div>
  );
}

export function UtilCell({ pct, tone = "" }: { pct: number; tone?: BarTone }) {
  return (
    <div className="util-cell">
      <Bar pct={pct} tone={tone} />
      <span className="pct">{pct}%</span>
    </div>
  );
}

export function CloudDot({ cloud }: { cloud: Cloud }) {
  return <span className={`cdot ${cloud}`} />;
}

/** Cluster scope selector: "" = all clusters, else a specific cluster name. */
export function ClusterFilter({
  clusters,
  value,
  onChange,
}: {
  clusters: string[];
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <div className="input cluster-filter">
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        <option value="">All clusters</option>
        {clusters.map((c) => (
          <option key={c} value={c}>
            {c}
          </option>
        ))}
      </select>
    </div>
  );
}
