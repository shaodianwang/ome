// Small reusable presentational pieces shared across panels.
import type { CSSProperties, ReactNode } from "react";

import type { BarTone, Tone } from "./types";
import { num } from "./util";

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

/** Full-width placeholder row for a table body (loading / error / empty). */
export function EmptyRow({ colSpan, children }: { colSpan: number; children: ReactNode }) {
  return (
    <tr>
      <td colSpan={colSpan} className="muted empty-cell">
        {children}
      </td>
    </tr>
  );
}

const PAGE_SIZES = [25, 50, 100];

/** List footer: "X–Y of Z <noun>" plus a page-size selector and prev/next paging.
 *  Shared by the Deployments and Models tables so they page identically. */
export function Pager({
  total,
  page,
  totalPages,
  pageSize,
  start,
  noun,
  onPage,
  onPageSize,
}: {
  total: number;
  page: number;
  totalPages: number;
  pageSize: number;
  start: number;
  noun: string;
  onPage: (p: number) => void;
  onPageSize: (s: number) => void;
}) {
  return (
    <div className="list-foot pager">
      <span>
        {total
          ? `${num(start + 1)}–${num(Math.min(start + pageSize, total))} of ${num(total)}`
          : "0"}{" "}
        {noun}
      </span>
      <div className="pager-ctrls">
        <label className="page-size">
          per page
          <select value={pageSize} onChange={(e) => onPageSize(Number(e.target.value))}>
            {PAGE_SIZES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <button className="btn ghost sm" disabled={page === 0} onClick={() => onPage(page - 1)}>
          ‹ Prev
        </button>
        <span className="page-of tnum">
          {page + 1} / {totalPages}
        </span>
        <button
          className="btn ghost sm"
          disabled={page >= totalPages - 1}
          onClick={() => onPage(page + 1)}
        >
          Next ›
        </button>
      </div>
    </div>
  );
}
