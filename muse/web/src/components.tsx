// Small reusable presentational pieces shared across panels.
import type { CSSProperties, ReactNode } from "react";

import type { Selected } from "./resource";
import type { BarTone, Tone } from "./types";
import { num } from "./util";
import { Track } from "./viz";

export function Pill({ tone, label, style }: { tone: Tone; label: string; style?: CSSProperties }) {
  return (
    <span className={`pill ${tone}`} style={style}>
      <span className="pdot" />
      {label}
    </span>
  );
}

/** Boxed table-cell bar — the bordered variant of the shared {@link Track}. */
export function Bar(props: {
  pct: number;
  tone?: BarTone;
  striped?: boolean;
  width?: number | string;
}) {
  return <Track {...props} bordered />;
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
      <select aria-label="Cluster" value={value} onChange={(e) => onChange(e.target.value)}>
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

/** Compact page-level status for partial or unavailable data, optionally retryable. */
export function DataNotice({
  tone,
  title,
  detail,
  action,
}: {
  tone: "danger" | "warning";
  title: string;
  detail?: ReactNode;
  action?: { label: string; onClick: () => void };
}) {
  return (
    <div className={`data-notice ${tone}`} role={tone === "danger" ? "alert" : "status"}>
      <div>
        <strong>{title}</strong>
        {detail ? <span>{detail}</span> : null}
      </div>
      {action ? (
        <button type="button" className="btn ghost sm" onClick={action.onClick}>
          {action.label}
        </button>
      ) : null}
    </div>
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

/** A list of deployment references, each a link to its detail; an empty note otherwise. */
export function RefList({
  items,
  onOpen,
  empty,
}: {
  items: { cluster: string; name: string; namespace?: string }[];
  onOpen: (ref: Selected) => void;
  empty: string;
}) {
  if (!items.length) return <div className="muted">{empty}</div>;
  return (
    <ul className="ref-list">
      {items.map((s) => (
        <li key={`${s.cluster}/${s.namespace ?? ""}/${s.name}`}>
          <button
            className="linkbtn mono"
            onClick={() =>
              onOpen({
                cluster: s.cluster,
                kind: "InferenceService",
                name: s.name,
                namespace: s.namespace,
              })
            }
          >
            {s.name}
          </button>
          <span className="muted">
            {" "}
            · {s.namespace ?? "—"} · {s.cluster}
          </span>
        </li>
      ))}
    </ul>
  );
}

/** A container's environment as stacked key/value rows — long keys and values wrap
 *  full-width rather than overlapping a fixed column. */
export function EnvList({ env }: { env: { name: string; value?: string }[] }) {
  return (
    <ul className="envlist">
      {env.map((e) => (
        <li key={e.name}>
          <span className="envk">{e.name}</span>=
          {e.value ?? <span className="muted">(from ref)</span>}
        </li>
      ))}
    </ul>
  );
}
