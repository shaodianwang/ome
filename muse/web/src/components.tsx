// Small reusable presentational pieces shared across panels.
import { useState } from "react";
import type { CSSProperties, ReactNode } from "react";

import { Copy } from "./icons";
import type { Selected } from "./resource";
import { argLines } from "./runtime";
import type { BarTone, Tone } from "./types";
import { copyText, COPY_FEEDBACK_MS, num } from "./util";
import { Track } from "./viz";

/** A one-click "copy this kubectl command" chip. muse never runs it — the
 *  operator runs it with their own (RBAC'd, audited) credentials. */
export function CopyCmd({ label, cmd }: { label: string; cmd: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="copybtn"
      title={cmd}
      onClick={async () => {
        if (await copyText(cmd)) {
          setCopied(true);
          setTimeout(() => setCopied(false), COPY_FEEDBACK_MS);
        }
      }}
    >
      {copied ? "✓ copied" : label}
    </button>
  );
}

/** A status pill. `dot` hides the leading status dot for purely-textual pills
 *  (scope/topology tags). */
export function Pill({
  tone,
  label,
  title,
  style,
  dot = true,
}: {
  tone: Tone;
  label: string;
  title?: string;
  style?: CSSProperties;
  dot?: boolean;
}) {
  return (
    <span className={`pill ${tone}`} title={title} style={style}>
      {dot ? <span className="pdot" /> : null}
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

/** The standard filter-bar search box: a growable input, optionally with a
 *  leading icon. */
export function SearchInput({
  ariaLabel,
  placeholder,
  value,
  onChange,
  icon,
}: {
  ariaLabel: string;
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
  icon?: ReactNode;
}) {
  return (
    <div className="input grow">
      {icon}
      <input
        aria-label={ariaLabel}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
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
        {detail ? (
          <>
            {" "}
            <span>{detail}</span>
          </>
        ) : null}
      </div>
      {action ? (
        <button type="button" className="btn ghost sm" onClick={action.onClick}>
          {action.label}
        </button>
      ) : null}
    </div>
  );
}

/** Stale-data notice for a snapshot-holding query: warn while refreshes fail but
 *  the last snapshot is still on screen; escalate to danger when nothing has
 *  loaded (omit `unavailableTitle` to render nothing in that case). Renders
 *  nothing while the query is healthy. */
export function StaleNotice({
  error,
  hasData,
  title,
  detail,
  unavailableTitle,
  unavailableDetail,
  onRetry,
}: {
  error: string | null;
  hasData: boolean;
  title: string;
  detail?: ReactNode;
  unavailableTitle?: string;
  unavailableDetail?: ReactNode;
  onRetry?: () => void;
}) {
  if (!error) return null;
  const action = onRetry ? { label: "Retry", onClick: onRetry } : undefined;
  if (!hasData) {
    if (!unavailableTitle) return null;
    return (
      <DataNotice
        tone="danger"
        title={unavailableTitle}
        detail={unavailableDetail}
        action={action}
      />
    );
  }
  return <DataNotice tone="warning" title={title} detail={detail} action={action} />;
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

/** A serving container's command: collapsible (collapsed by default) with a copy of
 *  the single-line invocation; expands to one flag per line. */
export function CommandBlock({
  label = "Command",
  command,
  args,
}: {
  label?: string;
  command?: string[];
  args?: string[];
}) {
  const [copied, setCopied] = useState(false);
  const oneLine = [...(command ?? []), ...(args ?? [])].join(" ");
  return (
    <details className="collapse">
      <summary>
        {label}
        <button
          className="icon-btn sm collapse-copy"
          title="Copy command"
          onClick={(e) => {
            e.stopPropagation();
            e.preventDefault();
            void navigator.clipboard?.writeText(oneLine);
            setCopied(true);
            setTimeout(() => setCopied(false), COPY_FEEDBACK_MS);
          }}
        >
          {copied ? "✓" : <Copy />}
        </button>
      </summary>
      <pre className="argbox">{argLines(command, args)}</pre>
    </details>
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
