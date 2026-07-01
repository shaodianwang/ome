// Shared slide-over drawer + the small presentational primitives its detail
// bodies are built from (sections, key/value facts, a copyable value, a
// condition list). Panel-specific bodies live with their panels.
import type { ReactNode } from "react";
import { useEffect, useState } from "react";

import type { ConditionItem } from "./api";
import { Close, Copy } from "./icons";

/** Right-side slide-over. Click-scrim or Escape closes; body scrolls. */
export function Drawer({
  title,
  subtitle,
  onClose,
  children,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  onClose: () => void;
  children: ReactNode;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="drawer-scrim" onClick={onClose}>
      <aside
        className="drawer"
        role="dialog"
        aria-modal="true"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="drawer-head">
          <div className="drawer-title">
            <h2>{title}</h2>
            {subtitle ? <div className="drawer-sub">{subtitle}</div> : null}
          </div>
          <button className="icon-btn" onClick={onClose} aria-label="Close">
            <Close />
          </button>
        </header>
        <div className="drawer-body">{children}</div>
      </aside>
    </div>
  );
}

/** A titled block within a drawer body. Renders nothing when it has no children. */
export function Section({
  title,
  right,
  children,
}: {
  title: string;
  right?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="dsec">
      <div className="dsec-head">
        <h3>{title}</h3>
        {right}
      </div>
      {children}
    </section>
  );
}

/** A key/value fact grid; rows with an empty value are dropped. */
export function Facts({ items }: { items: [string, ReactNode][] }) {
  const rows = items.filter(([, v]) => v != null && v !== "");
  if (!rows.length) return null;
  return (
    <dl className="facts">
      {rows.map(([k, v]) => (
        <div className="fact" key={k}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

/** Monospace value with a copy-to-clipboard button. */
export function Copyable({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <span className="copyable">
      <span className="mono">{text}</span>
      <button
        className="icon-btn sm"
        title="Copy"
        aria-label="Copy"
        onClick={() => {
          void navigator.clipboard?.writeText(text);
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        }}
      >
        {copied ? "✓" : <Copy />}
      </button>
    </span>
  );
}

/** A list of status conditions, tone-coded True/False/Unknown. */
export function Conditions({ conditions }: { conditions?: ConditionItem[] }) {
  if (!conditions || conditions.length === 0) return null;
  const tone = (status: string) =>
    status === "True" ? "ok" : status === "False" ? "bad" : "unknown";
  return (
    <ul className="conds">
      {conditions.map((c) => (
        <li key={c.type} className={tone(c.status)}>
          <div className="cond-top">
            <span className="cond-dot" />
            <b>{c.type}</b>
            {c.reason ? <span className="cond-reason">{c.reason}</span> : null}
          </div>
          {c.message ? <div className="cond-msg">{c.message}</div> : null}
        </li>
      ))}
    </ul>
  );
}
