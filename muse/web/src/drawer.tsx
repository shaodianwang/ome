// Shared slide-over drawer + the small presentational primitives its detail
// bodies are built from (sections, key/value facts, a copyable value, a
// condition list). Panel-specific bodies live with their panels.
import type { ReactNode } from "react";
import { useEffect, useId, useRef, useState } from "react";

import { Close, Copy } from "./icons";
import { COPY_FEEDBACK_MS, copyText } from "./util";

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
  const titleId = useId();
  const drawerRef = useRef<HTMLElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    const previousFocus =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    closeRef.current?.focus();

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        onCloseRef.current();
        return;
      }
      if (e.key !== "Tab") return;

      const focusable = Array.from(
        drawerRef.current?.querySelectorAll<HTMLElement>(
          'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
        ) ?? [],
      ).filter((element) => !element.hidden);
      if (!focusable.length) {
        e.preventDefault();
        drawerRef.current?.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = previousOverflow;
      previousFocus?.focus();
    };
  }, []);

  return (
    <div className="drawer-scrim" onClick={onClose}>
      <aside
        ref={drawerRef}
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        onClick={(e) => e.stopPropagation()}
      >
        <header className="drawer-head">
          <div className="drawer-title">
            <h2 id={titleId}>{title}</h2>
            {subtitle ? <div className="drawer-sub">{subtitle}</div> : null}
          </div>
          <button ref={closeRef} className="icon-btn" onClick={onClose} aria-label="Close">
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

/** Monospace value with a copy-to-clipboard button. `display` substitutes a
 *  short label for the copied text (e.g. an image tag for its full ref). */
export function Copyable({ text, display }: { text: string; display?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <span className="copyable">
      <span className="mono">{display ?? text}</span>
      <button
        className="icon-btn sm"
        title="Copy"
        aria-label="Copy"
        onClick={() => {
          void copyText(text).then((ok) => {
            if (!ok) return;
            setCopied(true);
            setTimeout(() => setCopied(false), COPY_FEEDBACK_MS);
          });
        }}
      >
        {copied ? "✓" : <Copy />}
      </button>
    </span>
  );
}
