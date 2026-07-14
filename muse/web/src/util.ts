// Small shared helpers used across panels: text search, URL/time formatting,
// accelerator-utilization math, and the empty-list message.

import type { BarTone } from "./types";

/** Capacity-rollup bucket for nodes with no topology label — not a real placement
 *  domain, so the placement planner excludes it. */
export const UNASSIGNED_GROUP = "unassigned";

/**
 * Case-insensitive "does any field contain the query?" — the filter behind every
 * panel search box. An empty query matches everything; `undefined` fields are
 * skipped, so callers can spread optional/derived fields freely.
 */
export function matches(query: string, ...fields: (string | undefined)[]): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return fields.some((f) => f != null && f.toLowerCase().includes(q));
}

/** Format an integer with locale grouping (e.g. 11556 → "11,556"). */
export const num = (v: number): string => v.toLocaleString();

// Metric formatters, shared across the dashboard + detail panels. `null` → em dash.
/** A rate/count: large values rounded, small keep one decimal. */
export const fmtRate = (v: number | null): string =>
  v == null ? "—" : v >= 100 ? String(Math.round(v)) : v.toFixed(1);
/** A latency in seconds → "Nms" (<1s) or "N.NNs". */
export const fmtLatency = (s: number | null): string =>
  s == null ? "—" : s < 1 ? `${Math.round(s * 1000)}ms` : `${s.toFixed(2)}s`;
/** A percentage with two decimals (for small error rates). */
export const fmtErrPct = (v: number | null): string => (v == null ? "—" : `${v.toFixed(2)}%`);
/** A percentage with one decimal. */
export const fmtPct = (v: number | null): string => (v == null ? "—" : `${v.toFixed(1)}%`);
/** An integer count. */
export const fmtInt = (v: number | null): string => (v == null ? "—" : String(Math.round(v)));

/** Shorten an opaque/long identifier (e.g. a topology-label hash) for display,
 *  keeping the head and tail so it's still recognisable. */
export function shortId(s: string, max = 16): string {
  return s.length > max ? `${s.slice(0, 6)}…${s.slice(-4)}` : s;
}

/** Host portion of a URL (for a compact endpoint chip), or the raw string if it
 *  doesn't parse. */
export function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

/** Relative "Ns/Nm/Nh/Nd ago" from an epoch-seconds timestamp. */
export function ago(atSecs: number): string {
  const secs = Math.max(0, Math.floor(Date.now() / 1000) - atSecs);
  if (secs >= 86_400) return `${Math.floor(secs / 86_400)}d ago`;
  if (secs >= 3_600) return `${Math.floor(secs / 3_600)}h ago`;
  if (secs >= 60) return `${Math.floor(secs / 60)}m ago`;
  return `${secs}s ago`;
}

/** Parse a formatted age ("7d" / "3h" / "10m" / "30s") back to seconds, for
 *  sorting. Returns -1 when it doesn't parse. */
export function ageToSecs(age?: string): number {
  const m = /^(\d+)\s*([smhd])/.exec(age ?? "");
  if (!m) return -1;
  const v = Number(m[1]);
  return m[2] === "d" ? v * 86400 : m[2] === "h" ? v * 3600 : m[2] === "m" ? v * 60 : v;
}

/** Accelerator utilization percent (allocated / allocatable), 0 when empty. */
export function utilPct(g: { allocatable: number; allocated: number }): number {
  return g.allocatable > 0 ? Math.round((g.allocated / g.allocatable) * 100) : 0;
}

/** Utilization tone by percent: ≥90 red, ≥75 amber, else green. */
export function utilTone(pct: number): BarTone {
  if (pct >= 90) return "red";
  if (pct >= 75) return "amber";
  return "green";
}

/** Copy to clipboard, resolving to whether it succeeded. The async Clipboard API
 *  no-ops in non-secure/unfocused/iframe contexts, so fall back to a `<textarea>` +
 *  `execCommand` and report the real outcome (callers flag "copied" only if so). */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall through to the legacy execCommand path below.
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}

/** Message for an empty list slot: error first, then loading, else the empty
 *  text. Callers pass the exact copy so wording stays panel-specific. */
export function emptyText(
  s: { loading: boolean; error: string | null },
  loading: string,
  empty: string,
): string {
  return s.error ?? (s.loading ? loading : empty);
}
