// Client engine for the deployment Logs tab: one dedicated EventSource per
// open tab (never the shared activity stream), a bounded ring buffer of parsed
// rows, and client-managed reconnect.
//
// Reconnect deliberately bypasses the browser's native EventSource retry: the
// native path replays the original URL verbatim, which would re-dump the whole
// initial catch-up window into the buffer on every server deploy. Instead the
// stream is closed on any error and reopened with `tail` dropped and
// `sinceSecs` recomputed from the newest line already received; the small
// remaining overlap is removed by deduplicating incoming lines against the
// buffer tail on (pod, timestamp, text).

const MS_PER_SEC = 1_000;

// -----------------------------------------------------------------------------
// Stream URL
// -----------------------------------------------------------------------------

/** The caller's narrowing of one log session; mirrors the server's query
 *  parameters for `GET /v1/logs/stream`. */
export interface LogStreamParams {
  cluster: string;
  namespace: string;
  isvc: string;
  component?: string;
  pod?: string;
  container?: string;
  /** History lines per tail at session start; the server clamps to its cap. */
  tail?: number;
  /** Catch-up window: only lines newer than now − N seconds. */
  sinceSecs?: number;
}

/** Build the stream URL: required identity plus whichever narrowing is set. */
export function logStreamUrl(endpoint: string, params: LogStreamParams): string {
  const sp = new URLSearchParams({
    cluster: params.cluster,
    namespace: params.namespace,
    isvc: params.isvc,
  });
  if (params.component) sp.set("component", params.component);
  if (params.pod) sp.set("pod", params.pod);
  if (params.container) sp.set("container", params.container);
  if (params.tail != null) sp.set("tail", String(params.tail));
  if (params.sinceSecs != null) sp.set("sinceSecs", String(params.sinceSecs));
  return `${endpoint}?${sp}`;
}

// -----------------------------------------------------------------------------
// Rows and the bounded ring buffer
// -----------------------------------------------------------------------------

/** One parsed log line. `seq` is a session-monotonic id for stable list keys. */
export interface LogLineRow {
  kind: "line";
  seq: number;
  pod: string;
  container: string;
  /** Kubelet RFC3339 timestamp; empty when the runtime omitted it. */
  ts: string;
  /** `ts` parsed to epoch milliseconds, or null when empty/unparseable. */
  tsMs: number | null;
  text: string;
  /** The server cut this line at its per-line byte cap. */
  truncated: boolean;
}
/** An out-of-band session condition rendered inline in the log flow. */
export interface LogNoticeRow {
  kind: "notice";
  seq: number;
  text: string;
}
/** A subtle divider (reconnects, session end) rendered inline. */
export interface LogMarkerRow {
  kind: "marker";
  seq: number;
  text: string;
}
export type LogRow = LogLineRow | LogNoticeRow | LogMarkerRow;

/** A bounded FIFO of log rows: pushing beyond capacity evicts the oldest row —
 *  memory stays flat over hour-long sessions — and counts what fell off so the
 *  view can say "older lines dropped" instead of silently forgetting. */
export class LogRing {
  private rows: LogRow[] = [];
  private head = 0;
  /** Rows evicted since the last clear. */
  evicted = 0;

  constructor(readonly capacity: number) {}

  get length(): number {
    return this.rows.length - this.head;
  }

  push(row: LogRow): void {
    this.rows.push(row);
    if (this.length > this.capacity) {
      this.head += 1;
      this.evicted += 1;
      // Reclaim the dead prefix once it matches the live span, so the backing
      // array is at most twice the capacity.
      if (this.head >= this.capacity) {
        this.rows = this.rows.slice(this.head);
        this.head = 0;
      }
    }
  }

  /** The buffered rows, oldest first, as a fresh array. */
  snapshot(): LogRow[] {
    return this.rows.slice(this.head);
  }

  clear(): void {
    this.rows = [];
    this.head = 0;
    this.evicted = 0;
  }
}

// -----------------------------------------------------------------------------
// Wire parsing
// -----------------------------------------------------------------------------

/** One `pods` roster entry: a pod the session is tailing. */
export interface LogRosterPod {
  name: string;
  component?: string;
  containers: string[];
}
/** A `pods` roster snapshot: the tailed pods plus how many matched in total. */
export interface LogRoster {
  pods: LogRosterPod[];
  total: number;
  capped: boolean;
}

/** A `notice` event; `kind` discriminates, the remaining fields are per-kind. */
export interface LogNotice {
  kind: string;
  [key: string]: unknown;
}

const parseJson = (data: string): unknown => {
  try {
    return JSON.parse(data);
  } catch {
    return null;
  }
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value != null && !Array.isArray(value);

/**
 * Parse one batched `log` frame — `{ p, ct, co?, ls: [[ts, line, truncated?],…] }`
 * — into line rows. A malformed frame yields no rows rather than a broken view.
 * `seq` numbers are assigned by the caller; rows here carry a placeholder.
 */
export function parseLogFrame(data: string): Omit<LogLineRow, "seq">[] {
  const frame = parseJson(data);
  if (!isRecord(frame)) return [];
  const { p: pod, ct: container, ls: lines } = frame;
  if (typeof pod !== "string" || typeof container !== "string" || !Array.isArray(lines)) return [];
  const rows: Omit<LogLineRow, "seq">[] = [];
  for (const entry of lines) {
    if (!Array.isArray(entry) || typeof entry[0] !== "string" || typeof entry[1] !== "string")
      continue;
    const ts = entry[0];
    const parsed = ts ? Date.parse(ts) : Number.NaN;
    rows.push({
      kind: "line",
      pod,
      container,
      ts,
      tsMs: Number.isFinite(parsed) ? parsed : null,
      text: entry[1],
      truncated: entry[2] === true,
    });
  }
  return rows;
}

/** Parse a `pods` roster snapshot; null when the payload is malformed. */
export function parseRoster(data: string): LogRoster | null {
  const snapshot = parseJson(data);
  if (!isRecord(snapshot) || !Array.isArray(snapshot.pods)) return null;
  const pods: LogRosterPod[] = [];
  for (const entry of snapshot.pods) {
    if (!isRecord(entry) || typeof entry.name !== "string") continue;
    pods.push({
      name: entry.name,
      component: typeof entry.component === "string" ? entry.component : undefined,
      containers: Array.isArray(entry.containers)
        ? entry.containers.filter((c): c is string => typeof c === "string")
        : [],
    });
  }
  return {
    pods,
    total: typeof snapshot.total === "number" ? snapshot.total : pods.length,
    capped: snapshot.capped === true,
  };
}

/** Parse a `notice` event; null when the payload is malformed. */
export function parseNotice(data: string): LogNotice | null {
  const notice = parseJson(data);
  if (!isRecord(notice) || typeof notice.kind !== "string") return null;
  return notice as LogNotice;
}

const str = (value: unknown): string => (typeof value === "string" ? value : "?");

/** Inline display text for a notice. The `truncated-pods` kind is not rendered
 *  inline — it drives the persistent cap bar — and `session-end` is a marker. */
export function noticeText(notice: LogNotice): string {
  switch (notice.kind) {
    case "dropped":
      return `${typeof notice.count === "number" ? notice.count : "?"} lines dropped by the server rate limit`;
    case "tail-error":
      return `${str(notice.pod)}/${str(notice.container)}: tail error — ${str(notice.error)} (retrying)`;
    case "tail-ended":
      return `${str(notice.pod)}/${str(notice.container)}: tail ended — pod left the roster`;
    case "session-end":
      return `session ended after ${typeof notice.afterSecs === "number" ? notice.afterSecs : "?"}s (server limit) — reconnecting`;
    default:
      return notice.kind;
  }
}

// -----------------------------------------------------------------------------
// Pod hues
// -----------------------------------------------------------------------------

/** Number of `.loghue-N` classes in styles/logs.css. */
export const LOG_HUE_COUNT = 6;

/** Stable pod-name → hue index (FNV-1a), so a pod keeps its color across
 *  renders, reconnects, and sessions. */
export function podHue(pod: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < pod.length; i++) {
    hash ^= pod.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0) % LOG_HUE_COUNT;
}

// -----------------------------------------------------------------------------
// Client-side grep
// -----------------------------------------------------------------------------

const escapeLiteral = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Compile the grep box: a literal (case-insensitive) substring by default;
 * wrap the pattern in slashes (`/pattern/`) for a regular expression, matching
 * the toolbar hint. An invalid regex falls back to a literal match of the raw
 * input, so a half-typed pattern never blanks the pane. The compiled regex is
 * case-insensitive and non-global, so `test` is safe to reuse across lines.
 */
export function compileGrep(input: string): RegExp | null {
  const trimmed = input.trim();
  if (!trimmed) return null;
  let regex: RegExp | null = null;
  if (trimmed.length > 2 && trimmed.startsWith("/") && trimmed.endsWith("/")) {
    try {
      regex = new RegExp(trimmed.slice(1, -1), "i");
    } catch {
      regex = null;
    }
  }
  return regex ?? new RegExp(escapeLiteral(trimmed), "i");
}

/** Split a line into plain/matched segments for highlight rendering. */
export function grepSegments(text: string, grep: RegExp): { text: string; hit: boolean }[] {
  const global = new RegExp(grep.source, "gi");
  const segments: { text: string; hit: boolean }[] = [];
  let consumed = 0;
  for (const match of text.matchAll(global)) {
    if (match[0] === "") continue; // zero-width match: nothing to highlight
    const index = match.index ?? 0;
    if (index > consumed) segments.push({ text: text.slice(consumed, index), hit: false });
    segments.push({ text: match[0], hit: true });
    consumed = index + match[0].length;
  }
  if (consumed < text.length || segments.length === 0) {
    segments.push({ text: text.slice(consumed), hit: false });
  }
  return segments;
}

// -----------------------------------------------------------------------------
// Reconnect math
// -----------------------------------------------------------------------------

/**
 * The reconnect catch-up window: whole seconds spanning now back to the newest
 * line received, rounded up so the overlap errs toward duplicates — which the
 * buffer-tail dedupe removes — rather than gaps. Never below one second (the
 * server rejects a non-positive `sinceSecs`).
 */
export function reconnectSinceSecs(lastTsMs: number, nowMs: number): number {
  return Math.max(1, Math.ceil((nowMs - lastTsMs) / MS_PER_SEC));
}

const dedupeKey = (pod: string, ts: string, text: string): string => `${pod} ${ts} ${text}`;

// -----------------------------------------------------------------------------
// The stream controller
// -----------------------------------------------------------------------------

export type LogStreamPhase = "connecting" | "live" | "reconnecting";

/** The EventSource surface the controller drives (injectable for tests). */
export interface LogEventSourceLike {
  onopen: (() => void) | null;
  onerror: (() => void) | null;
  addEventListener(type: string, listener: (event: { data: string }) => void): void;
  close(): void;
}

export interface LogStreamDependencies {
  createSource: (url: string) => LogEventSourceLike;
  scheduleReconnect: (callback: () => void, delayMs: number) => () => void;
  /** Clock override for tests; defaults to `Date.now`. */
  now?: () => number;
}

export interface LogStreamOptions {
  /** Ring-buffer capacity — the server-supplied client scrollback advice. */
  bufferLines: number;
  /** Wait before reopening a failed stream with a recomputed `sinceSecs`. */
  reconnectDelayMs: number;
  /** Called after every state change; the view re-reads `state()` and the ring. */
  onUpdate: () => void;
}

export interface LogStreamState {
  phase: LogStreamPhase;
  roster: LogRoster | null;
  /** The server pod cap in effect: tailing `shown` of `total` matching pods. */
  cap: { shown: number; total: number } | null;
  /** Lines accepted into the buffer since the last clear. */
  lineCount: number;
}

export interface LogStream {
  readonly ring: LogRing;
  state(): LogStreamState;
  start(): void;
  stop(): void;
  /** Empty the buffer and reset the per-view counters; the stream stays open. */
  clearRows(): void;
}

/**
 * One log-stream session against `GET /v1/logs/stream`. The caller owns the
 * lifecycle: `start()` when the Logs tab opens, `stop()` on tab switch or
 * unmount, and a fresh controller whenever the server-side narrowing changes.
 */
export function createLogStream(
  endpoint: string,
  params: LogStreamParams,
  options: LogStreamOptions,
  dependencies: LogStreamDependencies,
): LogStream {
  const now = dependencies.now ?? Date.now;
  const ring = new LogRing(options.bufferLines);

  let source: LogEventSourceLike | null = null;
  let cancelReconnect: (() => void) | null = null;
  let stopped = false;
  let seq = 0;

  let phase: LogStreamPhase = "connecting";
  let roster: LogRoster | null = null;
  let cap: { shown: number; total: number } | null = null;
  let lineCount = 0;
  /** Newest line timestamp seen — the reconnect resume cursor. */
  let lastTsMs: number | null = null;
  /** Overlap keys active for the current post-reconnect connection. */
  let overlap: Set<string> | null = null;

  const pushRow = (row: Omit<LogNoticeRow, "seq"> | Omit<LogMarkerRow, "seq">): void => {
    ring.push({ ...row, seq: seq++ });
  };

  const onLog = (event: { data: string }): void => {
    const lines = parseLogFrame(event.data);
    if (!lines.length) return;
    for (const line of lines) {
      if (overlap?.has(dedupeKey(line.pod, line.ts, line.text))) {
        continue;
      }
      if (line.tsMs != null && (lastTsMs == null || line.tsMs > lastTsMs)) {
        lastTsMs = line.tsMs;
      }
      ring.push({ ...line, seq: seq++ });
      lineCount += 1;
    }
    options.onUpdate();
  };

  const onPods = (event: { data: string }): void => {
    const snapshot = parseRoster(event.data);
    if (!snapshot) return;
    roster = snapshot;
    cap = snapshot.capped ? { shown: snapshot.pods.length, total: snapshot.total } : null;
    options.onUpdate();
  };

  const onNotice = (event: { data: string }): void => {
    const notice = parseNotice(event.data);
    if (!notice) return;
    switch (notice.kind) {
      case "truncated-pods":
        // Drives the persistent cap bar; the roster snapshot is authoritative,
        // so only fill in when the roster has not reported the cap yet.
        if (!cap && typeof notice.shown === "number" && typeof notice.total === "number") {
          cap = { shown: notice.shown, total: notice.total };
        }
        break;
      case "session-end":
        pushRow({ kind: "marker", text: noticeText(notice) });
        break;
      default:
        pushRow({ kind: "notice", text: noticeText(notice) });
    }
    options.onUpdate();
  };

  const connect = (url: string): void => {
    const next = dependencies.createSource(url);
    source = next;
    next.onopen = () => {
      if (source !== next) return;
      phase = "live";
      options.onUpdate();
    };
    next.onerror = () => {
      if (source !== next) return;
      // Close on any error — transient or terminal — so the browser's native
      // retry (which would replay the original catch-up window) never runs.
      next.close();
      source = null;
      phase = "reconnecting";
      pushRow({ kind: "marker", text: "stream interrupted — reconnecting" });
      options.onUpdate();
      cancelReconnect = dependencies.scheduleReconnect(() => {
        cancelReconnect = null;
        if (!stopped) reopen();
      }, options.reconnectDelayMs);
    };
    next.addEventListener("log", onLog);
    next.addEventListener("pods", onPods);
    next.addEventListener("notice", onNotice);
  };

  const reopen = (): void => {
    if (lastTsMs == null) {
      // Nothing received yet: the original request replays nothing the user
      // has seen, so reuse it (including its tail/since catch-up) verbatim.
      overlap = null;
      connect(logStreamUrl(endpoint, params));
      return;
    }
    const sinceSecs = reconnectSinceSecs(lastTsMs, now());
    // The server replays every line newer than its now − sinceSecs — its
    // clock, not ours — so any timestamped buffered line may arrive again.
    // The whole buffered tail forms the dedupe set: an exact
    // (pod, timestamp, text) key never collides with a genuinely new line,
    // so over-inclusion costs nothing. Untimestamped lines are excluded —
    // they cannot be told apart from legitimate repeats.
    const keys = new Set<string>();
    for (const row of ring.snapshot()) {
      if (row.kind === "line" && row.tsMs != null) {
        keys.add(dedupeKey(row.pod, row.ts, row.text));
      }
    }
    overlap = keys;
    connect(logStreamUrl(endpoint, { ...params, tail: undefined, sinceSecs }));
  };

  return {
    ring,
    state: () => ({ phase, roster, cap, lineCount }),
    start() {
      if (source || stopped) return;
      connect(logStreamUrl(endpoint, params));
    },
    stop() {
      stopped = true;
      cancelReconnect?.();
      cancelReconnect = null;
      source?.close();
      source = null;
    },
    clearRows() {
      ring.clear();
      lineCount = 0;
      options.onUpdate();
    },
  };
}
