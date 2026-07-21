import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode, UIEvent } from "react";

import { LOGS_STREAM_URL } from "../api";
import type { MetricsTimeRanges } from "../api";
import { compileGrep, createLogStream, grepSegments, podHue } from "../logStream";
import type { LogEventSourceLike, LogRow, LogStream } from "../logStream";
import type { Selected } from "../resource";
import { num } from "../util";
import { Dot, FilterChip } from "../viz";

/** Fixed row height for the virtualizer; must match `--log-row-h` in
 *  styles/logs.css. */
const ROW_PX = 20;
/** Rows rendered beyond the visible viewport on each side of the slice. */
const OVERSCAN_ROWS = 12;
/** Wait before a closed stream is reopened with a recomputed `sinceSecs`; the
 *  resume window bounds what a longer gap could miss, so this only trades
 *  reconnect churn against catch-up latency. */
const RECONNECT_DELAY_MS = 2_000;
/** How close to the bottom edge (px) still counts as following the tail. */
const FOLLOW_SNAP_PX = ROW_PX;

const GREP_PLACEHOLDER = "grep (client-side, /pattern/ for regex)";

const pad = (value: number, width: number) => String(value).padStart(width, "0");

/** Local wall-clock HH:MM:SS.mmm for a line timestamp; empty when unknown. */
function fmtLogTs(tsMs: number | null): string {
  if (tsMs == null) return "";
  const d = new Date(tsMs);
  return `${pad(d.getHours(), 2)}:${pad(d.getMinutes(), 2)}:${pad(d.getSeconds(), 2)}.${pad(d.getMilliseconds(), 3)}`;
}

/**
 * Live log viewer for one deployment: a toolbar of server-side narrowing
 * controls, a client-side grep, and a virtualized pane over the stream's
 * bounded ring buffer. Server-side filters (component / pod / container /
 * since) restart the stream so the server tails less; grep only narrows the
 * buffered view. Reconnects are client-managed by the stream engine
 * (see logStream.ts).
 */
export function LogsPane({
  sel,
  namespace,
  sinceOptions,
  bufferLines,
}: {
  sel: Selected;
  namespace?: string;
  sinceOptions?: MetricsTimeRanges;
  bufferLines?: number;
}) {
  // Server-side narrowing; each change tears the session down and reopens it.
  const [component, setComponent] = useState("");
  const [pod, setPod] = useState("");
  const [container, setContainer] = useState("");
  const [sinceId, setSinceId] = useState("");
  // Client-side view controls.
  const [grepInput, setGrepInput] = useState("");
  const [grepMode, setGrepMode] = useState<"highlight" | "filter">("highlight");
  const [followOn, setFollowOn] = useState(true);
  const [followSuspended, setFollowSuspended] = useState(false);
  const [paused, setPaused] = useState(false);
  const [frozen, setFrozen] = useState<LogRow[] | null>(null);
  const [wrap, setWrap] = useState(false);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportH, setViewportH] = useState(0);
  const [version, setVersion] = useState(0);

  const paneRef = useRef<HTMLDivElement | null>(null);
  const streamRef = useRef<LogStream | null>(null);
  /** Line count when follow was suspended — the resume chip's baseline. */
  const suspendBaseline = useRef(0);

  const activeSince = sinceOptions
    ? (sinceOptions.options.find((option) => option.id === sinceId) ??
      sinceOptions.options.find((option) => option.id === sinceOptions.default))
    : undefined;
  const sinceSecs = activeSince?.seconds;

  useEffect(() => {
    if (!namespace || bufferLines == null) return;
    setPaused(false);
    setFrozen(null);
    setFollowSuspended(false);
    setScrollTop(0);
    const stream = createLogStream(
      LOGS_STREAM_URL,
      {
        cluster: sel.cluster,
        namespace,
        isvc: sel.name,
        component: component || undefined,
        pod: pod || undefined,
        container: container || undefined,
        sinceSecs,
      },
      {
        bufferLines,
        reconnectDelayMs: RECONNECT_DELAY_MS,
        onUpdate: () => setVersion((v) => v + 1),
      },
      {
        createSource: (url) => new EventSource(url) as unknown as LogEventSourceLike,
        scheduleReconnect: (callback, delayMs) => {
          const timer = setTimeout(callback, delayMs);
          return () => clearTimeout(timer);
        },
      },
    );
    streamRef.current = stream;
    stream.start();
    return () => {
      stream.stop();
      streamRef.current = null;
    };
  }, [sel.cluster, sel.name, namespace, component, pod, container, sinceSecs, bufferLines]);

  // The pane's CSS fixes its height; measure once and track resizes.
  useEffect(() => {
    const pane = paneRef.current;
    if (!pane) return;
    const measure = () => setViewportH(pane.clientHeight);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(pane);
    return () => observer.disconnect();
  }, [namespace, bufferLines]);

  const stream = streamRef.current;
  const st = stream?.state() ?? null;
  const liveRows = useMemo(
    () => (stream ? stream.ring.snapshot() : []),
    // `version` is the ring's change signal — the memo must re-run on it.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [stream, version],
  );
  const grep = useMemo(() => compileGrep(grepInput), [grepInput]);
  const baseRows = paused && frozen ? frozen : liveRows;
  const viewRows = useMemo(
    () =>
      grep && grepMode === "filter"
        ? baseRows.filter((row) => row.kind !== "line" || grep.test(row.text))
        : baseRows,
    [baseRows, grep, grepMode],
  );

  const followActive = followOn && !followSuspended && !paused;
  const total = viewRows.length;
  useEffect(() => {
    if (!followActive || viewRows.length === 0) return;
    const pane = paneRef.current;
    if (pane) pane.scrollTop = pane.scrollHeight - pane.clientHeight;
  }, [followActive, viewRows]);

  if (!namespace) {
    return (
      <div className="muted attn-none">
        The namespace of this deployment is unknown; logs cannot be streamed.
      </div>
    );
  }
  if (bufferLines == null) {
    return (
      <div className="muted attn-none">
        Log streaming is enabled but the server did not supply a client buffer size.
      </div>
    );
  }

  const roster = st?.roster ?? null;
  const componentOptions = [...new Set((roster?.pods ?? []).map((p) => p.component ?? ""))]
    .filter(Boolean)
    .sort();
  const containerOptions = [...new Set((roster?.pods ?? []).flatMap((p) => p.containers))].sort();
  const podCount = roster?.pods.length ?? 0;
  const allPodsLabel = st?.cap
    ? `All (${num(st.cap.shown)} of ${num(st.cap.total)})`
    : `All (${num(podCount)})`;

  const phase = st?.phase ?? "connecting";
  const phaseLabel =
    phase === "live" ? "Live" : phase === "connecting" ? "Connecting" : "Reconnecting";
  const evicted = stream?.ring.evicted ?? 0;
  const pendingLines = followSuspended
    ? Math.max(0, (st?.lineCount ?? 0) - suspendBaseline.current)
    : 0;

  const scrollToTail = () => {
    const pane = paneRef.current;
    if (pane) pane.scrollTop = pane.scrollHeight - pane.clientHeight;
  };
  const resumeFollow = () => {
    setFollowOn(true);
    setFollowSuspended(false);
    scrollToTail();
  };
  const onScroll = (event: UIEvent<HTMLDivElement>) => {
    const pane = event.currentTarget;
    setScrollTop(pane.scrollTop);
    const atTail = pane.scrollHeight - pane.scrollTop - pane.clientHeight <= FOLLOW_SNAP_PX;
    if (atTail) {
      if (followSuspended) setFollowSuspended(false);
    } else if (followOn && !followSuspended) {
      suspendBaseline.current = st?.lineCount ?? 0;
      setFollowSuspended(true);
    }
  };
  const togglePause = () => {
    if (paused) {
      setPaused(false);
      setFrozen(null);
    } else {
      setPaused(true);
      setFrozen(liveRows);
    }
  };
  const clearRows = () => {
    streamRef.current?.clearRows();
    if (paused) setFrozen([]);
  };

  const renderRow = (row: LogRow): ReactNode => {
    if (row.kind === "notice") {
      return (
        <div className="log-noticerow" key={row.seq}>
          ⚠ {row.text}
        </div>
      );
    }
    if (row.kind === "marker") {
      return (
        <div className="log-marker" key={row.seq}>
          — {row.text} —
        </div>
      );
    }
    const shortName = row.pod.startsWith(`${sel.name}-`)
      ? row.pod.slice(sel.name.length + 1)
      : row.pod;
    const highlight = grep && grepMode === "highlight" && grep.test(row.text);
    return (
      <div className="logline" key={row.seq}>
        <span className="log-ts">{fmtLogTs(row.tsMs)}</span>
        <button
          type="button"
          className={`log-pfx loghue-${podHue(row.pod)}`}
          title={`${row.pod} · ${row.container} — click to filter to this pod`}
          onClick={() => setPod(row.pod)}
        >
          {shortName}
        </button>
        <span className="log-msg">
          {highlight && grep
            ? grepSegments(row.text, grep).map((segment, i) =>
                segment.hit ? <mark key={i}>{segment.text}</mark> : segment.text,
              )
            : row.text}
          {row.truncated ? (
            <span className="log-cut" title="line truncated at the server byte cap">
              …
            </span>
          ) : null}
        </span>
      </div>
    );
  };

  // Fixed-row virtualization: render only the scrolled-to slice (± overscan)
  // over a spacer sized for the full buffer. Wrapped lines have variable
  // height, so Wrap falls back to rendering the whole (bounded) buffer.
  const sliceStart = wrap ? 0 : Math.max(0, Math.floor(scrollTop / ROW_PX) - OVERSCAN_ROWS);
  const sliceEnd = wrap
    ? total
    : Math.min(total, Math.ceil((scrollTop + viewportH) / ROW_PX) + OVERSCAN_ROWS);
  const slice = viewRows.slice(sliceStart, sliceEnd);

  return (
    <div className="logview">
      <div className="logbar">
        <FilterChip
          label="Component"
          value={component}
          options={[
            { value: "", label: "All" },
            ...componentOptions.map((name) => ({ value: name, label: name })),
            // Keep the active narrowing selectable while the roster is narrowed.
            ...(component && !componentOptions.includes(component)
              ? [{ value: component, label: component }]
              : []),
          ]}
          onChange={setComponent}
        />
        <FilterChip
          label="Pod"
          value={pod}
          options={[
            { value: "", label: allPodsLabel },
            ...(roster?.pods ?? []).map((p) => ({ value: p.name, label: p.name })),
            ...(pod && !(roster?.pods ?? []).some((p) => p.name === pod)
              ? [{ value: pod, label: pod }]
              : []),
          ]}
          onChange={setPod}
        />
        <FilterChip
          label="Container"
          value={container}
          options={[
            { value: "", label: "All" },
            ...containerOptions.map((name) => ({ value: name, label: name })),
            ...(container && !containerOptions.includes(container)
              ? [{ value: container, label: container }]
              : []),
          ]}
          onChange={setContainer}
        />
        {activeSince && sinceOptions ? (
          <FilterChip
            label="Since"
            value={activeSince.id}
            options={sinceOptions.options.map((option) => ({
              value: option.id,
              label: option.label,
            }))}
            onChange={setSinceId}
            accent
          />
        ) : null}
        <label className="fv">
          <input
            type="text"
            aria-label="Grep buffered log lines"
            placeholder={GREP_PLACEHOLDER}
            value={grepInput}
            onChange={(event) => setGrepInput(event.target.value)}
          />
        </label>
        <FilterChip
          label="Matches"
          value={grepMode}
          options={[
            { value: "highlight", label: "highlight" },
            { value: "filter", label: "filter" },
          ]}
          onChange={(value) => setGrepMode(value === "filter" ? "filter" : "highlight")}
        />
        <span className="grow" />
        <span className="logstat">
          <Dot tone={phase === "live" ? "green" : "amber"} />
          {phaseLabel} · {num(podCount)} pods · {num(st?.lineCount ?? 0)} lines
        </span>
        <button
          type="button"
          className="btn"
          aria-pressed={followOn && !followSuspended}
          onClick={() => (followOn && !followSuspended ? setFollowOn(false) : resumeFollow())}
        >
          Follow
        </button>
        <button type="button" className="btn" aria-pressed={paused} onClick={togglePause}>
          Pause
        </button>
        <button
          type="button"
          className="btn"
          aria-pressed={wrap}
          onClick={() => setWrap((value) => !value)}
        >
          Wrap
        </button>
        <button type="button" className="btn" onClick={clearRows}>
          Clear
        </button>
      </div>

      {st?.cap ? (
        <div className="log-noticerow log-capbar">
          ⚠ streaming {num(st.cap.shown)} of {num(st.cap.total)} pods (server cap) — narrow by
          component or pod
        </div>
      ) : null}
      {evicted > 0 ? (
        <div className="log-noticerow log-capbar">
          buffer full — {num(evicted)} older lines dropped
        </div>
      ) : null}

      <div className="logpane-wrap">
        <div
          ref={paneRef}
          className={wrap ? "logpane wrap" : "logpane"}
          onScroll={onScroll}
          role="log"
          aria-label="Deployment logs"
        >
          {total === 0 ? (
            <div className="log-empty">
              {phase === "connecting"
                ? "Connecting to the log stream…"
                : grep && grepMode === "filter"
                  ? "No buffered lines match the grep."
                  : "No log lines yet."}
            </div>
          ) : wrap ? (
            viewRows.map(renderRow)
          ) : (
            <div className="logpane-canvas" style={{ height: total * ROW_PX }}>
              <div
                className="logpane-slice"
                style={{ transform: `translateY(${sliceStart * ROW_PX}px)` }}
              >
                {slice.map(renderRow)}
              </div>
            </div>
          )}
        </div>
        {followSuspended && pendingLines > 0 ? (
          <button type="button" className="log-resume" onClick={resumeFollow}>
            ↓ {num(pendingLines)} new lines — resume follow
          </button>
        ) : null}
      </div>
    </div>
  );
}
