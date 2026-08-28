// Log viewers for a deployment's pods: the static tail box and the stern-style
// live multi-pod stream.
import { useEffect, useMemo, useRef, useState } from "react";

import { logStreamUrl } from "../api";
import { SearchInput } from "../components";
import type { Selected } from "../resource";
import { num as n } from "../util";
import { Dot } from "../viz";

/** Terminal escape sequences (colors, cursor moves) — meaningless in HTML. */
// eslint-disable-next-line no-control-regex
const ANSI_RE = /\u001b\[[0-9;?]*[a-zA-Z]/g;

/** Log-level tint per line, so errors pop and debug noise recedes. */
function logLineClass(line: string): string {
  if (/\b(ERROR|FATAL|PANIC)\b/.test(line)) return "log-line err";
  if (/\bWARN(ING)?\b/.test(line)) return "log-line warn";
  if (/\b(DEBUG|TRACE)\b/.test(line)) return "log-line dim";
  return "log-line";
}

/** The inline log viewer: ANSI-stripped, line-filterable, closable; scrolled to
 *  the end on load because the newest lines are the ones that answer. */
export function LogBox({
  title,
  text,
  onClose,
}: {
  title: string;
  text: string;
  onClose: () => void;
}) {
  const [q, setQ] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const lines = useMemo(() => text.replace(ANSI_RE, "").split("\n"), [text]);
  const shown = q ? lines.filter((line) => line.toLowerCase().includes(q.toLowerCase())) : lines;

  useEffect(() => {
    const el = scrollRef.current;
    if (el && !q) el.scrollTop = el.scrollHeight;
  }, [lines, q]);

  return (
    <div className="pod-logs">
      <div className="pod-logs-head">
        <span className="eyebrow">{title}</span>
        <SearchInput
          ariaLabel="Filter log lines"
          placeholder="Filter lines…"
          value={q}
          onChange={setQ}
        />
        {q ? (
          <span className="muted">
            {n(shown.length)} of {n(lines.length)} lines
          </span>
        ) : null}
        <button className="linkbtn" onClick={onClose} title="Close logs">
          ✕ close
        </button>
      </div>
      <div className="pod-logs-body" ref={scrollRef}>
        {shown.length ? (
          shown.map((line, i) => (
            <div className={logLineClass(line)} key={i}>
              {line || " "}
            </div>
          ))
        ) : (
          <div className="log-line dim">{q ? "no matching lines" : "(no output)"}</div>
        )}
      </div>
    </div>
  );
}

/** One live-tail session keeps at most this many of the newest lines; older
 *  lines fall off so an hours-long stream can't grow the page unbounded. */
const STREAM_BUFFER = 2_000;
/** Trailing lines requested per pod on connect — just enough context to anchor
 *  the live tail without replaying history. */
const STREAM_TAIL = 10;

type StreamLine = { id: number; pod: string; line: string };

/** Stable per-pod color so interleaved lines are attributable at a glance. */
function podTint(pod: string): string {
  let h = 0;
  for (let i = 0; i < pod.length; i++) h = (h * 31 + pod.charCodeAt(i)) >>> 0;
  return `hsl(${h % 360} 55% 62%)`;
}

/** stern-style live tail: one SSE stream fanning in `line` events from every
 *  matching pod, each line prefixed with its (colored) pod name. Fills the pods
 *  card in place of the table while active. */
export function LogStreamBox({
  streamRef,
  scope,
  onClose,
}: {
  streamRef: Selected;
  scope: { component?: string; pod?: string };
  onClose: () => void;
}) {
  const [lines, setLines] = useState<StreamLine[]>([]);
  const [notices, setNotices] = useState<string[]>([]);
  const [status, setStatus] = useState<"connecting" | "live" | "ended">("connecting");
  const [q, setQ] = useState("");
  const [follow, setFollow] = useState(true);
  const [session, setSession] = useState(0);
  const scrollRef = useRef<HTMLDivElement>(null);
  const seqRef = useRef(0);

  useEffect(() => {
    setLines([]);
    setNotices([]);
    setStatus("connecting");
    const source = new EventSource(
      logStreamUrl(streamRef, { component: scope.component, pod: scope.pod, tail: STREAM_TAIL }),
    );
    // Batch arrivals between renders: a busy component emits lines far faster
    // than the page should re-render.
    let buffer: StreamLine[] = [];
    const flush = window.setInterval(() => {
      if (!buffer.length) return;
      const batch = buffer;
      buffer = [];
      setLines((prev) => [...prev, ...batch].slice(-STREAM_BUFFER));
    }, 250);
    source.onopen = () => setStatus("live");
    source.addEventListener("line", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data) as { pod: string; line: string };
        buffer.push({ id: seqRef.current++, pod: data.pod, line: data.line.replace(ANSI_RE, "") });
      } catch {
        // Skip an unparsable frame rather than killing the stream.
      }
    });
    source.addEventListener("notice", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data) as { message?: string };
        if (data.message) setNotices((prev) => [...prev, data.message ?? ""]);
      } catch {
        // Skip an unparsable frame rather than killing the stream.
      }
    });
    // No auto-reconnect: EventSource retry would replay every pod's tail and
    // interleave duplicates. Surface the drop; restarting is one click.
    source.onerror = () => {
      setStatus("ended");
      source.close();
    };
    return () => {
      source.close();
      window.clearInterval(flush);
    };
    // Reconnect only when the stream's identity changes — the scalar fields, not
    // the ref object, whose identity changes on every parent render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [streamRef.cluster, streamRef.name, streamRef.namespace, scope.component, scope.pod, session]);

  useEffect(() => {
    const el = scrollRef.current;
    if (el && follow) el.scrollTop = el.scrollHeight;
  }, [lines, follow]);

  const needle = q.toLowerCase();
  const shown = q
    ? lines.filter(
        (l) => l.line.toLowerCase().includes(needle) || l.pod.toLowerCase().includes(needle),
      )
    : lines;
  // Pod names repeat the deployment name; drop that prefix so the interesting
  // suffix (component, index) survives in a narrow column.
  const prefix = `${streamRef.name}-`;
  const shortPod = (pod: string) => (pod.startsWith(prefix) ? pod.slice(prefix.length) : pod);

  return (
    <div className="pod-logs pod-stream">
      <div className="pod-logs-head">
        <Dot tone={status === "live" ? "green" : status === "connecting" ? "amber" : "red"} />
        <span className="eyebrow">
          Live logs · {scope.pod ?? scope.component ?? "all components"}
        </span>
        <SearchInput
          ariaLabel="Filter streamed lines"
          placeholder="Filter lines…"
          value={q}
          onChange={setQ}
        />
        {q ? (
          <span className="muted">
            {n(shown.length)} of {n(lines.length)} lines
          </span>
        ) : null}
        <button
          className="linkbtn"
          onClick={() => setFollow(!follow)}
          title={
            follow
              ? "Stop auto-scrolling (lines keep arriving)"
              : "Resume auto-scrolling to the newest lines"
          }
        >
          {follow ? "❚❚ pause" : "⏵ follow"}
        </button>
        {status === "ended" ? (
          <button className="linkbtn" onClick={() => setSession(session + 1)} title="Reconnect">
            ↻ restart
          </button>
        ) : null}
        <button className="linkbtn" onClick={onClose} title="Stop streaming">
          ✕ close
        </button>
      </div>
      {notices.map((msg, i) => (
        <div key={i} className="muted stream-notice">
          {msg}
        </div>
      ))}
      <div className="pod-logs-body" ref={scrollRef}>
        {shown.length ? (
          shown.map((l) => (
            <div className={logLineClass(l.line)} key={l.id}>
              <span className="stream-pod" style={{ color: podTint(l.pod) }} title={l.pod}>
                {shortPod(l.pod)}
              </span>{" "}
              {l.line || " "}
            </div>
          ))
        ) : (
          <div className="log-line dim">
            {status === "connecting"
              ? "connecting…"
              : q
                ? "no matching lines"
                : "waiting for output…"}
          </div>
        )}
      </div>
    </div>
  );
}
