import { describe, expect, it } from "vitest";

import {
  compileGrep,
  createLogStream,
  grepSegments,
  LOG_HUE_COUNT,
  LogRing,
  logStreamUrl,
  parseLogFrame,
  parseNotice,
  parseRoster,
  podHue,
  reconnectSinceSecs,
} from "./logStream";
import type { LogEventSourceLike, LogRow, LogStreamParams } from "./logStream";

const PARAMS: LogStreamParams = {
  cluster: "cw",
  namespace: "team-a",
  isvc: "llama",
};

describe("logStreamUrl", () => {
  it("carries the required identity and only the narrowing that is set", () => {
    expect(logStreamUrl("/v1/logs/stream", PARAMS)).toBe(
      "/v1/logs/stream?cluster=cw&namespace=team-a&isvc=llama",
    );

    const narrowed = logStreamUrl("/v1/logs/stream", {
      ...PARAMS,
      component: "engine",
      pod: "llama-engine-0",
      container: "main",
      tail: 200,
      sinceSecs: 900,
    });
    expect(narrowed).toBe(
      "/v1/logs/stream?cluster=cw&namespace=team-a&isvc=llama" +
        "&component=engine&pod=llama-engine-0&container=main&tail=200&sinceSecs=900",
    );
  });

  it("percent-encodes parameter values", () => {
    const url = logStreamUrl("/v1/logs/stream", { ...PARAMS, namespace: "team a" });
    expect(url).toContain("namespace=team+a");
  });
});

const line = (seq: number, pod: string, ts: string, text: string): LogRow => ({
  kind: "line",
  seq,
  pod,
  container: "main",
  ts,
  tsMs: ts ? Date.parse(ts) : null,
  text,
  truncated: false,
});

describe("LogRing", () => {
  it("evicts the oldest rows beyond capacity and counts them", () => {
    const ring = new LogRing(3);
    for (let i = 0; i < 5; i++) ring.push(line(i, "p", "", `l${i}`));

    expect(ring.length).toBe(3);
    expect(ring.evicted).toBe(2);
    expect(ring.snapshot().map((row) => row.seq)).toEqual([2, 3, 4]);
    expect(ring.at(0)?.seq).toBe(2);
    expect(ring.at(3)).toBeUndefined();
    expect(ring.at(-1)).toBeUndefined();
  });

  it("clear resets contents and the eviction counter", () => {
    const ring = new LogRing(2);
    for (let i = 0; i < 4; i++) ring.push(line(i, "p", "", `l${i}`));
    ring.clear();

    expect(ring.length).toBe(0);
    expect(ring.evicted).toBe(0);
    ring.push(line(9, "p", "", "after"));
    expect(ring.snapshot().map((row) => row.seq)).toEqual([9]);
  });
});

describe("parseLogFrame", () => {
  it("parses batched pairs and the truncated triple, carrying the component", () => {
    const rows = parseLogFrame(
      JSON.stringify({
        p: "llama-engine-0",
        ct: "main",
        co: "engine",
        ls: [
          ["2026-07-20T15:04:01.113Z", "hello"],
          ["2026-07-20T15:04:01.221Z", "cut line", true],
        ],
      }),
    );

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      pod: "llama-engine-0",
      container: "main",
      component: "engine",
      ts: "2026-07-20T15:04:01.113Z",
      text: "hello",
      truncated: false,
    });
    expect(rows[0].tsMs).toBe(Date.parse("2026-07-20T15:04:01.113Z"));
    expect(rows[1].truncated).toBe(true);
  });

  it("keeps a line with an empty timestamp but leaves tsMs null", () => {
    const rows = parseLogFrame(
      JSON.stringify({ p: "pod-a", ct: "main", ls: [["", "no timestamp"]] }),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].tsMs).toBeNull();
    expect(rows[0].component).toBeUndefined();
  });

  it("yields no rows for malformed payloads instead of throwing", () => {
    expect(parseLogFrame("not json")).toEqual([]);
    expect(parseLogFrame(JSON.stringify({ p: "pod-a" }))).toEqual([]);
    expect(parseLogFrame(JSON.stringify({ p: "pod-a", ct: "main", ls: [[42, "x"]] }))).toEqual([]);
  });
});

describe("parseRoster / parseNotice", () => {
  it("parses a roster snapshot with cap accounting", () => {
    const roster = parseRoster(
      JSON.stringify({
        pods: [
          { name: "llama-engine-0", component: "engine", phase: "Running", containers: ["main"] },
          { name: "llama-decoder-0", phase: "Pending", containers: ["main", "sidecar"] },
        ],
        total: 5,
        capped: true,
      }),
    );

    expect(roster).not.toBeNull();
    expect(roster?.pods.map((p) => p.name)).toEqual(["llama-engine-0", "llama-decoder-0"]);
    expect(roster?.pods[1].component).toBeUndefined();
    expect(roster?.total).toBe(5);
    expect(roster?.capped).toBe(true);
    expect(parseRoster("nope")).toBeNull();
  });

  it("parses notices and rejects payloads without a kind", () => {
    expect(parseNotice(JSON.stringify({ kind: "dropped", count: 12 }))).toMatchObject({
      kind: "dropped",
      count: 12,
    });
    expect(parseNotice(JSON.stringify({ count: 12 }))).toBeNull();
  });
});

describe("podHue", () => {
  it("is stable and in range for every pod name", () => {
    const names = [
      "llama-engine-0-leader-0",
      "llama-engine-1-leader-0",
      "llama-decoder-0-leader-0",
      "llama-router-7d9f4b-x2k9p",
    ];
    for (const name of names) {
      const hue = podHue(name);
      expect(hue).toBe(podHue(name));
      expect(hue).toBeGreaterThanOrEqual(0);
      expect(hue).toBeLessThan(LOG_HUE_COUNT);
    }
    // Different names spread across more than one hue.
    expect(new Set(names.map(podHue)).size).toBeGreaterThan(1);
  });
});

describe("compileGrep / grepSegments", () => {
  it("matches literally (case-insensitive) by default, including regex metachars", () => {
    expect(compileGrep("  ")).toBeNull();
    const grep = compileGrep("token usage: 0.6")!;
    expect(grep.test("Prefill batch. TOKEN USAGE: 0.61")).toBe(true);
    expect(grep.test("no match")).toBe(false);

    const meta = compileGrep("p99 (ms)")!;
    expect(meta.test("latency p99 (ms): 840")).toBe(true);
  });

  it("treats /pattern/ as a regular expression", () => {
    const grep = compileGrep("/tok(en)? usage/")!;
    expect(grep.test("token usage: 0.61")).toBe(true);
    expect(grep.test("tok usage: 0.61")).toBe(true);
    expect(grep.test("usage token: 0.61")).toBe(false);
  });

  it("falls back to a literal match when the regex is invalid", () => {
    const grep = compileGrep("/foo(/")!;
    expect(grep.test("saw /foo(/ in the line")).toBe(true);
    expect(grep.test("foo(")).toBe(false);
  });

  it("splits a line into hit and miss segments for highlighting", () => {
    const grep = compileGrep("batch")!;
    expect(grepSegments("Prefill batch. Decode batch.", grep)).toEqual([
      { text: "Prefill ", hit: false },
      { text: "batch", hit: true },
      { text: ". Decode ", hit: false },
      { text: "batch", hit: true },
      { text: ".", hit: false },
    ]);
    expect(grepSegments("no hits here", grep)).toEqual([{ text: "no hits here", hit: false }]);
  });
});

describe("reconnectSinceSecs", () => {
  it("rounds the gap up to whole seconds and never goes below one", () => {
    const base = Date.parse("2026-07-20T15:04:00.000Z");
    expect(reconnectSinceSecs(base, base + 2_000)).toBe(2);
    expect(reconnectSinceSecs(base, base + 2_001)).toBe(3);
    expect(reconnectSinceSecs(base, base + 250)).toBe(1);
    // Clock skew (last line "in the future") still yields a valid request.
    expect(reconnectSinceSecs(base + 5_000, base)).toBe(1);
  });
});

// -----------------------------------------------------------------------------
// Controller behavior
// -----------------------------------------------------------------------------

class FakeLogSource implements LogEventSourceLike {
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  closeCalls = 0;
  private listeners = new Map<string, (event: { data: string }) => void>();

  constructor(readonly url: string) {}

  addEventListener(type: string, listener: (event: { data: string }) => void): void {
    this.listeners.set(type, listener);
  }

  close(): void {
    this.closeCalls += 1;
  }

  emit(type: string, payload: unknown): void {
    this.listeners.get(type)?.({ data: JSON.stringify(payload) });
  }
}

function harness(params: LogStreamParams = PARAMS, bufferLines = 100) {
  const sources: FakeLogSource[] = [];
  const scheduled: { callback: () => void; delayMs: number; cancelled: boolean }[] = [];
  const clock = { nowMs: Date.parse("2026-07-20T15:05:00.000Z") };
  let updates = 0;
  const stream = createLogStream(
    "/v1/logs/stream",
    params,
    { bufferLines, reconnectDelayMs: 2_000, onUpdate: () => (updates += 1) },
    {
      createSource: (url) => {
        const source = new FakeLogSource(url);
        sources.push(source);
        return source;
      },
      scheduleReconnect: (callback, delayMs) => {
        const pending = { callback, delayMs, cancelled: false };
        scheduled.push(pending);
        return () => {
          pending.cancelled = true;
        };
      },
      now: () => clock.nowMs,
    },
  );
  return { clock, scheduled, sources, stream, updates: () => updates };
}

const frame = (pod: string, entries: (string | boolean)[][]) => ({
  p: pod,
  ct: "main",
  co: "engine",
  ls: entries,
});

describe("log stream controller", () => {
  it("buffers parsed lines and tracks phase, roster, and cap", () => {
    const { sources, stream } = harness();
    stream.start();
    expect(sources).toHaveLength(1);
    expect(stream.state().phase).toBe("connecting");

    sources[0].onopen?.();
    expect(stream.state().phase).toBe("live");

    sources[0].emit("pods", {
      pods: [
        { name: "llama-engine-0", component: "engine", phase: "Running", containers: ["main"] },
      ],
      total: 3,
      capped: true,
    });
    expect(stream.state().roster?.pods).toHaveLength(1);
    expect(stream.state().cap).toEqual({ shown: 1, total: 3 });

    sources[0].emit("log", frame("llama-engine-0", [["2026-07-20T15:04:01.100Z", "one"]]));
    expect(stream.state().lineCount).toBe(1);
    expect(stream.ring.snapshot()).toHaveLength(1);

    // An uncapped roster clears the persistent cap.
    sources[0].emit("pods", {
      pods: [{ name: "llama-engine-0", phase: "Running", containers: ["main"] }],
      total: 1,
      capped: false,
    });
    expect(stream.state().cap).toBeNull();
  });

  it("reconnects with tail dropped and sinceSecs from the newest line, deduping the overlap", () => {
    const { clock, scheduled, sources, stream } = harness({ ...PARAMS, tail: 500, sinceSecs: 900 });
    stream.start();
    expect(sources[0].url).toContain("tail=500");
    expect(sources[0].url).toContain("sinceSecs=900");
    sources[0].onopen?.();

    sources[0].emit(
      "log",
      frame("llama-engine-0", [
        ["2026-07-20T15:04:58.000Z", "old"],
        ["2026-07-20T15:04:59.400Z", "newest"],
      ]),
    );

    sources[0].onerror?.();
    expect(sources[0].closeCalls).toBe(1);
    expect(stream.state().phase).toBe("reconnecting");
    // The interruption leaves a marker row in the flow.
    const interrupted = stream.ring.snapshot();
    expect(interrupted[interrupted.length - 1]).toMatchObject({ kind: "marker" });
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0].delayMs).toBe(2_000);

    // 3.6s later the reconnect fires: gap to the newest line is 4.2s → ceil 5s.
    clock.nowMs += 3_600;
    scheduled[0].callback();
    expect(sources).toHaveLength(2);
    expect(sources[1].url).not.toContain("tail=");
    expect(sources[1].url).toContain("sinceSecs=5");

    sources[1].onopen?.();
    expect(stream.state().phase).toBe("live");

    // The server replays the overlap; only the genuinely new line lands.
    sources[1].emit(
      "log",
      frame("llama-engine-0", [
        ["2026-07-20T15:04:58.000Z", "old"],
        ["2026-07-20T15:04:59.400Z", "newest"],
        ["2026-07-20T15:05:03.000Z", "fresh"],
      ]),
    );
    const texts = stream.ring
      .snapshot()
      .filter((row) => row.kind === "line")
      .map((row) => (row.kind === "line" ? row.text : ""));
    expect(texts).toEqual(["old", "newest", "fresh"]);
    expect(stream.state().deduped).toBe(2);
    expect(stream.state().lineCount).toBe(3);
  });

  it("reopens the original request when nothing was received before the failure", () => {
    const { scheduled, sources, stream } = harness({ ...PARAMS, tail: 200 });
    stream.start();
    sources[0].onerror?.();
    scheduled[0].callback();

    expect(sources).toHaveLength(2);
    expect(sources[1].url).toBe(sources[0].url);
    expect(sources[1].url).toContain("tail=200");
  });

  it("stop cancels a pending reconnect and closes the source", () => {
    const { scheduled, sources, stream } = harness();
    stream.start();
    sources[0].onerror?.();
    stream.stop();

    expect(scheduled[0].cancelled).toBe(true);
    scheduled[0].callback();
    expect(sources).toHaveLength(1);
  });

  it("turns notices into inline rows and counters, keeping truncated-pods out of the flow", () => {
    const { sources, stream } = harness();
    stream.start();
    sources[0].onopen?.();

    sources[0].emit("notice", { kind: "truncated-pods", total: 60, shown: 48 });
    expect(stream.state().cap).toEqual({ shown: 48, total: 60 });
    expect(stream.ring.length).toBe(0);

    sources[0].emit("notice", { kind: "dropped", count: 12 });
    sources[0].emit("notice", { kind: "tail-error", pod: "p1", container: "main", error: "403" });
    sources[0].emit("notice", { kind: "session-end", afterSecs: 3600 });

    expect(stream.state().droppedByServer).toBe(12);
    const rows = stream.ring.snapshot();
    expect(rows.map((row) => row.kind)).toEqual(["notice", "notice", "marker"]);
    expect(rows[0]).toMatchObject({ text: "12 lines dropped by the server rate limit" });
    expect(rows[1]).toMatchObject({ text: "p1/main: tail error — 403 (retrying)" });
  });

  it("clearRows empties the buffer and counters without touching the connection", () => {
    const { sources, stream } = harness();
    stream.start();
    sources[0].onopen?.();
    sources[0].emit("log", frame("llama-engine-0", [["2026-07-20T15:04:01.100Z", "one"]]));
    sources[0].emit("notice", { kind: "dropped", count: 3 });

    stream.clearRows();
    expect(stream.ring.length).toBe(0);
    expect(stream.state().lineCount).toBe(0);
    expect(stream.state().droppedByServer).toBe(0);
    expect(sources[0].closeCalls).toBe(0);

    sources[0].emit("log", frame("llama-engine-0", [["2026-07-20T15:04:02.100Z", "two"]]));
    expect(stream.state().lineCount).toBe(1);
  });
});
