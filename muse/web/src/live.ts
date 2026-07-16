// Live data plumbing: one shared SSE connection carrying two independent named
// event types. Activity is operator-facing history; invalidations are compact
// resource-domain hints that drive query refreshes, including status-only changes.
//
// One EventSource is shared across all panels (the browser caps connections per
// origin, and we only need one). Each `useLive` subscriber refetches when an
// invalidation for a resource it depends on arrives, debounced so a burst of
// changes coalesces. Activity formatting never participates in that decision.

import { useEffect, useRef, useState } from "react";

import { api } from "./api";
import type { EventItem, InvalidationEvent, MetricsSnapshot } from "./api";
import { METRICS_STREAM_URL, STREAM_URL } from "./api";

type ActivityListener = (event: EventItem) => void;
type InvalidationListener = (event: InvalidationEvent) => void;

let source: EventSource | null = null;
const activityListeners = new Set<ActivityListener>();
const invalidationListeners = new Set<InvalidationListener>();

function ensureSource() {
  if (source) return;
  source = new EventSource(STREAM_URL);
  source.addEventListener("activity", (raw) => {
    const msg = raw as MessageEvent<string>;
    let event: EventItem;
    try {
      event = JSON.parse(msg.data) as EventItem;
    } catch {
      return;
    }
    activityListeners.forEach((listener) => listener(event));
  });
  source.addEventListener("invalidation", (raw) => {
    const msg = raw as MessageEvent<string>;
    let event: InvalidationEvent;
    try {
      event = JSON.parse(msg.data) as InvalidationEvent;
    } catch {
      return;
    }
    if (!Array.isArray(event.resources)) return;
    invalidationListeners.forEach((listener) => listener(event));
  });
  // EventSource reconnects automatically on error; nothing to do here.
}

/** Subscribe only to operator-facing activity items. */
function onActivity(listener: ActivityListener): () => void {
  ensureSource();
  activityListeners.add(listener);
  return () => {
    activityListeners.delete(listener);
  };
}

/** Subscribe only to resource-domain query invalidations. */
function onInvalidation(listener: InvalidationListener): () => void {
  ensureSource();
  invalidationListeners.add(listener);
  return () => {
    invalidationListeners.delete(listener);
  };
}

/** Whether a batched invalidation affects a query's resource dependencies. */
export function shouldInvalidate(event: InvalidationEvent, kinds?: string[]): boolean {
  if (kinds?.length === 0) return false;
  if (!kinds) return true;
  return event.resources.some((resource) => kinds.includes(resource));
}

export interface LiveState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refetch: () => void;
}

export interface LiveOptions {
  /** Refetch when an event for one of these kinds arrives. Omit = any kind; pass
   *  `[]` to ignore the stream entirely (interval-only). */
  kinds?: string[];
  /** Also refetch on this interval (ms) — for data not carried on the stream. */
  intervalMs?: number;
  /** Minimum ms between event-triggered refetches; coalesces a burst of changes
   *  into one refetch so status churn can't cause a request storm. Default 3000. */
  throttleMs?: number;
}

/**
 * Fetch `fetcher()` once, then refetch when a relevant SSE event arrives (and
 * optionally on a slow interval). The fetcher and options are read through refs,
 * so passing inline closures/objects per render is fine.
 */
export function useLive<T>(fetcher: () => Promise<T>, options: LiveOptions = {}): LiveState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const optionsRef = useRef(options);
  optionsRef.current = options;

  const load = useRef(() => {
    fetcherRef
      .current()
      .then((d) => {
        setData(d);
        setError(null);
      })
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }).current;

  useEffect(() => {
    load();

    // Coalesce event-triggered refetches: at most one per throttle window, so a
    // burst of resource changes (status churn on a busy cluster) can't turn into
    // a refetch storm. `kinds: []` opts out of the stream entirely.
    let lastRun = Date.now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const off = onInvalidation((event) => {
      const opts = optionsRef.current;
      if (!shouldInvalidate(event, opts.kinds)) return;
      const throttle = opts.throttleMs ?? 3000;
      const wait = Math.max(0, throttle - (Date.now() - lastRun));
      clearTimeout(timer);
      timer = setTimeout(() => {
        lastRun = Date.now();
        load();
      }, wait);
    });

    let interval: ReturnType<typeof setInterval> | undefined;
    if (optionsRef.current.intervalMs) {
      interval = setInterval(() => {
        lastRun = Date.now();
        load();
      }, optionsRef.current.intervalMs);
    }

    return () => {
      off();
      clearTimeout(timer);
      if (interval) clearInterval(interval);
    };
  }, [load]);

  return { data, error, loading, refetch: load };
}

/**
 * Live activity feed: fetch the recent backlog once, then append streamed events
 * in real time (capped at `limit`). There is no per-event refetch — the SSE
 * payload *is* the new item — so a chatty cluster costs no extra requests.
 */
export function useEventFeed(limit = 60): {
  events: EventItem[];
  error: string | null;
  loading: boolean;
} {
  const [events, setEvents] = useState<EventItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    api
      .events()
      .then((r) => {
        if (alive) {
          setEvents(r.items.slice(0, limit));
          setError(null);
        }
      })
      .catch((e: unknown) => {
        if (alive) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    const off = onActivity((event) => {
      setEvents((prev) => [event, ...prev].slice(0, limit));
    });
    return () => {
      alive = false;
      off();
    };
  }, [limit]);

  return { events, error, loading };
}

// ---------------------------------------------------------------------------
// Metrics: a second shared SSE connection carrying the full fleet metrics
// snapshot, pushed by the server on each refresh. Separate from the resource
// change stream above (different data, different cadence); one connection for the
// whole app, and subscribers never poll.
// ---------------------------------------------------------------------------

let metricsSource: EventSource | null = null;
const metricsListeners = new Set<(snap: MetricsSnapshot) => void>();
let lastMetrics: MetricsSnapshot | null = null;

function ensureMetricsSource() {
  if (metricsSource) return;
  metricsSource = new EventSource(METRICS_STREAM_URL);
  metricsSource.onmessage = (msg) => {
    let snap: MetricsSnapshot;
    try {
      snap = JSON.parse(msg.data) as MetricsSnapshot;
    } catch {
      return;
    }
    lastMetrics = snap;
    metricsListeners.forEach((l) => l(snap));
  };
  // EventSource reconnects automatically on error; nothing to do here.
}

/**
 * Subscribe to the live fleet metrics snapshot. The server pushes a fresh snapshot
 * on each refresh, so there is no polling — the SSE payload *is* the data. Pass
 * `enabled=false` (e.g. metrics unconfigured) to skip the connection entirely.
 */
export function useMetrics(enabled = true): { data: MetricsSnapshot | null } {
  const [data, setData] = useState<MetricsSnapshot | null>(lastMetrics);
  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    // The SSE pushes the snapshot on connect and on every refresh — that's the live
    // path. A one-time GET on mount is a fallback so metrics still show if a proxy
    // drops the stream; it's served from the server's cached snapshot (no upstream
    // cost) and there's no polling (mirrors useEventFeed: fetch once, then stream).
    api
      .metrics()
      .then((s) => {
        if (!alive) return;
        lastMetrics = s;
        setData(s);
      })
      .catch(() => {});
    ensureMetricsSource();
    if (lastMetrics) setData(lastMetrics);
    const listener = (snap: MetricsSnapshot) => {
      if (alive) setData(snap);
    };
    metricsListeners.add(listener);
    return () => {
      alive = false;
      metricsListeners.delete(listener);
    };
  }, [enabled]);
  return { data };
}
