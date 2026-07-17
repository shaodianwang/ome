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
import { metricsTransportState } from "./dataState";
import { createMetricsStream } from "./metricsStream";
import type { MetricsEventSource } from "./metricsStream";

type ActivityListener = (event: EventItem) => void;
type InvalidationListener = (event: InvalidationEvent) => void;

let source: EventSource | null = null;
const activityListeners = new Set<ActivityListener>();
const invalidationListeners = new Set<InvalidationListener>();
export type StreamConnectionState = "connecting" | "live" | "reconnecting";
const sourceStatusListeners = new Set<(state: StreamConnectionState) => void>();
let sourceConnectionState: StreamConnectionState = "connecting";

function publishSourceStatus(state: StreamConnectionState) {
  sourceConnectionState = state;
  sourceStatusListeners.forEach((listener) => listener(state));
}

function ensureSource() {
  if (source) return;
  source = new EventSource(STREAM_URL);
  source.onopen = () => publishSourceStatus("live");
  source.onerror = () => publishSourceStatus("reconnecting");
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
}

/** Subscribe only to operator-facing activity items. */
function onActivity(listener: ActivityListener): () => void {
  ensureSource();
  activityListeners.add(listener);
  return () => {
    activityListeners.delete(listener);
  };
}

function onSourceStatus(listener: (state: StreamConnectionState) => void): () => void {
  ensureSource();
  sourceStatusListeners.add(listener);
  listener(sourceConnectionState);
  return () => {
    sourceStatusListeners.delete(listener);
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
  /** Epoch seconds of the most recent successful fetch; retained across failures. */
  lastSuccessAt: number | null;
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
  const [lastSuccessAt, setLastSuccessAt] = useState<number | null>(null);

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
        setLastSuccessAt(Math.floor(Date.now() / 1000));
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

  return { data, error, loading, lastSuccessAt, refetch: load };
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
  connection: StreamConnectionState;
  refetch: () => void;
} {
  const [events, setEvents] = useState<EventItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [connection, setConnection] = useState<StreamConnectionState>(sourceConnectionState);
  const [revision, setRevision] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
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
      setError(null);
      setEvents((prev) => [event, ...prev].slice(0, limit));
    });
    const offStatus = onSourceStatus(setConnection);
    return () => {
      alive = false;
      off();
      offStatus();
    };
  }, [limit, revision]);

  return {
    events,
    error,
    loading,
    connection,
    refetch: () => setRevision((value) => value + 1),
  };
}

// ---------------------------------------------------------------------------
// Metrics: a second shared SSE connection carrying the full fleet metrics
// snapshot, pushed by the server on each refresh. Separate from the resource
// change stream above (different data, different cadence); one connection for the
// whole app, and subscribers never poll.
// ---------------------------------------------------------------------------

const metricsStream = createMetricsStream(METRICS_STREAM_URL, {
  createSource: (url) => new EventSource(url) as unknown as MetricsEventSource,
  scheduleReconnect: (callback, delayMs) => {
    const timer = setTimeout(callback, delayMs);
    return () => clearTimeout(timer);
  },
});

/**
 * Subscribe to the live fleet metrics snapshot. The server pushes a fresh snapshot
 * on each refresh, so there is no polling — the SSE payload *is* the data. Pass
 * `enabled=false` (e.g. metrics unconfigured) to skip the connection entirely.
 */
export function useMetrics(
  enabled = true,
  reconnectSecs?: number,
): {
  data: MetricsSnapshot | null;
  error: string | null;
  loading: boolean;
} {
  const [data, setData] = useState<MetricsSnapshot | null>(metricsStream.currentSnapshot());
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [streamError, setStreamError] = useState<string | null>(null);
  useEffect(() => {
    if (!enabled) {
      setFetchError(null);
      setStreamError(null);
      return;
    }
    let alive = true;
    metricsStream.configureReconnect(reconnectSecs);
    // The SSE pushes the snapshot on connect and on every refresh — that's the live
    // path. A one-time GET on mount is a fallback so metrics still show if a proxy
    // drops the stream; it's served from the server's cached snapshot (no upstream
    // cost) and there's no polling (mirrors useEventFeed: fetch once, then stream).
    api
      .metrics()
      .then((s) => {
        if (!alive) return;
        metricsStream.publishSnapshot(s);
        setFetchError(null);
      })
      .catch((e: unknown) => {
        if (!alive) return;
        setFetchError(e instanceof Error ? e.message : String(e));
      });
    const current = metricsStream.currentSnapshot();
    if (current) setData(current);
    const unsubscribe = metricsStream.subscribe({
      onSnapshot: (snapshot) => {
        if (!alive) return;
        setData(snapshot);
        setFetchError(null);
      },
      onStatus: (nextError) => {
        if (alive) setStreamError(nextError);
      },
    });
    return () => {
      alive = false;
      unsubscribe();
    };
  }, [enabled, reconnectSecs]);
  return { data, ...metricsTransportState(data, fetchError, streamError, enabled) };
}
