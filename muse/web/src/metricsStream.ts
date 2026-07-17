import type { MetricsSnapshot } from "./api";

// Ready-state values are fixed by the EventSource web platform contract.
const EVENT_SOURCE_CLOSED = 2;
const MAX_TIMER_DELAY_MS = 2_147_483_647;
const RECONNECTING_ERROR = "Live metrics stream disconnected; reconnecting.";
const DISCONNECTED_ERROR = "Live metrics stream disconnected.";

export interface MetricsEventSource {
  readonly readyState: number;
  onopen: (() => void) | null;
  onmessage: ((event: { data: string }) => void) | null;
  onerror: (() => void) | null;
  close: () => void;
}

interface MetricsStreamDependencies {
  createSource: (url: string) => MetricsEventSource;
  scheduleReconnect: (callback: () => void, delayMs: number) => () => void;
}

interface MetricsStreamSubscriber {
  onSnapshot: (snapshot: MetricsSnapshot) => void;
  onStatus: (error: string | null) => void;
}

export interface MetricsStream {
  configureReconnect: (seconds?: number) => void;
  currentSnapshot: () => MetricsSnapshot | null;
  publishSnapshot: (snapshot: MetricsSnapshot) => boolean;
  subscribe: (subscriber: MetricsStreamSubscriber) => () => void;
}

/**
 * Own one shared metrics EventSource across all hook subscribers. Browser-managed
 * CONNECTING retries retain their source; only terminal CLOSED sources are
 * replaced, after the deployment-configured delay.
 */
export function createMetricsStream(
  url: string,
  dependencies: MetricsStreamDependencies,
): MetricsStream {
  const subscribers = new Set<MetricsStreamSubscriber>();
  let source: MetricsEventSource | null = null;
  let snapshot: MetricsSnapshot | null = null;
  let error: string | null = null;
  let reconnectDelayMs: number | null = null;
  let cancelReconnect: (() => void) | null = null;
  let reconnectGeneration = 0;
  let terminallyClosed = false;

  const publishStatus = (nextError: string | null) => {
    error = nextError;
    subscribers.forEach((subscriber) => subscriber.onStatus(nextError));
  };

  const publishSnapshot = (nextSnapshot: MetricsSnapshot): boolean => {
    if (snapshot && nextSnapshot.updatedAt <= snapshot.updatedAt) return false;
    snapshot = nextSnapshot;
    subscribers.forEach((subscriber) => subscriber.onSnapshot(nextSnapshot));
    return true;
  };

  const stop = () => {
    reconnectGeneration += 1;
    cancelReconnect?.();
    cancelReconnect = null;
    const current = source;
    source = null;
    current?.close();
    terminallyClosed = false;
    error = null;
  };

  const scheduleTerminalReconnect = () => {
    if (
      source ||
      cancelReconnect ||
      subscribers.size === 0 ||
      !terminallyClosed ||
      reconnectDelayMs === null
    )
      return;
    const generation = ++reconnectGeneration;
    cancelReconnect = dependencies.scheduleReconnect(() => {
      if (generation !== reconnectGeneration) return;
      cancelReconnect = null;
      if (source || subscribers.size === 0 || !terminallyClosed) return;
      terminallyClosed = false;
      connect();
    }, reconnectDelayMs);
  };

  const connect = () => {
    if (source || cancelReconnect || subscribers.size === 0 || terminallyClosed) return;
    const next = dependencies.createSource(url);
    source = next;

    next.onopen = () => {
      if (source !== next) return;
      publishStatus(null);
    };
    next.onmessage = (message) => {
      if (source !== next) return;
      let parsed: MetricsSnapshot;
      try {
        parsed = JSON.parse(message.data) as MetricsSnapshot;
      } catch {
        return;
      }
      publishStatus(null);
      publishSnapshot(parsed);
    };
    next.onerror = () => {
      if (source !== next) return;
      if (next.readyState !== EVENT_SOURCE_CLOSED) {
        publishStatus(RECONNECTING_ERROR);
        return;
      }

      next.close();
      source = null;
      terminallyClosed = true;
      publishStatus(reconnectDelayMs === null ? DISCONNECTED_ERROR : RECONNECTING_ERROR);
      scheduleTerminalReconnect();
    };
  };

  return {
    configureReconnect(seconds) {
      const milliseconds = seconds === undefined ? null : seconds * 1000;
      const nextDelay =
        milliseconds !== null &&
        Number.isFinite(milliseconds) &&
        milliseconds > 0 &&
        milliseconds <= MAX_TIMER_DELAY_MS
          ? milliseconds
          : null;
      if (nextDelay === reconnectDelayMs) return;
      reconnectDelayMs = nextDelay;
      reconnectGeneration += 1;
      cancelReconnect?.();
      cancelReconnect = null;
      if (terminallyClosed) {
        publishStatus(nextDelay === null ? DISCONNECTED_ERROR : RECONNECTING_ERROR);
        scheduleTerminalReconnect();
      }
    },
    currentSnapshot: () => snapshot,
    publishSnapshot,
    subscribe(subscriber) {
      subscribers.add(subscriber);
      subscriber.onStatus(error);
      connect();
      return () => {
        if (!subscribers.delete(subscriber)) return;
        if (subscribers.size === 0) stop();
      };
    },
  };
}
