import { describe, expect, it } from "vitest";

import type { MetricsSnapshot } from "./api";
import { createMetricsStream } from "./metricsStream";
import type { MetricsEventSource } from "./metricsStream";

class FakeEventSource implements MetricsEventSource {
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  closeCalls = 0;

  close() {
    this.closeCalls += 1;
  }

  emitOpen() {
    this.readyState = 1;
    this.onopen?.();
  }

  emitMessage(snapshot: MetricsSnapshot) {
    this.onmessage?.({ data: JSON.stringify(snapshot) });
  }

  emitError(readyState: number) {
    this.readyState = readyState;
    this.onerror?.();
  }
}

function harness() {
  const sources: FakeEventSource[] = [];
  const scheduled: { callback: () => void; delayMs: number; cancelled: boolean }[] = [];
  const stream = createMetricsStream("/metrics", {
    createSource: () => {
      const source = new FakeEventSource();
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
  });
  return { scheduled, sources, stream };
}

const snapshot = (updatedAt: number): MetricsSnapshot => ({ updatedAt, clusters: {} });
const last = <T>(values: T[]): T | undefined => values[values.length - 1];

describe("shared metrics stream", () => {
  it("leaves transient CONNECTING recovery to EventSource and clears errors on data", () => {
    const { scheduled, sources, stream } = harness();
    const statuses: (string | null)[] = [];
    const snapshots: MetricsSnapshot[] = [];
    stream.configureReconnect(5);
    stream.subscribe({
      onSnapshot: (value) => snapshots.push(value),
      onStatus: (s) => statuses.push(s),
    });

    sources[0].emitError(0);
    expect(last(statuses)).toBe("Live metrics stream disconnected; reconnecting.");
    expect(sources).toHaveLength(1);
    expect(scheduled).toHaveLength(0);

    sources[0].emitMessage(snapshot(1));
    expect(snapshots).toEqual([snapshot(1)]);
    expect(last(statuses)).toBeNull();
  });

  it("replaces a terminal CLOSED source after the configured delay", () => {
    const { scheduled, sources, stream } = harness();
    const statuses: (string | null)[] = [];
    const snapshots: MetricsSnapshot[] = [];
    stream.configureReconnect(7);
    stream.subscribe({
      onSnapshot: (value) => snapshots.push(value),
      onStatus: (s) => statuses.push(s),
    });
    const closed = sources[0];

    closed.emitError(2);
    expect(closed.closeCalls).toBe(1);
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0].delayMs).toBe(7_000);
    expect(last(statuses)).toBe("Live metrics stream disconnected; reconnecting.");

    closed.emitOpen();
    closed.emitMessage(snapshot(1));
    expect(last(statuses)).toBe("Live metrics stream disconnected; reconnecting.");
    expect(snapshots).toEqual([]);

    scheduled[0].callback();
    expect(sources).toHaveLength(2);
    closed.emitError(2);
    expect(scheduled).toHaveLength(1);
    expect(closed.closeCalls).toBe(1);
    sources[1].emitOpen();
    sources[1].emitMessage(snapshot(2));
    expect(last(statuses)).toBeNull();
    expect(snapshots).toEqual([snapshot(2)]);
  });

  it("uses one source for multiple subscribers", () => {
    const { sources, stream } = harness();
    stream.configureReconnect(5);
    const first: number[] = [];
    const second: number[] = [];
    const offFirst = stream.subscribe({
      onSnapshot: (value) => first.push(value.updatedAt),
      onStatus: () => {},
    });
    const offSecond = stream.subscribe({
      onSnapshot: (value) => second.push(value.updatedAt),
      onStatus: () => {},
    });

    expect(sources).toHaveLength(1);
    sources[0].emitMessage(snapshot(1));
    expect(first).toEqual([1]);
    expect(second).toEqual([1]);

    offFirst();
    sources[0].emitMessage(snapshot(2));
    expect(first).toEqual([1]);
    expect(second).toEqual([1, 2]);

    offSecond();
    stream.subscribe({ onSnapshot: () => {}, onStatus: () => {} });
    expect(sources[0].closeCalls).toBe(1);
    expect(sources).toHaveLength(2);
  });

  it("cancels terminal recovery when the last subscriber leaves", () => {
    const { scheduled, sources, stream } = harness();
    stream.configureReconnect(5);
    const unsubscribe = stream.subscribe({ onSnapshot: () => {}, onStatus: () => {} });

    sources[0].emitError(2);
    expect(scheduled).toHaveLength(1);
    unsubscribe();
    expect(scheduled[0].cancelled).toBe(true);

    scheduled[0].callback();
    expect(sources).toHaveLength(1);
  });

  it("does not let a delayed GET snapshot replace newer stream data", () => {
    const { sources, stream } = harness();
    const received: number[] = [];
    stream.subscribe({
      onSnapshot: (value) => received.push(value.updatedAt),
      onStatus: () => {},
    });

    sources[0].emitMessage(snapshot(2));
    expect(stream.publishSnapshot(snapshot(1))).toBe(false);
    expect(stream.publishSnapshot(snapshot(2))).toBe(false);
    expect(stream.currentSnapshot()).toEqual(snapshot(2));
    expect(received).toEqual([2]);
  });

  it("publishes an accepted GET fallback to every subscriber", () => {
    const { stream } = harness();
    const first: number[] = [];
    const second: number[] = [];
    stream.subscribe({ onSnapshot: (value) => first.push(value.updatedAt), onStatus: () => {} });
    stream.subscribe({ onSnapshot: (value) => second.push(value.updatedAt), onStatus: () => {} });

    expect(stream.publishSnapshot(snapshot(1))).toBe(true);
    expect(first).toEqual([1]);
    expect(second).toEqual([1]);
  });

  it("accepts the browser timer boundary and rejects delays above it", () => {
    const supported = harness();
    supported.stream.configureReconnect(2_147_483);
    supported.stream.subscribe({ onSnapshot: () => {}, onStatus: () => {} });
    supported.sources[0].emitError(2);
    expect(supported.scheduled[0].delayMs).toBe(2_147_483_000);

    const { scheduled, sources, stream } = harness();
    const statuses: (string | null)[] = [];
    stream.configureReconnect(2_147_484);
    stream.subscribe({ onSnapshot: () => {}, onStatus: (status) => statuses.push(status) });

    sources[0].emitError(2);
    expect(scheduled).toHaveLength(0);
    expect(last(statuses)).toBe("Live metrics stream disconnected.");
  });
});
