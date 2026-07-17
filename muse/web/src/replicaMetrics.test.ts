import { describe, expect, it } from "vitest";

import type { MetricsSnapshot, PromSeries } from "./api";
import type { ReplicaPerfRow } from "./replicaMetrics";
import { replicaDiagnostics, replicaIdentity, selectReplica } from "./replicaMetrics";

const series = (namespace: string, pod: string, component: string, value: number): PromSeries => ({
  metric: { namespace, inferenceservice: "model", pod, component },
  value: [10, String(value)],
});

describe("replicaDiagnostics", () => {
  it("joins instant engine metrics by exact namespace, pod, and component", () => {
    const snap: MetricsSnapshot = {
      updatedAt: 10,
      clusters: {
        test: {
          replica_request_rate: {
            data: {
              resultType: "vector",
              result: [
                series("team-a", "engine-0", "engine", 2),
                series("team-b", "engine-0", "engine", 50),
                { metric: { namespace: "team-a", inferenceservice: "model" }, value: [10, "9"] },
              ],
            },
          },
          replica_throughput_tokens: {
            data: {
              resultType: "vector",
              result: [series("team-a", "engine-0", "engine", 120)],
            },
          },
          replica_kv_cache_usage: {
            data: {
              resultType: "vector",
              result: [series("team-a", "engine-0", "engine", 0.72)],
            },
          },
        },
      },
    };

    expect(replicaDiagnostics(snap, "test", "team-a", "model")).toEqual({
      configured: true,
      rows: [
        {
          namespace: "team-a",
          pod: "engine-0",
          component: "engine",
          qps: 2,
          throughput: 120,
          p99: null,
          ttft: null,
          running: null,
          queued: null,
          kvCache: 0.72,
        },
      ],
    });
  });
});

describe("selectReplica", () => {
  it("keeps an exact identity and falls back to the first current replica", () => {
    const row = (namespace: string, pod: string, component: string): ReplicaPerfRow => ({
      namespace,
      pod,
      component,
      qps: null,
      throughput: null,
      p99: null,
      ttft: null,
      running: null,
      queued: null,
      kvCache: null,
    });
    const rows = [
      row("team-a", "engine-0", "engine"),
      row("team-a", "engine-0", "decoder"),
      row("team-b", "engine-0", "engine"),
    ];

    expect(selectReplica(rows, replicaIdentity(rows[2]))).toBe(rows[2]);
    expect(selectReplica(rows, replicaIdentity(row("team-a", "gone", "engine")))).toBe(rows[0]);
    expect(selectReplica([], replicaIdentity(rows[2]))).toBeUndefined();
  });
});
