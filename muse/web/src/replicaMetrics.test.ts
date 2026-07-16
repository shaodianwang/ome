import { describe, expect, it } from "vitest";

import type { MetricsSnapshot, PromSeries } from "./api";
import { replicaDiagnostics } from "./replicaMetrics";

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
