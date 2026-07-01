import { useState } from "react";

import type { PanelKey } from "../App";
import { api } from "../api";
import { Pill } from "../components";
import { Plus, Search } from "../icons";
import { useLive } from "../live";
import { toWorkloadRows } from "../map";
import { matches } from "../util";

export function Workloads({ goto }: { goto: (p: PanelKey) => void }) {
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });
  const rollouts = useLive(api.rollouts, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });
  const [q, setQ] = useState("");

  const all = toWorkloadRows(isvcs.data?.items ?? [], rollouts.data?.items ?? []);
  const workloads = all.filter((w) =>
    matches(q, w.name, w.namespace, w.model, w.runtime, w.placement),
  );

  return (
    <section className="panel active" id="panel-workloads">
      <div className="page-head">
        <div>
          <h1>Workloads</h1>
          <p>
            Every OME InferenceService across the fleet. One InferenceService on the control plane;
            placement fans it out to one or more workload clusters.
          </p>
        </div>
        <div className="actions">
          <button className="btn primary" onClick={() => goto("deploy")}>
            <Plus />
            New deployment
          </button>
        </div>
      </div>

      <div className="filterbar">
        <div className="input grow">
          <Search />
          <input
            placeholder="Filter by name, model, namespace, cluster…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>

      <div className="card">
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th>Name</th>
                <th>Namespace</th>
                <th>Model</th>
                <th>Runtime</th>
                <th>Mode</th>
                <th>Placement</th>
                <th className="right">Ready</th>
                <th>Rollout</th>
                <th className="right">Age</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {workloads.map((w) => (
                <tr key={w.id}>
                  <td className="cell-name mono">{w.name}</td>
                  <td className="muted">{w.namespace}</td>
                  <td className="mono" style={{ fontSize: 12 }}>
                    {w.model}
                  </td>
                  <td className="mono" style={{ fontSize: 12 }}>
                    {w.runtime}
                  </td>
                  <td>
                    <Pill tone={w.mode.tone} label={w.mode.label} />
                  </td>
                  <td className="mono muted">{w.placement}</td>
                  <td className="num">{w.ready}</td>
                  <td>
                    <Pill tone={w.rollout.tone} label={w.rollout.label} />
                  </td>
                  <td className="num muted">{w.age}</td>
                  <td>
                    <Pill tone={w.status.tone} label={w.status.label} />
                  </td>
                </tr>
              ))}
              {workloads.length === 0 ? (
                <tr>
                  <td colSpan={10} className="muted empty-cell">
                    {q.trim() && all.length
                      ? `No InferenceServices match "${q}"`
                      : (isvcs.error ??
                        (isvcs.loading ? "Loading InferenceServices…" : "No InferenceServices"))}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className="list-foot">
        Showing <b style={{ color: "var(--fg)" }}>{workloads.length}</b> InferenceServices across
        the fleet
      </div>
    </section>
  );
}
