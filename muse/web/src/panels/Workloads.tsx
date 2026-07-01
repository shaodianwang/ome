import { Fragment, useState } from "react";
import type { CSSProperties } from "react";

import type { PanelKey } from "../App";
import { Pill } from "../components";
import { ArrowRight, ChevronRight, Columns, Filter, Plus, Search } from "../icons";
import { api } from "../api";
import { useLive } from "../live";
import { toWorkloadRows } from "../map";
import type { WorkloadDetail } from "../types";

const sectionLabel: CSSProperties = {
  fontSize: 11,
  textTransform: "uppercase",
  letterSpacing: ".06em",
  color: "var(--fg-3)",
  marginBottom: 10,
};

function Detail({ d }: { d: WorkloadDetail }) {
  return (
    <div className="detail-inner">
      <div className="detail-grid">
        <div>
          <div style={sectionLabel}>Components · prefill/decode disaggregation</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            {d.components.map((c, i) => (
              <div className="comp-card" key={i}>
                <div className="ch">
                  <Pill tone={c.badge.tone} label={c.badge.label} />
                  <h4>{c.title}</h4>
                </div>
                <dl className="kv">
                  {c.kv.map(([k, v], j) => (
                    <Fragment key={j}>
                      <dt>{k}</dt>
                      <dd>{v}</dd>
                    </Fragment>
                  ))}
                </dl>
              </div>
            ))}
          </div>

          {d.placement ? (
            <>
              <div style={{ ...sectionLabel, margin: "16px 0 10px" }}>
                Placement · fanned out by control plane
              </div>
              <table className="tbl" style={{ border: "1px solid var(--border)", borderRadius: 6, overflow: "hidden" }}>
                <thead>
                  <tr>
                    {d.placement.cols.map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {d.placement.rows.map((r, i) => (
                    <tr key={i}>
                      {r.map((cell, j) => (
                        <td key={j} className="mono">
                          {j === r.length - 1 ? <Pill tone="green" label={cell} /> : cell}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          ) : null}
        </div>

        <div>
          {d.canary ? (
            <>
              <div style={sectionLabel}>Canary progress</div>
              <div className="comp-card">
                <div className="canary-track">
                  <div className="canary-steps">
                    {d.canary.steps.map((s, i) => (
                      <span key={i} className={`cstep ${s}`} />
                    ))}
                  </div>
                  {d.canary.labels.length ? (
                    <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: "var(--fg-3)", marginTop: 2 }}>
                      {d.canary.labels.map((l) => (
                        <span key={l}>{l}</span>
                      ))}
                    </div>
                  ) : null}
                </div>
                {d.placement ? <hr className="div" style={{ margin: "14px 0" }} /> : null}
                <dl className="kv" style={d.placement ? undefined : { marginTop: 14 }}>
                  {d.canary.kv.map(([k, v], j) => (
                    <Fragment key={j}>
                      <dt>{k}</dt>
                      <dd>{v}</dd>
                    </Fragment>
                  ))}
                </dl>
                {d.showActions ? (
                  <div style={{ display: "flex", gap: 8, marginTop: 14 }}>
                    <button className="btn primary sm" style={{ flex: 1 }}>
                      <ArrowRight />
                      Promote
                    </button>
                    <button className="btn ghost sm" style={{ flex: 1 }}>
                      Pause
                    </button>
                    <button className="btn ghost sm" style={{ color: "#ff8585", borderColor: "rgba(240,68,68,.3)" }}>
                      Rollback
                    </button>
                  </div>
                ) : null}
              </div>
            </>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export function Workloads({ goto }: { goto: (p: PanelKey) => void }) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const toggle = (id: string) => setOpen((o) => ({ ...o, [id]: !o[id] }));

  const isvcs = useLive(api.inferenceServices, { kinds: ["InferenceService"], throttleMs: 5_000, intervalMs: 30_000 });
  const rollouts = useLive(api.rollouts, { kinds: ["InferenceService"], throttleMs: 5_000, intervalMs: 30_000 });
  const workloads = toWorkloadRows(isvcs.data?.items ?? [], rollouts.data?.items ?? []);

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
          <button className="btn ghost">
            <Columns />
            Columns
          </button>
          <button className="btn primary" onClick={() => goto("deploy")}>
            <Plus />
            New deployment
          </button>
        </div>
      </div>

      <div className="filterbar">
        <div className="input grow">
          <Search />
          <input placeholder="Filter by name, model, namespace, cluster…" />
          <span className="kbd">/</span>
        </div>
        <div className="input">
          <select>
            <option>All modes</option>
            <option>RawDeployment</option>
            <option>MultiNode</option>
            <option>PD-Disaggregated</option>
            <option>OMENative</option>
          </select>
        </div>
        <div className="input">
          <select>
            <option>All status</option>
            <option>Ready</option>
            <option>Degraded</option>
            <option>Failed</option>
            <option>Rolling out</option>
          </select>
        </div>
        <div className="input">
          <select>
            <option>All namespaces</option>
            <option>genai-prod</option>
            <option>research</option>
            <option>platform</option>
          </select>
        </div>
        <button className="btn ghost sm">
          <Filter />
          More filters
        </button>
      </div>

      <div className="card">
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th style={{ width: 22 }} />
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
                <Fragment key={w.id}>
                  <tr
                    className={w.detail ? "clickable" : undefined}
                    onClick={w.detail ? () => toggle(w.id) : undefined}
                  >
                    <td>
                      {w.detail ? (
                        <ChevronRight
                          width={14}
                          height={14}
                          style={{
                            color: "var(--fg-3)",
                            transition: "transform .15s",
                            transform: open[w.id] ? "rotate(90deg)" : "none",
                          }}
                        />
                      ) : null}
                    </td>
                    <td className="cell-name mono">{w.name}</td>
                    <td className="muted">{w.namespace}</td>
                    <td className="mono" style={{ fontSize: 12 }}>
                      {w.model}
                      <div className="sub">{w.modelSub}</div>
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
                  {w.detail && open[w.id] ? (
                    <tr className="detail-row">
                      <td colSpan={11}>
                        <Detail d={w.detail} />
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              ))}
              {workloads.length === 0 ? (
                <tr>
                  <td colSpan={11} className="muted" style={{ textAlign: "center", padding: 24 }}>
                    {isvcs.error ??
                      (isvcs.loading ? "Loading InferenceServices…" : "No InferenceServices")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginTop: 14,
          fontSize: "var(--t-sm)",
          color: "var(--fg-3)",
        }}
      >
        <span>
          Showing <b style={{ color: "var(--fg)" }}>{workloads.length}</b> InferenceServices across
          the fleet
        </span>
      </div>
    </section>
  );
}
