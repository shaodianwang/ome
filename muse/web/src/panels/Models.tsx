import { Fragment } from "react";

import { Bar, Pill } from "../components";
import { Check, Plus, Refresh, Search, Spinner } from "../icons";
import { api } from "../api";
import { useLive } from "../live";
import { toModelCards } from "../map";
import type { ModelCard, PipelineNode, StepState } from "../types";

const srcClass: Record<ModelCard["source"], string> = { hf: "src-hf", meta: "src-meta", s3: "src-s3", oci: "src-oci" };
const srcLabel: Record<ModelCard["source"], string> = { hf: "HF", meta: "M", s3: "S3", oci: "OCI" };

function linkState(a: StepState, b: StepState): string {
  if (a === "done" && b === "done") return "done";
  if (a === "done" && b === "cur") return "active";
  return "";
}

function NodeDot({ node }: { node: PipelineNode }) {
  if (node.state === "done")
    return (
      <div className="dot">
        <Check />
      </div>
    );
  if (node.state === "cur")
    return (
      <div className="dot">
        <Spinner />
      </div>
    );
  return <div className="dot" />;
}

const dot6 = { display: "inline-block", width: 6, height: 6, borderRadius: "50%" } as const;

export function Models() {
  const base = useLive(api.baseModels, { kinds: ["BaseModel"], throttleMs: 8_000, intervalMs: 30_000 });
  const cluster = useLive(api.clusterBaseModels, { kinds: ["ClusterBaseModel"], throttleMs: 8_000, intervalMs: 30_000 });
  const models = toModelCards(base.data?.items ?? [], cluster.data?.items ?? []);
  const empty = base.error ?? cluster.error ?? (base.loading || cluster.loading ? "Loading models…" : "No models registered");

  return (
    <section className="panel active" id="panel-models">
      <div className="page-head">
        <div>
          <h1>Model registry</h1>
          <p>
            BaseModel and ClusterBaseModel sources. MUSE drives the onboarding pipeline: import →
            convert (PyTorch → safetensors) → generate config → replicate across clouds.
          </p>
        </div>
        <div className="actions">
          <button className="btn ghost">
            <Refresh />
            Sync sources
          </button>
          <button className="btn primary">
            <Plus />
            Onboard model
          </button>
        </div>
      </div>

      <div className="filterbar">
        <div className="input grow">
          <Search />
          <input placeholder="Filter models by name, architecture, source…" />
        </div>
        <div className="input">
          <select>
            <option>All sources</option>
            <option>HuggingFace</option>
            <option>S3</option>
            <option>OCI</option>
            <option>internal-Meta</option>
          </select>
        </div>
        <div className="input">
          <select>
            <option>All formats</option>
            <option>safetensors</option>
            <option>pytorch</option>
          </select>
        </div>
        <div className="input">
          <select>
            <option>All states</option>
            <option>Ready</option>
            <option>Converting</option>
            <option>In Transit</option>
            <option>Importing</option>
          </select>
        </div>
      </div>

      <div className="model-grid">
        {models.map((m) => (
          <div className="mcard" key={m.name}>
            <div className="top">
              <div className={`src-logo ${srcClass[m.source]}`}>{srcLabel[m.source]}</div>
              <div className="mh">
                <div className="nm">
                  {m.name} <Pill tone={m.status.tone} label={m.status.label} />
                </div>
                <div className="src">{m.uri}</div>
              </div>
            </div>

            <div className="specs">
              {m.specs.map((s, i) => (
                <span key={i} className={`tag ${s.accent ? "accent" : ""}`}>
                  {s.label}
                </span>
              ))}
            </div>

            <div className="pipeline">
              <div className="ppl">
                {m.pipeline.map((n, i) => (
                  <Fragment key={i}>
                    <div className={`node ${n.state}`}>
                      <NodeDot node={n} />
                      <div className="lab">{n.label}</div>
                    </div>
                    {i < m.pipeline.length - 1 ? (
                      <div className={`link ${linkState(n.state, m.pipeline[i + 1].state)}`} />
                    ) : null}
                  </Fragment>
                ))}
              </div>

              {m.xfer ? (
                <div className="xfer" style={{ marginTop: 16 }}>
                  <div className="xh">
                    {m.xfer.routeFrom && m.xfer.routeTo ? (
                      <div className="route">
                        <span className={`cdot ${m.xfer.routeFrom.cloud}`} style={dot6} />
                        {m.xfer.routeFrom.label} <span className="arr">→</span>{" "}
                        <span className={`cdot ${m.xfer.routeTo.cloud}`} style={dot6} />
                        {m.xfer.routeTo.label} <span className="muted">{m.xfer.routeNote}</span>
                      </div>
                    ) : (
                      <span className="muted">{m.xfer.heading}</span>
                    )}
                    <span
                      className="mono"
                      style={{ color: m.xfer.tone === "violet" ? "var(--violet)" : "var(--accent-2)" }}
                    >
                      {m.xfer.pct}%
                    </span>
                  </div>
                  <Bar pct={m.xfer.pct} tone={m.xfer.tone} striped />
                  <div className="stats">
                    {m.xfer.stats.map(([k, v], i) => (
                      <span key={i}>
                        {k} <b>{v}</b>
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}

              {m.replicas ? (
                <div style={{ display: "flex", gap: 6, marginTop: 14, flexWrap: "wrap", alignItems: "center" }}>
                  <span style={{ fontSize: 11, color: "var(--fg-3)" }}>Replicas:</span>
                  {m.replicas.map((r, i) => (
                    <span key={i} className="pill slate" style={{ padding: "2px 8px" }}>
                      <span className={`cdot ${r.cloud}`} style={{ ...dot6, marginRight: 4 }} />
                      {r.label}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
          </div>
        ))}
      </div>
      {models.length === 0 ? (
        <div className="muted" style={{ padding: 24, fontSize: "var(--t-sm)" }}>
          {empty}
        </div>
      ) : null}
    </section>
  );
}
