import { Check, Info, Search, Upload } from "../icons";

// Static highlighted YAML preview (mirrors the mockup). Trusted, constant markup.
const YAML_HTML = `<span class="c">apiVersion:</span> <span class="d">ome.io/v1beta1</span>
<span class="k">kind:</span> <span class="d">InferenceService</span>
<span class="k">metadata:</span>
  <span class="k">name:</span> <span class="s">llama-3.1-405b-instruct</span>
  <span class="k">namespace:</span> <span class="s">genai-prod</span>
<span class="k">spec:</span>
  <span class="k">model:</span>
    <span class="k">name:</span> <span class="s">llama-3.1-405b-instruct</span>
    <span class="k">kind:</span> <span class="s">ClusterBaseModel</span>
  <span class="c"># runtime auto-selected — omit to let OME pick</span>
  <span class="k">placement:</span>
    <span class="k">strategy:</span> <span class="s">BestFit</span>
    <span class="k">accelerator:</span> <span class="s">H100</span>
  <span class="k">engine:</span>            <span class="c"># prefill</span>
    <span class="k">minReplicas:</span> <span class="n">4</span>
    <span class="k">maxReplicas:</span> <span class="n">16</span>
  <span class="k">decoder:</span>           <span class="c"># decode</span>
    <span class="k">minReplicas:</span> <span class="n">4</span>
    <span class="k">maxReplicas:</span> <span class="n">16</span>
  <span class="k">rollout:</span>
    <span class="k">groups:</span>
    <span class="d">-</span> <span class="k">canary:</span>
        <span class="k">steps:</span> [<span class="n">10</span>, <span class="n">25</span>, <span class="n">50</span>, <span class="n">100</span>]
        <span class="k">analysis:</span>
          <span class="k">p95LatencyMs:</span> <span class="n">600</span>
          <span class="k">errorRate:</span> <span class="s">"0.5%"</span>
  <span class="k">annotations:</span>
    <span class="d">ome.io/deploymentMode:</span> <span class="s">PDDisaggregated</span>`;

export function Deploy() {
  return (
    <section className="panel active" id="panel-deploy">
      <div className="page-head">
        <div>
          <h1>Deploy a model</h1>
          <p>
            Create one InferenceService on the control plane. OME selects a runtime, places it onto
            workload clusters, and drives the rollout. You don't pick nodes — you declare intent.
          </p>
        </div>
      </div>

      <div
        className="summary-note"
        style={{
          marginBottom: 16,
          borderRadius: 8,
          border: "1px solid color-mix(in srgb, var(--amber) 30%, var(--border))",
          background: "var(--amber-dim)",
          color: "var(--fg-2)",
        }}
      >
        <Info />
        <div>
          <b style={{ color: "var(--amber)" }}>Preview.</b> The write plane isn't wired yet — this
          form is a design preview with placeholder values and doesn't create anything. When built,
          Deploy will create one InferenceService on the control plane and let placement fan it out.
        </div>
      </div>

      <div className="deploy-layout">
        <div className="card">
          {/* 1 model */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">1</span>
              <h3>Model</h3>
            </div>
            <p className="sec-desc">
              Pick a Ready model from the registry. Selecting a model seeds architecture, format,
              and capabilities for runtime matching.
            </p>
            <div style={{ marginLeft: 29 }}>
              <div className="field">
                <label>Base model</label>
                <div className="input">
                  <Search />
                  <input defaultValue="llama-3.1-405b-instruct" />
                </div>
                <span className="hint">
                  arch: llama · format: safetensors · fp8 · 405B · TEXT_GENERATION — Ready in 3
                  clouds
                </span>
              </div>
            </div>
          </div>

          {/* 2 runtime */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">2</span>
              <h3>Runtime</h3>
            </div>
            <p className="sec-desc">
              Let OME auto-select the best ServingRuntime via the fetch-match-score pipeline, or pin
              one explicitly.
            </p>
            <div style={{ marginLeft: 29 }}>
              <div className="field">
                <label style={{ justifyContent: "space-between" }}>
                  <span className="flexrow">
                    Auto-select runtime{" "}
                    <span className="pill blue" style={{ padding: "1px 7px" }}>
                      <span className="pdot" />
                      recommended
                    </span>
                  </span>
                  <span className="toggle">
                    <input type="checkbox" defaultChecked />
                    <span className="tk" />
                  </span>
                </label>
                <div className="input" style={{ background: "var(--bg-2)", opacity: 0.85 }}>
                  <Check />
                  <input
                    defaultValue="sglang-h100-pd  (matched: fp8 · llama · PD)"
                    disabled
                    style={{ color: "var(--green)" }}
                  />
                </div>
                <span className="hint">
                  score 0.96 — beats vllm-h100 (0.81) on fp8 + PD support
                </span>
              </div>
            </div>
          </div>

          {/* 3 deployment mode */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">3</span>
              <h3>Deployment mode</h3>
            </div>
            <p className="sec-desc">How the runtime is laid out across nodes.</p>
            <div className="radio-cards">
              <label className="rcard">
                <input type="radio" name="mode" />
                <div className="rt">
                  <span className="rk" />
                  RawDeployment
                </div>
                <div className="rd">Single-node Deployment + HPA. Smallest models.</div>
              </label>
              <label className="rcard">
                <input type="radio" name="mode" />
                <div className="rt">
                  <span className="rk" />
                  MultiNode
                </div>
                <div className="rd">LeaderWorkerSet across nodes. Tensor-parallel.</div>
              </label>
              <label className="rcard">
                <input type="radio" name="mode" defaultChecked />
                <div className="rt">
                  <span className="rk" />
                  PD-Disaggregated
                </div>
                <div className="rd">Split prefill / decode Components for throughput.</div>
              </label>
            </div>
          </div>

          {/* 4 placement */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">4</span>
              <h3>Placement</h3>
            </div>
            <p className="sec-desc">
              Let OME place by live capacity (reads each cluster's Kueue), or pin to a specific
              workload cluster.
            </p>
            <div className="radio-cards" style={{ gridTemplateColumns: "1fr 1fr" }}>
              <label className="rcard">
                <input type="radio" name="place" defaultChecked />
                <div className="rt">
                  <span className="rk" />
                  Let OME place
                </div>
                <div className="rd">
                  Scheduler picks the best-fit cluster by accelerator + free capacity.
                </div>
              </label>
              <label className="rcard">
                <input type="radio" name="place" />
                <div className="rt">
                  <span className="rk" />
                  Pin to cluster
                </div>
                <div className="rd">Force a single named workload cluster.</div>
              </label>
            </div>
            <div className="field-row" style={{ marginTop: 16 }}>
              <div className="field">
                <label>Accelerator requirement</label>
                <div className="input">
                  <select>
                    <option>H100</option>
                    <option>H100 (RoCE / RDMA)</option>
                    <option>A100-80G</option>
                    <option>TPU-v5e</option>
                    <option>TPU-v5p</option>
                  </select>
                </div>
              </div>
              <div className="field">
                <label>
                  Preferred clouds <span className="hint">soft constraint</span>
                </label>
                <div className="input">
                  <select>
                    <option>Any (Meta · GCP · OCI · CoreWeave)</option>
                    <option>Meta only</option>
                    <option>GCP only</option>
                    <option>Avoid CoreWeave</option>
                  </select>
                </div>
              </div>
            </div>
          </div>

          {/* 5 scaling */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">5</span>
              <h3>Replicas &amp; autoscaling</h3>
            </div>
            <div className="field-row">
              <div className="field">
                <label>Min replicas</label>
                <div className="input">
                  <input defaultValue="4" className="mono" />
                </div>
              </div>
              <div className="field">
                <label>Max replicas</label>
                <div className="input">
                  <input defaultValue="16" className="mono" />
                </div>
              </div>
            </div>
            <div className="field-row">
              <div className="field">
                <label>Autoscaler</label>
                <div className="input">
                  <select>
                    <option>KEDA (concurrency)</option>
                    <option>HPA (GPU util)</option>
                    <option>None — fixed</option>
                  </select>
                </div>
              </div>
              <div className="field">
                <label>Target concurrency / replica</label>
                <div className="input">
                  <input defaultValue="64" className="mono" />
                </div>
              </div>
            </div>
          </div>

          {/* 6 rollout */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">6</span>
              <h3>Rollout strategy</h3>
            </div>
            <p className="sec-desc">
              How future revisions roll out. Applies on every update after the first deploy.
            </p>
            <div className="radio-cards">
              <label className="rcard">
                <input type="radio" name="roll" />
                <div className="rt">
                  <span className="rk" />
                  None
                </div>
                <div className="rd">Recreate in place. Brief unavailability.</div>
              </label>
              <label className="rcard">
                <input type="radio" name="roll" defaultChecked />
                <div className="rt">
                  <span className="rk" />
                  Canary
                </div>
                <div className="rd">Shift traffic in steps with metric gates.</div>
              </label>
              <label className="rcard">
                <input type="radio" name="roll" />
                <div className="rt">
                  <span className="rk" />
                  Blue-Green
                </div>
                <div className="rd">Warm full green, then cut over.</div>
              </label>
            </div>
            <div
              style={{
                marginLeft: 29,
                marginTop: 16,
                background: "var(--bg-2)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                padding: 14,
              }}
            >
              <div
                style={{
                  fontSize: 11,
                  textTransform: "uppercase",
                  letterSpacing: ".06em",
                  color: "var(--fg-3)",
                  marginBottom: 12,
                }}
              >
                Canary steps preview
              </div>
              <div className="canary-steps">
                <span
                  className="cstep done"
                  style={{ background: "var(--accent)", borderColor: "var(--accent)" }}
                />
                <span
                  className="cstep"
                  style={{
                    background: "var(--accent-dim)",
                    borderColor: "color-mix(in srgb, var(--accent) 40%, transparent)",
                  }}
                />
                <span
                  className="cstep"
                  style={{
                    background: "var(--accent-dim)",
                    borderColor: "color-mix(in srgb, var(--accent) 40%, transparent)",
                  }}
                />
                <span
                  className="cstep"
                  style={{
                    background: "var(--accent-dim)",
                    borderColor: "color-mix(in srgb, var(--accent) 40%, transparent)",
                  }}
                />
              </div>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  fontSize: 11,
                  color: "var(--fg-2)",
                  marginTop: 8,
                  fontFamily: "var(--mono)",
                }}
              >
                <span>10%</span>
                <span>25%</span>
                <span>50%</span>
                <span>100%</span>
              </div>
              <div style={{ fontSize: 11, color: "var(--fg-3)", marginTop: 10 }}>
                Gate: p95 ≤ 600ms · error ≤ 0.5% · 5m soak per step · auto-promote
              </div>
            </div>
          </div>
        </div>

        {/* SUMMARY / YAML */}
        <div className="yaml-card">
          <div className="card" style={{ marginBottom: 16 }}>
            <div className="summary-note">
              <Info />
              <div>
                Deploy creates <b>one</b> InferenceService on the control plane. OME placement fans
                it out to the chosen workload cluster(s) — you are not creating per-cluster objects.
              </div>
            </div>
            <div className="card-body" style={{ padding: "16px 20px" }}>
              <dl className="kv" style={{ gridTemplateColumns: "120px 1fr", gap: "8px 12px" }}>
                <dt>Model</dt>
                <dd>llama-3.1-405b-instruct</dd>
                <dt>Runtime</dt>
                <dd style={{ color: "var(--green)" }}>sglang-h100-pd (auto)</dd>
                <dt>Mode</dt>
                <dd>PDDisaggregated</dd>
                <dt>Placement</dt>
                <dd>OME-scheduled · H100</dd>
                <dt>Replicas</dt>
                <dd>4 – 16 (KEDA)</dd>
                <dt>Rollout</dt>
                <dd>Canary 10/25/50/100</dd>
                <dt>Est. GPUs</dt>
                <dd>128 (16 pods × 8)</dd>
              </dl>
              <button
                className="btn primary"
                disabled
                title="Write plane not yet wired"
                style={{
                  width: "100%",
                  justifyContent: "center",
                  marginTop: 18,
                  padding: 11,
                  opacity: 0.5,
                  cursor: "not-allowed",
                }}
              >
                <Upload />
                Deploy InferenceService (not yet wired)
              </button>
            </div>
          </div>

          <div className="card">
            <div className="card-head" style={{ padding: "10px 16px" }}>
              <h3 style={{ fontSize: 12, fontWeight: 600 }}>InferenceService — preview</h3>
              <div className="right">
                <span className="tag">ome.io/v1beta1</span>
              </div>
            </div>
            <pre className="yaml" dangerouslySetInnerHTML={{ __html: YAML_HTML }} />
          </div>
        </div>
      </div>
    </section>
  );
}
