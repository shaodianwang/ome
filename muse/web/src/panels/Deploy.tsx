import { Info, Search, Upload } from "../icons";

export function Deploy() {
  return (
    <section className="panel active" id="panel-deploy">
      <div className="page-head">
        <div>
          <h1>Deploy a model</h1>
          <p>
            Pick a model and where it should run. The system selects how to serve it, places it on
            the best-fit cluster, and manages the rollout — you declare intent, not infrastructure.
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
          <b style={{ color: "var(--amber)" }}>Preview.</b> Deploying from the console isn't wired
          up yet — this form is a design preview with placeholder values and doesn't create
          anything.
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
              Pick a ready model. Its architecture, format, and capabilities are used to choose how
              to serve it.
            </p>
            <div style={{ marginLeft: 29 }}>
              <div className="field">
                <label>Model</label>
                <div className="input">
                  <Search />
                  <input defaultValue="llama-3.1-405b-instruct" />
                </div>
                <span className="hint">llama · safetensors · fp8 · 405B — ready in 3 clouds</span>
              </div>
            </div>
          </div>

          {/* 2 placement */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">2</span>
              <h3>Where it runs</h3>
            </div>
            <p className="sec-desc">Place it automatically by free capacity, or pin a cluster.</p>
            <div className="radio-cards" style={{ gridTemplateColumns: "1fr 1fr" }}>
              <label className="rcard">
                <input type="radio" name="place" defaultChecked />
                <div className="rt">
                  <span className="rk" />
                  Automatic
                </div>
                <div className="rd">
                  Runs on the cluster with the most free capacity for the accelerator you pick.
                </div>
              </label>
              <label className="rcard">
                <input type="radio" name="place" />
                <div className="rt">
                  <span className="rk" />
                  Pin to a cluster
                </div>
                <div className="rd">Force a single named cluster.</div>
              </label>
            </div>
            <div className="field-row" style={{ marginTop: 16 }}>
              <div className="field">
                <label>Accelerator</label>
                <div className="input">
                  <select>
                    <option>H100</option>
                    <option>H100 (RDMA)</option>
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

          {/* 3 scaling */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">3</span>
              <h3>Size &amp; autoscaling</h3>
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
                <label>Autoscaling</label>
                <div className="input">
                  <select>
                    <option>On request load</option>
                    <option>On GPU utilization</option>
                    <option>Off — fixed size</option>
                  </select>
                </div>
              </div>
              <div className="field">
                <label>Target requests / replica</label>
                <div className="input">
                  <input defaultValue="64" className="mono" />
                </div>
              </div>
            </div>
          </div>

          {/* 4 rollout */}
          <div className="form-section">
            <div className="sec-title">
              <span className="sec-num">4</span>
              <h3>Rollout</h3>
            </div>
            <p className="sec-desc">How future updates roll out after the first deploy.</p>
            <div className="radio-cards">
              <label className="rcard">
                <input type="radio" name="roll" />
                <div className="rt">
                  <span className="rk" />
                  Replace
                </div>
                <div className="rd">Swap in place. Brief unavailability.</div>
              </label>
              <label className="rcard">
                <input type="radio" name="roll" defaultChecked />
                <div className="rt">
                  <span className="rk" />
                  Canary
                </div>
                <div className="rd">Shift traffic in steps, checking health at each.</div>
              </label>
              <label className="rcard">
                <input type="radio" name="roll" />
                <div className="rt">
                  <span className="rk" />
                  Blue-green
                </div>
                <div className="rd">Warm the new version fully, then cut over.</div>
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
                Advances a step only while latency and error rate stay within target.
              </div>
            </div>
          </div>
        </div>

        {/* SUMMARY */}
        <div className="yaml-card">
          <div className="card">
            <div className="summary-note">
              <Info />
              <div>
                One deployment, placed on the best-fit cluster automatically — you don't create
                per-cluster objects.
              </div>
            </div>
            <div className="card-body" style={{ padding: "16px 20px" }}>
              <dl className="kv" style={{ gridTemplateColumns: "120px 1fr", gap: "8px 12px" }}>
                <dt>Model</dt>
                <dd>llama-3.1-405b-instruct</dd>
                <dt>Where</dt>
                <dd>Automatic · H100</dd>
                <dt>Size</dt>
                <dd>4 – 16 replicas</dd>
                <dt>Rollout</dt>
                <dd>Canary 10/25/50/100</dd>
                <dt>Est. GPUs</dt>
                <dd>128 at full scale</dd>
              </dl>
              <button
                className="btn primary"
                disabled
                title="Deploying isn't wired yet"
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
                Deploy (not yet wired)
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
