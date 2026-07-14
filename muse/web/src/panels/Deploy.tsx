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

      <div className="summary-note warn">
        <Info />
        <div>
          <b className="warn-label">Preview.</b> Deploying from the console isn't wired up yet —
          this form is a design preview with placeholder values and doesn't create anything.
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
            <div className="sec-indent">
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
            <div className="radio-cards cols-2">
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
            <div className="field-row spaced">
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
            <div className="canary-preview">
              <div className="eyebrow">Canary steps preview</div>
              <div className="canary-steps">
                <span className="cstep done" />
                <span className="cstep" />
                <span className="cstep" />
                <span className="cstep" />
              </div>
              <div className="canary-scale">
                <span>10%</span>
                <span>25%</span>
                <span>50%</span>
                <span>100%</span>
              </div>
              <div className="canary-hint">
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
            <div className="card-body pad-lg">
              <dl className="kv summary">
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
              <button className="btn primary block" disabled title="Deploying isn't wired yet">
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
