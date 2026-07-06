import { useState } from "react";

import { api } from "../api";
import type { IsvcDetail } from "../api";
import { Pill } from "../components";
import { Copyable, Facts } from "../drawer";
import { Copy } from "../icons";
import { useEventFeed, useLive } from "../live";
import { replicaRollup } from "../map";
import { ago, num as n } from "../util";
import { Dot, UtilRow } from "../viz";

export interface Selected {
  cluster: string;
  name: string;
}

/** Full-page detail for one deployment: health, replicas, model/runtime,
 *  placement, a test box, and recent activity. No k8s conditions or deployment
 *  mode — health is shown in plain terms. */
export function DeploymentDetail({ sel, onBack }: { sel: Selected; onBack: () => void }) {
  const detail = useLive<IsvcDetail>(() => api.inferenceService(sel.cluster, sel.name), {
    kinds: ["InferenceService"],
    throttleMs: 4_000,
  });
  const cfg = useLive(api.config, { kinds: [] });
  const feed = useEventFeed(200);
  const d = detail.data;

  const activity = feed.events.filter(
    (e) => e.kind === "InferenceService" && e.name === sel.name && e.cluster === sel.cluster,
  );

  const status = !d
    ? null
    : d.ready === true
      ? { label: "Ready", tone: "green" as const }
      : d.ready === false
        ? { label: "Not ready", tone: "red" as const }
        : { label: "Unknown", tone: "slate" as const };

  const standby = (d?.placement?.candidates ?? []).filter(
    (c) => c.cluster !== d?.placement?.cluster,
  );

  return (
    <section className="panel active">
      <div className="detail-head">
        <button className="backlink" onClick={onBack}>
          ‹ Deployments
        </button>
        <h1 className="detail-title mono">{sel.name}</h1>
        <span className="detail-meta">
          {sel.cluster}
          {d?.namespace ? ` · ${d.namespace}` : ""}
        </span>
        {status ? <Pill tone={status.tone} label={status.label} /> : null}
        {d?.age ? <span className="detail-meta">· {d.age}</span> : null}
      </div>

      {!d ? (
        <div className="muted attn-none">
          {detail.error ?? (detail.loading ? "Loading…" : "Not found")}
        </div>
      ) : (
        <>
          {d.url ? (
            <div className="detail-endpoint">
              <span className="eyebrow">Endpoint</span>
              <Copyable text={d.url} />
            </div>
          ) : null}

          {d.ready === false && (d.reason || d.message) ? (
            <div className="notice bad">
              {d.reason ? <b>{d.reason}</b> : null}
              {d.message ? <div>{d.message}</div> : null}
            </div>
          ) : null}

          <div className="detail-grid2">
            <div className="card">
              <div className="card-head">
                <h3>Replicas</h3>
                <span className="sub">{replicaRollup(d.components).label} ready</span>
              </div>
              <div className="card-body">
                {d.components && d.components.length ? (
                  <div className="urows">
                    {d.components.map((c) => {
                      const cur = c.currentReplicas ?? 0;
                      const des = c.desiredReplicas ?? 0;
                      const pct = des > 0 ? Math.round((cur / des) * 100) : cur > 0 ? 100 : 0;
                      const tone = c.ready ? "green" : cur < des ? "amber" : "";
                      return (
                        <UtilRow
                          key={c.name}
                          name={c.name}
                          pct={pct}
                          tone={tone}
                          value={`${cur}/${des}`}
                        />
                      );
                    })}
                  </div>
                ) : (
                  <div className="muted">No component detail.</div>
                )}
                {d.loadedCopies != null || d.failedCopies ? (
                  <div className="detail-load">
                    <Dot tone={d.failedCopies ? "amber" : "green"} />
                    model {n(d.loadedCopies ?? 0)} loaded
                    {d.failedCopies ? ` · ${n(d.failedCopies)} failed` : ""}
                  </div>
                ) : null}
              </div>
            </div>

            <div className="card">
              <div className="card-head">
                <h3>Model &amp; runtime</h3>
              </div>
              <div className="card-body">
                <Facts
                  items={[
                    ["Model", d.model ? <span className="mono">{d.model}</span> : null],
                    ["Runtime", d.runtime ? <span className="mono">{d.runtime}</span> : null],
                    ["Namespace", d.namespace],
                    ["Where", sel.cluster],
                    ["Age", d.age],
                  ]}
                />
              </div>
            </div>
          </div>

          {d.placement && (d.placement.cluster || standby.length) ? (
            <div className="card section-gap">
              <div className="card-head">
                <h3>Placement</h3>
                <span className="sub">where it runs</span>
              </div>
              <div className="card-body">
                <div className="urows">
                  {d.placement.cluster ? (
                    <div className="place-row">
                      <Dot tone="green" />
                      <span className="mono">{d.placement.cluster}</span>
                      <span className="muted">active</span>
                    </div>
                  ) : null}
                  {standby.map((c, i) => (
                    <div className="place-row" key={i}>
                      <Dot tone="slate" />
                      <span className="mono">{c.cluster}</span>
                      <span className="muted">standby</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : null}

          <div className="card section-gap">
            <div className="card-head">
              <h3>Test</h3>
            </div>
            <div className="card-body">
              <ChatBox
                cluster={sel.cluster}
                name={sel.name}
                endpoint={d.url}
                defaultModel={d.model ?? ""}
                enabled={cfg.data?.chat === true}
              />
            </div>
          </div>

          {activity.length ? (
            <div className="card section-gap">
              <div className="card-head">
                <h3>Recent activity</h3>
              </div>
              <div className="card-body">
                <ul className="mini-feed">
                  {activity.slice(0, 12).map((e, i) => (
                    <li key={i}>
                      <span className="mono">{e.action === "deleted" ? "removed" : "updated"}</span>{" "}
                      · {ago(e.at)}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          ) : null}
        </>
      )}
    </section>
  );
}

/** In-page inference test: a live chat when the proxy is enabled, else a
 *  copyable curl against the endpoint. */
function ChatBox({
  cluster,
  name,
  endpoint,
  defaultModel,
  enabled,
}: {
  cluster: string;
  name: string;
  endpoint?: string;
  defaultModel: string;
  enabled: boolean;
}) {
  const [model, setModel] = useState(defaultModel);
  const [prompt, setPrompt] = useState("");
  const [reply, setReply] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);

  if (!endpoint) {
    return <div className="muted">No endpoint yet — this deployment isn't addressable.</div>;
  }

  if (!enabled) {
    const curl = [
      `curl -s ${endpoint}/v1/chat/completions \\`,
      `  -H 'content-type: application/json' \\`,
      `  -d '${JSON.stringify({ model: defaultModel, messages: [{ role: "user", content: "Hello" }] })}'`,
    ].join("\n");
    return (
      <div className="tryit">
        <div className="muted cell-sm">Live testing is off on this console — run it yourself:</div>
        <pre className="curl">{curl}</pre>
        <button
          className="btn ghost sm"
          onClick={() => {
            void navigator.clipboard?.writeText(curl);
            setCopied(true);
            setTimeout(() => setCopied(false), 1200);
          }}
        >
          <Copy />
          {copied ? "Copied" : "Copy curl"}
        </button>
      </div>
    );
  }

  const send = () => {
    if (!prompt.trim() || busy) return;
    setBusy(true);
    setErr(null);
    setReply(null);
    api
      .chat(cluster, name, { model, messages: [{ role: "user", content: prompt }] })
      .then((r) => setReply(r.choices?.[0]?.message?.content ?? JSON.stringify(r)))
      .catch((e: unknown) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setBusy(false));
  };

  return (
    <div className="tryit">
      <div className="chat-model">
        <label>model</label>
        <input value={model} onChange={(e) => setModel(e.target.value)} />
      </div>
      <textarea
        className="chat-input"
        placeholder="Send a test prompt…"
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
      />
      <button className="btn primary sm" onClick={send} disabled={busy || !prompt.trim()}>
        {busy ? "Sending…" : "Send"}
      </button>
      {err ? <div className="notice bad">{err}</div> : null}
      {reply ? <div className="chat-reply">{reply}</div> : null}
    </div>
  );
}
