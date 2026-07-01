import { useState } from "react";

import type { PanelKey } from "../App";
import { api } from "../api";
import type { IsvcDetail } from "../api";
import { ClusterFilter, Pill } from "../components";
import { Copyable, Drawer, Facts, Section } from "../drawer";
import { Copy, Link, Plus, Search } from "../icons";
import { useEventFeed, useLive } from "../live";
import { replicaRollup, toWorkloadRows } from "../map";
import { matches } from "../util";

interface Selected {
  cluster: string;
  name: string;
}

export function Workloads({ goto }: { goto: (p: PanelKey) => void }) {
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });
  const [q, setQ] = useState("");
  const [cluster, setCluster] = useState("");
  const [selected, setSelected] = useState<Selected | null>(null);

  const all = toWorkloadRows(isvcs.data?.items ?? []);
  const clusters = [...new Set(all.map((w) => w.cluster))].sort();
  const workloads = all.filter(
    (w) => (cluster === "" || w.cluster === cluster) && matches(q, w.name, w.model, w.cluster),
  );

  return (
    <section className="panel active" id="panel-workloads">
      <div className="page-head">
        <div>
          <h1>Workloads</h1>
          <p>
            Every model deployment across the fleet. Click a row to see its endpoint, replicas, and
            to send it a test request.
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
        <ClusterFilter clusters={clusters} value={cluster} onChange={setCluster} />
        <div className="input grow">
          <Search />
          <input
            placeholder="Filter by name or model…"
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
                <th>Model</th>
                <th className="right">Replicas</th>
                <th>Endpoint</th>
                <th className="right">Age</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {workloads.map((w) => (
                <tr
                  key={w.id}
                  className="rowlink"
                  onClick={() => setSelected({ cluster: w.cluster, name: w.name })}
                >
                  <td className="cell-name mono">{w.name}</td>
                  <td className="mono cell-sm">{w.model}</td>
                  <td className="num">
                    <span className={`repl ${w.replicas.tone}`}>{w.replicas.label}</span>
                    {w.load ? <div className="cell-sub">{w.load}</div> : null}
                  </td>
                  <td className="endpoint-cell">
                    {w.endpoint ? (
                      <a
                        className="ep-link mono"
                        href={w.endpoint}
                        target="_blank"
                        rel="noreferrer"
                        title={w.endpoint}
                        onClick={(e) => e.stopPropagation()}
                      >
                        <Link />
                        {hostOf(w.endpoint)}
                      </a>
                    ) : (
                      <span className="muted">—</span>
                    )}
                  </td>
                  <td className="num muted">{w.age}</td>
                  <td>
                    <Pill tone={w.status.tone} label={w.status.label} />
                    {w.reason ? <div className="cell-sub bad">{w.reason}</div> : null}
                  </td>
                </tr>
              ))}
              {workloads.length === 0 ? (
                <tr>
                  <td colSpan={6} className="muted empty-cell">
                    {q.trim() && all.length
                      ? `No deployments match "${q}"`
                      : (isvcs.error ??
                        (isvcs.loading ? "Loading deployments…" : "No deployments"))}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className="list-foot">
        Showing <b>{workloads.length}</b> deployments
        {cluster ? ` on ${cluster}` : " across the fleet"}
      </div>

      {selected ? <WorkloadDrawer sel={selected} onClose={() => setSelected(null)} /> : null}
    </section>
  );
}

/** Host portion of a URL for a compact endpoint chip. */
function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

function ago(atSecs: number): string {
  const secs = Math.max(0, Math.floor(Date.now() / 1000) - atSecs);
  if (secs >= 86_400) return `${Math.floor(secs / 86_400)}d ago`;
  if (secs >= 3_600) return `${Math.floor(secs / 3_600)}h ago`;
  if (secs >= 60) return `${Math.floor(secs / 60)}m ago`;
  return `${secs}s ago`;
}

function WorkloadDrawer({ sel, onClose }: { sel: Selected; onClose: () => void }) {
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

  return (
    <Drawer title={sel.name} subtitle={sel.cluster} onClose={onClose}>
      {!d ? (
        <div className="muted drawer-empty">
          {detail.error ?? (detail.loading ? "Loading…" : "Not found")}
        </div>
      ) : (
        <>
          <div className="drawer-toprow">
            {status ? <Pill tone={status.tone} label={status.label} /> : null}
          </div>

          {d.reason ? (
            <div className="notice bad">
              <b>{d.reason}</b>
              {d.message ? <div>{d.message}</div> : null}
            </div>
          ) : null}

          <Section title="Try it">
            <ChatBox
              cluster={sel.cluster}
              name={sel.name}
              endpoint={d.url}
              defaultModel={d.model ?? ""}
              enabled={cfg.data?.chat === true}
            />
          </Section>

          <Section title="Overview">
            <Facts
              items={[
                ["Model", d.model ? <span className="mono">{d.model}</span> : null],
                ["Endpoint", d.url ? <Copyable text={d.url} /> : null],
                ["Replicas", replicaRollup(d.components).label],
                ["Where", sel.cluster],
                ["Age", d.age],
              ]}
            />
          </Section>

          {activity.length ? (
            <Section title="Recent activity">
              <ul className="mini-feed">
                {activity.slice(0, 8).map((e, i) => (
                  <li key={i}>
                    <span className="mono">{e.action === "deleted" ? "removed" : "updated"}</span> ·{" "}
                    {ago(e.at)}
                  </li>
                ))}
              </ul>
            </Section>
          ) : null}
        </>
      )}
    </Drawer>
  );
}

/** In-drawer inference test: a live chat when the proxy is enabled, else a
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
