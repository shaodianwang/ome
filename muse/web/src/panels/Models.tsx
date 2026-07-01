import { useState } from "react";

import { api } from "../api";
import { Pill } from "../components";
import { Refresh, Search } from "../icons";
import { useLive } from "../live";
import { toModelCards } from "../map";

export function Models() {
  const base = useLive(api.baseModels, { kinds: ["BaseModel"], throttleMs: 8_000, intervalMs: 30_000 });
  const cluster = useLive(api.clusterBaseModels, { kinds: ["ClusterBaseModel"], throttleMs: 8_000, intervalMs: 30_000 });
  const [q, setQ] = useState("");

  const all = toModelCards(base.data?.items ?? [], cluster.data?.items ?? []);
  const needle = q.trim().toLowerCase();
  const models = all.filter(
    (m) =>
      !needle ||
      m.name.toLowerCase().includes(needle) ||
      m.specs.some((s) => s.label.toLowerCase().includes(needle)),
  );

  const empty =
    base.error ??
    cluster.error ??
    (base.loading || cluster.loading ? "Loading models…" : "No models registered");

  return (
    <section className="panel active" id="panel-models">
      <div className="page-head">
        <div>
          <h1>Model registry</h1>
          <p>
            BaseModel and ClusterBaseModel sources across the fleet, with the architecture, format,
            size and capabilities OME auto-parses at onboarding.
          </p>
        </div>
        <div className="actions">
          <button className="btn ghost" onClick={() => { base.refetch(); cluster.refetch(); }}>
            <Refresh />
            Refresh
          </button>
        </div>
      </div>

      <div className="filterbar">
        <div className="input grow">
          <Search />
          <input
            placeholder="Filter by name, architecture, format, capability…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>

      <div className="model-grid">
        {models.map((m) => (
          <div className="mcard" key={`${m.scoped ? "c" : "n"}/${m.name}`}>
            <div className="mh">
              <div className="nm">
                {m.name} <Pill tone={m.status.tone} label={m.status.label} />
              </div>
              <div className="src">
                {m.scoped ? "ClusterBaseModel" : "BaseModel"} · {m.where}
              </div>
            </div>
            {m.specs.length ? (
              <div className="specs">
                {m.specs.map((s, i) => (
                  <span key={i} className={`tag ${s.accent ? "accent" : ""}`}>
                    {s.label}
                  </span>
                ))}
              </div>
            ) : null}
          </div>
        ))}
      </div>
      {models.length === 0 ? (
        <div className="muted" style={{ padding: 24, fontSize: "var(--t-sm)" }}>
          {needle && all.length ? `No models match "${q}"` : empty}
        </div>
      ) : null}
    </section>
  );
}
