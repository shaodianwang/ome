import type { ReactNode } from "react";
import { useState } from "react";

import type { PanelKey } from "../App";
import { api } from "../api";
import type { FtwItem, IsvcItem, ModelDetail } from "../api";
import { ClusterFilter, Pill } from "../components";
import { Copyable, Drawer, Facts, Section } from "../drawer";
import { Plus, Refresh, Search } from "../icons";
import { useLive } from "../live";
import { modelUsage, stateTone, toModelCards } from "../map";
import { emptyText, matches } from "../util";

interface Selected {
  cluster: string;
  name: string;
  scoped: boolean;
}

export function Models({ goto }: { goto: (p: PanelKey) => void }) {
  const base = useLive(api.baseModels, {
    kinds: ["BaseModel"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const cluster = useLive(api.clusterBaseModels, {
    kinds: ["ClusterBaseModel"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const ftws = useLive(api.finetunedWeights, {
    kinds: ["FineTunedWeight"],
    throttleMs: 8_000,
    intervalMs: 60_000,
  });
  const [q, setQ] = useState("");
  const [clusterSel, setClusterSel] = useState("");
  const [selected, setSelected] = useState<Selected | null>(null);

  const usage = modelUsage(isvcs.data?.items ?? []);
  const all = toModelCards(base.data?.items ?? [], cluster.data?.items ?? [], usage);
  const clusters = [...new Set(all.map((m) => m.cluster))].sort();
  const models = all.filter(
    (m) =>
      (clusterSel === "" || m.cluster === clusterSel) &&
      matches(q, m.name, m.source, ...m.specs.map((s) => s.label)),
  );

  return (
    <section className="panel active" id="panel-models">
      <div className="page-head">
        <div>
          <h1>Models</h1>
          <p>
            Models registered across the fleet, with the architecture, size and capabilities OME
            parses at onboarding. Click one to see where it's served and what can run it.
          </p>
        </div>
        <div className="actions">
          <button
            className="btn ghost"
            onClick={() => {
              base.refetch();
              cluster.refetch();
            }}
          >
            <Refresh />
            Refresh
          </button>
        </div>
      </div>

      <div className="filterbar">
        <ClusterFilter clusters={clusters} value={clusterSel} onChange={setClusterSel} />
        <div className="input grow">
          <Search />
          <input
            placeholder="Filter by name, architecture, size, capability…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>

      <div className="model-grid">
        {models.map((m) => (
          <button
            type="button"
            className="mcard"
            key={`${m.scoped ? "c" : "n"}/${m.cluster}/${m.name}`}
            onClick={() => setSelected({ cluster: m.cluster, name: m.name, scoped: m.scoped })}
          >
            <div className="mh">
              <div className="nm">
                {m.name} <Pill tone={m.status.tone} label={m.status.label} />
                {m.disabled ? <span className="tag warn">disabled</span> : null}
              </div>
              <div className="src">{m.where}</div>
            </div>
            <div className="mmeta">
              {m.source ? <span className={`src-badge ${m.source}`}>{m.source}</span> : null}
              <span className={`use-chip ${m.usedBy ? "on" : ""}`}>
                {m.usedBy ? `${m.usedBy} serving` : "unused"}
              </span>
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
          </button>
        ))}
      </div>
      {models.length === 0 ? (
        <div className="muted empty-note">
          {q.trim() && all.length
            ? `No models match "${q}"`
            : emptyText(
                { loading: base.loading || cluster.loading, error: base.error ?? cluster.error },
                "Loading models…",
                "No models registered",
              )}
        </div>
      ) : null}

      {selected ? (
        <ModelDrawer
          sel={selected}
          isvcs={isvcs.data?.items ?? []}
          ftws={ftws.data?.items ?? []}
          goto={goto}
          onClose={() => setSelected(null)}
        />
      ) : null}
    </section>
  );
}

/** Facts from a model's parsed configuration (primitive values only, capped). */
function configFacts(cfg?: Record<string, unknown>): [string, ReactNode][] {
  if (!cfg) return [];
  return Object.entries(cfg)
    .filter(([, v]) => v == null || typeof v !== "object")
    .slice(0, 24)
    .map(([k, v]) => [k, String(v)] as [string, ReactNode]);
}

function ModelDrawer({
  sel,
  isvcs,
  ftws,
  goto,
  onClose,
}: {
  sel: Selected;
  isvcs: IsvcItem[];
  ftws: FtwItem[];
  goto: (p: PanelKey) => void;
  onClose: () => void;
}) {
  const detail = useLive<ModelDetail>(
    () =>
      sel.scoped
        ? api.clusterBaseModel(sel.cluster, sel.name)
        : api.baseModel(sel.cluster, sel.name),
    { kinds: [sel.scoped ? "ClusterBaseModel" : "BaseModel"], throttleMs: 5_000 },
  );
  const d = detail.data;
  const serving = isvcs.filter((s) => s.model === sel.name);
  const adapters = ftws.filter((w) => w.baseModel === sel.name);

  return (
    <Drawer title={sel.name} subtitle={sel.cluster} onClose={onClose}>
      {!d ? (
        <div className="muted drawer-empty">
          {detail.error ?? (detail.loading ? "Loading…" : "Not found")}
        </div>
      ) : (
        <>
          <div className="drawer-toprow">
            {d.state ? <Pill tone={stateTone(d.state)} label={d.state} /> : null}
            <span className={`use-chip ${serving.length ? "on" : ""}`}>
              {serving.length ? `${serving.length} serving` : "unused"}
            </span>
            {d.disabled ? <span className="tag warn">disabled</span> : null}
            <button className="btn primary sm drawer-cta" onClick={() => goto("deploy")}>
              <Plus />
              Deploy
            </button>
          </div>

          <Section title="Overview">
            <Facts
              items={[
                ["Display name", d.displayName],
                ["Architecture", d.architecture],
                ["Parameters", d.parameterSize],
                ["Format", d.format],
                ["Quantization", d.quantization],
                ["Max tokens", d.maxTokens != null ? d.maxTokens.toLocaleString() : null],
                ["Vendor", d.vendor],
                ["Version", d.version],
              ]}
            />
            {d.capabilities && d.capabilities.length ? (
              <div className="chips">
                {d.capabilities.map((c) => (
                  <span className="tag" key={c}>
                    {c}
                  </span>
                ))}
              </div>
            ) : null}
          </Section>

          {d.storageUri ? (
            <Section title="Storage">
              <Facts items={[["Source", d.source]]} />
              <Copyable text={d.storageUri} />
            </Section>
          ) : null}

          <Section title={`Serving (${serving.length})`}>
            {serving.length ? (
              <ul className="ref-list">
                {serving.map((s) => (
                  <li key={`${s.cluster}/${s.namespace}/${s.name}`}>
                    <span className="mono">{s.name}</span>
                    <span className="muted">
                      {" "}
                      · {s.namespace ?? "—"} · {s.cluster}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="muted">No deployments reference this model yet.</div>
            )}
          </Section>

          {adapters.length ? (
            <Section title={`Fine-tuned weights (${adapters.length})`}>
              <ul className="ref-list">
                {adapters.map((w) => (
                  <li key={`${w.cluster}/${w.namespace}/${w.name}`}>
                    <span className="mono">{w.name}</span>
                    {w.modelType ? <span className="muted"> · {w.modelType}</span> : null}
                  </li>
                ))}
              </ul>
            </Section>
          ) : null}

          {configFacts(d.modelConfiguration).length ? (
            <Section title="Model configuration">
              <Facts items={configFacts(d.modelConfiguration)} />
            </Section>
          ) : null}
        </>
      )}
    </Drawer>
  );
}
