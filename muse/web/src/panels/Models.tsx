import type { ReactNode } from "react";
import { useState } from "react";

import { api } from "../api";
import type { FtwItem, IsvcItem, ModelDetail, ModelItem } from "../api";
import { Pill, RefList } from "../components";
import { Copyable, Drawer, Facts, Section } from "../drawer";
import { Refresh } from "../icons";
import { useLive } from "../live";
import { modelReference, modelUsage, stateTone } from "../map";
import type { Column, Tab } from "../ResourceTable";
import { ResourceTable } from "../ResourceTable";
import { isLegacySelected, resourceKey, sameResource, selectionKey } from "../resource";
import type { RouteSelected, Selected } from "../resource";
import { ageToSecs, num } from "../util";

type ModelRow = ModelItem & { kind: "BaseModel" | "ClusterBaseModel"; used: number };

const modelIdentity = (m: ModelRow): Selected => ({
  cluster: m.cluster,
  kind: m.kind,
  name: m.name,
  namespace: m.kind === "BaseModel" ? m.namespace : undefined,
});

/** Parameter size ("405B" / "47B" / "350M") to a comparable number, for sorting. */
const sizeVal = (s?: string): number => {
  const m = /^([\d.]+)\s*([bmk]?)/i.exec(s ?? "");
  if (!m) return -1;
  const v = parseFloat(m[1]);
  const u = m[2].toLowerCase();
  return u === "b" ? v * 1e9 : u === "m" ? v * 1e6 : u === "k" ? v * 1e3 : v;
};

const COLUMNS: Column<ModelRow>[] = [
  {
    key: "name",
    label: "Name",
    cellClass: "cell-name mono",
    render: (m) => (
      <>
        {m.name}
        {m.disabled ? <span className="tag warn">disabled</span> : null}
        {m.namespace ? <div className="cell-sub muted">{m.namespace}</div> : null}
      </>
    ),
    sort: (m) => m.name.toLowerCase(),
  },
  {
    key: "arch",
    label: "Architecture",
    render: (m) => m.architecture ?? "—",
    sort: (m) => (m.architecture ?? "").toLowerCase(),
  },
  {
    key: "size",
    label: "Size",
    right: true,
    cellClass: "num",
    render: (m) => (m.parameterSize && m.parameterSize !== "0" ? m.parameterSize : "—"),
    sort: (m) => sizeVal(m.parameterSize),
  },
  {
    key: "quant",
    label: "Quant",
    cellClass: "cell-sm",
    render: (m) => m.quantization ?? "—",
    sort: (m) => (m.quantization ?? "").toLowerCase(),
  },
  {
    key: "format",
    label: "Format",
    cellClass: "cell-sm muted",
    render: (m) => m.format ?? "—",
    sort: (m) => (m.format ?? "").toLowerCase(),
  },
  {
    key: "source",
    label: "Source",
    render: (m) => (m.source ? <span className={`src-badge ${m.source}`}>{m.source}</span> : "—"),
    sort: (m) => (m.source ?? "").toLowerCase(),
  },
  {
    key: "used",
    label: "Used by",
    right: true,
    cellClass: "num",
    render: (m) => <span className={`use-chip ${m.used ? "on" : ""}`}>{m.used || "—"}</span>,
    sort: (m) => m.used,
  },
  {
    key: "cluster",
    label: "Cluster",
    cellClass: "mono cell-sm muted",
    render: (m) => m.cluster,
    sort: (m) => m.cluster.toLowerCase(),
  },
  {
    key: "age",
    label: "Age",
    right: true,
    cellClass: "num muted",
    render: (m) => m.age ?? "—",
    sort: (m) => ageToSecs(m.age ?? ""),
  },
];

const TABS: Tab<ModelRow>[] = [
  { key: "all", label: "All", match: () => true },
  { key: "ready", label: "Ready", match: (m) => m.state === "Ready", empty: "No ready models." },
  {
    key: "progress",
    label: "In progress",
    match: (m) => !!m.state && m.state !== "Ready" && m.state !== "Failed",
    empty: "Nothing importing.",
  },
  {
    key: "failed",
    label: "Failed",
    match: (m) => m.state === "Failed",
    empty: "No failed models.",
  },
];

export function Models({
  selected,
  setSelected,
  openDeployment,
}: {
  selected: RouteSelected | null;
  setSelected: (sel: RouteSelected | null) => void;
  openDeployment: (sel: Selected) => void;
}) {
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
  const [filter, setFilter] = useState("all");

  const usage = modelUsage(isvcs.data?.items ?? []);
  const rows: ModelRow[] = [
    ...(cluster.data?.items ?? []).map((m) => ({
      ...m,
      kind: "ClusterBaseModel" as const,
      used:
        usage.get(resourceKey({ cluster: m.cluster, kind: "ClusterBaseModel", name: m.name })) ?? 0,
    })),
    ...(base.data?.items ?? []).map((m) => {
      const identity: Selected = {
        cluster: m.cluster,
        kind: "BaseModel",
        name: m.name,
        namespace: m.namespace,
      };
      return { ...m, kind: "BaseModel" as const, used: usage.get(resourceKey(identity)) ?? 0 };
    }),
  ];

  return (
    <>
      <ResourceTable<ModelRow>
        title="Models"
        subtitle="Registered model inventory across clusters."
        actions={
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
        }
        rows={rows}
        status={{
          loading: base.loading || cluster.loading,
          error: base.error ?? cluster.error,
        }}
        noun="models"
        searchPlaceholder="Filter by name, architecture, size, quantization…"
        searchFields={(m) => [m.name, m.architecture, m.source, m.quantization, m.format]}
        clusterOf={(m) => m.cluster}
        rowKey={(m) => resourceKey(modelIdentity(m))}
        rowTone={(m) => stateTone(m.state)}
        onRowClick={(m) => setSelected(modelIdentity(m))}
        columns={COLUMNS}
        tabs={TABS}
        filter={filter}
        setFilter={setFilter}
      />
      {selected ? (
        <ModelDrawer
          key={selectionKey(selected)}
          sel={selected}
          isvcs={isvcs.data?.items ?? []}
          ftws={ftws.data?.items ?? []}
          openDeployment={openDeployment}
          onClose={() => setSelected(null)}
        />
      ) : null}
    </>
  );
}

/** Model onboarding progress: node download counts and any failing condition.
 *  This is the dominant wait state for a new BaseModel — a bare "Importing"
 *  pill with no counts tells the operator nothing. */
function DownloadProgress({ d }: { d: ModelDetail }) {
  const failing = (d.conditions ?? []).filter((c) => c.status === "False");
  const counts = d.nodesReady != null || d.nodesFailed != null;
  if (!counts && !failing.length) return null;
  return (
    <Section title="Onboarding">
      {counts ? (
        <Facts
          items={[
            ["Nodes ready", d.nodesReady != null ? num(d.nodesReady) : null],
            [
              "Nodes failed",
              d.nodesFailed ? <span className="txt-bad">{num(d.nodesFailed)}</span> : null,
            ],
          ]}
        />
      ) : null}
      {failing.map((c, i) => (
        <div className="notice bad" key={`${c.type}-${i}`}>
          <b>
            {c.type}
            {c.reason ? ` · ${c.reason}` : ""}
          </b>
          {c.message ? <div>{c.message}</div> : null}
        </div>
      ))}
    </Section>
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
  openDeployment,
  onClose,
}: {
  sel: RouteSelected;
  isvcs: IsvcItem[];
  ftws: FtwItem[];
  openDeployment: (sel: Selected) => void;
  onClose: () => void;
}) {
  const legacy = isLegacySelected(sel);
  const detail = useLive<ModelDetail>(
    () => (legacy ? api.legacyModel(sel.cluster, sel.name) : api.model(sel)),
    {
      kinds: legacy ? ["ClusterBaseModel", "BaseModel"] : [sel.kind],
      throttleMs: 5_000,
    },
  );
  const d = detail.data;
  const resolved: Selected | undefined = legacy
    ? d
      ? {
          cluster: sel.cluster,
          kind: d.namespace ? "BaseModel" : "ClusterBaseModel",
          name: sel.name,
          namespace: d.namespace,
        }
      : undefined
    : sel;
  const serving = resolved ? isvcs.filter((s) => sameResource(modelReference(s), resolved)) : [];
  const adapters = ftws.filter(
    (w) =>
      resolved != null &&
      w.cluster === resolved.cluster &&
      w.baseModel === resolved.name &&
      (resolved.kind === "BaseModel"
        ? w.baseModelNamespace === resolved.namespace
        : !w.baseModelNamespace),
  );

  return (
    <Drawer
      title={sel.name}
      subtitle={[resolved?.namespace, sel.cluster].filter(Boolean).join(" · ")}
      onClose={onClose}
    >
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
          </div>

          <DownloadProgress d={d} />

          <Section title="Overview">
            <Facts
              items={[
                ["Display name", d.displayName],
                ["Architecture", d.architecture],
                ["Parameters", d.parameterSize],
                ["Format", d.format],
                ["Quantization", d.quantization],
                ["Max tokens", d.maxTokens != null ? num(d.maxTokens) : null],
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
            <RefList
              items={serving}
              onOpen={openDeployment}
              empty="No deployments reference this model yet."
            />
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
