import { useState } from "react";

import { api } from "../api";
import type { IsvcItem, RuntimeItem } from "../api";
import { Pill } from "../components";
import { Copyable, Drawer, Facts, Section } from "../drawer";
import { useLive } from "../live";
import type { Column, Tab } from "../ResourceTable";
import { ResourceTable } from "../ResourceTable";

type RuntimeRow = RuntimeItem & { scoped: boolean; used: number };

interface Selected {
  cluster: string;
  name: string;
  scoped: boolean;
}

/** Last path segment of an image ref — drops the registry/repo prefix for display. */
const imageShort = (img?: string): string => (img ? (img.split("/").pop() ?? img) : "—");

/** Join up to two entries, else "first +N". */
const summ = (a?: string[]): string =>
  !a || a.length === 0 ? "—" : a.length <= 2 ? a.join(", ") : `${a[0]} +${a.length - 1}`;

/** What a runtime serves at a glance: architectures if declared, else formats. */
const serves = (r: RuntimeItem): string =>
  r.architectures?.length ? summ(r.architectures) : summ(r.formats);

/** Prefill/decode disaggregated = a decoder component is present. */
const isDisaggregated = (r: RuntimeItem): boolean =>
  !!r.components?.some((c) => c.role === "decoder");
/** Any component runs a multi-node worker group. */
const isMultiNode = (r: RuntimeItem): boolean => !!r.components?.some((c) => c.multiNode);
const cap = (s: string): string => s.charAt(0).toUpperCase() + s.slice(1);

const COLUMNS: Column<RuntimeRow>[] = [
  {
    key: "name",
    label: "Name",
    cellClass: "cell-name mono",
    render: (r) => (
      <>
        {r.name}
        {isMultiNode(r) ? <span className="tag">multi-node</span> : null}
        {isDisaggregated(r) ? <span className="tag">PD</span> : null}
        {r.disabled ? <span className="tag warn">disabled</span> : null}
      </>
    ),
    sort: (r) => r.name.toLowerCase(),
  },
  {
    key: "engine",
    label: "Engine",
    cellClass: "mono cell-sm muted",
    render: (r) => imageShort(r.image),
    sort: (r) => imageShort(r.image).toLowerCase(),
  },
  {
    key: "serves",
    label: "Serves",
    cellClass: "cell-sm",
    render: (r) => serves(r),
    sort: (r) => serves(r).toLowerCase(),
  },
  {
    key: "used",
    label: "Used by",
    right: true,
    cellClass: "num",
    render: (r) => <span className={`use-chip ${r.used ? "on" : ""}`}>{r.used || "—"}</span>,
    sort: (r) => r.used,
  },
];

const TABS: Tab<RuntimeRow>[] = [
  { key: "all", label: "All", match: () => true },
  {
    key: "multinode",
    label: "Multi-node",
    match: isMultiNode,
    empty: "No multi-node runtimes.",
  },
  {
    key: "pd",
    label: "Disaggregated",
    match: isDisaggregated,
    empty: "No PD-disaggregated runtimes.",
  },
];

export function Runtimes({
  openDeployment,
}: {
  openDeployment: (sel: { cluster: string; name: string }) => void;
}) {
  const scoped = useLive(api.clusterServingRuntimes, {
    kinds: ["ClusterServingRuntime"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const nsScoped = useLive(api.servingRuntimes, {
    kinds: ["ServingRuntime"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const [filter, setFilter] = useState("all");
  const [selected, setSelected] = useState<Selected | null>(null);

  // Count deployments referencing each runtime name, across the fleet.
  const usage = new Map<string, number>();
  for (const s of isvcs.data?.items ?? []) {
    if (s.runtime) usage.set(s.runtime, (usage.get(s.runtime) ?? 0) + 1);
  }
  const rows: RuntimeRow[] = [
    ...(scoped.data?.items ?? []).map((r) => ({
      ...r,
      scoped: true,
      used: usage.get(r.name) ?? 0,
    })),
    ...(nsScoped.data?.items ?? []).map((r) => ({
      ...r,
      scoped: false,
      used: usage.get(r.name) ?? 0,
    })),
  ];

  return (
    <>
      <ResourceTable<RuntimeRow>
        title="Runtimes"
        subtitle="Serving runtimes across the fleet — the engines that serve your models."
        rows={rows}
        status={{
          loading: scoped.loading || nsScoped.loading,
          error: scoped.error ?? nsScoped.error,
        }}
        noun="runtimes"
        searchPlaceholder="Filter by name, framework, architecture…"
        searchFields={(r) => [r.name, r.image, r.architectures?.join(" "), r.formats?.join(" ")]}
        clusterOf={(r) => r.cluster}
        rowKey={(r) => `${r.scoped ? "c" : "n"}/${r.cluster}/${r.name}`}
        rowTone={(r) => (r.disabled ? "slate" : "green")}
        onRowClick={(r) => setSelected({ cluster: r.cluster, name: r.name, scoped: r.scoped })}
        columns={COLUMNS}
        tabs={TABS}
        filter={filter}
        setFilter={setFilter}
      />
      {selected ? (
        <RuntimeDrawer
          sel={selected}
          isvcs={isvcs.data?.items ?? []}
          openDeployment={openDeployment}
          onClose={() => setSelected(null)}
        />
      ) : null}
    </>
  );
}

function RuntimeDrawer({
  sel,
  isvcs,
  openDeployment,
  onClose,
}: {
  sel: Selected;
  isvcs: IsvcItem[];
  openDeployment: (sel: { cluster: string; name: string }) => void;
  onClose: () => void;
}) {
  const detail = useLive<RuntimeItem>(
    () =>
      sel.scoped
        ? api.clusterServingRuntime(sel.cluster, sel.name)
        : api.servingRuntime(sel.cluster, sel.name),
    { kinds: [sel.scoped ? "ClusterServingRuntime" : "ServingRuntime"], throttleMs: 5_000 },
  );
  const d = detail.data;
  const serving = isvcs.filter((s) => s.runtime === sel.name);

  return (
    <Drawer title={sel.name} subtitle={sel.cluster} onClose={onClose}>
      {!d ? (
        <div className="muted drawer-empty">
          {detail.error ?? (detail.loading ? "Loading…" : "Not found")}
        </div>
      ) : (
        <>
          <div className="drawer-toprow">
            {isDisaggregated(d) ? <Pill tone="cyan" label="disaggregated" /> : null}
            {isMultiNode(d) ? <Pill tone="violet" label="multi-node" /> : null}
            <span className={`use-chip ${serving.length ? "on" : ""}`}>
              {serving.length ? `${serving.length} serving` : "unused"}
            </span>
            {d.disabled ? <span className="tag warn">disabled</span> : null}
          </div>

          <Section title="Engine">
            {d.image ? (
              <Copyable text={d.image} />
            ) : (
              <div className="muted">No image declared.</div>
            )}
            <Facts items={[["Protocols", d.protocols?.join(", ")]]} />
          </Section>

          <Section title="Topology">
            <Facts
              items={(d.components ?? []).map((c): [string, string] => [
                cap(c.role),
                c.multiNode ? `multi-node${c.nodes ? ` · ${c.nodes} nodes` : ""}` : "single-node",
              ])}
            />
            {isDisaggregated(d) ? (
              <p className="panel-note">
                Prefill/decode disaggregated — engine and decoder scale independently.
              </p>
            ) : null}
          </Section>

          {d.architectures?.length || d.formats?.length || d.quantizations?.length ? (
            <Section title="Serves models">
              <Facts
                items={[
                  ["Architectures", d.architectures?.join(", ")],
                  ["Formats", d.formats?.join(", ")],
                  ["Quantizations", d.quantizations?.join(", ")],
                ]}
              />
            </Section>
          ) : null}

          <Section title={`Used by (${serving.length})`}>
            {serving.length ? (
              <ul className="ref-list">
                {serving.map((s) => (
                  <li key={`${s.cluster}/${s.namespace}/${s.name}`}>
                    <button
                      className="linkbtn mono"
                      onClick={() => openDeployment({ cluster: s.cluster, name: s.name })}
                    >
                      {s.name}
                    </button>
                    <span className="muted">
                      {" "}
                      · {s.namespace ?? "—"} · {s.cluster}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="muted">No deployments use this runtime yet.</div>
            )}
          </Section>
        </>
      )}
    </Drawer>
  );
}
