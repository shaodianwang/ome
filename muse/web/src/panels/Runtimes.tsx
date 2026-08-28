import { useState } from "react";

import { api } from "../api";
import type { IsvcItem, RuntimeDetail, RuntimeItem } from "../api";
import { CommandBlock, EnvList, Pill, RefList } from "../components";
import { Copyable, Facts } from "../drawer";
import { ISVC_USAGE_LIVE_OPTIONS, useLive } from "../live";
import { runtimeReference, runtimeUsage } from "../map";
import type { Column, Tab } from "../ResourceTable";
import { ResourceTable } from "../ResourceTable";
import { isLegacySelected, resourceKey, sameResource, selectionKey } from "../resource";
import type { RouteSelected, Selected } from "../resource";
import { fmtRes, imageShort, isDisaggregated, isMultiNode, serves } from "../runtime";
import { ageToSecs } from "../util";
import { Card } from "../viz";

type RuntimeRow = RuntimeItem & {
  kind: "ServingRuntime" | "ClusterServingRuntime";
  used: number;
};

const runtimeIdentity = (r: RuntimeRow): Selected => ({
  cluster: r.cluster,
  kind: r.kind,
  name: r.name,
  namespace: r.kind === "ServingRuntime" ? r.namespace : undefined,
});

const cap = (s: string): string => s.charAt(0).toUpperCase() + s.slice(1);

/** Row scope at a glance: cluster-wide, or pinned to its namespace. */
const scopeLabel = (r: RuntimeRow): string =>
  r.kind === "ClusterServingRuntime" ? "cluster" : `ns:${r.namespace ?? "—"}`;

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
        <span className="cell-sub">
          <Pill tone="slate" dot={false} label={scopeLabel(r)} />
        </span>
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
  {
    key: "cluster",
    label: "Cluster",
    cellClass: "mono cell-sm muted",
    render: (r) => r.cluster,
    sort: (r) => r.cluster.toLowerCase(),
  },
  {
    key: "age",
    label: "Age",
    right: true,
    cellClass: "num muted",
    render: (r) => r.age ?? "—",
    sort: (r) => ageToSecs(r.age ?? ""),
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
  {
    key: "cluster",
    label: "Cluster",
    match: (r) => r.kind === "ClusterServingRuntime",
    empty: "No cluster-scoped runtimes.",
  },
  {
    key: "namespaced",
    label: "Namespaced",
    match: (r) => r.kind === "ServingRuntime",
    empty: "No namespaced runtimes.",
  },
];

export function Runtimes({
  selected,
  setSelected,
  openDeployment,
}: {
  selected: RouteSelected | null;
  setSelected: (sel: RouteSelected | null) => void;
  openDeployment: (sel: Selected) => void;
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
  const isvcs = useLive(api.inferenceServices, ISVC_USAGE_LIVE_OPTIONS);
  const [filter, setFilter] = useState("all");

  const usage = runtimeUsage(isvcs.data?.items ?? []);
  const rows: RuntimeRow[] = [
    ...(scoped.data?.items ?? []).map((r) => ({
      ...r,
      kind: "ClusterServingRuntime" as const,
      used:
        usage.get(
          resourceKey({ cluster: r.cluster, kind: "ClusterServingRuntime", name: r.name }),
        ) ?? 0,
    })),
    ...(nsScoped.data?.items ?? []).map((r) => {
      const identity: Selected = {
        cluster: r.cluster,
        kind: "ServingRuntime",
        name: r.name,
        namespace: r.namespace,
      };
      return {
        ...r,
        kind: "ServingRuntime" as const,
        used: usage.get(resourceKey(identity)) ?? 0,
      };
    }),
  ];

  if (selected) {
    return (
      <RuntimePage
        key={selectionKey(selected)}
        sel={selected}
        isvcs={isvcs.data?.items ?? []}
        openDeployment={openDeployment}
        onBack={() => setSelected(null)}
      />
    );
  }

  return (
    <ResourceTable<RuntimeRow>
      title="Runtimes"
      subtitle="Serving runtime inventory across clusters."
      rows={rows}
      status={{
        loading: scoped.loading || nsScoped.loading,
        error: scoped.error ?? nsScoped.error,
      }}
      noun="runtimes"
      searchPlaceholder="Filter by name, framework, architecture…"
      searchFields={(r) => [
        r.name,
        r.namespace,
        r.image,
        r.architectures?.join(" "),
        r.formats?.join(" "),
      ]}
      clusterOf={(r) => r.cluster}
      rowKey={(r) => resourceKey(runtimeIdentity(r))}
      rowTone={(r) => (r.disabled ? "slate" : "green")}
      onRowClick={(r) => setSelected(runtimeIdentity(r))}
      columns={COLUMNS}
      tabs={TABS}
      filter={filter}
      setFilter={setFilter}
    />
  );
}

function RuntimePage({
  sel,
  isvcs,
  openDeployment,
  onBack,
}: {
  sel: RouteSelected;
  isvcs: IsvcItem[];
  openDeployment: (sel: Selected) => void;
  onBack: () => void;
}) {
  const legacy = isLegacySelected(sel);
  const detail = useLive<RuntimeDetail>(
    () => (legacy ? api.legacyRuntime(sel.cluster, sel.name) : api.runtime(sel)),
    {
      kinds: legacy ? ["ClusterServingRuntime", "ServingRuntime"] : [sel.kind],
      throttleMs: 5_000,
    },
  );
  const d = detail.data;
  const resolved: Selected | undefined = legacy
    ? d
      ? {
          cluster: sel.cluster,
          kind: d.namespace ? "ServingRuntime" : "ClusterServingRuntime",
          name: sel.name,
          namespace: d.namespace,
        }
      : undefined
    : sel;
  const serving = resolved ? isvcs.filter((s) => sameResource(runtimeReference(s), resolved)) : [];

  return (
    <section className="panel active">
      <div className="detail-head">
        <button className="backlink" onClick={onBack}>
          ‹ Runtimes
        </button>
        <h1 className="detail-title mono">{sel.name}</h1>
        <span className="detail-meta">
          {[resolved?.namespace, sel.cluster].filter(Boolean).join(" · ")}
        </span>
        {d && isDisaggregated(d) ? <Pill tone="cyan" label="disaggregated" /> : null}
        {d && isMultiNode(d) ? <Pill tone="violet" label="multi-node" /> : null}
        {d?.disabled ? <span className="tag warn">disabled</span> : null}
      </div>

      {!d ? (
        <div className="muted attn-none">
          {detail.error ?? (detail.loading ? "Loading…" : "Not found")}
        </div>
      ) : (
        <>
          <div className="detail-grid2">
            <Card title="Engine">
              {d.image ? (
                <Copyable text={d.image} />
              ) : (
                <div className="muted">No image declared.</div>
              )}
              <Facts items={[["Protocols", d.protocols?.join(", ")]]} />
            </Card>

            <Card title="Topology">
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
            </Card>
          </div>

          {d.architectures?.length || d.formats?.length || d.quantizations?.length ? (
            <Card title="Serves models" className="section-gap">
              <Facts
                items={[
                  ["Architectures", d.architectures?.join(", ")],
                  ["Formats", d.formats?.join(", ")],
                  ["Quantizations", d.quantizations?.join(", ")],
                ]}
              />
            </Card>
          ) : null}

          {d.containers?.length ? (
            <Card title="Configuration" className="section-gap">
              {d.containers.map((c) => (
                <div key={c.role} className="rt-cfg">
                  {d.containers!.length > 1 ? <div className="eyebrow">{cap(c.role)}</div> : null}
                  {c.command?.length || c.args?.length ? (
                    <CommandBlock command={c.command} args={c.args} />
                  ) : null}
                  {c.resources ? (
                    <Facts
                      items={[
                        ["Requests", fmtRes(c.resources.requests)],
                        ["Limits", fmtRes(c.resources.limits)],
                      ]}
                    />
                  ) : null}
                  {c.env?.length ? (
                    <details className="collapse">
                      <summary>Environment ({c.env.length})</summary>
                      <EnvList env={c.env} />
                    </details>
                  ) : null}
                </div>
              ))}
            </Card>
          ) : null}

          <Card title={`Used by (${serving.length})`} className="section-gap">
            <RefList
              items={serving}
              onOpen={openDeployment}
              empty="No deployments use this runtime yet."
            />
          </Card>
        </>
      )}
    </section>
  );
}
