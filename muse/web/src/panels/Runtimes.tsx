import { useState } from "react";

import { api } from "../api";
import type { IsvcItem, RuntimeDetail, RuntimeItem } from "../api";
import { EnvList, Pill, RefList } from "../components";
import { Copyable, Facts } from "../drawer";
import { Copy } from "../icons";
import { useLive } from "../live";
import type { Column, Tab } from "../ResourceTable";
import { ResourceTable } from "../ResourceTable";
import type { Selected } from "../types";
import { Card } from "../viz";

type RuntimeRow = RuntimeItem & { scoped: boolean; used: number };

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

/** Format a resource map as "cpu 4 · memory 32Gi · nvidia.com/gpu 8", or null. */
const fmtRes = (m?: Record<string, string>): string | null =>
  m && Object.keys(m).length
    ? Object.entries(m)
        .map(([k, v]) => `${k} ${v}`)
        .join(" · ")
    : null;

/** Lay a command out one flag per line: the executable (and short flags) on the
 *  first line, then each `--long-flag` with its values on its own indented line. */
const argLines = (command?: string[], args?: string[]): string => {
  const lines: string[] = [];
  for (const t of [...(command ?? []), ...(args ?? [])]) {
    if (t.startsWith("--") || lines.length === 0) lines.push(t);
    else lines[lines.length - 1] += ` ${t}`;
  }
  return lines.map((l, i) => (i === 0 ? l : `  ${l}`)).join("\n");
};

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
  selected,
  setSelected,
  openDeployment,
}: {
  selected: Selected | null;
  setSelected: (sel: Selected | null) => void;
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
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 8_000,
    intervalMs: 30_000,
  });
  const [filter, setFilter] = useState("all");

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

  if (selected) {
    return (
      <RuntimePage
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
      searchFields={(r) => [r.name, r.image, r.architectures?.join(" "), r.formats?.join(" ")]}
      clusterOf={(r) => r.cluster}
      rowKey={(r) => `${r.scoped ? "c" : "n"}/${r.cluster}/${r.name}`}
      rowTone={(r) => (r.disabled ? "slate" : "green")}
      onRowClick={(r) => setSelected({ cluster: r.cluster, name: r.name })}
      columns={COLUMNS}
      tabs={TABS}
      filter={filter}
      setFilter={setFilter}
    />
  );
}

/** A serving container's command: collapsible (collapsed by default) with a copy of
 *  the single-line invocation; expands to one flag per line. */
function CommandBlock({ command, args }: { command?: string[]; args?: string[] }) {
  const [copied, setCopied] = useState(false);
  const oneLine = [...(command ?? []), ...(args ?? [])].join(" ");
  return (
    <details className="collapse">
      <summary>
        Command
        <button
          className="icon-btn sm collapse-copy"
          title="Copy command"
          onClick={(e) => {
            e.stopPropagation();
            e.preventDefault();
            void navigator.clipboard?.writeText(oneLine);
            setCopied(true);
            setTimeout(() => setCopied(false), 1200);
          }}
        >
          {copied ? "✓" : <Copy />}
        </button>
      </summary>
      <pre className="argbox">{argLines(command, args)}</pre>
    </details>
  );
}

function RuntimePage({
  sel,
  isvcs,
  openDeployment,
  onBack,
}: {
  sel: Selected;
  isvcs: IsvcItem[];
  openDeployment: (sel: Selected) => void;
  onBack: () => void;
}) {
  // Scope isn't carried in the URL, so resolve either kind: cluster-scoped first,
  // then namespaced.
  const detail = useLive<RuntimeDetail>(
    () =>
      api
        .clusterServingRuntime(sel.cluster, sel.name)
        .catch(() => api.servingRuntime(sel.cluster, sel.name)),
    { kinds: ["ClusterServingRuntime", "ServingRuntime"], throttleMs: 5_000 },
  );
  const d = detail.data;
  const serving = isvcs.filter((s) => s.runtime === sel.name);

  return (
    <section className="panel active">
      <div className="detail-head">
        <button className="backlink" onClick={onBack}>
          ‹ Runtimes
        </button>
        <h1 className="detail-title mono">{sel.name}</h1>
        <span className="detail-meta">{sel.cluster}</span>
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
