import type { DepFilter, PanelKey } from "../App";
import { api } from "../api";
import { Plus } from "../icons";
import { useLive } from "../live";
import { toWorkloadRows } from "../map";
import type { Column, Tab } from "../ResourceTable";
import { ResourceTable } from "../ResourceTable";
import type { Selected, WorkloadRow } from "../types";
import { ageToSecs, hostOf } from "../util";
import { DeploymentDetail } from "./DeploymentDetail";

const numOr = (s: string, fallback = -1) => {
  const v = parseInt(s, 10);
  return isNaN(v) ? fallback : v;
};

const COLUMNS: Column<WorkloadRow>[] = [
  {
    key: "name",
    label: "Name",
    cellClass: "cell-name mono",
    render: (w) => (
      <>
        {w.name}
        {w.reason ? <div className="cell-sub bad">{w.reason}</div> : null}
      </>
    ),
    sort: (w) => w.name.toLowerCase(),
  },
  {
    key: "model",
    label: "Model",
    cellClass: "mono cell-sm",
    render: (w) => w.model,
    sort: (w) => w.model.toLowerCase(),
  },
  {
    key: "replicas",
    label: "Replicas",
    right: true,
    cellClass: "num",
    render: (w) => <span className={`repl ${w.replicas.tone}`}>{w.replicas.label}</span>,
    sort: (w) => numOr(w.replicas.label),
  },
  {
    key: "loaded",
    label: "Load",
    cellClass: "cell-sm muted",
    render: (w) => w.load ?? "—",
    sort: (w) => numOr(w.load ?? ""),
  },
  {
    key: "endpoint",
    label: "Endpoint",
    cellClass: "endpoint-cell",
    render: (w) =>
      w.endpoint ? (
        <a
          className="ep-link mono"
          href={w.endpoint}
          target="_blank"
          rel="noreferrer"
          title={w.endpoint}
          onClick={(e) => e.stopPropagation()}
        >
          {hostOf(w.endpoint)}
        </a>
      ) : (
        <span className="muted">—</span>
      ),
    sort: (w) => (w.endpoint ?? "").toLowerCase(),
  },
  {
    key: "cluster",
    label: "Cluster",
    cellClass: "mono cell-sm muted",
    render: (w) => w.cluster,
    sort: (w) => w.cluster.toLowerCase(),
  },
  {
    key: "age",
    label: "Age",
    right: true,
    cellClass: "num muted",
    render: (w) => w.age,
    sort: (w) => ageToSecs(w.age),
  },
];

const TABS: Tab<WorkloadRow>[] = [
  { key: "all", label: "All", match: () => true },
  {
    key: "attention",
    label: "Needs attention",
    match: (w) => w.status.tone === "red",
    empty: "Nothing needs attention.",
  },
  {
    key: "ready",
    label: "Ready",
    match: (w) => w.status.tone === "green",
    empty: "No ready deployments.",
  },
];

export function Workloads({
  goto,
  filter,
  setFilter,
  cluster,
  setCluster,
  query,
  setQuery,
  selected,
  setSelected,
  openModel,
  openRuntime,
}: {
  goto: (p: PanelKey) => void;
  filter: DepFilter;
  setFilter: (f: DepFilter) => void;
  cluster: string;
  setCluster: (c: string) => void;
  query: string;
  setQuery: (q: string) => void;
  selected: Selected | null;
  setSelected: (s: Selected | null) => void;
  openModel: (sel: Selected) => void;
  openRuntime: (sel: Selected) => void;
}) {
  const isvcs = useLive(api.inferenceServices, {
    kinds: ["InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });

  if (selected) {
    return (
      <DeploymentDetail
        sel={selected}
        onBack={() => setSelected(null)}
        openModel={openModel}
        openRuntime={openRuntime}
      />
    );
  }

  return (
    <ResourceTable<WorkloadRow>
      title="Deployments"
      subtitle="Every model deployment across the fleet. Click a row for detail and a test request."
      actions={
        <button className="btn primary" onClick={() => goto("deploy")}>
          <Plus />
          New deployment
        </button>
      }
      rows={toWorkloadRows(isvcs.data?.items ?? [])}
      status={isvcs}
      noun="deployments"
      searchPlaceholder="Filter by name or model…"
      searchFields={(w) => [w.name, w.model, w.cluster]}
      clusterOf={(w) => w.cluster}
      rowKey={(w) => w.id}
      rowTone={(w) => w.status.tone}
      onRowClick={(w) => setSelected({ cluster: w.cluster, name: w.name })}
      columns={COLUMNS}
      tabs={TABS}
      filter={filter}
      setFilter={(f) => setFilter(f as DepFilter)}
      cluster={cluster}
      setCluster={setCluster}
      query={query}
      setQuery={setQuery}
    />
  );
}
