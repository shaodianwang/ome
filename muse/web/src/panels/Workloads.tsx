import type { DepFilter } from "../App";
import { api } from "../api";
import { hasUnambiguousDeploymentMetricIdentity } from "../fleet";
import { ISVC_LIVE_OPTIONS, useLive } from "../live";
import { COMPONENT_TONE_CLASS, toWorkloadRows, transitionLabel } from "../map";
import type { Column, Tab } from "../ResourceTable";
import { ResourceTable } from "../ResourceTable";
import { isvcSelected, resourceKey } from "../resource";
import type { Selected } from "../resource";
import type { WorkloadRow } from "../types";
import { ageToSecs, hostOf } from "../util";
import { DeploymentDetail } from "./DeploymentDetail";
import type { DeploymentMetricIdentity } from "./DeploymentDetail";

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
        {w.namespace ? <span className="cell-sub muted">{w.namespace}</span> : null}
        {w.reason ? <span className="cell-sub bad">{w.reason}</span> : null}
      </>
    ),
    sort: (w) => w.name.toLowerCase(),
  },
  {
    key: "model",
    label: "Model",
    cellClass: "mono cell-sm",
    render: (w) => (
      <>
        {w.model}
        {w.load ? <span className="cell-sub muted">{w.load}</span> : null}
      </>
    ),
    sort: (w) => w.model.toLowerCase(),
  },
  {
    key: "runtime",
    label: "Runtime",
    cellClass: "mono cell-sm muted",
    render: (w) => w.runtime,
    sort: (w) => w.runtime.toLowerCase(),
  },
  {
    key: "replicas",
    label: "Replicas",
    cellClass: "num",
    render: (w) =>
      w.components.length > 0 ? (
        <span className="comp-cells">
          {w.components.map((c) => (
            <span
              key={c.name}
              className={`repl ${COMPONENT_TONE_CLASS[c.tone]}`}
              title={
                c.transition ? `${c.transition.from ?? "(new)"} → ${c.transition.to}` : c.revision
              }
            >
              {c.label}
              {c.transition ? ` · ${transitionLabel(c.transition)}` : ""}
            </span>
          ))}
        </span>
      ) : (
        <span className={`repl ${w.replicas.tone}`}>{w.replicas.label}</span>
      ),
    sort: (w) => numOr(w.replicas.label),
  },
  {
    key: "accels",
    label: "Accels",
    right: true,
    cellClass: "num",
    render: (w) => (w.accelerators != null ? w.accelerators.toLocaleString() : "—"),
    sort: (w) => w.accelerators ?? -1,
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
    key: "mode",
    label: "Mode",
    cellClass: "cell-sm muted",
    render: (w) => w.mode ?? "—",
    sort: (w) => (w.mode ?? "").toLowerCase(),
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
    key: "rolling",
    label: "Rolling out",
    match: (w) => w.rolling,
    empty: "No rollouts in progress.",
  },
  {
    key: "ready",
    label: "Ready",
    match: (w) => w.status.tone === "green",
    empty: "No ready deployments.",
  },
];

export function Workloads({
  filter,
  setFilter,
  cluster,
  setCluster,
  namespace,
  setNamespace,
  query,
  setQuery,
  selected,
  setSelected,
  openModel,
  openRuntime,
  openNode,
}: {
  filter: DepFilter;
  setFilter: (f: DepFilter) => void;
  cluster: string;
  setCluster: (c: string) => void;
  namespace: string;
  setNamespace: (ns: string) => void;
  query: string;
  setQuery: (q: string) => void;
  selected: Selected | null;
  setSelected: (s: Selected | null) => void;
  openModel: (sel: Selected) => void;
  openRuntime: (sel: Selected) => void;
  openNode: (cluster: string, node: string) => void;
}) {
  const isvcs = useLive(api.inferenceServices, ISVC_LIVE_OPTIONS);

  if (selected) {
    const metricIdentity: DeploymentMetricIdentity = isvcs.error
      ? "unavailable"
      : !isvcs.data
        ? "loading"
        : hasUnambiguousDeploymentMetricIdentity(isvcs.data.items, selected)
          ? "unambiguous"
          : "ambiguous";
    return (
      <DeploymentDetail
        key={resourceKey(selected)}
        sel={selected}
        onBack={() => setSelected(null)}
        openModel={openModel}
        openRuntime={openRuntime}
        openNode={openNode}
        metricIdentity={metricIdentity}
      />
    );
  }

  return (
    <ResourceTable<WorkloadRow>
      title="Deployments"
      subtitle="Model serving deployments across clusters."
      rows={toWorkloadRows(isvcs.data?.items ?? [])}
      status={isvcs}
      noun="deployments"
      searchPlaceholder="Filter by name, namespace, model, or runtime…"
      searchFields={(w) => [w.name, w.namespace, w.model, w.runtime, w.cluster]}
      clusterOf={(w) => w.cluster}
      namespaceOf={(w) => w.namespace}
      namespace={namespace}
      setNamespace={setNamespace}
      rowKey={(w) => w.id}
      rowTone={(w) => w.status.tone}
      onRowClick={(w) => setSelected(isvcSelected(w.cluster, w.name, w.namespace))}
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
