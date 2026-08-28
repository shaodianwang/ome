import { useState } from "react";

import { api } from "../api";
import type { AttentionItem } from "../api";
import { DataNotice } from "../components";
import { useLive } from "../live";
import type { Selected } from "../resource";
import { matches, num as n } from "../util";
import { Card, Dot, FilterChip } from "../viz";

const CATEGORY_LABEL: Record<AttentionItem["category"], string> = {
  cluster: "Cluster",
  deployment: "Deployment",
  rollout: "Rollout",
  pods: "Pods",
  model: "Model",
};

/** Fleet-wide triage: every finding that needs an operator, most severe first,
 *  each row linking to the page that diagnoses it. */
export function Attention({
  openDeployment,
  openModel,
  openCapacityCluster,
}: {
  openDeployment: (sel: Selected) => void;
  openModel: (sel: Selected) => void;
  openCapacityCluster: (cluster: string) => void;
}) {
  const [severity, setSeverity] = useState("all");
  const [category, setCategory] = useState("all");
  const [cluster, setCluster] = useState("all");
  const [query, setQuery] = useState("");
  const attention = useLive(api.attention, {
    kinds: ["InferenceService", "Pod", "BaseModel", "ClusterBaseModel", "Cluster"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });

  const items = attention.data?.items ?? [];
  const clusters = [...new Set(items.map((i) => i.cluster))].sort();
  const categories = [...new Set(items.map((i) => i.category))];
  const criticals = items.filter((i) => i.severity === "critical").length;

  const shown = items
    .filter((i) => severity === "all" || i.severity === severity)
    .filter((i) => category === "all" || i.category === category)
    .filter((i) => cluster === "all" || i.cluster === cluster)
    .filter((i) => matches(query, i.name, i.cluster, i.summary, i.detail));

  const open = (i: AttentionItem) => {
    if (i.category === "cluster") {
      openCapacityCluster(i.cluster);
    } else if (i.category === "model") {
      openModel({
        cluster: i.cluster,
        kind: i.kind === "ClusterBaseModel" ? "ClusterBaseModel" : "BaseModel",
        name: i.name,
        namespace: i.namespace,
      });
    } else {
      openDeployment({
        cluster: i.cluster,
        kind: "InferenceService",
        name: i.name,
        namespace: i.namespace,
      });
    }
  };

  const stale = attention.error != null && attention.data != null;
  const sub = items.length
    ? `${n(criticals)} critical · ${n(items.length - criticals)} warning${items.length - criticals === 1 ? "" : "s"}`
    : undefined;

  return (
    <>
      {stale ? (
        <DataNotice
          tone="warning"
          title="Attention refresh failed"
          detail="Showing the last successful snapshot."
        />
      ) : null}
      <Card title="Needs attention" sub={sub}>
        {items.length ? (
          <>
            <div className="filterbar">
              <FilterChip
                label="Severity"
                value={severity}
                options={[
                  { value: "all", label: "All" },
                  { value: "critical", label: `Critical (${n(criticals)})` },
                  { value: "warning", label: `Warning (${n(items.length - criticals)})` },
                ]}
                onChange={setSeverity}
              />
              <FilterChip
                label="Type"
                value={category}
                options={[
                  { value: "all", label: "All" },
                  ...categories.map((c) => ({ value: c, label: CATEGORY_LABEL[c] })),
                ]}
                onChange={setCategory}
              />
              {clusters.length > 1 ? (
                <FilterChip
                  label="Cluster"
                  value={cluster}
                  options={[
                    { value: "all", label: "All" },
                    ...clusters.map((c) => ({ value: c, label: c })),
                  ]}
                  onChange={setCluster}
                />
              ) : null}
              <div className="input grow">
                <input
                  aria-label="Filter findings"
                  placeholder="Filter by name, cluster, or reason…"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
              </div>
            </div>
            <table className="place-tbl">
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Type</th>
                  <th>Name</th>
                  <th>Cluster</th>
                  <th>Why</th>
                </tr>
              </thead>
              <tbody>
                {shown.length ? (
                  shown.map((i, idx) => (
                    <tr className="link" key={idx} onClick={() => open(i)}>
                      <td>
                        <Dot tone={i.severity === "critical" ? "red" : "amber"} />{" "}
                        {i.severity === "critical" ? "Critical" : "Warning"}
                      </td>
                      <td className="muted">{CATEGORY_LABEL[i.category]}</td>
                      <td className="mono">{i.name}</td>
                      <td className="mono muted">{i.cluster}</td>
                      <td>
                        {i.summary}
                        {i.detail ? <span className="muted"> — {i.detail}</span> : null}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="muted empty-cell">
                      No findings match the current filters.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </>
        ) : (
          <div className="muted attn-none">
            {attention.error && !attention.data
              ? "Attention data is unavailable."
              : attention.loading && !attention.data
                ? "Loading…"
                : "Nothing needs attention — the fleet is clean."}
          </div>
        )}
      </Card>
    </>
  );
}
