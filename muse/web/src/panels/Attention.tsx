import { useState } from "react";

import { api } from "../api";
import type { AttentionItem } from "../api";
import { openAttentionItem } from "../attention";
import { SearchInput, StaleNotice } from "../components";
import { ATTENTION_LIVE_OPTIONS, useLive } from "../live";
import type { Selected } from "../resource";
import { matches, num as n, staleEmptyText } from "../util";
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
  const attention = useLive(api.attention, ATTENTION_LIVE_OPTIONS);

  const items = attention.data?.items ?? [];
  const clusters = [...new Set(items.map((i) => i.cluster))].sort();
  const categories = [...new Set(items.map((i) => i.category))];
  const criticals = items.filter((i) => i.severity === "critical").length;

  const shown = items
    .filter((i) => severity === "all" || i.severity === severity)
    .filter((i) => category === "all" || i.category === category)
    .filter((i) => cluster === "all" || i.cluster === cluster)
    .filter((i) => matches(query, i.name, i.cluster, i.summary, i.detail));

  const open = (i: AttentionItem) =>
    openAttentionItem(i, { openDeployment, openModel, openCapacityCluster });

  const sub = items.length
    ? `${n(criticals)} critical · ${n(items.length - criticals)} warning${items.length - criticals === 1 ? "" : "s"}`
    : undefined;

  return (
    <>
      <StaleNotice
        error={attention.error}
        hasData={attention.data != null}
        title="Attention refresh failed"
        detail="Showing the last successful snapshot."
      />
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
              <SearchInput
                ariaLabel="Filter findings"
                placeholder="Filter by name, cluster, or reason…"
                value={query}
                onChange={setQuery}
              />
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
                        <button
                          type="button"
                          className="row-open"
                          onClick={(event) => {
                            event.stopPropagation();
                            open(i);
                          }}
                        >
                          <Dot tone={i.severity === "critical" ? "red" : "amber"} />{" "}
                          {i.severity === "critical" ? "Critical" : "Warning"}
                        </button>
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
            {staleEmptyText(attention, {
              stale: "Nothing needs attention — the fleet is clean.",
              unavailable: "Attention data is unavailable.",
              loading: "Loading…",
              empty: "Nothing needs attention — the fleet is clean.",
            })}
          </div>
        )}
      </Card>
    </>
  );
}
