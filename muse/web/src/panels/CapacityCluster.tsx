import { Fragment as F, useEffect, useState } from "react";

import { api } from "../api";
import type { CapacityResp, NodeDetail, NodeItem } from "../api";
import { CopyCmd, DataNotice, EmptyRow, Pager, SearchInput } from "../components";
import type { ClusterCapacityState } from "../dataState";
import { isvcSelected } from "../resource";
import type { Selected } from "../resource";
import { matches, num as n, shortId } from "../util";
import { Card, UtilRow } from "../viz";

// Conditions where `True` means trouble (standard k8s node pressures).
const BAD_WHEN_TRUE = ["MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"];

/** Color a condition only where its polarity is known: Ready (True=good), the
 *  standard pressures (True=bad). Custom detector conditions vary (`CWActive: True`
 *  is good, `GPUFault: True` is bad), so leave them neutral. */
function condTone(c: { type: string; status: string }): string {
  if (c.type === "Ready") return c.status === "True" ? "txt-ok" : "txt-bad";
  if (BAD_WHEN_TRUE.includes(c.type)) return c.status === "True" ? "txt-bad" : "txt-ok";
  return "";
}

/** The selected cluster's drill-down cards: node-health breakdown and top
 *  accelerator consumers. */
export function ClusterOverview({
  active,
  consumers,
  nodes,
  nodesLoading,
  nodesError,
  onRetryNodes,
}: {
  active: ClusterCapacityState;
  consumers: NonNullable<CapacityResp["consumers"]>;
  nodes: NodeItem[];
  nodesLoading: boolean;
  nodesError: string | null;
  onRetryNodes: () => void;
}) {
  const maxConsumer = Math.max(1, ...consumers.map((c) => c.accelerators));
  // Node-health breakdown, from the selected cluster's accelerator nodes.
  const issueCounts = new Map<string, number>();
  let unhealthyAccel = 0;
  for (const nd of nodes) {
    if (!nd.accelerator || !nd.issues?.length) continue;
    unhealthyAccel++;
    for (const iss of nd.issues) issueCounts.set(iss, (issueCounts.get(iss) ?? 0) + 1);
  }
  const nodeIssues = [...issueCounts.entries()].sort((a, b) => b[1] - a[1]);
  return (
    <div className="grid g2">
      <Card title="Node health" sub="why accelerator nodes are unavailable">
        {nodesLoading ? (
          <p className="panel-note">Loading nodes…</p>
        ) : nodesError ? (
          <DataNotice
            tone="danger"
            title="Node inventory unavailable"
            detail={nodesError}
            action={{ label: "Retry", onClick: onRetryNodes }}
          />
        ) : nodeIssues.length ? (
          <div className="urows">
            {nodeIssues.map(([iss, count]) => (
              <UtilRow
                key={iss}
                name={iss}
                pct={unhealthyAccel > 0 ? Math.round((count / unhealthyAccel) * 100) : 0}
                tone={iss === "cordoned" ? "amber" : "red"}
                value={n(count)}
                showPct={false}
              />
            ))}
          </div>
        ) : (
          <p className="panel-note">All accelerator nodes are Ready and schedulable.</p>
        )}
      </Card>
      <Card title="Top consumers" sub="namespaces holding the most accelerators">
        {active.status !== "ready" || active.capacityError ? (
          <p className="panel-note">Capacity data is unavailable for this cluster.</p>
        ) : consumers.length ? (
          <div className="urows">
            {consumers.map((c, i) => (
              <UtilRow
                key={i}
                name={c.namespace}
                tag={c.kind.toUpperCase()}
                pct={Math.round((c.accelerators / maxConsumer) * 100)}
                tone="violet"
                value={n(c.accelerators)}
                showPct={false}
              />
            ))}
          </div>
        ) : (
          <p className="panel-note">No accelerator workloads scheduled here.</p>
        )}
      </Card>
    </div>
  );
}

/** The selected cluster's node explorer: filtered, unhealthy-first, paged, with
 *  a per-node drill-down. Kept mounted on the fleet view (rendering nothing) so
 *  filter and paging state survive switching tabs. */
export function ClusterNodes({
  show,
  tab,
  nodeQuery,
  nodes,
  nodesLoading,
  nodesError,
  pageSize,
  onPageSize,
  openDeployment,
}: {
  show: boolean;
  tab: string;
  /** Deep-linked filter (a node name or topology id); local edits take over. */
  nodeQuery?: string;
  nodes: NodeItem[];
  nodesLoading: boolean;
  nodesError: string | null;
  pageSize: number;
  onPageSize: (size: number) => void;
  openDeployment: (s: Selected) => void;
}) {
  const [nodeQ, setNodeQ] = useState(nodeQuery ?? "");
  const [nodePage, setNodePage] = useState(0);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [detail, setDetail] = useState<NodeDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  // Follow deep links that arrive while mounted (the card survives tab
  // switches precisely so its state can be driven from outside).
  useEffect(() => {
    if (nodeQuery != null) setNodeQ(nodeQuery);
  }, [nodeQuery]);

  useEffect(() => {
    setNodePage(0);
    setExpanded(null);
  }, [tab, nodeQ]);

  // Node explorer: filtered, unhealthy-first, paged. Topology ids (island /
  // NVLink domain / zone) are searchable so capacity units can link here.
  const nodeRows = nodes
    .filter((nd) =>
      matches(
        nodeQ,
        nd.name,
        nd.instanceType,
        nd.island,
        nd.nvlinkDomain,
        nd.zone,
        (nd.issues ?? []).join(" "),
      ),
    )
    .sort((a, b) => {
      const ai = a.issues?.length ? 1 : 0;
      const bi = b.issues?.length ? 1 : 0;
      return ai !== bi ? bi - ai : (b.accelerator?.free ?? 0) - (a.accelerator?.free ?? 0);
    });
  const nodeTotalPages = Math.max(1, Math.ceil(nodeRows.length / pageSize));
  const nodeClamped = Math.min(nodePage, nodeTotalPages - 1);
  const nodeStart = nodeClamped * pageSize;
  const nodePageRows = nodeRows.slice(nodeStart, nodeStart + pageSize);
  const toggleNode = (name: string) => {
    if (expanded === name) {
      setExpanded(null);
      return;
    }
    setExpanded(name);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(true);
    api
      .nodeDetail(tab, name)
      .then(setDetail)
      .catch((error: unknown) => {
        setDetail(null);
        setDetailError(error instanceof Error ? error.message : String(error));
      })
      .finally(() => setDetailLoading(false));
  };

  if (!show) return null;
  return (
    <Card
      title="Nodes"
      sub={`${n(nodeRows.length)} node${nodeRows.length === 1 ? "" : "s"} · unhealthy first`}
    >
      <div className="filterbar">
        <SearchInput
          ariaLabel="Search nodes"
          placeholder="Filter nodes by name, type, or issue…"
          value={nodeQ}
          onChange={setNodeQ}
        />
      </div>
      <div className="tbl-wrap">
        <table className="tbl">
          <thead>
            <tr>
              <th>Node</th>
              <th>Type</th>
              <th className="right">Accel free/total</th>
              <th>Health</th>
              <th>Island / domain</th>
              <th className="right">Age</th>
            </tr>
          </thead>
          <tbody>
            {nodePageRows.map((nd) => (
              <F key={nd.name}>
                <tr className="clickable" onClick={() => toggleNode(nd.name)}>
                  <td className="cell-name mono">
                    <button
                      type="button"
                      className="row-open"
                      aria-expanded={expanded === nd.name}
                      onClick={(event) => {
                        event.stopPropagation();
                        toggleNode(nd.name);
                      }}
                    >
                      {nd.name}
                    </button>
                  </td>
                  <td className="mono muted">{nd.instanceType ?? "—"}</td>
                  <td className="num">
                    {nd.accelerator
                      ? `${n(nd.accelerator.free)}/${n(nd.accelerator.allocatable)}`
                      : "—"}
                  </td>
                  <td>
                    {nd.issues?.length ? (
                      nd.issues.map((iss, i) => (
                        <span key={i} className="tag">
                          {iss}
                        </span>
                      ))
                    ) : (
                      <span className="txt-ok">Ready</span>
                    )}
                  </td>
                  <td className="mono muted">
                    {[nd.island, nd.nvlinkDomain]
                      .filter((x): x is string => !!x)
                      .map((x) => shortId(x))
                      .join(" / ") || "—"}
                  </td>
                  <td className="num muted">{nd.age ?? "—"}</td>
                </tr>
                {expanded === nd.name ? (
                  <tr className="node-detail-row">
                    <td colSpan={6}>
                      <div className="node-detail">
                        <div className="node-cmds">
                          <span className="node-cmds-lbl">run with your own creds</span>
                          <CopyCmd
                            label="describe"
                            cmd={`kubectl --context ${tab} describe node ${nd.name}`}
                          />
                          <CopyCmd
                            label="drain"
                            cmd={`kubectl --context ${tab} drain ${nd.name} --ignore-daemonsets --delete-emptydir-data`}
                          />
                          <CopyCmd
                            label="debug shell"
                            cmd={`kubectl --context ${tab} debug node/${nd.name} -it --image=busybox`}
                          />
                        </div>
                        {detailLoading ? (
                          <p className="panel-note">Loading node detail…</p>
                        ) : detail && detail.name === nd.name ? (
                          <div className="node-detail-grid">
                            <div>
                              <div className="eyebrow">Conditions</div>
                              {detail.conditions?.length ? (
                                detail.conditions.map((c, i) => (
                                  <div className="cond-row" key={i}>
                                    <span>{c.type}</span>
                                    <span className={`cond-rs ${condTone(c)}`}>{c.status}</span>
                                  </div>
                                ))
                              ) : (
                                <span className="muted">—</span>
                              )}
                            </div>
                            <div>
                              <div className="eyebrow">Taints</div>
                              {detail.taints?.length ? (
                                <div className="frag-list">
                                  {detail.taints.map((t, i) => (
                                    <span className="frag-chip mono" key={i}>
                                      {t.key}
                                      {t.value ? `=${t.value}` : ""}:{t.effect}
                                    </span>
                                  ))}
                                </div>
                              ) : (
                                <span className="muted">none</span>
                              )}
                            </div>
                            <div>
                              <div className="eyebrow">
                                Pods on node ({n(detail.podsOnNode?.length ?? 0)})
                              </div>
                              {detail.podsOnNode?.length ? (
                                <ul className="node-pods">
                                  {detail.podsOnNode.slice(0, 20).map((p, i) => (
                                    <li key={i}>
                                      {p.deployment ? (
                                        <button
                                          className="linkbtn mono"
                                          title={`open deployment ${p.deployment}`}
                                          onClick={() =>
                                            openDeployment(
                                              isvcSelected(
                                                detail.cluster,
                                                p.deployment ?? "",
                                                p.namespace,
                                              ),
                                            )
                                          }
                                        >
                                          {p.namespace}/{p.name}
                                        </button>
                                      ) : (
                                        <span className="mono">
                                          {p.namespace}/{p.name}
                                        </span>
                                      )}
                                      {p.accelerators ? (
                                        <span className="node-pod-accel tnum">
                                          {n(p.accelerators)}
                                        </span>
                                      ) : null}
                                    </li>
                                  ))}
                                </ul>
                              ) : (
                                <span className="muted">none</span>
                              )}
                            </div>
                          </div>
                        ) : detailError ? (
                          <p className="panel-note txt-bad">{detailError}</p>
                        ) : (
                          <p className="panel-note">Node detail is unavailable.</p>
                        )}
                      </div>
                    </td>
                  </tr>
                ) : null}
              </F>
            ))}
            {nodeRows.length === 0 ? (
              <EmptyRow colSpan={6}>
                {nodesLoading
                  ? "Loading nodes…"
                  : nodesError
                    ? "Node inventory unavailable"
                    : nodeQ.trim()
                      ? `No nodes match "${nodeQ}"`
                      : "No nodes"}
              </EmptyRow>
            ) : null}
          </tbody>
        </table>
      </div>
      <Pager
        total={nodeRows.length}
        page={nodeClamped}
        totalPages={nodeTotalPages}
        pageSize={pageSize}
        start={nodeStart}
        noun="nodes"
        onPage={setNodePage}
        onPageSize={onPageSize}
      />
    </Card>
  );
}
