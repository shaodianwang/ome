// System panel (OEP-0021.2): an admin view of OME's own control plane — the
// controller manager, scheduler, webhook, model-agent DaemonSet — per aggregated
// cluster. Read-only; it reports desired-vs-ready health from the read plane's
// informer caches. The component set is server-configured; with none configured
// this panel shows an explicit empty state rather than a misleading blank.
import { useState } from "react";

import { api } from "../api";
import type { SystemComponentItem } from "../api";
import { DataNotice, StaleNotice } from "../components";
import { Drawer, Facts, Section } from "../drawer";
import { useLive } from "../live";
import { fleetHealth, groupSummaries, statusTone, summarizeComponents } from "../systemStatus";
import type { ComponentSummary } from "../systemStatus";
import { Card, Dot, StatTile } from "../viz";

const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

/** `updated` is only interesting mid-rollout — when it differs from ready. */
function countLabel(row: SystemComponentItem): string {
  const base = `${row.ready}/${row.desired}`;
  return row.updated != null && row.updated !== row.ready
    ? `${base} · ${row.updated} updated`
    : base;
}

/** One cluster's health line inside a component card; opens the detail drawer. */
function ClusterLine({ row, onOpen }: { row: SystemComponentItem; onOpen: () => void }) {
  const detail = [
    row.message,
    row.delivery ? `Argo ${row.delivery.resourceSyncStatus ?? row.delivery.syncStatus}` : undefined,
  ]
    .filter(Boolean)
    .join(" · ");
  return (
    <li>
      <button type="button" className="sys-cluster" onClick={onOpen}>
        <span className="sys-cluster-name">{row.cluster}</span>
        <span className="sys-cluster-status">
          <Dot tone={statusTone(row.status)} />
          {cap(row.status)}
        </span>
        <span className="sys-cluster-count mono">{countLabel(row)}</span>
        {detail ? <span className="sys-cluster-msg">{detail}</span> : null}
      </button>
    </li>
  );
}

function ComponentCard({
  summary,
  onOpen,
}: {
  summary: ComponentSummary;
  onOpen: (row: SystemComponentItem) => void;
}) {
  // The k8s object name (first cluster that resolved one) — the real identity
  // behind the friendly display name.
  const object = summary.clusters.find((c) => c.objects?.length)?.objects?.[0];
  const sub = [
    summary.kind,
    summary.clusters[0]?.namespace,
    object,
    `${summary.healthy}/${summary.total} clusters healthy`,
  ]
    .filter(Boolean)
    .join(" · ");
  return (
    <Card
      title={
        <span className="sys-card-title">
          <Dot tone={statusTone(summary.worst)} />
          {summary.name}
        </span>
      }
      sub={sub}
    >
      <ul className="sys-clusters">
        {summary.clusters.map((row) => (
          <ClusterLine key={row.cluster} row={row} onOpen={() => onOpen(row)} />
        ))}
      </ul>
    </Card>
  );
}

/** Slide-over detail for one component on one cluster: identity, counts, the
 *  workload's conditions, and its backing pods. */
function SystemDrawer({ row, onClose }: { row: SystemComponentItem; onClose: () => void }) {
  return (
    <Drawer
      title={
        <span className="sys-card-title">
          <Dot tone={statusTone(row.status)} />
          {row.name}
        </span>
      }
      subtitle={`${row.kind} · ${row.cluster}`}
      onClose={onClose}
    >
      <Section title="Overview">
        <Facts
          items={[
            ["Status", cap(row.status)],
            ["Namespace", row.namespace],
            ["Object", (row.objects ?? []).join(", ")],
            ["Ready / desired", `${row.ready}/${row.desired}`],
            ["Updated", row.updated != null ? String(row.updated) : ""],
            ["Cluster", row.cluster],
            ["Detail", row.message ?? ""],
            ["Argo application", row.delivery?.application],
            ["Argo CD sync", row.delivery?.resourceSyncStatus ?? row.delivery?.syncStatus],
            ["Git revision", row.delivery?.desiredRevision],
            ["Last sync", row.delivery?.operationPhase],
            ["Requires pruning", row.delivery?.requiresPruning ? "Yes" : ""],
          ]}
        />
      </Section>
      {row.conditions?.length ? (
        <Section title="Conditions">
          <ul className="sys-conditions">
            {row.conditions.map((c) => (
              <li key={c.type}>
                <span className="sys-cond-type">{c.type}</span>
                <span className="sys-cond-status mono">{c.status}</span>
                {c.reason ? <span className="sys-cond-reason">{c.reason}</span> : null}
                {c.message ? <span className="sys-cond-msg">{c.message}</span> : null}
              </li>
            ))}
          </ul>
        </Section>
      ) : null}
      {row.pods?.length ? (
        <Section title={`Pods · ${row.pods.length}`}>
          <ul className="sys-pods">
            {row.pods.map((p) => (
              <li key={p.name}>
                <span className="mono sys-pod-name">{p.name}</span>
                <span>{p.phase}</span>
                {p.node ? <span className="mono sys-pod-node">{p.node}</span> : null}
              </li>
            ))}
          </ul>
        </Section>
      ) : null}
    </Drawer>
  );
}

export function System() {
  const live = useLive(api.system, {
    kinds: ["Deployment", "DaemonSet", "ArgoApplication"],
    intervalMs: 30_000,
  });
  const [selected, setSelected] = useState<SystemComponentItem | null>(null);
  const rows = live.data?.items ?? [];
  const summaries = summarizeComponents(rows);
  const groups = groupSummaries(summaries);
  const fleet = fleetHealth(summaries);
  const attention = summaries.filter((s) => s.worst !== "healthy" && s.worst !== "unknown").length;

  return (
    <section className="panel active">
      <div className="page-head">
        <div>
          <h1>System</h1>
          <p>Health of OME&rsquo;s control-plane components across clusters.</p>
        </div>
      </div>

      <StaleNotice
        error={live.error}
        hasData={live.data != null}
        title="System status refresh failed"
        detail={`Showing the last snapshot. ${live.error}`}
        unavailableTitle="System status unavailable"
        unavailableDetail={live.error}
        onRetry={live.refetch}
      />

      {!live.error && !live.loading && rows.length === 0 ? (
        <DataNotice
          tone="warning"
          title="No system components configured"
          detail="Set the read plane's system-components config (MUSE_SYSTEM_COMPONENTS_JSON / Helm config.systemComponents) to surface the controller, scheduler, and other OME components here."
        />
      ) : null}

      {summaries.length ? (
        <>
          <div className="tiles">
            <StatTile
              label="Components healthy"
              value={`${fleet.healthy}/${fleet.total}`}
              tone={statusTone(fleet.worst)}
              sub="on every cluster"
            />
            <StatTile
              label="Needs attention"
              value={attention}
              tone={attention ? "amber" : "green"}
              sub="degraded / down / missing"
            />
          </div>

          {groups.map((g) => (
            <div key={g.group}>
              <div className="page-head sys-group-head">
                <div>
                  <h2>
                    <Dot tone={statusTone(g.worst)} /> {cap(g.group)}
                  </h2>
                </div>
              </div>
              <div className="grid g3">
                {g.components.map((summary) => (
                  <ComponentCard
                    key={JSON.stringify([summary.group, summary.name])}
                    summary={summary}
                    onOpen={setSelected}
                  />
                ))}
              </div>
            </div>
          ))}
        </>
      ) : null}

      {selected ? <SystemDrawer row={selected} onClose={() => setSelected(null)} /> : null}
    </section>
  );
}
