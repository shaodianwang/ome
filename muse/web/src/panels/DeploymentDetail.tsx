import { useState } from "react";
import type { ReactNode } from "react";

import { api } from "../api";
import type { EventItem, IsvcDetail } from "../api";
import { DataNotice, Pill, StaleNotice } from "../components";
import { metricsHealth } from "../dataState";
import { Copyable, Facts } from "../drawer";
import { useEventFeed, useLive, useMetrics } from "../live";
import type { StreamConnectionState } from "../live";
import {
  canaryNote,
  componentReplicas,
  componentTone,
  componentTransition,
  conditionRank,
  replicaRollup,
  runtimeRefOf,
  shortRevision,
  sortConditions,
  transitionLabel,
} from "../map";
import { isvcMetrics, seriesOf } from "../metrics";
import type { IsvcMetrics } from "../metrics";
import { replicaDiagnostics } from "../replicaMetrics";
import { resourceKey, selectResource } from "../resource";
import type { ResourceRef, Selected } from "../resource";
import { ago, fmtErrPct, fmtInt, fmtLatency, fmtRate, num as n } from "../util";
import { Card, Dot, Gauge, Track } from "../viz";
import { ChatBox } from "./ChatBox";
import { Monitoring } from "./DeploymentMonitoring";
import { PodsCard } from "./DeploymentPods";
import { RuntimeOverviewCard } from "./RuntimeOverviewCard";

/** Console-style detail for one deployment, modelled on a cloud console (EC2 /
 *  OCI): a persistent identity header over a few substantial tabs.
 *  Overview balances current health with invocation, followed by the full
 *  runtime configuration. Monitoring owns the time-series; Activity is the
 *  change log. */
const TABS = [
  { id: "overview", label: "Overview" },
  { id: "monitoring", label: "Monitoring" },
  { id: "activity", label: "Activity" },
] as const;
type TabId = (typeof TABS)[number]["id"];
export type DeploymentMetricIdentity = "loading" | "unavailable" | "ambiguous" | "unambiguous";

export function DeploymentDetail({
  sel,
  onBack,
  openModel,
  openRuntime,
  openNode,
  metricIdentity,
}: {
  sel: Selected;
  onBack: () => void;
  openModel: (sel: Selected) => void;
  openRuntime: (sel: Selected) => void;
  openNode: (cluster: string, node: string) => void;
  metricIdentity: DeploymentMetricIdentity;
}) {
  const [tab, setTab] = useState<TabId>("overview");
  const [metricsRangeId, setMetricsRangeId] = useState("");
  const detail = useLive<IsvcDetail>(() => api.inferenceService(sel), {
    kinds: ["InferenceService"],
    throttleMs: 4_000,
  });
  const cfg = useLive(api.config, { kinds: [] });
  const feed = useEventFeed(200);
  const d = detail.data;

  // Query only this deployment. The server coalesces identical requests and
  // applies the configured Query Gateway concurrency limit.
  const metricsOn = !!cfg.data?.metrics;
  const namespace = d?.namespace ?? sel.namespace;
  const requestedRange = metricsRangeId || cfg.data?.metricsTimeRanges?.default;
  const metricsEnabled = metricsOn && !!d && !!namespace && metricIdentity === "unambiguous";
  const metrics = useMetrics(
    () =>
      api.metrics({
        cluster: sel.cluster,
        namespace,
        inferenceservice: sel.name,
        range: requestedRange,
      }),
    metricsEnabled,
    cfg.data?.metricsRefreshSecs,
    `${sel.cluster}|${namespace ?? ""}|${sel.name}|${requestedRange ?? ""}`,
  );
  const m = isvcMetrics(
    metrics.data,
    sel.cluster,
    namespace,
    sel.name,
    metricIdentity === "unambiguous",
  );
  const replicas = replicaDiagnostics(metrics.data, sel.cluster, namespace, sel.name);
  const metricsSource = metrics.data?.sources?.[sel.cluster];
  const clusterMetrics = metrics.data
    ? {
        updatedAt: metrics.data.updatedAt,
        clusters: { [sel.cluster]: metrics.data.clusters[sel.cluster] ?? {} },
        sources: metricsSource ? { [sel.cluster]: metricsSource } : {},
      }
    : null;
  const metricState = metricsHealth(clusterMetrics);

  const activity = feed.events.filter(
    (e) =>
      e.kind === "InferenceService" &&
      e.name === sel.name &&
      e.cluster === sel.cluster &&
      e.namespace === (d?.namespace ?? sel.namespace),
  );

  // Header health pill: the flat ready flag, sharpened by per-component
  // readiness — a deployment reporting Ready while a component is short of its
  // replica target reads Degraded, not Ready.
  const status = !d
    ? null
    : d.ready === false
      ? { label: "Not ready", tone: "red" as const }
      : (d.components ?? []).some((c) => componentTone(c) !== "ok")
        ? { label: "Degraded", tone: "amber" as const }
        : d.ready === true
          ? { label: "Ready", tone: "green" as const }
          : { label: "Unknown", tone: "slate" as const };

  const meta = [sel.cluster, d?.namespace, d?.deploymentMode, d?.age].filter(Boolean).join(" · ");

  return (
    <section className="panel active detail-page">
      <button className="backlink" onClick={onBack}>
        ‹ Deployments
      </button>
      <div className="detail-head">
        <h1 className="detail-title mono">{sel.name}</h1>
        {status ? <Pill tone={status.tone} label={status.label} /> : null}
        <span className="detail-meta">{meta}</span>
      </div>

      {d && d.ready === false && (d.reason || d.message) ? (
        <div className="notice bad">
          {d.reason ? <b>{d.reason}</b> : null}
          {d.message ? <div>{d.message}</div> : null}
        </div>
      ) : null}

      <StaleNotice
        error={detail.error}
        hasData={!!d}
        title="Deployment refresh failed"
        detail={
          detail.lastSuccessAt
            ? `Showing status from ${ago(detail.lastSuccessAt)}. ${detail.error}`
            : detail.error
        }
        onRetry={detail.refetch}
      />

      {cfg.error ? (
        <DataNotice
          tone="danger"
          title="Console configuration unavailable"
          detail={cfg.error}
          action={{ label: "Retry", onClick: cfg.refetch }}
        />
      ) : null}

      {!d ? (
        <div className="muted attn-none">
          {detail.error ?? (detail.loading ? "Loading…" : "Not found")}
        </div>
      ) : (
        <>
          <div className="dtabs">
            {TABS.map((t) => (
              <button
                key={t.id}
                className={`dtab${tab === t.id ? " active" : ""}`}
                onClick={() => setTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>

          {tab === "overview" ? (
            <OverviewPane
              d={d}
              m={m}
              sel={sel}
              metricsOn={metricsOn}
              podInspect={cfg.data?.podInspect === true}
              podLogStream={cfg.data?.podLogStream === true}
              chat={cfg.data?.chat === true}
              chatTimeoutSecs={cfg.data?.chatTimeoutSecs}
              chatClientGraceSecs={cfg.data?.chatClientGraceSecs}
              openModel={openModel}
              openRuntime={openRuntime}
              openNode={openNode}
              goMonitoring={() => setTab("monitoring")}
            />
          ) : null}
          {tab === "monitoring" ? (
            metricsOn && !m.identityScoped ? (
              <div className="muted attn-none">
                {metricIdentity === "loading"
                  ? "Loading deployment identity…"
                  : metricIdentity === "ambiguous"
                    ? "Serving metrics are unavailable because this deployment name exists in multiple namespaces."
                    : "Deployment inventory is unavailable; serving metrics are hidden."}
              </div>
            ) : (
              <Monitoring
                m={m}
                replicas={replicas}
                metricsOn={metricsOn}
                policy={cfg.data?.dashboardPolicy}
                timeRanges={cfg.data?.metricsTimeRanges}
                selectedTimeRangeId={metricsRangeId}
                onTimeRangeChange={setMetricsRangeId}
                loading={metrics.loading || !metricState.ready}
                error={metrics.error}
                queryCount={metricState.queryCount}
                failedQueryCount={metricState.failedQueryCount}
                updatedAt={metrics.data?.updatedAt}
                source={metricsSource}
              />
            )
          ) : null}
          {tab === "activity" ? (
            <ActivityPane
              activity={activity}
              error={feed.error}
              loading={feed.loading}
              connection={feed.connection}
            />
          ) : null}
        </>
      )}
    </section>
  );
}

/** One compact stat in the Overview headline strip (a glance, not a tile wall). */
function StripStat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="s2">
      <span className="lbl">{label}</span>
      <span className="val">{value}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Overview — health and deployment details beside invocation, followed by the
// full runtime configuration. The headline strip links out to Monitoring.
// ---------------------------------------------------------------------------
function OverviewPane({
  d,
  m,
  sel,
  metricsOn,
  podInspect,
  podLogStream,
  chat,
  chatTimeoutSecs,
  chatClientGraceSecs,
  openModel,
  openRuntime,
  openNode,
  goMonitoring,
}: {
  d: IsvcDetail;
  m: IsvcMetrics;
  sel: Selected;
  metricsOn: boolean;
  podInspect: boolean;
  podLogStream: boolean;
  chat: boolean;
  chatTimeoutSecs?: number;
  chatClientGraceSecs?: number;
  openModel: (sel: Selected) => void;
  openRuntime: (sel: Selected) => void;
  openNode: (cluster: string, node: string) => void;
  goMonitoring: () => void;
}) {
  const showMetrics = metricsOn && m.present;
  const modelRef: ResourceRef | undefined =
    d.modelRef ?? (d.model ? { kind: "ClusterBaseModel", name: d.model } : undefined);
  const runtimeRef: ResourceRef | undefined =
    runtimeRefOf(d) ?? (d.runtime ? { kind: "ClusterServingRuntime", name: d.runtime } : undefined);
  const runtimeSel = runtimeRef ? selectResource(sel.cluster, runtimeRef) : undefined;
  const standby = (d.placement?.candidates ?? []).filter((c) => c.cluster !== d.placement?.cluster);
  const components = d.components ?? [];
  const ready = replicaRollup(d.components).label;
  const qps = seriesOf(m, "request_rate").last;
  const errRate = seriesOf(m, "error_rate").last;
  const errPct = qps && qps > 0 ? ((errRate ?? 0) / qps) * 100 : null;

  return (
    <>
      <div className="statstrip">
        <StripStat label="Replicas" value={ready} />
        {d.accelerators != null ? (
          <StripStat label="Accels held" value={n(d.accelerators)} />
        ) : null}
        {showMetrics ? <StripStat label="QPS" value={fmtRate(qps)} /> : null}
        {showMetrics ? (
          <StripStat label="p99" value={fmtLatency(seriesOf(m, "latency_p99").last)} />
        ) : null}
        {showMetrics ? <StripStat label="Error" value={fmtErrPct(errPct)} /> : null}
        {showMetrics ? (
          <StripStat label="In-flight" value={fmtInt(seriesOf(m, "running_reqs").last)} />
        ) : null}
        {d.loadedCopies != null ? (
          <StripStat
            label="Model copies"
            value={`${n(d.loadedCopies)}${d.failedCopies ? ` · ${n(d.failedCopies)} failed` : ""}`}
          />
        ) : null}
        {metricsOn ? (
          <button className="linkbtn strip-link" onClick={goMonitoring}>
            Monitoring →
          </button>
        ) : null}
      </div>

      <div className="deployment-workspace">
        <div className="deployment-health">
          <Card title="Components" sub={`${ready} ready`}>
            {components.length ? (
              <div className="comp-tiles">
                {components.map((c) => {
                  const { ready: readyCount, total } = componentReplicas(c);
                  const cur = readyCount ?? 0;
                  const des = total ?? 0;
                  const transition = componentTransition(c);
                  // Mid-rollout, progress = Instances ready on the target
                  // revision; old-revision pods keep `cur` high under surge.
                  const prog =
                    transition && c.updatedReadyReplicas != null ? c.updatedReadyReplicas : cur;
                  const pct = des > 0 ? Math.round((prog / des) * 100) : prog > 0 ? 100 : 0;
                  const tone = componentTone(c);
                  const barTone = tone === "ok" ? "green" : tone === "warn" ? "amber" : "red";
                  return (
                    <div key={c.name} className={`comp-tile ${tone}`}>
                      <div className="ct-head">
                        <Dot tone={barTone} />
                        <span className="ct-name">{c.name}</span>
                        <span className="ct-replicas tnum">
                          {cur}/{des}
                        </span>
                      </div>
                      {transition ? (
                        <div
                          className="ct-rev"
                          title={`${transition.from ?? "(new)"} → ${transition.to}`}
                        >
                          {transitionLabel(transition)}
                        </div>
                      ) : c.revision ? (
                        <div className="ct-rev" title={c.revision}>
                          {shortRevision(c.revision)}
                        </div>
                      ) : null}
                      <Track pct={pct} tone={barTone} />
                      <div className="ct-foot">
                        {[c.rolloutPhase, `${pct}%`].filter(Boolean).join(" · ")}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="muted">No component detail.</div>
            )}
            {d.loadedCopies != null || d.failedCopies ? (
              <div className="detail-load">
                <Dot tone={d.failedCopies ? "amber" : "green"} />
                model {n(d.loadedCopies ?? 0)} loaded
                {d.failedCopies ? ` · ${n(d.failedCopies)} failed` : ""}
              </div>
            ) : null}
          </Card>

          <Card title="Conditions">
            {d.conditions && d.conditions.length ? (
              <div className="cond-tiles">
                {sortConditions(d.conditions).map((c, i) => {
                  const rank = conditionRank(c);
                  const cls = rank === "bad" ? " bad" : rank === "disabled" ? " off" : "";
                  return (
                    <div key={`${c.type}-${i}`} className={`cond-tile${cls}`} title={c.message}>
                      <Dot
                        tone={rank === "bad" ? "red" : c.status === "True" ? "green" : "slate"}
                      />
                      <span className="ct-type">{c.type}</span>
                      <span className="ct-reason">{c.reason || c.status}</span>
                      {rank === "bad" && c.message ? (
                        <div className="ct-message">{c.message}</div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="muted">No conditions reported.</div>
            )}
          </Card>

          {d.canary ? (
            <Card title="Rollout" sub={canaryNote(d.canary)}>
              <Gauge pct={d.canary.trafficWeight} />
              <p className="panel-note">
                {d.canary.trafficWeight}% of traffic on the new revision.
                {d.canary.failedChecks && !d.canary.failingChecks?.length
                  ? ` ${n(d.canary.failedChecks)} analysis ${d.canary.failedChecks === 1 ? "check is" : "checks are"} failing.`
                  : ""}
              </p>
              {d.canary.failingChecks?.map((c) => (
                <div className="detail-load" key={c.name}>
                  <Dot tone="red" />
                  <span className="mono">{c.name}</span>
                  {c.threshold ? <span className="muted">want {c.threshold}</span> : null}
                  {c.value ? <span className="muted">· got {c.value}</span> : null}
                  {c.message ? <span> — {c.message}</span> : null}
                </div>
              ))}
            </Card>
          ) : null}

          <div className="deployment-details">
            <Card title="Deployment details">
              <Facts
                items={[
                  [
                    "Model",
                    modelRef ? (
                      <button
                        className="linkbtn mono"
                        onClick={() => openModel(selectResource(sel.cluster, modelRef))}
                      >
                        {modelRef.name}
                      </button>
                    ) : null,
                  ],
                  ["Mode", d.deploymentMode],
                  ["Namespace", d.namespace],
                  ["Cluster", sel.cluster],
                  ["Age", d.age],
                  [
                    "Placement",
                    d.placement?.cluster || standby.length ? (
                      <div className="deployment-placement">
                        {d.placement?.cluster ? (
                          <div className="place-row">
                            <Dot tone="green" />
                            <span className="mono">{d.placement.cluster}</span>
                            <span className="muted">active</span>
                          </div>
                        ) : null}
                        {standby.map((c, i) => (
                          <div className="place-row" key={`${c.cluster}-${i}`}>
                            <Dot tone="slate" />
                            <span className="mono">{c.cluster}</span>
                            <span className="muted">standby</span>
                          </div>
                        ))}
                      </div>
                    ) : null,
                  ],
                ]}
              />
              {d.url ? (
                <div className="about-endpoint">
                  <span className="lbl">Endpoint</span>
                  <Copyable text={d.url} />
                </div>
              ) : null}
            </Card>
          </div>

          <Card title="Invoke & test" sub="OpenAI-compatible" className="deployment-invoke">
            <ChatBox
              resource={sel}
              endpoint={d.url}
              defaultModel={d.model ?? ""}
              enabled={chat}
              timeoutSecs={chatTimeoutSecs}
              clientGraceSecs={chatClientGraceSecs}
            />
          </Card>
        </div>

        <div className="deployment-pods">
          <PodsCard
            sel={sel}
            podInspect={podInspect}
            podLogStream={podLogStream}
            openNode={(node) => openNode(sel.cluster, node)}
          />
        </div>
      </div>

      <div className="deployment-runtime">
        {runtimeSel ? (
          <RuntimeOverviewCard
            key={resourceKey(runtimeSel)}
            sel={runtimeSel}
            source={d.resolvedRuntimeRef?.source}
            openRuntime={openRuntime}
          />
        ) : (
          <Card title="Runtime">
            <p className="muted">Runtime auto-selected — resolves when replicas are running.</p>
          </Card>
        )}
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Activity — recent changes for this deployment (from the shared event feed).
// ---------------------------------------------------------------------------
function ActivityPane({
  activity,
  error,
  loading,
  connection,
}: {
  activity: EventItem[];
  error: string | null;
  loading: boolean;
  connection: StreamConnectionState;
}) {
  if (!activity.length) {
    if (connection === "reconnecting") {
      return (
        <DataNotice
          tone="warning"
          title="Live activity disconnected"
          detail="Reconnecting to the event stream."
        />
      );
    }
    if (error) {
      return <DataNotice tone="danger" title="Activity unavailable" detail={error} />;
    }
    return (
      <div className="muted attn-none">
        {loading || connection === "connecting" ? "Loading activity…" : "No recent activity."}
      </div>
    );
  }
  return (
    <>
      {connection === "reconnecting" ? (
        <DataNotice
          tone="warning"
          title="Live activity disconnected"
          detail="Reconnecting; showing the events already received."
        />
      ) : error ? (
        <DataNotice
          tone="warning"
          title="Activity refresh failed"
          detail="Showing the events already received."
        />
      ) : null}
      <Card title="Activity" sub="recent changes">
        <ul className="mini-feed">
          {activity.slice(0, 50).map((e, i) => (
            <li key={i}>
              <span className="mono">
                {e.note ?? (e.action === "deleted" ? "removed" : "updated")}
              </span>{" "}
              · {ago(e.at)}
            </li>
          ))}
        </ul>
      </Card>
    </>
  );
}
