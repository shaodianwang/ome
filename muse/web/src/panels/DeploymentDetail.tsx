import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";

import { api, isNotFoundError, logStreamUrl } from "../api";
import type { EventItem, IsvcDetail, IsvcPod, PodInspect, RuntimeDetail } from "../api";
import { CommandBlock, CopyCmd, DataNotice, EnvList, Pill } from "../components";
import { metricsHealth } from "../dataState";
import { Copyable, Facts } from "../drawer";
import { useEventFeed, useLive, useMetrics } from "../live";
import type { StreamConnectionState } from "../live";
import {
  componentReplicas,
  componentTone,
  conditionRank,
  replicaRollup,
  runtimeRefOf,
  shortRevision,
  sortConditions,
} from "../map";
import { isvcMetrics, seriesOf } from "../metrics";
import type { IsvcMetrics } from "../metrics";
import { replicaDiagnostics } from "../replicaMetrics";
import { resourceKey, selectResource } from "../resource";
import type { ResourceRef, Selected } from "../resource";
import { imageShort, isDisaggregated, servesTags, topologySummary } from "../runtime";
import { ago, fmtErrPct, fmtInt, fmtLatency, fmtRate, matches, num as n } from "../util";
import { Card, Dot, FilterChip, Gauge, Track } from "../viz";
import { ChatBox } from "./ChatBox";
import { Monitoring } from "./DeploymentMonitoring";

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
  metricIdentity,
}: {
  sel: Selected;
  onBack: () => void;
  openModel: (sel: Selected) => void;
  openRuntime: (sel: Selected) => void;
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

      {d && detail.error ? (
        <DataNotice
          tone="warning"
          title="Deployment refresh failed"
          detail={
            detail.lastSuccessAt
              ? `Showing status from ${ago(detail.lastSuccessAt)}. ${detail.error}`
              : detail.error
          }
          action={{ label: "Retry", onClick: detail.refetch }}
        />
      ) : null}
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
                  const pct = des > 0 ? Math.round((cur / des) * 100) : cur > 0 ? 100 : 0;
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
                      {c.revision ? (
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
            <Card title="Rollout" sub={`canary · step ${d.canary.currentStep}`}>
              <Gauge pct={d.canary.trafficWeight} />
              <p className="panel-note">
                {d.canary.trafficWeight}% of traffic on the new revision.
              </p>
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
          <PodsCard sel={sel} podInspect={podInspect} podLogStream={podLogStream} />
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

/** Page size for the pods table — big deployments are navigated by the filters,
 *  never by scrolling thousands of rows. */
const POD_PAGE = 50;

const podProblematic = (p: IsvcPod) => !p.ready || p.reason != null || p.restarts > 0;

/** The deployment's pods as a diagnosis view, not an inventory dump: healthy
 *  pods collapse into per-component counts, and the table shows only the pods
 *  needing attention unless the operator widens the filters. */
function PodsCard({
  sel,
  podInspect,
  podLogStream,
}: {
  sel: Selected;
  podInspect: boolean;
  podLogStream: boolean;
}) {
  const [component, setComponent] = useState("all");
  const [show, setShow] = useState<"problems" | "all">("problems");
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(0);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [stream, setStream] = useState<{ component?: string; pod?: string } | null>(null);
  const pods = useLive(() => api.inferenceServicePods(sel), {
    kinds: ["Pod", "InferenceService"],
    throttleMs: 5_000,
    intervalMs: 30_000,
  });
  const items = pods.data?.pods ?? [];
  const problems = items.filter(podProblematic);

  // Per-component health rollup — the collapsed view of the healthy majority.
  const componentNames = [...new Set(items.map((p) => p.component ?? "other"))].sort();
  const perComponent = componentNames.map((name) => {
    const of = items.filter((p) => (p.component ?? "other") === name);
    return { name, total: of.length, bad: of.filter(podProblematic).length };
  });

  const scoped = items
    .filter((p) => component === "all" || (p.component ?? "other") === component)
    .filter((p) => matches(query, p.name, p.node, p.reason))
    .filter((p) => show === "all" || podProblematic(p));
  const start = Math.min(page * POD_PAGE, Math.max(0, scoped.length - 1));
  const visible = scoped.slice(start, start + POD_PAGE);

  const sub = items.length
    ? problems.length
      ? `${n(problems.length)} of ${n(items.length)} need attention`
      : `${n(items.length)} · all healthy`
    : undefined;

  if (!items.length) {
    return (
      <Card title="Pods">
        <div className="muted">
          {pods.error
            ? `Pod inventory unavailable: ${pods.error}`
            : pods.loading
              ? "Loading pods…"
              : "No pods found for this deployment."}
        </div>
      </Card>
    );
  }

  return (
    <Card title="Pods" sub={sub}>
      <div className="pods-rollup">
        {perComponent.map((c) => (
          <span key={c.name} className={`repl ${c.bad ? "amber" : "green"}`}>
            {c.name} {n(c.total - c.bad)}/{n(c.total)} healthy
          </span>
        ))}
      </div>
      <div className="filterbar">
        <FilterChip
          label="Component"
          value={component}
          options={[
            { value: "all", label: "All" },
            ...componentNames.map((name) => ({ value: name, label: name })),
          ]}
          onChange={(value) => {
            setComponent(value);
            setPage(0);
          }}
        />
        <FilterChip
          label="Show"
          value={show}
          options={[
            { value: "problems", label: `Problems (${n(problems.length)})` },
            { value: "all", label: `All (${n(items.length)})` },
          ]}
          onChange={(value) => {
            setShow(value as "problems" | "all");
            setPage(0);
          }}
        />
        <div className="input grow">
          <input
            aria-label="Filter pods"
            placeholder="Filter by pod name, node, or reason…"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setPage(0);
            }}
          />
        </div>
        {podLogStream && !stream ? (
          <button
            className="linkbtn"
            onClick={() => setStream({ component: component === "all" ? undefined : component })}
            title="Follow logs live across this deployment's pods"
          >
            ⏵ stream logs
          </button>
        ) : null}
      </div>
      {stream ? (
        <LogStreamBox
          streamRef={{
            cluster: sel.cluster,
            kind: "InferenceService",
            name: sel.name,
            namespace: pods.data?.namespace ?? sel.namespace,
          }}
          scope={stream}
          onClose={() => setStream(null)}
        />
      ) : scoped.length ? (
        <>
          <div className="pods-scroll">
            <table className="place-tbl pods-tbl">
              <thead>
                <tr>
                  <th>Pod</th>
                  <th>Component</th>
                  <th>Phase</th>
                  <th className="tnum">Restarts</th>
                  <th>Reason</th>
                  <th>Node</th>
                  <th className="tnum">Age</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((p) => (
                  <PodRow
                    key={p.name}
                    p={p}
                    podRef={{
                      cluster: sel.cluster,
                      kind: "InferenceService",
                      name: sel.name,
                      namespace: pods.data?.namespace ?? sel.namespace,
                    }}
                    podInspect={podInspect}
                    onStream={podLogStream ? (pod) => setStream({ pod }) : undefined}
                    expanded={expanded === p.name}
                    onToggle={() => setExpanded(expanded === p.name ? null : p.name)}
                  />
                ))}
              </tbody>
            </table>
          </div>
          {scoped.length > POD_PAGE ? (
            <div className="pods-pager">
              <span className="muted">
                {n(start + 1)}–{n(start + visible.length)} of {n(scoped.length)}
              </span>
              <button className="linkbtn" disabled={page === 0} onClick={() => setPage(page - 1)}>
                ‹ prev
              </button>
              <button
                className="linkbtn"
                disabled={start + POD_PAGE >= scoped.length}
                onClick={() => setPage(page + 1)}
              >
                next ›
              </button>
            </div>
          ) : null}
        </>
      ) : (
        <div className="muted attn-none">
          {show === "problems" && !query
            ? `All ${n(items.length)} pods are healthy.`
            : "No pods match the current filters."}
        </div>
      )}
    </Card>
  );
}

/** One pod row plus, when expanded, the live drill-down. */
function PodRow({
  p,
  podRef,
  podInspect,
  onStream,
  expanded,
  onToggle,
}: {
  p: IsvcPod;
  podRef: Selected;
  podInspect: boolean;
  onStream?: (pod: string) => void;
  expanded: boolean;
  onToggle: () => void;
}) {
  const bad = !p.ready || p.reason != null;
  const phaseTone = p.phase === "Running" && p.ready ? "green" : bad ? "red" : "amber";
  return (
    <Fragment>
      <tr className={`link${bad ? " pod-bad" : ""}`} onClick={onToggle}>
        <td className="mono pod-name" title={p.name}>
          {p.name}
        </td>
        <td>{p.component ?? "—"}</td>
        <td>
          <Dot tone={phaseTone} /> {p.phase ?? "—"}
          {p.phase === "Running" && !p.ready ? <span className="muted"> · not ready</span> : null}
        </td>
        <td className={`tnum${p.restarts > 0 ? " txt-bad" : ""}`}>{p.restarts || "—"}</td>
        <td className={p.reason ? "txt-bad" : "muted"}>
          {p.reason ?? (p.lastRestart ? `last: ${p.lastRestart}` : "—")}
        </td>
        <td className="mono muted pod-node" title={p.node}>
          {p.node ?? "unscheduled"}
        </td>
        <td className="tnum muted">{p.age ?? "—"}</td>
      </tr>
      {expanded ? (
        <tr className="pod-expand">
          <td colSpan={7}>
            <PodExpand p={p} podRef={podRef} podInspect={podInspect} onStream={onStream} />
          </td>
        </tr>
      ) : null}
    </Fragment>
  );
}

/** The expanded pod: live status with full messages, its Kubernetes Events, and
 *  a bounded log tail — the post-mortem, in place. kubectl commands remain as a
 *  copy-paste escape hatch for anything deeper. */
function PodExpand({
  p,
  podRef,
  podInspect,
  onStream,
}: {
  p: IsvcPod;
  podRef: Selected;
  podInspect: boolean;
  onStream?: (pod: string) => void;
}) {
  const [inspect, setInspect] = useState<PodInspect | null>(null);
  const [inspectError, setInspectError] = useState<string | null>(null);
  const [logs, setLogs] = useState<{ title: string; text: string } | null>(null);
  const [logsBusy, setLogsBusy] = useState(false);

  useEffect(() => {
    if (!podInspect) return;
    let cancelled = false;
    setInspect(null);
    setInspectError(null);
    api
      .inferenceServicePodInspect(podRef, p.name)
      .then((d) => {
        if (!cancelled) setInspect(d);
      })
      .catch((e: unknown) => {
        if (!cancelled) setInspectError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
    // Refetch when the expanded pod changes; podRef fields are part of p.name's identity.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [p.name, podInspect]);

  const fetchLogs = (container: string | undefined, previous: boolean) => {
    setLogsBusy(true);
    const title = `${container ?? "logs"}${previous ? " · previous run" : ""}`;
    api
      .inferenceServicePodLogs(podRef, p.name, { container, previous })
      .then((d) => setLogs({ title: `${title} · last ${n(d.tail)} lines`, text: d.text }))
      .catch((e: unknown) => setLogs({ title, text: e instanceof Error ? e.message : String(e) }))
      .finally(() => setLogsBusy(false));
  };

  const containers = inspect?.containers ?? [];
  const warnings = (inspect?.events ?? []).filter((e) => e.type === "Warning");
  const normals = (inspect?.events ?? []).filter((e) => e.type !== "Warning");
  // Pod-level conditions worth their own line. `Ready`/`ContainersReady` merely
  // restate "some container isn't ready" — the container rows already show
  // which and why — so only the informative ones (Unschedulable, …) remain.
  const failingConds = (inspect?.conditions ?? []).filter(
    (c) =>
      c.status === "False" &&
      (c.reason || c.message) &&
      c.type !== "Ready" &&
      c.type !== "ContainersReady",
  );
  // The diagnosis headline: the most important warning, else the pod's reason.
  const headline = warnings[0] ?? (p.reason ? { reason: p.reason } : null);
  const kubectl = `kubectl --context ${podRef.cluster}${podRef.namespace ? ` -n ${podRef.namespace}` : ""}`;

  return (
    <div className="pod-inspect">
      {!podInspect ? (
        <p className="panel-note">
          Live inspection isn't configured on this console — using the cached status.
        </p>
      ) : inspectError ? (
        <p className="panel-note txt-bad">Live inspection failed: {inspectError}</p>
      ) : !inspect ? (
        <p className="panel-note">Inspecting…</p>
      ) : null}

      {inspect && headline ? (
        <div className="pod-headline">
          <Dot tone="red" />
          <div>
            <b>{headline.reason}</b>
            {"count" in headline && headline.count && headline.count > 1 ? (
              <span className="pod-count">×{n(headline.count)}</span>
            ) : null}
            {"lastSeen" in headline && headline.lastSeen ? (
              <span className="muted"> · last {headline.lastSeen} ago</span>
            ) : null}
            {"message" in headline && headline.message ? (
              <div className="pod-msg">{headline.message}</div>
            ) : null}
          </div>
        </div>
      ) : null}

      <div className="pod-sections">
        <div className="pod-section">
          <div className="eyebrow">Containers</div>
          <ul className="pod-containers">
            {(containers.length
              ? containers.map((c) => ({
                  key: c.name,
                  ready: c.ready,
                  title: c.name,
                  state: c.state === "running" && c.runningFor ? `up ${c.runningFor}` : c.state,
                  reason: c.reason,
                  restarts: c.restarts,
                  lastExit: c.lastExit
                    ? `${c.lastExit}${c.lastExitAt ? ` · ${c.lastExitAt} ago` : ""}`
                    : undefined,
                  message: c.message ?? c.lastExitMessage,
                }))
              : // Live detail not (yet) available: fall back to the cached view.
                (p.containers ?? []).map((c) => ({
                  key: c.name,
                  ready: c.ready,
                  title: c.name,
                  state: c.state,
                  reason: c.reason,
                  restarts: c.restarts,
                  lastExit: c.lastRestart,
                  message: undefined as string | undefined,
                }))
            ).map((c) => (
              <li key={c.key}>
                <div className="pod-container-line">
                  <Dot tone={c.ready ? "green" : "amber"} />
                  <span className="mono">{c.title}</span>
                  {c.state ? <span className="tag">{c.state}</span> : null}
                  {c.reason ? <span className="tag warn">{c.reason}</span> : null}
                  {c.restarts ? <span className="tag warn">{n(c.restarts)} restarts</span> : null}
                </div>
                {c.lastExit ? (
                  <div className="pod-kv">
                    <span className="lbl">last exit</span>
                    <span className="txt-bad">{c.lastExit}</span>
                  </div>
                ) : null}
                {c.message ? <div className="pod-msg">{c.message}</div> : null}
              </li>
            ))}
          </ul>
          {failingConds.length ? (
            <div className="pod-events">
              {failingConds.map((c, i) => (
                <div className="pod-event bad" key={`${c.type}-${i}`}>
                  <b>{c.reason ?? c.type}</b>
                  {c.message ? <div className="pod-msg">{c.message}</div> : null}
                </div>
              ))}
            </div>
          ) : null}
        </div>

        {inspect ? (
          <div className="pod-section">
            <div className="eyebrow">Events</div>
            {warnings.length ? (
              warnings.map((e, i) => (
                <div className="pod-event bad" key={i}>
                  <div className="pod-event-head">
                    <b>{e.reason ?? "warning"}</b>
                    {e.count && e.count > 1 ? (
                      <span className="pod-count">×{n(e.count)}</span>
                    ) : null}
                    {e.lastSeen ? <span className="muted right">{e.lastSeen} ago</span> : null}
                  </div>
                  {e.message ? <div className="pod-msg">{e.message}</div> : null}
                </div>
              ))
            ) : (
              <p className="panel-note">No warnings.</p>
            )}
            {normals.length ? (
              <details className="collapse">
                <summary>{n(normals.length)} routine events</summary>
                {normals.map((e, i) => (
                  <div className="pod-event" key={i}>
                    <div className="pod-event-head">
                      <b>{e.reason ?? e.type ?? "event"}</b>
                      {e.count && e.count > 1 ? (
                        <span className="pod-count">×{n(e.count)}</span>
                      ) : null}
                      {e.lastSeen ? <span className="muted right">{e.lastSeen} ago</span> : null}
                    </div>
                    {e.message ? <div className="pod-msg">{e.message}</div> : null}
                  </div>
                ))}
              </details>
            ) : null}
          </div>
        ) : null}
      </div>

      {podInspect ? (
        <div className="pod-actions">
          {(containers.length ? containers : [{ name: undefined as string | undefined }]).map(
            (c) => (
              <Fragment key={c.name ?? "all"}>
                <button
                  className="copybtn"
                  disabled={logsBusy}
                  onClick={() => fetchLogs(c.name, false)}
                >
                  logs{c.name && containers.length > 1 ? `: ${c.name}` : ""}
                </button>
                {p.restarts > 0 ? (
                  <button
                    className="copybtn"
                    disabled={logsBusy}
                    onClick={() => fetchLogs(c.name, true)}
                  >
                    previous run{c.name && containers.length > 1 ? `: ${c.name}` : ""}
                  </button>
                ) : null}
              </Fragment>
            ),
          )}
          {onStream ? (
            <button className="copybtn" onClick={() => onStream(p.name)}>
              ⏵ follow live
            </button>
          ) : null}
          <span className="pod-actions-sep" />
          <CopyCmd label="kubectl describe" cmd={`${kubectl} describe pod ${p.name}`} />
          {p.node ? (
            <CopyCmd
              label="describe node"
              cmd={`kubectl --context ${podRef.cluster} describe node ${p.node}`}
            />
          ) : null}
        </div>
      ) : (
        <div className="pod-actions">
          <CopyCmd label="kubectl describe" cmd={`${kubectl} describe pod ${p.name}`} />
          <CopyCmd label="kubectl logs" cmd={`${kubectl} logs ${p.name} --tail=200`} />
          {p.restarts > 0 ? (
            <CopyCmd label="kubectl logs --previous" cmd={`${kubectl} logs ${p.name} --previous`} />
          ) : null}
        </div>
      )}

      {logs ? <LogBox title={logs.title} text={logs.text} onClose={() => setLogs(null)} /> : null}
    </div>
  );
}

/** Terminal escape sequences (colors, cursor moves) — meaningless in HTML. */
// eslint-disable-next-line no-control-regex
const ANSI_RE = /\u001b\[[0-9;?]*[a-zA-Z]/g;

/** Log-level tint per line, so errors pop and debug noise recedes. */
function logLineClass(line: string): string {
  if (/\b(ERROR|FATAL|PANIC)\b/.test(line)) return "log-line err";
  if (/\bWARN(ING)?\b/.test(line)) return "log-line warn";
  if (/\b(DEBUG|TRACE)\b/.test(line)) return "log-line dim";
  return "log-line";
}

/** The inline log viewer: ANSI-stripped, line-filterable, closable; scrolled to
 *  the end on load because the newest lines are the ones that answer. */
function LogBox({ title, text, onClose }: { title: string; text: string; onClose: () => void }) {
  const [q, setQ] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const lines = useMemo(() => text.replace(ANSI_RE, "").split("\n"), [text]);
  const shown = q ? lines.filter((line) => line.toLowerCase().includes(q.toLowerCase())) : lines;

  useEffect(() => {
    const el = scrollRef.current;
    if (el && !q) el.scrollTop = el.scrollHeight;
  }, [lines, q]);

  return (
    <div className="pod-logs">
      <div className="pod-logs-head">
        <span className="eyebrow">{title}</span>
        <div className="input grow">
          <input
            aria-label="Filter log lines"
            placeholder="Filter lines…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
        {q ? (
          <span className="muted">
            {n(shown.length)} of {n(lines.length)} lines
          </span>
        ) : null}
        <button className="linkbtn" onClick={onClose} title="Close logs">
          ✕ close
        </button>
      </div>
      <div className="pod-logs-body" ref={scrollRef}>
        {shown.length ? (
          shown.map((line, i) => (
            <div className={logLineClass(line)} key={i}>
              {line || " "}
            </div>
          ))
        ) : (
          <div className="log-line dim">{q ? "no matching lines" : "(no output)"}</div>
        )}
      </div>
    </div>
  );
}

/** One live-tail session keeps at most this many of the newest lines; older
 *  lines fall off so an hours-long stream can't grow the page unbounded. */
const STREAM_BUFFER = 2_000;
/** Trailing lines requested per pod on connect — just enough context to anchor
 *  the live tail without replaying history. */
const STREAM_TAIL = 10;

type StreamLine = { id: number; pod: string; line: string };

/** Stable per-pod color so interleaved lines are attributable at a glance. */
function podTint(pod: string): string {
  let h = 0;
  for (let i = 0; i < pod.length; i++) h = (h * 31 + pod.charCodeAt(i)) >>> 0;
  return `hsl(${h % 360} 55% 62%)`;
}

/** stern-style live tail: one SSE stream fanning in `line` events from every
 *  matching pod, each line prefixed with its (colored) pod name. Fills the pods
 *  card in place of the table while active. */
function LogStreamBox({
  streamRef,
  scope,
  onClose,
}: {
  streamRef: Selected;
  scope: { component?: string; pod?: string };
  onClose: () => void;
}) {
  const [lines, setLines] = useState<StreamLine[]>([]);
  const [notices, setNotices] = useState<string[]>([]);
  const [status, setStatus] = useState<"connecting" | "live" | "ended">("connecting");
  const [q, setQ] = useState("");
  const [follow, setFollow] = useState(true);
  const [session, setSession] = useState(0);
  const scrollRef = useRef<HTMLDivElement>(null);
  const seqRef = useRef(0);

  useEffect(() => {
    setLines([]);
    setNotices([]);
    setStatus("connecting");
    const source = new EventSource(
      logStreamUrl(streamRef, { component: scope.component, pod: scope.pod, tail: STREAM_TAIL }),
    );
    // Batch arrivals between renders: a busy component emits lines far faster
    // than the page should re-render.
    let buffer: StreamLine[] = [];
    const flush = window.setInterval(() => {
      if (!buffer.length) return;
      const batch = buffer;
      buffer = [];
      setLines((prev) => [...prev, ...batch].slice(-STREAM_BUFFER));
    }, 250);
    source.onopen = () => setStatus("live");
    source.addEventListener("line", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data) as { pod: string; line: string };
        buffer.push({ id: seqRef.current++, pod: data.pod, line: data.line.replace(ANSI_RE, "") });
      } catch {
        // Skip an unparsable frame rather than killing the stream.
      }
    });
    source.addEventListener("notice", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data) as { message?: string };
        if (data.message) setNotices((prev) => [...prev, data.message ?? ""]);
      } catch {
        // Skip an unparsable frame rather than killing the stream.
      }
    });
    // No auto-reconnect: EventSource retry would replay every pod's tail and
    // interleave duplicates. Surface the drop; restarting is one click.
    source.onerror = () => {
      setStatus("ended");
      source.close();
    };
    return () => {
      source.close();
      window.clearInterval(flush);
    };
    // Reconnect only when the stream's identity changes — the scalar fields, not
    // the ref object, whose identity changes on every parent render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [streamRef.cluster, streamRef.name, streamRef.namespace, scope.component, scope.pod, session]);

  useEffect(() => {
    const el = scrollRef.current;
    if (el && follow) el.scrollTop = el.scrollHeight;
  }, [lines, follow]);

  const needle = q.toLowerCase();
  const shown = q
    ? lines.filter(
        (l) => l.line.toLowerCase().includes(needle) || l.pod.toLowerCase().includes(needle),
      )
    : lines;
  // Pod names repeat the deployment name; drop that prefix so the interesting
  // suffix (component, index) survives in a narrow column.
  const prefix = `${streamRef.name}-`;
  const shortPod = (pod: string) => (pod.startsWith(prefix) ? pod.slice(prefix.length) : pod);

  return (
    <div className="pod-logs pod-stream">
      <div className="pod-logs-head">
        <Dot tone={status === "live" ? "green" : status === "connecting" ? "amber" : "red"} />
        <span className="eyebrow">
          Live logs · {scope.pod ?? scope.component ?? "all components"}
        </span>
        <div className="input grow">
          <input
            aria-label="Filter streamed lines"
            placeholder="Filter lines…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
        {q ? (
          <span className="muted">
            {n(shown.length)} of {n(lines.length)} lines
          </span>
        ) : null}
        <button
          className="linkbtn"
          onClick={() => setFollow(!follow)}
          title={
            follow
              ? "Stop auto-scrolling (lines keep arriving)"
              : "Resume auto-scrolling to the newest lines"
          }
        >
          {follow ? "❚❚ pause" : "⏵ follow"}
        </button>
        {status === "ended" ? (
          <button className="linkbtn" onClick={() => setSession(session + 1)} title="Reconnect">
            ↻ restart
          </button>
        ) : null}
        <button className="linkbtn" onClick={onClose} title="Stop streaming">
          ✕ close
        </button>
      </div>
      {notices.map((msg, i) => (
        <div key={i} className="muted stream-notice">
          {msg}
        </div>
      ))}
      <div className="pod-logs-body" ref={scrollRef}>
        {shown.length ? (
          shown.map((l) => (
            <div className={logLineClass(l.line)} key={l.id}>
              <span className="stream-pod" style={{ color: podTint(l.pod) }} title={l.pod}>
                {shortPod(l.pod)}
              </span>{" "}
              {l.line || " "}
            </div>
          ))
        ) : (
          <div className="log-line dim">
            {status === "connecting"
              ? "connecting…"
              : q
                ? "no matching lines"
                : "waiting for output…"}
          </div>
        )}
      </div>
    </div>
  );
}

/** Inline runtime summary for the deployment overview: identity, image, node
 *  topology, what it serves, and each serving container's command and
 *  environment. Fetch failures degrade to a notice inside the card so the rest
 *  of the overview always renders. */
function RuntimeOverviewCard({
  sel,
  source,
  openRuntime,
}: {
  sel: Selected;
  source?: "spec" | "pods";
  openRuntime: (sel: Selected) => void;
}) {
  // A definitive not-found resolves to `null`, so the card can tell "this
  // runtime does not exist" apart from a transient fetch failure (which
  // surfaces via `error` and must not assert nonexistence).
  const detail = useLive<RuntimeDetail | null>(
    () =>
      api.runtime(sel).catch((e: unknown) => {
        if (isNotFoundError(e)) return null;
        throw e;
      }),
    { kinds: ["ServingRuntime", "ClusterServingRuntime"], throttleMs: 5_000 },
  );
  const d = detail.data;
  const open = (
    <span className="right">
      <button className="linkbtn" onClick={() => openRuntime(sel)}>
        Open →
      </button>
    </span>
  );
  const sub = [sel.kind, sel.namespace].filter(Boolean).join(" · ");
  if (!d) {
    return (
      <Card title="Runtime" sub={sub} actions={open}>
        {detail.loading ? (
          <div className="muted">Loading runtime…</div>
        ) : detail.error ? (
          <div className="muted" title={detail.error}>
            Runtime details unavailable right now.
          </div>
        ) : (
          <DataNotice
            tone="warning"
            title="Runtime not found"
            detail={`${sel.name} is referenced but does not exist on ${sel.cluster}.`}
          />
        )}
      </Card>
    );
  }
  const tags = servesTags(d);
  const topology = topologySummary(d.components);
  return (
    <Card title="Runtime" sub={sub} actions={open}>
      <Facts
        items={[
          [
            "Name",
            <>
              <span className="mono">{d.name}</span>
              {source === "pods" ? (
                <>
                  {" "}
                  <span
                    className="pill slate"
                    title="spec.runtime was omitted; resolved from the serving-runtime label on running pods"
                  >
                    auto-selected
                  </span>
                </>
              ) : null}
            </>,
          ],
          ["Image", d.image ? <Copyable text={d.image} display={imageShort(d.image)} /> : null],
          [
            "Topology",
            topology ? (
              <>
                {topology}
                {isDisaggregated(d) ? (
                  <>
                    {" "}
                    <span className="pill violet">PD</span>
                  </>
                ) : null}
              </>
            ) : null,
          ],
          [
            "Serves",
            tags.length ? (
              <span className="rt-serves">
                {tags.map((t) => (
                  <span key={t} className="tag">
                    {t}
                  </span>
                ))}
              </span>
            ) : null,
          ],
          [
            "Protocols",
            d.protocols?.length ? (
              <span className="rt-serves">
                {d.protocols.map((p) => (
                  <span key={p} className="tag">
                    {p}
                  </span>
                ))}
              </span>
            ) : null,
          ],
        ]}
      />
      {(d.containers ?? []).map((c) => (
        <Fragment key={c.role}>
          <div className="rt-role">{c.role}</div>
          {c.command?.length || c.args?.length ? (
            <CommandBlock
              label={`Command & args (${(c.command?.length ?? 0) + (c.args?.length ?? 0)})`}
              command={c.command}
              args={c.args}
            />
          ) : null}
          {c.env?.length ? (
            <details className="collapse">
              <summary>Env ({c.env.length})</summary>
              <EnvList env={c.env} />
            </details>
          ) : null}
        </Fragment>
      ))}
    </Card>
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
              <span className="mono">{e.action === "deleted" ? "removed" : "updated"}</span> ·{" "}
              {ago(e.at)}
            </li>
          ))}
        </ul>
      </Card>
    </>
  );
}
