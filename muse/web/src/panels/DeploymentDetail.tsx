import { Fragment, useState } from "react";
import type { ReactNode } from "react";

import { api, isNotFoundError } from "../api";
import type { EventItem, IsvcDetail, RuntimeDetail } from "../api";
import { CommandBlock, DataNotice, EnvList, Pill } from "../components";
import { metricsHealth } from "../dataState";
import { Copyable, Facts } from "../drawer";
import { useEventFeed, useLive, useMetrics } from "../live";
import type { StreamConnectionState } from "../live";
import { replicaRollup, runtimeRefOf } from "../map";
import { isvcMetrics, seriesOf } from "../metrics";
import type { IsvcMetrics } from "../metrics";
import { replicaDiagnostics } from "../replicaMetrics";
import { resourceKey, selectResource } from "../resource";
import type { ResourceRef, Selected } from "../resource";
import { imageShort, isDisaggregated, servesTags, topologySummary } from "../runtime";
import { ago, fmtErrPct, fmtInt, fmtLatency, fmtRate, num as n } from "../util";
import { Card, Dot, Gauge, UtilRow } from "../viz";
import { ChatBox } from "./ChatBox";
import { Monitoring } from "./DeploymentMonitoring";

/** Console-style detail for one deployment, modelled on a cloud console (EC2 /
 *  OCI): a persistent identity header over a few substantial tabs.
 *  Overview holds config *and* current state (components, conditions, rollout,
 *  placement) — deliberately not split into thin per-facet tabs. Monitoring owns
 *  the time-series; Activity is the change log. */
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
  const detail = useLive<IsvcDetail>(() => api.inferenceService(sel), {
    kinds: ["InferenceService"],
    throttleMs: 4_000,
  });
  const cfg = useLive(api.config, { kinds: [] });
  const feed = useEventFeed(200);
  const d = detail.data;

  // This deployment's serving metrics (from the live snapshot; SSE-pushed).
  const metricsOn = !!cfg.data?.metrics;
  const metrics = useMetrics(metricsOn, cfg.data?.metricsRefreshSecs);
  const namespace = d?.namespace ?? sel.namespace;
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

  const status = !d
    ? null
    : d.ready === true
      ? { label: "Ready", tone: "green" as const }
      : d.ready === false
        ? { label: "Not ready", tone: "red" as const }
        : { label: "Unknown", tone: "slate" as const };

  const meta = [sel.cluster, d?.namespace, d?.deploymentMode, d?.age].filter(Boolean).join(" · ");

  return (
    <section className="panel active">
      <button className="backlink" onClick={onBack}>
        ‹ Deployments
      </button>
      <div className="detail-head">
        <h1 className="detail-title mono">{sel.name}</h1>
        {status ? <Pill tone={status.tone} label={status.label} /> : null}
        <span className="detail-meta">{meta}</span>
      </div>

      {d?.url ? (
        <div className="detail-endpoint">
          <span className="eyebrow">Endpoint</span>
          <Copyable text={d.url} />
        </div>
      ) : null}

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
// Overview — config, identity, and current state (components, conditions,
// rollout, placement). All the static/state facts in one place; the headline
// strip links out to Monitoring for the charts.
// ---------------------------------------------------------------------------
function OverviewPane({
  d,
  m,
  sel,
  metricsOn,
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

      <div className="detail-grid2">
        <Card title="Details">
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
            ]}
          />
        </Card>

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

      <div className="detail-grid2 section-gap">
        <Card title="Components" sub={`${ready} ready`}>
          {components.length ? (
            <div className="urows">
              {components.map((c) => {
                const cur = c.currentReplicas ?? 0;
                const des = c.desiredReplicas ?? 0;
                const pct = des > 0 ? Math.round((cur / des) * 100) : cur > 0 ? 100 : 0;
                const tone = c.ready ? "green" : cur < des ? "amber" : "";
                const extra = [c.revision, c.rolloutPhase].filter(Boolean).join(" · ");
                return (
                  <UtilRow
                    key={c.name}
                    name={
                      <>
                        {c.name}
                        {extra ? <span className="muted"> · {extra}</span> : null}
                      </>
                    }
                    pct={pct}
                    tone={tone}
                    value={`${cur}/${des}`}
                  />
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

        <Card title="Invoke & test" sub="OpenAI-compatible">
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

      <div className="detail-grid2 section-gap">
        <Card title="Conditions">
          {d.conditions && d.conditions.length ? (
            d.conditions.map((c, i) => (
              <div className="cond-row" key={i}>
                <Dot
                  tone={c.status === "True" ? "green" : c.status === "False" ? "red" : "slate"}
                />
                <span className="mono">{c.type}</span>
                <span className="cond-rs">{c.reason || c.status}</span>
              </div>
            ))
          ) : (
            <div className="muted">No conditions reported.</div>
          )}
        </Card>

        {d.placement?.cluster || standby.length ? (
          <Card title="Placement" sub="where it runs">
            {d.placement?.cluster ? (
              <div className="place-row">
                <Dot tone="green" />
                <span className="mono">{d.placement.cluster}</span>
                <span className="muted">active</span>
              </div>
            ) : null}
            {standby.map((c, i) => (
              <div className="place-row" key={i}>
                <Dot tone="slate" />
                <span className="mono">{c.cluster}</span>
                <span className="muted">standby</span>
              </div>
            ))}
          </Card>
        ) : null}
      </div>

      {d.canary ? (
        <Card title="Rollout" sub={`canary · step ${d.canary.currentStep}`} className="section-gap">
          <Gauge pct={d.canary.trafficWeight} />
          <p className="panel-note">{d.canary.trafficWeight}% of traffic on the new revision.</p>
        </Card>
      ) : null}
    </>
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
