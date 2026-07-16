import { useState } from "react";
import type { ReactNode } from "react";

import { api } from "../api";
import type { EventItem, IsvcDetail } from "../api";
import { Pill } from "../components";
import { Copyable, Facts } from "../drawer";
import { useEventFeed, useLive, useMetrics } from "../live";
import { replicaRollup } from "../map";
import { isvcMetrics, seriesOf } from "../metrics";
import type { IsvcMetrics } from "../metrics";
import type { Selected } from "../types";
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

export function DeploymentDetail({
  sel,
  onBack,
  openModel,
  openRuntime,
}: {
  sel: Selected;
  onBack: () => void;
  openModel: (sel: Selected) => void;
  openRuntime: (sel: Selected) => void;
}) {
  const [tab, setTab] = useState<TabId>("overview");
  const detail = useLive<IsvcDetail>(() => api.inferenceService(sel.cluster, sel.name), {
    kinds: ["InferenceService"],
    throttleMs: 4_000,
  });
  const cfg = useLive(api.config, { kinds: [] });
  const feed = useEventFeed(200);
  const d = detail.data;

  // This deployment's serving metrics (from the live snapshot; SSE-pushed).
  const metricsOn = !!cfg.data?.metrics;
  const metrics = useMetrics(metricsOn);
  const m = isvcMetrics(metrics.data, sel.cluster, sel.name);

  const activity = feed.events.filter(
    (e) => e.kind === "InferenceService" && e.name === sel.name && e.cluster === sel.cluster,
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
            <Monitoring m={m} metricsOn={metricsOn} policy={cfg.data?.dashboardPolicy} />
          ) : null}
          {tab === "activity" ? <ActivityPane activity={activity} /> : null}
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
                d.model ? (
                  <button
                    className="linkbtn mono"
                    onClick={() => openModel({ cluster: sel.cluster, name: d.model! })}
                  >
                    {d.model}
                  </button>
                ) : null,
              ],
              [
                "Runtime",
                d.runtime ? (
                  <button
                    className="linkbtn mono"
                    onClick={() => openRuntime({ cluster: sel.cluster, name: d.runtime! })}
                  >
                    {d.runtime}
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

        <Card title="Invoke & test" sub="OpenAI-compatible">
          <ChatBox
            cluster={sel.cluster}
            name={sel.name}
            endpoint={d.url}
            defaultModel={d.model ?? ""}
            enabled={chat}
            timeoutSecs={chatTimeoutSecs}
            clientGraceSecs={chatClientGraceSecs}
          />
        </Card>
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
      </div>

      {d.canary ? (
        <Card title="Rollout" sub={`canary · step ${d.canary.currentStep}`} className="section-gap">
          <Gauge pct={d.canary.trafficWeight} />
          <p className="panel-note">{d.canary.trafficWeight}% of traffic on the new revision.</p>
        </Card>
      ) : null}

      {d.placement?.cluster || standby.length ? (
        <Card title="Placement" sub="where it runs" className="section-gap">
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
    </>
  );
}

// ---------------------------------------------------------------------------
// Activity — recent changes for this deployment (from the shared event feed).
// ---------------------------------------------------------------------------
function ActivityPane({ activity }: { activity: EventItem[] }) {
  if (!activity.length) {
    return <div className="muted attn-none">No recent activity.</div>;
  }
  return (
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
  );
}
