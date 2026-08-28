// The deployment detail's pods card: per-component rollup, problem-first table,
// per-pod drill-down, and entry points into the log viewers.
import { Fragment, useEffect, useState } from "react";

import { api } from "../api";
import type { IsvcPod, PodInspect } from "../api";
import { CopyCmd, SearchInput } from "../components";
import { useLive } from "../live";
import { isvcSelected } from "../resource";
import type { Selected } from "../resource";
import { matches, num as n } from "../util";
import { Card, Dot, FilterChip } from "../viz";
import { LogBox, LogStreamBox } from "./PodLogs";

/** Page size for the pods table — big deployments are navigated by the filters,
 *  never by scrolling thousands of rows. */
const POD_PAGE = 50;

const podProblematic = (p: IsvcPod) => !p.ready || p.reason != null || p.restarts > 0;

/** The deployment's pods as a diagnosis view, not an inventory dump: healthy
 *  pods collapse into per-component counts, and the table shows only the pods
 *  needing attention unless the operator widens the filters. */
export function PodsCard({
  sel,
  podInspect,
  podLogStream,
  openNode,
}: {
  sel: Selected;
  podInspect: boolean;
  podLogStream: boolean;
  /** Opens the Capacity node explorer filtered to the given node. */
  openNode: (node: string) => void;
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
  const deployRef = isvcSelected(sel.cluster, sel.name, pods.data?.namespace ?? sel.namespace);

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
        <SearchInput
          ariaLabel="Filter pods"
          placeholder="Filter by pod name, node, or reason…"
          value={query}
          onChange={(value) => {
            setQuery(value);
            setPage(0);
          }}
        />
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
        <LogStreamBox streamRef={deployRef} scope={stream} onClose={() => setStream(null)} />
      ) : scoped.length ? (
        <>
          <div className="pods-scroll">
            <table className="place-tbl pods-tbl">
              <thead>
                <tr>
                  <th>Pod</th>
                  <th>Component</th>
                  <th>Phase</th>
                  <th className="tnum">Ready</th>
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
                    podRef={deployRef}
                    podInspect={podInspect}
                    onStream={podLogStream ? (pod) => setStream({ pod }) : undefined}
                    openNode={openNode}
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
  openNode,
  expanded,
  onToggle,
}: {
  p: IsvcPod;
  podRef: Selected;
  podInspect: boolean;
  onStream?: (pod: string) => void;
  openNode: (node: string) => void;
  expanded: boolean;
  onToggle: () => void;
}) {
  const bad = !p.ready || p.reason != null;
  const phaseTone = p.phase === "Running" && p.ready ? "green" : bad ? "red" : "amber";
  const readyContainers = p.containers?.filter((c) => c.ready).length;
  const node = p.node;
  return (
    <Fragment>
      <tr className={`link${bad ? " pod-bad" : ""}`} onClick={onToggle}>
        <td className="mono pod-name" title={p.name}>
          <button
            type="button"
            className="row-open"
            aria-expanded={expanded}
            onClick={(event) => {
              event.stopPropagation();
              onToggle();
            }}
          >
            {p.name}
          </button>
        </td>
        <td>{p.component ?? "—"}</td>
        <td>
          <Dot tone={phaseTone} /> {p.phase ?? "—"}
        </td>
        <td className={`tnum${!p.ready ? " txt-bad" : ""}`}>
          {p.containers?.length ? `${readyContainers}/${p.containers.length}` : p.ready ? "✓" : "✗"}
        </td>
        <td className={`tnum${p.restarts > 0 ? " txt-bad" : ""}`}>{p.restarts || "—"}</td>
        <td className={p.reason ? "txt-bad" : "muted"}>
          {p.reason ?? (p.lastRestart ? `last: ${p.lastRestart}` : "—")}
        </td>
        <td className="mono pod-node" title={node ? `${node} · open in Capacity` : undefined}>
          {node ? (
            <button
              type="button"
              className="row-open"
              onClick={(event) => {
                event.stopPropagation();
                openNode(node);
              }}
            >
              {node}
            </button>
          ) : (
            <span className="muted">unscheduled</span>
          )}
        </td>
        <td className="tnum muted">{p.age ?? "—"}</td>
      </tr>
      {expanded ? (
        <tr className="pod-expand">
          <td colSpan={8}>
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
