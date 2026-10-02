#!/usr/bin/env python3
"""Generate the OME Grafana dashboards under `dashboards/`.

The dashboards are generated rather than hand-edited so they share one panel
vocabulary — units, threshold steps, legend behavior — across every dashboard.

Regenerate after editing this file:

    python3 grafana/gen_dashboards.py

"""

import json
import pathlib

OUT = pathlib.Path(__file__).parent / "dashboards"

# Control-plane pods carry namespace/pod only; they are selected by pod name.
CP = 'cluster="$cluster",pod=~"ome-controller-manager.*",pod=~"$pod"'

# OME's own long-running components, by pod-name prefix. `redundant` marks a
# leader-elected or multi-replica component where the meaningful availability
# question is "was at least one replica up", as opposed to a DaemonSet where
# every node's agent counts independently.
COMPONENTS = [
    ("Controller manager", "ome-controller-manager.*", True),
    ("Scheduler", "ome-scheduler.*", True),
    ("Model agent", "ome-model-agent-daemonset.*", False),
]

DS = {"type": "prometheus", "uid": "${datasource}"}

# Threshold steps, mirroring MUSE's dashboardPolicy. `None` is the base step.
T_NONE = [(None, "text")]


def steps(pairs):
    return [{"color": c, "value": v} for v, c in pairs]


def target(expr, legend=None, instant=False, table=False):
    t = {"datasource": DS, "expr": expr, "refId": chr(65 + target.n % 26)}
    target.n += 1
    if legend:
        t["legendFormat"] = legend
    if instant:
        t["instant"] = True
    if table:
        # Emit label columns rather than a single time series, so a table
        # panel can list the offending objects by name.
        t["format"] = "table"
    return t


target.n = 0


def up_by_version(up_selector, pod_info_selector=None):
    """Count healthy scrape targets by the image tag KSM reports for each pod."""
    if pod_info_selector is None:
        pod_info_selector = up_selector
    return (
        "sum by (version) ("
        f"up{{{up_selector}}} "
        "* on (cluster, namespace, pod) group_left(version) "
        "max by (cluster, namespace, pod, version) ("
        f'label_replace(kube_pod_container_info{{{pod_info_selector}}}, '
        '"version", "$1", "image", ".*:(.*)")))'
    )


def panel(kind, title, targets, gp, unit=None, thresholds=T_NONE, desc=None,
          decimals=None, minv=None, maxv=None, stacked=False):
    """One panel. `gp` is (x, y, w, h) on Grafana's 24-column grid."""
    x, y, w, h = gp
    custom = {}
    if kind == "timeseries":
        custom = {
            # Thin marks, no point clutter, recessive fill.
            "lineWidth": 2,
            "fillOpacity": 8,
            "showPoints": "never",
            "spanNulls": True,
        }
        if stacked:
            custom["stacking"] = {"mode": "normal", "group": "A"}
    field = {
        "defaults": {
            "custom": custom,
            "color": {"mode": "thresholds" if kind == "stat" else "palette-classic"},
            "thresholds": {"mode": "absolute", "steps": steps(thresholds)},
        },
        "overrides": [],
    }
    if unit:
        field["defaults"]["unit"] = unit
    if decimals is not None:
        field["defaults"]["decimals"] = decimals
    if minv is not None:
        field["defaults"]["min"] = minv
    if maxv is not None:
        field["defaults"]["max"] = maxv
    p = {
        "type": kind,
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": field,
    }
    if desc:
        p["description"] = desc
    if kind == "stat":
        p["options"] = {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "textMode": "auto",
            "graphMode": "area",
            "colorMode": "value",
        }
    elif kind == "table":
        # Drop the columns an instant table query always carries and never
        # needs: the shared timestamp and the constant sample value.
        p["options"] = {"showHeader": True}
        p["transformations"] = [
            {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True}}}
        ]
    else:
        # A legend is always present for multi-series; single-series panels are
        # named by their title, so the list stays off.
        multi = len(targets) > 1 or any("legendFormat" in t for t in targets)
        p["options"] = {
            "legend": {
                "displayMode": "list" if multi else "hidden",
                "placement": "bottom",
                "showLegend": multi,
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        }
    return p


def row(title, y):
    return {
        "type": "row",
        "title": title,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "collapsed": False,
        "panels": [],
    }


def collapse_after(panels, expanded):
    """Fold each row's panels inside it, leaving the first `expanded` rows open.

    Grafana only honours a shut row when its children are nested in the row's
    own panels list. Setting collapsed on a flat list draws the row shut and
    leaves every panel on the page underneath it, still querying.
    """
    out, current = [], None
    rows = 0
    for panel_ in panels:
        if panel_["type"] == "row":
            rows += 1
            current = panel_
            current["collapsed"] = rows > expanded
            out.append(current)
        elif current is not None and current["collapsed"]:
            current["panels"].append(panel_)
        else:
            out.append(panel_)
    return out


def var_ds():
    return {
        "type": "datasource",
        "name": "datasource",
        "label": "Data source",
        "query": "prometheus",
        "current": {},
        "refresh": 1,
    }


def var_query(name, label, query, multi=False, include_all=False, all_value=None):
    v = {
        "type": "query",
        "name": name,
        "label": label,
        "datasource": DS,
        "query": {"query": query, "refId": name},
        "definition": query,
        "refresh": 2,  # on time-range change
        "sort": 1,
        "multi": multi,
        "includeAll": include_all,
        "current": {},
    }
    if all_value:
        v["allValue"] = all_value
    return v


def dashboard(uid, title, description, variables, panels, tags):
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": tags,
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "time": {"from": "now-6h", "to": "now"},
        "refresh": "1m",
        "timezone": "",
        "templating": {"list": variables},
        "panels": panels,
    }


# ---------------------------------------------------------------------------
# 1. Control plane — the time-series companion to MUSE's System panel
#    (OEP-0021.2). Standard controller-runtime + process metrics, scraped from
#    OME control-plane pods via the prometheus.io/scrape annotation.
# ---------------------------------------------------------------------------
def control_plane():
    p, y = [], 0
    job = CP

    p.append(row("Availability", y)); y += 1
    p.append(panel("stat", "Targets up", [target(f'sum(up{{{job}}})')], (0, y, 6, 4),
                   thresholds=[(None, "red"), (1, "green")],
                   desc="Scrape targets responding. 0 means the control plane is "
                        "unreachable or not scraped."))
    p.append(panel("stat", "Reconcile rate",
                   [target(f'sum(rate(controller_runtime_reconcile_total{{{job}}}[5m]))')],
                   (6, y, 6, 4), unit="reqps"))
    p.append(panel("stat", "Reconcile errors",
                   [target(f'sum(rate(controller_runtime_reconcile_errors_total{{{job}}}[5m]))')],
                   (12, y, 6, 4), unit="reqps", thresholds=[(None, "green"), (0.01, "yellow"), (0.1, "red")]))
    p.append(panel("stat", "Max queue depth",
                   [target(f'max(workqueue_depth{{{job}}})')], (18, y, 6, 4),
                   thresholds=[(None, "green"), (50, "yellow"), (200, "red")],
                   desc="A queue that grows without draining means reconciles are "
                        "falling behind."))
    y += 4

    p.append(panel(
        "timeseries", "Controller up by version",
        [target(up_by_version(job), "{{version}}")],
        (0, y, 24, 7), minv=0,
        desc="Scrape targets responding, grouped by the image tag reported by "
             "kube-state-metrics. During a rollout, old and new versions appear "
             "as separate series. Follows the Cluster and Pod pickers. Requires "
             "kube_pod_container_info."))
    y += 7

    p.append(row("Reconcile", y)); y += 1
    p.append(panel("timeseries", "Reconciles by controller",
                   [target(f'sum by (controller) (rate(controller_runtime_reconcile_total{{{job}}}[5m]))',
                           "{{controller}}")],
                   (0, y, 12, 8), unit="reqps"))
    p.append(panel("timeseries", "Reconcile errors by controller",
                   [target(f'sum by (controller) (rate(controller_runtime_reconcile_errors_total{{{job}}}[5m]))',
                           "{{controller}}")],
                   (12, y, 12, 8), unit="reqps"))
    p.append(panel("timeseries", "Reconcile duration p99", [target(
        f'histogram_quantile(0.99, sum by (controller, le) (rate(controller_runtime_reconcile_time_seconds_bucket{{{job}}}[5m])))',
        "{{controller}}")], (0, y + 8, 12, 8), unit="s"))
    p.append(panel("timeseries", "Work queue depth",
                   [target(f'sum by (name) (workqueue_depth{{{job}}})', "{{name}}")],
                   (12, y + 8, 12, 8)))
    y += 16

    p.append(row("Admission webhooks", y)); y += 1
    p.append(panel("timeseries", "Webhook request rate",
                   [target(f'sum by (webhook) (rate(controller_runtime_webhook_requests_total{{{job}}}[5m]))',
                           "{{webhook}}")], (0, y, 12, 7), unit="reqps",
                   desc="OME serves its admission webhooks from the controller-manager, "
                        "so webhook failures and controller health share a fate."))
    p.append(panel("timeseries", "Webhook latency p99", [target(
        f'histogram_quantile(0.99, sum by (webhook, le) (rate(controller_runtime_webhook_latency_seconds_bucket{{{job}}}[5m])))',
        "{{webhook}}")], (12, y, 12, 7), unit="s"))
    y += 7

    p.append(row("Process", y)); y += 1
    p.append(panel("timeseries", "CPU",
                   [target(f'sum by (pod) (rate(process_cpu_seconds_total{{{job}}}[5m]))', "{{pod}}")],
                   (0, y, 8, 7), unit="none", desc="Cores."))
    p.append(panel("timeseries", "Resident memory",
                   [target(f'sum by (pod) (process_resident_memory_bytes{{{job}}})', "{{pod}}")],
                   (8, y, 8, 7), unit="bytes"))
    p.append(panel("timeseries", "Goroutines",
                   [target(f'sum by (pod) (go_goroutines{{{job}}})', "{{pod}}")],
                   (16, y, 8, 7), desc="A monotonic climb is the classic leak signature."))
    y += 7

    # OME's own instrumentation (the `ome_*` family), exported by the controller
    # alongside the generic controller-runtime metrics above. These describe what
    # OME is managing, not just how hard the reconciler is working.
    p.append(row("OMENative", y)); y += 1
    p.append(panel("timeseries", "Per-revision Services by component", [target(
        f'sum by (component) (ome_omenative_per_revision_service_total{{{job}}})',
        "{{component}}")], (0, y, 8, 7),
        desc="Kubernetes Services OMENative currently generates per revision, split by "
             "component (engine / router / decoder). Step changes track rollouts."))
    p.append(panel("timeseries", "Pod create → ready", [
        target(f'histogram_quantile(0.50, sum by (le) (rate(ome_omenative_pod_create_to_ready_seconds_bucket{{{job}}}[5m])))', "p50"),
        target(f'histogram_quantile(0.99, sum by (le) (rate(ome_omenative_pod_create_to_ready_seconds_bucket{{{job}}}[5m])))', "p99"),
    ], (8, y, 8, 7), unit="s",
        desc="How long a serving pod takes to become ready — the rollout-speed SLO. "
             "Regressions here are usually image pull or model localization."))
    p.append(panel("timeseries", "Migrations & gate bypasses", [
        target(f'sum(rate(ome_omenative_auto_migrations_total{{{job}}}[5m]))', "Auto migrations"),
        target(f'sum(rate(ome_omenative_ratio_gate_bypassed_total{{{job}}}[5m]))', "Ratio-gate bypassed"),
    ], (16, y, 8, 7), unit="reqps",
        desc="A bypassed RatioBalanced gate means a rollout proceeded without the "
             "ratio being satisfied — rare, and worth knowing about."))
    y += 7

    # kube-state-metrics series describe OME's managed objects, so they carry
    # the CR's identity and the KSM pod's -- scoping them by the
    # controller-manager pod name matches nothing.
    p.append(row("Managed resources — from kube-state-metrics", y)); y += 1
    p.append(panel("timeseries", "InferenceService transition status", [target(
        f'sum by (status) (ome_inferenceservice_transition_status{{cluster="$cluster"}})',
        "{{status}}")],
        (0, y, 8, 7), desc="ISVCs by transition state. A state that stops draining is a "
                           "stuck rollout. Needs the OME CustomResourceState config "
                           "applied to kube-state-metrics."))
    p.append(panel("timeseries", "BaseModel state", [
        target(f'sum by (state) (ome_basemodel_state{{cluster="$cluster"}})',
               "namespaced/{{state}}"),
        target(f'sum by (state) (ome_clusterbasemodel_state{{cluster="$cluster"}})',
               "cluster/{{state}}"),
    ], (8, y, 8, 7),
        desc="Model lifecycle states. Models parked in an importing/failed state are "
             "why a deployment cannot start."))
    p.append(panel("timeseries", "Status update rate", [target(
        f'sum(rate(ome_isvc_status_update_total{{{job}}}[5m]))')], (16, y, 8, 7),
        unit="reqps", desc="ISVC status writes/s. A high steady rate with no spec "
                           "changes is status churn against the apiserver."))
    y += 7

    p.append(row("Canary sampler", y)); y += 1
    p.append(panel("timeseries", "Sampler inflight & queue", [
        target(f'sum(ome_canary_sampler_inflight{{{job}}})', "Inflight"),
        target(f'sum(ome_canary_sampler_queue_depth{{{job}}})', "Queue depth"),
    ], (0, y, 12, 7)))
    p.append(panel("timeseries", "Sampler starvation", [target(
        f'sum(rate(ome_canary_sampler_starved_total{{{job}}}[5m]))')], (12, y, 12, 7),
        unit="reqps", thresholds=[(None, "green"), (0.01, "yellow")],
        desc="Non-zero means the canary sampler could not get traffic to evaluate."))
    y += 7

    # TPU slice provisioning is opt-in (ome.controller.tpuSliceProvisioning in
    # the chart), so most clusters have no series for this row. It comes last
    # and starts folded, and a folded row's panels do not query until opened.
    rows_open = sum(q["type"] == "row" for q in p)
    p.append(row("TPU slice provisioning", y)); y += 1
    # The gauge exports a state only while some slice is in it. Filling the
    # gaps with 0 keeps the stack's height the number of slices that exist.
    p.append(panel("timeseries", "Slices by state", [
        target(f'sum(max by (slice_type, topology) '
               f'(ome_tpu_slices{{{job},state="{state}"}})) or vector(0)', state)
        for state in ("ready", "pending", "terminating", "orphaned")
    ], (0, y, 12, 8), minv=0, stacked=True,
        desc="TPU slices the controller provisioned that exist now. pending includes "
             "a slice the provider failed to provision. orphaned means the slice's "
             "InferenceReplica is gone, so no reconcile will release it: it holds TPU "
             "capacity until deleted by hand. Any orphaned slice is a leak. Only the "
             "leader reports; max by (slice_type, topology) collapses the per-pod series "
             "so a leader change does not inflate the count."))
    p.append(panel("timeseries", "Slice provisioning p90", [target(
        f'histogram_quantile(0.90, sum by (slice_type, topology, le) '
        f'(rate(ome_tpu_slice_provision_duration_seconds_bucket{{{job}}}[1h])))',
        "{{slice_type}} {{topology}}")], (12, y, 12, 8), unit="s",
        desc="From the controller creating a slice to the slice first reporting a "
             "ready state, over the trailing hour. An opted-in pod is not created "
             "until its slice is ready, so this adds directly to a scale-up. Only "
             "slices the current leader created are timed."))
    p.append(panel("timeseries", "Slices created and released per hour", [
        target(f'sum(increase(ome_tpu_slice_created_total{{{job}}}[1h]))', "created"),
        target(f'sum(increase(ome_tpu_slice_released_total{{{job}}}[1h]))', "released"),
        target(f'sum(increase(ome_tpu_slice_release_deferred_total{{{job}}}[1h]))', "release deferred"),
        target(f'sum(increase(ome_tpu_slice_lost_total{{{job}}}[1h]))', "lost"),
    ], (0, y + 8, 12, 7), minv=0,
        desc="Over the trailing hour, counted by the leader. released counts a slice "
             "when the API server removes it, whoever deleted it. release deferred "
             "counts each attempt to release a slice kept back because pods of "
             "another workload hold chips on its hosts. lost counts a slice deleted "
             "from outside while pods ran on it; their Instance is rebuilt on a new "
             "slice."))
    p.append(panel("timeseries", "Slice create failures per hour", [target(
        f'sum by (reason) (increase(ome_tpu_slice_create_failures_total{{{job}}}[1h]))',
        "{{reason}}")], (12, y + 8, 12, 7), minv=0,
        desc="Failed slice creates over the trailing hour, by the API server's status "
             "reason. Forbidden, Invalid and OwnershipConflict need an operator; "
             "InternalError, TooManyRequests and Unknown usually clear on retry. Every "
             "retry counts, so a create that keeps failing keeps climbing. Empty until "
             "a create fails. After a controller restart, the first failure of each "
             "reason is not counted: it only starts that reason's series."))

    return dashboard(
        "ome-control-plane", "OME · Control plane",
        "Time-series health of OME's own control plane — the trend companion to MUSE's "
        "System panel (OEP-0021.2), which shows current desired-vs-ready state. Requires "
        "the control-plane pods to be scraped (prometheus.io/scrape annotation).",
        [var_ds(),
         var_query("cluster", "Cluster",
                   "label_values(ome_isvc_status_update_total, cluster)"),
         var_query("pod", "Pod",
                   'label_values(ome_isvc_status_update_total{cluster="$cluster"}, pod)',
                   multi=True, include_all=True, all_value=".*")],
        collapse_after(p, expanded=rows_open), ["ome", "control-plane"])


# ---------------------------------------------------------------------------
# 2. ome-scheduler (OEP-0022) — topology-aware gang placement. It is built on the
#    upstream Kubernetes scheduler framework, so it exports the standard
#    `scheduler_*` metric set; the gang/domain metrics OEP-0022 describes are
#    OME-specific and are wired in once their names are settled (see the note
#    panel at the bottom of the dashboard).
# ---------------------------------------------------------------------------
def scheduler():
    p, y = [], 0
    s = 'cluster="$cluster",pod=~"ome-scheduler.*",pod=~"$pod"'

    # Pod-level health comes from kube-state-metrics, which observes the pods from
    # outside. It is available even when the scheduler's own /metrics endpoint is
    # not scraped — so this row answers "is it running?" while the scheduler_*
    # rows below answer "is it scheduling?".
    ksm = 'cluster="$cluster",pod=~"ome-scheduler.*"'
    p.append(row("Pod health (kube-state-metrics)", y)); y += 1
    p.append(panel("stat", "Running pods",
                   [target(f'sum(kube_pod_status_phase{{{ksm},phase="Running"}})')],
                   (0, y, 6, 4), thresholds=[(None, "red"), (1, "green")]))
    p.append(panel("stat", "Ready pods",
                   [target(f'sum(kube_pod_status_ready{{{ksm},condition="true"}})')],
                   (6, y, 6, 4), thresholds=[(None, "red"), (1, "green")]))
    p.append(panel("stat", "Not ready",
                   [target(f'sum(kube_pod_status_ready{{{ksm},condition="false"}})')],
                   (12, y, 6, 4), thresholds=[(None, "green"), (1, "red")]))
    p.append(panel("stat", "Zones covered", [target(
        f'count(count by (topology_kubernetes_io_zone) '
        f'(kube_pod_status_ready{{{ksm},condition="true"}}))')], (18, y, 6, 4),
        thresholds=[(None, "red"), (2, "green")],
        desc="Distinct zones with a ready replica. 1 means an outage in that zone "
             "takes the scheduler with it."))
    y += 4
    p.append(panel("timeseries", "Pods by phase",
                   [target(f'sum by (phase) (kube_pod_status_phase{{{ksm}}})', "{{phase}}")],
                   (0, y, 12, 7), stacked=True))
    p.append(panel("timeseries", "Ready replicas by zone", [target(
        f'sum by (topology_kubernetes_io_zone) (kube_pod_status_ready{{{ksm},condition="true"}})',
        "{{topology_kubernetes_io_zone}}")], (12, y, 12, 7)))
    y += 7

    p.append(row("Scheduling activity — requires the scheduler's own /metrics", y)); y += 1
    p.append(panel("stat", "Targets up", [target(f'sum(up{{{s}}})')], (0, y, 6, 4),
                   thresholds=[(None, "red"), (1, "green")],
                   desc="Scheduler replicas being scraped for their OWN metrics. 0 while "
                        "the Pod-health row above shows Running means the pods are fine "
                        "but nothing is scraping their /metrics endpoint."))
    p.append(panel("stat", "Leaders", [target(f'sum(leader_election_master_status{{{s}}})')],
                   (6, y, 6, 4), thresholds=[(None, "red"), (1, "green"), (2, "red")],
                   desc="Exactly 1 is correct. 0 = nothing is scheduling; >1 = split brain, "
                        "which for a scheduler means two brains binding the same nodes."))
    p.append(panel("stat", "Scheduling rate",
                   [target(f'sum(rate(scheduler_schedule_attempts_total{{{s}}}[5m]))')],
                   (12, y, 6, 4), unit="reqps"))
    p.append(panel("stat", "Pending pods", [target(f'sum(scheduler_pending_pods{{{s}}})')],
                   (18, y, 6, 4), thresholds=[(None, "green"), (20, "yellow"), (100, "red")],
                   desc="Pods the scheduler has not placed. A gang that cannot fit parks here."))
    y += 4

    p.append(panel(
        "timeseries", "Scheduler up by version",
        [target(up_by_version(s), "{{version}}")],
        (0, y, 24, 7), minv=0,
        desc="Scrape targets responding, grouped by the image tag reported by "
             "kube-state-metrics. During a rollout, old and new versions appear "
             "as separate series. Follows the Cluster and Pod pickers. Requires "
             "kube_pod_container_info."))
    y += 7

    p.append(row("Outcomes — is work getting placed?", y)); y += 1
    p.append(panel("timeseries", "Attempts by result",
                   [target(f'sum by (result) (rate(scheduler_schedule_attempts_total{{{s}}}[5m]))',
                           "{{result}}")], (0, y, 12, 8), unit="reqps", stacked=True,
                   desc="result = scheduled | unschedulable | error. The 'unschedulable' "
                        "series also counts benign gang admission yields, so a steady "
                        "share of it is normal on a cluster creating gangs. Genuine "
                        "starvation is no_fit in the Gang admission row."))
    p.append(panel("timeseries", "Pending pods by queue",
                   [target(f'sum by (queue) (scheduler_pending_pods{{{s}}})', "{{queue}}")],
                   (12, y, 12, 8), stacked=True,
                   desc="active = ready to try; backoff = waiting after a failure; "
                        "unschedulable = nothing fits; gated = held by a scheduling gate "
                        "(how gang admission parks pods)."))
    p.append(panel("timeseries", "Unschedulable rate", [target(
        f'sum(rate(scheduler_schedule_attempts_total{{{s},result="unschedulable"}}[5m]))')],
        (0, y + 8, 12, 7), unit="reqps",
        desc="Non-zero whenever gangs are being created: each two-phase admission yield "
             "lands here — a leader that arrives before its worker returns Unschedulable "
             "and rebinds milliseconds later. This does NOT mean gangs are failing to "
             "find a domain. Compare against no_fit in the Gang admission row before "
             "reading it as capacity starvation."))
    p.append(panel("timeseries", "Attempts per pod (p99)", [target(
        f'histogram_quantile(0.99, sum by (le) (rate(scheduler_pod_scheduling_attempts_bucket{{{s}}}[5m])))')],
        (12, y + 8, 12, 7),
        desc="How many tries a pod needs before it lands. Climbing = churn/contention."))
    y += 15

    p.append(row("Gang admission — yielding, or actually starving?", y)); y += 1
    p.append(panel("timeseries", "Domain placement decisions", [target(
        f'sum by (result) (rate(ome_scheduler_gang_pin_total{{{s}}}[5m]))', "{{result}}")],
        (0, y, 12, 7), unit="reqps", stacked=True,
        desc="pinned/adopted are successful placements. no_fit means the planner "
             "examined every domain and none could hold the remaining members. "
             "stale_replan/topology_replan are re-plans of an existing commitment."))
    p.append(panel("timeseries", "Starvation rate (no_fit)", [target(
        f'sum(rate(ome_scheduler_gang_pin_total{{{s},result="no_fit"}}[5m]))')],
        (12, y, 12, 7), unit="reqps",
        desc="Alert on this, not on 'Unschedulable rate'. Sustained non-zero means "
             "gangs genuinely cannot find a domain."))
    p.append(panel("timeseries", "Pinned groups", [target(
        f'sum(ome_scheduler_pinned_groups{{{s}}})')], (0, y + 7, 12, 7),
        desc="Gangs holding a domain capacity reservation. A floor that climbs and "
             "never returns means pins are leaking."))
    p.append(panel("timeseries", "Permit gate, activations, unwinds", [
        target(f'sum by (result) (rate(ome_scheduler_gang_gate_total{{{s}}}[5m]))',
               "gate {{result}}"),
        target(f'sum by (trigger) (rate(ome_scheduler_gang_activation_total{{{s}}}[5m]))',
               "activation {{trigger}}"),
        target(f'sum(rate(ome_scheduler_gang_unwind_total{{{s}}}[5m]))', "unwind"),
    ], (12, y + 7, 12, 7), unit="reqps",
        desc="gate wait/admit should stay balanced. activations are the retries that "
             "clear a yield. unwind counts gangs torn down after a member failed to "
             "schedule (gate timeout or bind failure)."))
    y += 14

    p.append(row("Latency — how fast, and which hook is slow", y)); y += 1
    p.append(panel("timeseries", "End-to-end placement latency", [
        target(f'histogram_quantile(0.50, sum by (le) (rate(scheduler_pod_scheduling_duration_seconds_bucket{{{s}}}[5m])))', "p50"),
        target(f'histogram_quantile(0.99, sum by (le) (rate(scheduler_pod_scheduling_duration_seconds_bucket{{{s}}}[5m])))', "p99"),
    ], (0, y, 12, 8), unit="s",
        desc="Queueing + attempts, i.e. what a user waits. For a gang this includes "
             "time parked waiting for the whole group to be placeable."))
    p.append(panel("timeseries", "Attempt duration p99", [target(
        f'histogram_quantile(0.99, sum by (le) (rate(scheduler_scheduling_attempt_duration_seconds_bucket{{{s}}}[5m])))')],
        (12, y, 12, 8), unit="s", desc="One scheduling cycle, excluding queue wait."))
    p.append(panel("timeseries", "Framework extension points p99", [target(
        f'histogram_quantile(0.99, sum by (extension_point, le) (rate(scheduler_framework_extension_point_duration_seconds_bucket{{{s}}}[5m])))',
        "{{extension_point}}")], (0, y + 8, 12, 8), unit="s",
        desc="Per-hook latency (Filter, Score, Reserve, Permit, Bind…). OEP-0022's "
             "performance thresholds are set on exactly these."))
    p.append(panel("timeseries", "Plugin execution p99", [target(
        f'histogram_quantile(0.99, sum by (plugin, le) (rate(scheduler_plugin_execution_duration_seconds_bucket{{{s}}}[5m])))',
        "{{plugin}}")], (12, y + 8, 12, 8), unit="s",
        desc="Isolates OME's gang/topology plugin cost from the stock plugins."))
    y += 16

    p.append(row("Preemption & queue churn", y)); y += 1
    p.append(panel("timeseries", "Preemption attempts", [target(
        f'sum(rate(scheduler_preemption_attempts_total{{{s}}}[5m]))')], (0, y, 8, 7), unit="reqps"))
    p.append(panel("timeseries", "Preemption victims p99", [target(
        f'histogram_quantile(0.99, sum by (le) (rate(scheduler_preemption_victims_bucket{{{s}}}[5m])))')],
        (8, y, 8, 7), desc="Pods evicted per preemption — a large gang preempting is "
                           "visible here as a spike."))
    p.append(panel("timeseries", "Incoming pods by event", [target(
        f'topk(10, sum by (event) (rate(scheduler_queue_incoming_pods_total{{{s}}}[5m])))',
        "{{event}}")], (16, y, 8, 7), unit="reqps",
        desc="What is waking the scheduler. A hot requeue loop shows up as one "
             "dominant event."))
    y += 7

    p.append(row("Process", y)); y += 1
    p.append(panel("timeseries", "CPU",
                   [target(f'sum by (pod) (rate(process_cpu_seconds_total{{{s}}}[5m]))', "{{pod}}")],
                   (0, y, 8, 7), unit="none", desc="Cores."))
    p.append(panel("timeseries", "Resident memory",
                   [target(f'sum by (pod) (process_resident_memory_bytes{{{s}}})', "{{pod}}")],
                   (8, y, 8, 7), unit="bytes",
                   desc="The scheduler caches the whole node/pod set; growth tracks fleet size."))
    p.append(panel("timeseries", "Goroutines",
                   [target(f'sum by (pod) (go_goroutines{{{s}}})', "{{pod}}")], (16, y, 8, 7)))
    y += 7

    # A few OEP-0022 gang/topology metrics ("largest gang that still fits" per
    # domain, gate-timeout counts) have no settled name in-tree, so the Gang
    # admission row cannot chart them. Ship a note telling the operator how to
    # discover them rather than panels whose queries are guesses reading "No data".
    p.append({
        "type": "text", "title": "Scraping, and the gang metrics still missing",
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 6},
        "options": {"mode": "markdown", "content": (
            "**If the scheduling rows are empty, the scheduler is not being scraped.** "
            "Pod-level health above comes from kube-state-metrics, which watches the "
            "pods from outside; the `scheduler_*` rows need the scheduler's own "
            "/metrics endpoint scraped. A Kubernetes scheduler serves it on a secure "
            "port, so the pod template usually needs `prometheus.io/scrape: \"true\"`, "
            "`prometheus.io/scheme: \"https\"`, `prometheus.io/port: \"10259\"`, plus RBAC "
            "for the `/metrics` non-resource URL.\n\n"
            "The Gang admission row plots the `ome_scheduler_*` metrics the gangpack "
            "plugin exports today. Three signals OEP-0022 describes are still absent "
            "from the binary and so cannot be charted: the per-domain **largest gang "
            "that still fits**, a gate-**timeout** count (`gang_unwind_total` counts the "
            "resulting teardown, not the timeout itself), and the bookkeeping-drift "
            "signal.\n\n"
            "Note that `scheduler_schedule_attempts_total{result=\"unschedulable\"}` "
            "counts gang admission yields alongside real failures — a leader that "
            "arrives before its worker returns Unschedulable and rebinds milliseconds "
            "later. Read `gang_pin_total{result=\"no_fit\"}` for genuine starvation.\n\n"
            "To find what this build actually exports, run in **Explore**:\n\n"
            "```\ngroup by (__name__) ({cluster=\"$cluster\", pod=~\"ome-scheduler.*\"})\n```\n\n"
            "Anything not prefixed `scheduler_`, `go_`, `process_`, or "
            "`leader_election_` is OME-specific. Add those panels here and to "
            "`grafana/gen_dashboards.py`."
        )},
    })

    return dashboard(
        "ome-scheduler", "OME · Scheduler",
        "Placement health for ome-scheduler (OEP-0022) — topology-aware gang scheduling "
        "for accelerator workloads. Built on the upstream Kubernetes scheduler framework, "
        "so the standard scheduler_* metric set applies.",
        [var_ds(),
         var_query("cluster", "Cluster",
                   "label_values(ome_isvc_status_update_total, cluster)"),
         var_query("pod", "Pod",
                   'label_values(kube_pod_status_ready{cluster="$cluster",pod=~"ome-scheduler.*"}, pod)',
                   multi=True, include_all=True, all_value=".*")],
        p, ["ome", "scheduler", "placement"])


# ---------------------------------------------------------------------------
# 3. OME router — the `router_*` metric family exported by OME's own router pods.
#    Distinct from the engine metrics on the Serving dashboard: these describe the
#    router's view (which backends are in the pool, what it added in latency,
#    what it rejected), which is where "the engines are fine but requests fail"
#    problems actually show up.
# ---------------------------------------------------------------------------
def router():
    p, y = [], 0
    r = 'cluster="$cluster",pod=~"$pod"'

    def hq(metric, quant, by=""):
        grp = f"{by}, le" if by else "le"
        return (f'histogram_quantile({quant}, sum by ({grp}) '
                f'(rate({metric}_bucket{{{r}}}[5m])))')

    reqs = f'sum(rate(router_request_duration_seconds_count{{{r}}}[5m]))'

    p.append(row("Backend pool — which engines is the router actually using?", y)); y += 1
    p.append(panel("stat", "Registered backends",
                   [target(f'sum(router_pool_total_count{{{r}}})')], (0, y, 6, 4)))
    p.append(panel("stat", "Healthy backends",
                   [target(f'sum(router_pool_healthy_count{{{r}}})')], (6, y, 6, 4),
                   desc="Compare with Registered. Healthy < Registered means engines "
                        "are failing the router's health check."))
    p.append(panel("stat", "Ejected backends",
                   [target(f'sum(router_pool_ejected_count{{{r}}})')], (12, y, 6, 4),
                   thresholds=[(None, "green"), (1, "yellow"), (3, "red")],
                   desc="Backends the router removed from rotation. Non-zero is always "
                        "worth a look."))
    p.append(panel("stat", "Inflight requests",
                   [target(f'sum(router_pool_inflight_reqs{{{r}}})')], (18, y, 6, 4)))
    y += 4
    p.append(panel("timeseries", "Healthy vs registered backends", [
        target(f'sum(router_pool_total_count{{{r}}})', "Registered"),
        target(f'sum(router_pool_healthy_count{{{r}}})', "Healthy"),
    ], (0, y, 12, 8), desc="The gap between these two lines is the story: engines that "
                           "are registered but not healthy are capacity you are paying "
                           "for and not serving from."))
    p.append(panel("timeseries", "Ejections & inflight", [
        target(f'sum(router_pool_ejected_count{{{r}}})', "Ejected"),
        target(f'sum(router_pool_inflight_reqs{{{r}}})', "Inflight"),
    ], (12, y, 12, 8)))
    y += 8

    p.append(row("Traffic", y)); y += 1
    p.append(panel("timeseries", "Request rate", [target(reqs, "Total")], (0, y, 12, 8),
                   unit="reqps", desc="All terminal outcomes through the router."))
    p.append(panel("timeseries", "Request rate per router instance", [
        target(f'avg(rate(router_request_duration_seconds_count{{{r}}}[5m]))', "avg"),
        target(f'quantile(0.50, rate(router_request_duration_seconds_count{{{r}}}[5m]))', "p50"),
        target(f'quantile(0.95, rate(router_request_duration_seconds_count{{{r}}}[5m]))', "p95"),
        target(f'max(rate(router_request_duration_seconds_count{{{r}}}[5m]))', "max"),
    ], (12, y, 12, 8), unit="reqps",
        desc="Distribution across router pods. A wide avg-to-max spread means traffic "
             "is not evenly spread over the routers."))
    p.append(panel("timeseries", "Gate rejections",
                   [target(f'sum(rate(router_gate_rejections_total{{{r}}}[5m]))')],
                   (0, y + 8, 12, 7), unit="reqps",
                   thresholds=[(None, "green"), (0.01, "yellow")],
                   desc="Requests the router refused at the gate — rejected before any "
                        "engine saw them, so they are invisible in engine metrics."))
    p.append(panel("timeseries", "Request payload size", [
        target(hq("router_request_payload_bytes", 0.50), "p50"),
        target(hq("router_request_payload_bytes", 0.99), "p99"),
    ], (12, y + 8, 12, 7), unit="bytes",
        desc="A shift here often explains a latency shift."))
    y += 15

    p.append(row("Latency — router overhead vs engine time", y)); y += 1
    p.append(panel("timeseries", "Request duration through router", [
        target(hq("router_request_duration_seconds", 0.50), "p50"),
        target(hq("router_request_duration_seconds", 0.99), "p99"),
    ], (0, y, 12, 8), unit="s"))
    p.append(panel("timeseries", "Where the time goes (p99)", [
        target(hq("router_request_duration_seconds", 0.99), "Total"),
        target(hq("router_ingress_overhead_seconds", 0.99), "Router ingress overhead"),
        target(hq("router_egress_to_engine_seconds", 0.99), "Egress to engine"),
    ], (12, y, 12, 8), unit="s",
        desc="All three are seconds, so they share one axis. If Total is high while "
             "overhead and egress are flat, the engine is slow — not the router."))
    y += 8

    p.append(row("Prefill/decode groups & autoscaling", y)); y += 1
    p.append(panel("timeseries", "PD groups", [
        target(f'sum(router_pd_group_count{{{r}}})', "Groups"),
        target(f'sum(router_pd_group_engine_count{{{r}}})', "Engines in groups"),
    ], (0, y, 12, 7),
        desc="Prefill/decode-disaggregated topology as the router sees it."))
    p.append(panel("timeseries", "Autoscale score",
                   [target(f'avg(router_autoscale_score{{{r}}})')], (12, y, 12, 7),
                   desc="The router's own load signal. Correlate with replica changes "
                        "to see whether autoscaling is reacting to it."))

    return dashboard(
        "ome-router", "OME · Router",
        "OME router fleet health from the router_* metric family: backend pool "
        "membership, gate rejections, the router-vs-engine latency split, and PD group "
        "topology. This is the request path *before* the engine, so it catches what "
        "engine-side metrics cannot: a backend ejected from the pool, or a request "
        "rejected at the gate.",
        [var_ds(),
         var_query("cluster", "Cluster", "label_values(router_pool_total_count, cluster)"),
         var_query("pod", "Router pod",
                   'label_values(router_pool_total_count{cluster="$cluster"}, pod)',
                   multi=True, include_all=True, all_value=".*")],
        p, ["ome", "router", "serving"])


# ---------------------------------------------------------------------------
# 4. SLO — the on-call landing page. Two service-level indicators (did the
#    deployment succeed, and how long did it take) plus the availability of
#    the components that have to be up for either to mean anything.
#
#    Unlike the other three dashboards this one is a claim about history, so
#    it needs a long-retention store. OME's bundled Prometheus keeps 30m and
#    does not scrape kube-state-metrics; point this at the fleet store.
# ---------------------------------------------------------------------------
def slo():
    p, y = [], 0

    # Every series on this dashboard is exported by the controller-manager, so
    # they share one selector. The namespace matcher resolves against the same
    # metric the template variable is populated from, which keeps the filter
    # self-consistent on a store that renames a colliding `namespace` label to
    # `exported_namespace`: the variable and the query move together.
    ttr_sel = f'cluster="$cluster",namespace=~"$namespace"'
    ttr = f'ome_isvc_time_to_ready_seconds_bucket{{{ttr_sel}}}'

    def ttr_quantile(q, extra="", by=""):
        sel = f'ome_isvc_time_to_ready_seconds_bucket{{{ttr_sel}{extra}}}'
        grp = f"{by}, le" if by else "le"
        return (f"histogram_quantile({q}, sum by ({grp}) "
                f"(rate({sel}[$__rate_interval])))")

    # "At least one replica up" for a redundant component; "what fraction of
    # agents are up" for the per-node DaemonSet. Both are then averaged over
    # the window to give an uptime percentage.
    def uptime_expr(pod_re, redundant):
        agg = "max" if redundant else "avg"
        return (f'avg_over_time(({agg}(up{{cluster="$cluster",'
                f'pod=~"{pod_re}"}}))[$__range:])')

    # Three nines for the control plane that everything else depends on.
    slo_three_nines = [(None, "red"), (0.99, "yellow"), (0.999, "green")]

    p.append(row("SLO summary — over the selected time range", y)); y += 1
    p.append(panel("stat", "Deployments observed",
                   [target(f'sum(increase(ome_isvc_time_to_ready_seconds_count'
                           f'{{{ttr_sel}}}[$__range]))')],
                   (0, y, 6, 4), decimals=0,
                   desc="Deployments that reached Ready in the window. This is the "
                        "sample count behind every percentile on this dashboard — "
                        "read it first, because a p95 over a handful of deploys is "
                        "noise rather than a signal."))
    p.append(panel("stat", "Time to ready p95 · first deploy",
                   [target(ttr_quantile(0.95, extra=',deploy_kind="create"'))],
                   (6, y, 6, 4), unit="s",
                   thresholds=[(None, "green"), (1800, "yellow"), (3600, "red")],
                   desc="End-to-end: from ISVC creation to the aggregate Ready "
                        "condition going True, so it includes admission, runtime "
                        "selection, and model localization."))
    p.append(panel("stat", "Time to ready p95 · rollout",
                   [target(ttr_quantile(0.95, extra=',deploy_kind="update"'))],
                   (12, y, 6, 4), unit="s",
                   thresholds=[(None, "green"), (300, "yellow"), (900, "red")],
                   desc="Same measurement for an already-serving ISVC, timed from "
                        "when it lost readiness. Faster than a first deploy because "
                        "the model is already resident."))
    p.append(panel("stat", "Controller uptime",
                   [target(uptime_expr("ome-controller-manager.*", True))],
                   (18, y, 6, 4), unit="percentunit", decimals=3,
                   thresholds=slo_three_nines,
                   desc="Fraction of the window with at least one controller-manager "
                        "replica scraping successfully."))
    y += 4

    # SLI 1 (deployment success rate) has no panels here. It was built on
    # ome_inferenceservice_transition_status, which reports only on the
    # RawDeployment path -- see the note panel at the bottom. Numbering follows
    # the operational model, so SLI 2 keeps its number rather than being
    # renumbered into the gap.
    p.append(row("SLI 2 — Deployment latency (time to ready)", y)); y += 1
    p.append(panel("timeseries", "Time to ready percentiles", [
        target(ttr_quantile(0.50), "p50"),
        target(ttr_quantile(0.90), "p90"),
        target(ttr_quantile(0.95), "p95"),
        target(ttr_quantile(0.99), "p99"),
    ], (0, y, 12, 8), unit="s",
        desc="All deploy kinds together. Because first deploys and rollouts have "
             "genuinely different durations, read the split panel beside this one "
             "before drawing a conclusion from the combined p99."))
    p.append(panel("timeseries", "p95 by deploy kind", [
        target(ttr_quantile(0.95, by="deploy_kind"), "{{deploy_kind}}"),
    ], (12, y, 12, 8), unit="s",
        desc="create = the ISVC had never converged; update = it had been Ready "
             "and lost it. A first deploy that flaps through Ready=False before "
             "coming up is counted as an update."))
    p.append(panel("timeseries", "Slowest InferenceServices (p95)", [target(
        f'topk(10, histogram_quantile(0.95, sum by (namespace, isvc, le) '
        f'(rate({ttr}[$__rate_interval]))))', "{{namespace}}/{{isvc}}")],
        (0, y + 8, 12, 7), unit="s",
        desc="Which workloads carry the tail."))
    p.append(panel("timeseries", "Deploy rate", [target(
        f'sum(rate(ome_isvc_time_to_ready_seconds_count{{{ttr_sel}}}[$__rate_interval])) '
        f'* 3600', "Deploys/hour")], (12, y + 8, 12, 7),
        desc="Sample volume behind the percentiles above, over time. A p99 computed "
             "from a handful of deploys an hour is noise, not a signal — check this "
             "before treating a latency change as a regression."))
    y += 15

    p.append(row("Component uptime", y)); y += 1
    width = 24 // len(COMPONENTS)
    for i, (label, pod_re, redundant) in enumerate(COMPONENTS):
        note = ("Fraction of the window with at least one replica up."
                if redundant else
                "Mean fraction of node agents up — a DaemonSet, so one dead "
                "agent is one node that cannot localize models, not an outage.")
        p.append(panel("stat", label, [target(uptime_expr(pod_re, redundant))],
                       (i * width, y, width, 4), unit="percentunit", decimals=3,
                       thresholds=slo_three_nines, desc=note))
    y += 4
    p.append(panel("timeseries", "Availability over time", [
        target(f'{"max" if redundant else "avg"}(up{{cluster="$cluster",'
               f'pod=~"{pod_re}"}})', label)
        for label, pod_re, redundant in COMPONENTS
    ], (0, y, 12, 8), unit="percentunit", minv=0, maxv=1,
        desc="Instantaneous availability. Every drop to zero is a window in which "
             "that component was doing nothing at all."))
    p.append(panel("timeseries", "Restarts", [
        target(f'sum(changes(process_start_time_seconds{{cluster="$cluster",'
               f'pod=~"{pod_re}"}}[$__rate_interval]))', label)
        for label, pod_re, _ in COMPONENTS
    ], (12, y, 12, 8),
        desc="Process starts. A component can hold 100% scrape uptime while "
             "crash-looping fast enough to never miss a scrape."))
    y += 8
    p.append(panel("timeseries", "Controller leader continuity", [target(
        'max(leader_election_master_status{cluster="$cluster",'
        'pod=~"ome-controller-manager.*"})', "Leader held")],
        (0, y, 12, 7), minv=0, maxv=1, thresholds=[(None, "red"), (1, "green")],
        desc="The controller runs multiple replicas behind leader election, so "
             "being scraped is not the same as reconciling. 0 here means every "
             "replica is up and none of them owns the work."))
    p.append({
        "type": "text", "title": "What this dashboard needs",
        "gridPos": {"x": 12, "y": y, "w": 12, "h": 7},
        "options": {"mode": "markdown", "content": (
            "**This is not the bundled Prometheus.** The chart's Prometheus keeps "
            "`retention: 30m` and exists to feed KEDA; it cannot answer a question "
            "about yesterday. Point this dashboard at your long-retention store.\n\n"
            "**SLI 1 (deployment success rate) is not on this dashboard.** It was "
            "built on `ome_inferenceservice_transition_status`, which reports "
            "`status.modelStatus.transitionStatus` — a field written only by the "
            "RawDeployment status path. Nothing on the OMENative path sets it, so "
            "the ratio silently excluded every OMENative InferenceService while "
            "still reporting a confident percentage. Measuring it correctly needs a "
            "counter on the readiness edge, which both deployment modes share; "
            "until that exists, no number here is better than a wrong one.\n\n"
            "**SLI 2 needs the OME control-plane pods scraped** — "
            "`ome_isvc_time_to_ready_seconds` is exported by the controller-manager.\n\n"
            "There is no warm/cold model split here. Model-resident nodes are "
            "selected by a hard `nodeSelector`, so a serving pod is only created "
            "once the weights are already local — the download happens before the "
            "pod exists and is invisible to this measurement."
        )},
    })

    return dashboard(
        "ome-slo", "OME · SLO",
        "Service-level indicators for OME deployments: how long a deployment took to "
        "reach Ready, and whether the components responsible for it were running. "
        "Requires a long-retention Prometheus scraping the OME control plane. "
        "Deployment success rate is deliberately absent — see the note panel.",
        [var_ds(),
         var_query("cluster", "Cluster", "label_values(up, cluster)"),
         # Sourced from the same metric the panels query, so the variable and the
         # filter agree even on a store that renames a colliding namespace label.
         var_query("namespace", "Namespace",
                   'label_values(ome_isvc_time_to_ready_seconds_count'
                   '{cluster="$cluster"}, namespace)',
                   multi=True, include_all=True, all_value=".*")],
        p, ["ome", "slo", "on-call"])


# ---------------------------------------------------------------------------
# 5. Multi-cluster control plane — the management cluster running the OEP-0018
#    placer and the quota plane. The placement stack registers its own metrics,
#    so fleet membership, per-ISVC placement and declared policy are reported
#    directly; the surrounding component health is still generic
#    controller-runtime, kube-state-metrics and apiserver signal. The coverage
#    panel lists what remains uncovered rather than leaving the reader to infer
#    it from absent panels.
# ---------------------------------------------------------------------------
#
# Selector notes, because three scopes coexist on this board:
#   * ome_workload_cluster_* and ome_isvc_placement_candidate* carry their OWN
#     `cluster` label holding the MEMBER name, which shadows the collector's
#     control-plane label. They must be selected on cluster_id.
#   * ome_isvc_placement_phase / _policy_info have no cluster label of their
#     own, so the collector's `cluster` is the control plane.
#   * Quota lives on the MEMBERS, not the control plane: the quota-manager runs
#     here in projection mode while capacity and budgets are emitted per member.
#     The member's full cluster id ends with the short member name, hence the
#     suffix regex.
# endpoint="metrics" pins the ServiceMonitor scrape; the OME pods are also
# scraped by a pod-annotation job, so unfiltered aggregates double-count.
# ---------------------------------------------------------------------------
MC_EP = 'endpoint="metrics"'
MC_FLEET = f'{MC_EP},cluster_id=~"$control_plane"'
MC_PLACE = f'{MC_EP},cluster=~"$control_plane"'
MC_MEMBER = 'cluster=~".*($workload_cluster)$"'
MC_CAP = f'{MC_EP},{MC_MEMBER}'
MC_BUDGET_ALL = f'{MC_CAP},plane="workload",role="ClusterQueue"'
MC_BUDGET = f'{MC_BUDGET_ALL},quota=~"$tenant"'

MC_OK_BAD = [(None, "green"), (1, "red")]
MC_BAD_OK = [(None, "red"), (1, "green")]
MC_ERRORS = [(None, "green"), (1, "yellow"), (51, "red")]

MC_PHASE_MAP = [{"type": "value", "options": {
    "Placed": {"color": "green", "index": 0},
    "Admitting": {"color": "yellow", "index": 1},
    "Racing": {"color": "yellow", "index": 2},
    "Pending": {"color": "orange", "index": 3},
    "Failed": {"color": "red", "index": 4}}}]


def mc_measured(expr):
    """Read zero where a capacity pass ran, and No data where none did.

    The capacity families are Reset and repopulated each pass, so a cluster with
    nothing to report emits no series -- indistinguishable from a cluster running
    no quota manager. A bare `or vector(0)` calls both zero; no fallback at all
    calls both No data. Neither is true, and these are the panels that say
    whether anything is wrong, so a wrong answer here is expensive both ways.

    ome_quota_capacity_allocatable is present exactly where a pass ran and
    matched a flavor, which makes its presence the difference between the two.
    It only decides the empty case: where the metric itself has series, they win.
    """
    return f'{expr} or (0 * sum(ome_quota_capacity_allocatable{{{MC_CAP}}}))'


def mc_ratio(title, num, num_label, den, den_label, gp, desc=None,
             thresholds=T_NONE, decimals=None):
    """A stat carrying a numerator and denominator side by side, e.g. 2 / 2.

    Grafana cannot render a literal "a/b" string from two queries, so the pair
    is shown as two labelled values in one horizontal stat. The denominator is
    coloured neutral so the numerator reads as the primary figure.
    """
    t_num = target(num, num_label, instant=True)
    t_den = target(den, den_label, instant=True)
    p = panel("stat", title, [t_num, t_den], gp, desc=desc,
              thresholds=thresholds, decimals=decimals)
    p["options"].update({"textMode": "value_and_name", "graphMode": "none",
                         "justifyMode": "center", "orientation": "horizontal"})
    p["fieldConfig"]["overrides"] = [{
        "matcher": {"id": "byFrameRefID", "options": t_den["refId"]},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "text"}}]}]
    return p


def mc_labelled_table(title, expr, gp, keep, rename, desc=None, overrides=None):
    """An instant table narrowed to named label columns and renamed for reading.

    The generic table panel only drops Time and Value; these tables also have to
    hide the Prometheus plumbing labels (pod, endpoint, service, k8s_*) that
    would otherwise dominate the row.
    """
    p = panel("table", title, [target(expr, instant=True, table=True)], gp, desc=desc)
    p["transformations"] = [
        {"id": "filterFieldsByName", "options": {"include": {"names": keep}}},
        {"id": "organize", "options": {"excludeByName": {}, "indexByName": {},
                                       "renameByName": rename}},
    ]
    if overrides:
        p["fieldConfig"]["overrides"] = overrides
    return p


def mc_control_plane():
    p, y = [], 0

    # ---------------------------- overview ---------------------------------
    p.append(row("🌐 Overview ($control_plane)", y)); y += 1

    p.append(mc_ratio(
        "Workload clusters",
        f'sum(max by (cluster) (ome_workload_cluster_ready{{{MC_FLEET}}}))', "ready",
        f'count(max by (cluster) (ome_workload_cluster_ready{{{MC_FLEET}}}))', "total",
        (0, y, 6, 4), thresholds=MC_BAD_OK, decimals=0,
        desc="Members whose Ready condition is True, over all members registered on this "
             "control plane. Below total means a member is unreachable. max by (cluster) "
             "collapses the per-pod series so a controller rollout does not inflate the count."))
    p.append(mc_ratio(
        "Allocatable accelerators",
        f'sum(ome_quota_capacity_allocatable{{{MC_CAP}}})', "allocatable",
        f'(sum(ome_quota_capacity_allocatable{{{MC_CAP}}}) '
        f'+ sum(ome_quota_capacity_unavailable{{{MC_CAP}}}))', "total",
        (6, y, 6, 4), thresholds=MC_BAD_OK, decimals=0,
        desc="Schedulable accelerators over all accelerators present on the selected members. "
             "The gap is the unavailable pool: cordoned, unhealthy or parked. Cluster-wide -- "
             "capacity metrics carry no quota label, so $tenant does not narrow this."))
    p.append(panel(
        "stat", "Governed quota",
        [target(f'sum(ome_quota_budget_nominal{{{MC_BUDGET_ALL}}}) or vector(0)', instant=True)],
        (12, y, 6, 4), decimals=0,
        desc="Total nominal quota handed to tenants across the selected members. Spans ALL "
             "tenants regardless of $tenant, so it is the fleet-wide governed total."))
    p.append(panel(
        "stat", "Quota usage",
        [target(f'sum(ome_quota_budget_admitted{{{MC_BUDGET_ALL}}}) / '
                f'clamp_min(sum(ome_quota_budget_nominal{{{MC_BUDGET_ALL}}}), 1)', instant=True)],
        (18, y, 6, 4), unit="percentunit", decimals=0,
        desc="Admitted over governed across all tenants. Approaching 100% means quota, not "
             "hardware, is the ceiling. clamp_min avoids divide-by-zero when nothing is governed."))
    y += 4

    p.append(panel(
        "stat", "Total tenants",
        [target(f'count(count by (quota) (ome_quota_budget_nominal{{{MC_BUDGET_ALL}}})) or vector(0)',
                instant=True)],
        (0, y, 6, 4), decimals=0,
        desc="Distinct quota tenants on the selected members. Spans ALL tenants regardless of "
             "$tenant."))
    p.append(panel(
        "stat", "Total ISVCs",
        [target(f'count(max by (exported_namespace, isvc) '
                f'(ome_isvc_placement_phase{{{MC_PLACE}}})) or vector(0)', instant=True)],
        (6, y, 6, 4), decimals=0,
        desc="InferenceServices the placement controller manages on this control plane, in "
             "every phase."))
    p.append(panel(
        "stat", "Failed placements",
        [target(f'count(max by (exported_namespace, isvc) '
                f'(ome_isvc_placement_phase{{{MC_PLACE},phase="Failed"}})) or vector(0)',
                instant=True)],
        (12, y, 6, 4), thresholds=MC_OK_BAD, decimals=0,
        desc="ISVCs whose placement failed terminally. Zero is healthy; the natural alert "
             "target."))
    p.append(panel(
        "stat", "OME control plane errors",
        [target(f'sum(increase(controller_runtime_reconcile_errors_total'
                f'{{{MC_PLACE},pod=~"ome-.*manager.*"}}[$__range])) or vector(0)', instant=True)],
        (18, y, 6, 4), thresholds=MC_ERRORS, decimals=0,
        desc="Reconcile errors across both OME control-plane components, counted over the "
             "selected time range so the number follows the time picker. Shown whole because "
             "increase() extrapolates. Container restarts live per component further down."))
    y += 4

    p.append(panel(
        "timeseries", "Managed object counts (control plane etcd)",
        [target(f'max by (cluster) (apiserver_storage_objects'
                f'{{cluster=~"$control_plane",job="kube-apiserver",'
                f'resource="workloadclusters.ome.io"}})', "WorkloadClusters"),
         target(f'max by (cluster) (apiserver_storage_objects'
                f'{{cluster=~"$control_plane",job="kube-apiserver",'
                f'resource="inferenceservices.ome.io"}})', "InferenceServices"),
         target(f'max by (cluster) (apiserver_storage_objects'
                f'{{cluster=~"$control_plane",job="kube-apiserver",'
                f'resource="acceleratorquotas.ome.io"}})', "AcceleratorQuotas")],
        (0, y, 12, 7), decimals=0,
        desc="Straight from the control plane apiserver, so it still counts objects when a "
             "controller is wedged."))
    p.append(panel(
        "timeseries", "Member readiness over time",
        [target(f'max by (cluster) (ome_workload_cluster_ready{{{MC_FLEET}}})', "{{cluster}}")],
        (12, y, 12, 7), minv=0, maxv=1, decimals=0,
        desc="1 = Ready, 0 = not. Collapsed with max by (cluster) so a controller rollout does "
             "not split each member into one broken segment per pod generation."))
    y += 7

    # ------------------------------ fleet ----------------------------------
    p.append(row("🛰️ Workload cluster fleet ($control_plane)", y)); y += 1

    p.append(mc_labelled_table(
        "Member state",
        f'max by (cluster, status, reason, cluster_id) '
        f'(ome_workload_cluster_status{{{MC_FLEET}}})',
        (0, y, 12, 8),
        ["cluster", "status", "reason", "cluster_id"],
        {"cluster": "Workload cluster", "status": "Ready", "reason": "Reason",
         "cluster_id": "Control plane"},
        desc="Current Ready condition per member with its reason, one row per member.",
        overrides=[{"matcher": {"id": "byName", "options": "Ready"},
                    "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                   {"id": "mappings", "value": [{"type": "value", "options": {
                                       "True": {"color": "green", "index": 0},
                                       "False": {"color": "red", "index": 1},
                                       "Unknown": {"color": "orange", "index": 2}}}]}]}]))
    p.append(panel(
        "timeseries", "WorkloadCluster reconcile rate and errors",
        [target(f'sum(rate(controller_runtime_reconcile_total'
                f'{{{MC_PLACE},pod=~"ome-controller-manager.*",controller="workloadcluster"}}'
                f'[$__rate_interval]))', "reconciles/s"),
         target(f'sum(rate(controller_runtime_reconcile_errors_total'
                f'{{{MC_PLACE},pod=~"ome-controller-manager.*",controller="workloadcluster"}}'
                f'[$__rate_interval]))', "errors/s")],
        (12, y, 12, 8), unit="reqps",
        desc="Connection probes run on a health interval, so a flat non-zero rate is normal. A "
             "rising error rate means members are unreachable."))
    y += 8

    # ---------------------------- placement --------------------------------
    p.append(row("🎯 InferenceService ($isvc) placement details", y)); y += 1

    pie = panel("piechart", "ISVCs by placement phase",
                [target(f'count by (phase) (max by (exported_namespace, isvc, phase) '
                        f'(ome_isvc_placement_phase{{{MC_PLACE},isvc=~"$isvc"}}))',
                        "{{phase}}", instant=True)],
                (0, y, 6, 7),
                desc="Pending = nothing matched or nothing connected. Admitting = fanned out, "
                     "not yet admitted. Placed = admitted. Failed = terminal. Racing is the "
                     "legacy name for Admitting and may appear during upgrades.")
    pie["options"] = {"legend": {"displayMode": "list", "placement": "right", "showLegend": True},
                      "pieType": "donut",
                      "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}
    pie["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": n},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
        for n, c in (("Placed", "green"), ("Failed", "red"),
                     ("Admitting", "yellow"), ("Racing", "yellow"),
                     ("Pending", "orange"))]
    p.append(pie)

    p.append(mc_labelled_table(
        "Placement phase",
        f'max by (exported_namespace, isvc, phase) '
        f'(ome_isvc_placement_phase{{{MC_PLACE},isvc=~"$isvc"}})',
        (6, y, 8, 7),
        ["exported_namespace", "isvc", "phase"],
        {"exported_namespace": "Namespace", "isvc": "InferenceService", "phase": "Phase"},
        desc="The namespace arrives as exported_namespace because the controller pod's own "
             "namespace already owns the namespace label on this scrape path.",
        overrides=[{"matcher": {"id": "byName", "options": "Phase"},
                    "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                   {"id": "mappings", "value": MC_PHASE_MAP}]}]))
    p.append(mc_labelled_table(
        "Where each ISVC landed",
        f'max by (exported_namespace, isvc, cluster, phase) '
        f'(ome_isvc_placement_candidate{{{MC_FLEET},isvc=~"$isvc"}})',
        (14, y, 10, 7),
        ["exported_namespace", "isvc", "cluster", "phase"],
        {"exported_namespace": "Namespace", "isvc": "InferenceService",
         "cluster": "Workload cluster", "phase": "Candidate phase"},
        desc="Every cluster this ISVC was fanned out to. Admitting means the derived ISVC "
             "exists but has not been admitted; legacy Placed may appear with the same meaning "
             "during upgrades. Single mode expects one Admitted winner; All and Split list "
             "every serving home."))
    y += 7

    p.append(mc_labelled_table(
        "Declared placement policy",
        f'max by (exported_namespace, isvc, mode, requirements, cluster_selector, spread) '
        f'(ome_isvc_placement_policy_info{{{MC_PLACE},isvc=~"$isvc"}})',
        (0, y, 12, 8),
        ["exported_namespace", "isvc", "mode", "requirements", "cluster_selector", "spread"],
        {"exported_namespace": "Namespace", "isvc": "InferenceService", "mode": "Mode",
         "requirements": "Accelerator requirements", "cluster_selector": "Cluster selector",
         "spread": "Spread"},
        desc="What the ISVC asked for, from spec.placement. Diff this against 'Where each ISVC "
             "landed' to see intent against outcome."))
    p.append(panel(
        "timeseries", "Replicas per home (admitted vs ready)",
        [target(f'max by (exported_namespace, isvc, cluster) '
                f'(ome_isvc_placement_candidate_admitted_replicas{{{MC_FLEET},isvc=~"$isvc"}})',
                "admitted {{isvc}} @ {{cluster}}"),
         target(f'max by (exported_namespace, isvc, cluster) '
                f'(ome_isvc_placement_candidate_ready_replicas{{{MC_FLEET},isvc=~"$isvc"}})',
                "ready {{isvc}} @ {{cluster}}")],
        (12, y, 12, 8), minv=0, decimals=0,
        desc="Admitted is what the home's Kueue let through; ready is what actually serves. A "
             "persistent gap means pods are admitted but not starting."))
    y += 8

    # ------------------------- member capacity -----------------------------
    p.append(row("🖥️ Member ($workload_cluster) capacity details", y)); y += 1

    p.append(panel(
        "stat", "Nodes (cluster)",
        [target(f'sum(ome_quota_capacity_nodes{{{MC_CAP}}})', instant=True)],
        (0, y, 12, 4), decimals=0,
        desc="Nodes backing member capacity, all states."))
    p.append(panel(
        "stat", "Unattributed (cluster)",
        [target(mc_measured(f'sum(ome_quota_capacity_unattributed{{{MC_CAP}}})'), instant=True)],
        (12, y, 12, 4), thresholds=MC_OK_BAD, decimals=0,
        desc="Accelerators no quota claims. Zero on a member that ran a capacity pass and found "
             "nothing wrong; No data on one that measured no capacity at all. The family is "
             "absent in both cases, so the fallback is scoped to allocatable rather than "
             "unconditional -- a fake zero and a blank panel are each wrong for one of them."))
    y += 4

    p.append(panel(
        "timeseries", "Capacity: allocatable vs unavailable",
        [target(f'sum by (resource, flavor) (ome_quota_capacity_allocatable{{{MC_CAP}}})',
                "allocatable {{resource}}/{{flavor}}"),
         target(f'sum by (resource, flavor) (ome_quota_capacity_unavailable{{{MC_CAP}}})',
                "unavailable {{resource}}/{{flavor}}")],
        (0, y, 8, 8), decimals=0,
        desc="Capacity on the selected members by resource and flavor. Cluster-wide."))
    p.append(panel(
        "timeseries", "Nodes by state",
        [target(f'sum by (state) (ome_quota_capacity_nodes{{{MC_CAP}}})', "{{state}}")],
        (8, y, 8, 8), decimals=0, stacked=True,
        desc="Node population by state. Cluster-wide."))
    p.append(panel(
        "timeseries", "Kueue objects written",
        [target(f'sum(rate(ome_quota_backend_applied_total{{{MC_CAP}}}[$__rate_interval]))',
                "applied/s")],
        (16, y, 8, 8), unit="reqps",
        desc="The materializer's write path into Kueue. Flat zero while workloads wait means "
             "projection is stalled. Carries no quota label, so it is cluster-wide."))
    y += 8

    p.append(panel(
        "timeseries", "Unattributed accelerators by reason",
        [target(mc_measured(
            f'sum by (reason) (ome_quota_capacity_unattributed{{{MC_CAP}}})'), "{{reason}}")],
        (0, y, 8, 8), decimals=0,
        desc="Which fix an unclaimed accelerator needs. NoMatchingFlavor means no "
             "ResourceFlavor's node labels matched; AmbiguousFlavor means two matched equally "
             "well, so attributing the node to either would be a guess. The total above says "
             "something is wrong -- this says what, and they need different repairs."))
    p.append(panel(
        "timeseries", "Unattributed nodes by reason",
        [target(mc_measured(
            f'sum by (reason) (ome_quota_capacity_unattributed_nodes{{{MC_CAP}}})'),
            "{{reason}}")],
        (8, y, 8, 8), decimals=0,
        desc="Distinct machines, not node-resource pairs: a node advertising two unclaimed "
             "resources counts once. Read against the panel to its left to tell one badly "
             "labelled machine from a whole pool."))
    p.append(panel(
        "timeseries", "Kueue objects reaped",
        [target(mc_measured(
            f'sum by (trigger) (rate(ome_quota_backend_swept_total{{{MC_CAP}}}'
            f'[$__rate_interval]))'), "{{trigger}}")],
        (16, y, 8, 8), unit="reqps",
        desc="The other half of the write path. materialize is routine orphan collection when "
             "the tree no longer names an object; finalize is a node being deleted. Written "
             "without reaped cannot show a churn loop, where the materializer rewrites every "
             "pass what the sweep has just removed."))
    y += 8

    # --------------------------- tenant quota ------------------------------
    p.append(row("🧮 Tenant ($tenant) quota details", y)); y += 1

    p.append(panel(
        "stat", "Admitted (tenant)",
        [target(f'sum(ome_quota_budget_admitted{{{MC_BUDGET}}}) or vector(0)', instant=True)],
        (0, y, 8, 4), decimals=0,
        desc="Admitted against tenant quota. Follows $tenant."))
    p.append(panel(
        "stat", "Reserved not admitted",
        [target(f'(sum(ome_quota_budget_reserved{{{MC_BUDGET}}}) '
                f'- sum(ome_quota_budget_admitted{{{MC_BUDGET}}})) or vector(0)', instant=True)],
        (8, y, 8, 4), thresholds=MC_OK_BAD, decimals=0,
        desc="Quota held but not serving. Sustained non-zero means workloads are stuck in "
             "admission. Follows $tenant."))
    p.append(panel(
        "stat", "Borrowed (tenant)",
        [target(f'sum(ome_quota_budget_borrowed{{{MC_BUDGET}}}) or vector(0)', instant=True)],
        (16, y, 8, 4), decimals=0,
        desc="Borrowed from other tenants' unused quota. Follows $tenant."))
    y += 4

    p.append(panel(
        "timeseries", "Nominal vs admitted by tenant",
        [target(f'sum by (quota) (ome_quota_budget_nominal{{{MC_BUDGET}}})', "nominal {{quota}}"),
         target(f'sum by (quota) (ome_quota_budget_admitted{{{MC_BUDGET}}})', "admitted {{quota}}")],
        (0, y, 12, 8), decimals=0,
        desc="Granted quota against what is admitted, per tenant."))
    p.append(panel(
        "timeseries", "Utilization by tenant",
        [target(f'sum by (quota) (ome_quota_budget_admitted{{{MC_BUDGET}}}) / '
                f'clamp_min(sum by (quota) (ome_quota_budget_nominal{{{MC_BUDGET}}}), 1)',
                "{{quota}}")],
        (12, y, 12, 8), unit="percentunit",
        desc="Admitted over nominal per tenant. Sustained near 1 means the tenant is quota-bound."))
    y += 8

    # ------------------------ component health -----------------------------
    p.append(row("🔧 Control plane ($control_plane) component health", y)); y += 1

    for comp, cont, lock in [
            ("ome-controller-manager", "manager", "ome-controller-manager-leader-lock"),
            ("ome-quota-manager", "quota-manager", "ome-quota-manager-leader-lock")]:
        sel = f'{MC_PLACE},pod=~"{comp}.*"'
        ksm = 'cluster=~"$control_plane",namespace="ome"'

        p.append(panel(
            "stat", f"{comp} ready",
            [target(f'max(kube_deployment_status_replicas_available'
                    f'{{{ksm},deployment="{comp}"}}) / clamp_min(max('
                    f'kube_deployment_spec_replicas{{{ksm},deployment="{comp}"}}), 1)',
                    instant=True)],
            (0, y, 8, 4), unit="percentunit", thresholds=MC_BAD_OK,
            desc="Available over desired replicas."))
        p.append(panel(
            "stat", f"{comp} leader",
            [target(f'sum(leader_election_master_status{{{sel},name="{lock}"}}) or vector(0)',
                    instant=True)],
            (8, y, 8, 4), decimals=0,
            desc="Must be exactly 1. Zero means no active leader and nothing reconciles; above "
                 "1 means split brain."))
        p.append(panel(
            "stat", f"{comp} restarts",
            [target(f'sum(increase(kube_pod_container_status_restarts_total'
                    f'{{{ksm},container="{cont}"}}[$__range])) or vector(0)', instant=True)],
            (16, y, 8, 4), thresholds=MC_OK_BAD, decimals=0,
            desc="Container restarts over the selected time range."))
        y += 4

        p.append(panel(
            "timeseries", f"{comp} CPU (cores)",
            [target(f'sum by (pod) (rate(process_cpu_seconds_total{{{sel}}}'
                    f'[$__rate_interval]))', "{{pod}}")],
            (0, y, 6, 7), minv=0,
            desc="Per-pod CPU. Only the leader does real work, so one hot pod is expected."))
        p.append(panel(
            "timeseries", f"{comp} memory (RSS)",
            [target(f'sum by (pod) (process_resident_memory_bytes{{{sel}}})', "{{pod}}")],
            (6, y, 6, 7), unit="bytes",
            desc="Resident set size per pod."))
        p.append(panel(
            "timeseries", f"{comp} reconcile rate by controller",
            [target(f'sum by (controller) (rate(controller_runtime_reconcile_total{{{sel}}}'
                    f'[$__rate_interval]))', "{{controller}}")],
            (12, y, 6, 7), unit="reqps",
            desc="Work actually being done, per controller."))
        p.append(panel(
            "timeseries", f"{comp} reconcile errors",
            [target(f'sum by (controller) (rate(controller_runtime_reconcile_errors_total'
                    f'{{{sel}}}[$__rate_interval]))', "{{controller}}")],
            (18, y, 6, 7), unit="reqps",
            desc="Sustained non-zero means a controller cannot make progress."))
        y += 7

        p.append(panel(
            "timeseries", f"{comp} reconcile p99",
            [target(f'histogram_quantile(0.99, sum by (controller, le) (rate('
                    f'controller_runtime_reconcile_time_seconds_bucket{{{sel}}}'
                    f'[$__rate_interval])))', "{{controller}}")],
            (0, y, 12, 7), unit="s",
            desc="Tail reconcile latency. Cross-cluster fan-out makes the placer slower than a "
                 "local controller."))
        p.append(panel(
            "timeseries", f"{comp} queue depth / saturation",
            [target(f'max by (name) (workqueue_unfinished_work_seconds{{{sel}}})',
                    "oldest unfinished {{name}}"),
             target(f'sum by (controller) (controller_runtime_active_workers{{{sel}}}) / '
                    f'clamp_min(sum by (controller) ('
                    f'controller_runtime_max_concurrent_reconciles{{{sel}}}), 1)',
                    "saturation {{controller}}")],
            (12, y, 12, 7),
            desc="Oldest unfinished item in seconds, plus active over max workers. Saturation "
                 "pinned at 1 means the controller is the bottleneck."))
        y += 7

    p.append(panel(
        "timeseries", "Apiserver client calls by code",
        [target(f'sum by (code) (rate(rest_client_requests_total'
                f'{{{MC_PLACE},pod=~"ome-.*manager.*"}}[$__rate_interval]))', "{{code}}")],
        (0, y, 12, 7), unit="reqps",
        desc="Control-plane apiserver traffic from the OME components. A rise in 4xx or 5xx "
             "precedes reconcile errors."))
    p.append(panel(
        "timeseries", "Webhook requests by code",
        [target(f'sum by (code) (rate(controller_runtime_webhook_requests_total'
                f'{{{MC_PLACE},pod=~"ome-.*manager.*"}}[$__rate_interval])) or vector(0)',
                "{{code}}")],
        (12, y, 12, 7), unit="reqps",
        desc="Admission webhook traffic. The placer disables the ISVC webhooks in "
             "control-plane role, so quiet is expected here."))
    y += 7

    # --------------------------- coverage ----------------------------------
    p.append(row("📖 Coverage limits", y)); y += 1
    p.append({
        "type": "text",
        "title": "What this board cannot tell you, and the fix for each",
        "datasource": DS,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 10},
        "options": {"mode": "markdown", "content": (
            "The placement stack registers its own metrics, so member health, per-ISVC "
            "placement and declared policy are first-class here rather than inferred from "
            "generic signals. Quota lives here too, rather than on a board of its own: "
            "capacity is the denominator every tenant number is read against, and the two "
            "were being kept in step by hand.\n\n"
            "Cohort rows are absent from the tenant panels on purpose -- a cohort's "
            "nominal contains its children's, so a sum over both double-counts every "
            "leaf. What remains uncovered:\n\n"
            "| Gap | Why it is missing | Fix |\n"
            "|---|---|---|\n"
            "| InferenceService *serving* state — Ready, failed, in-progress per namespace | "
            "The platform kube-state-metrics on a management cluster carries no OME "
            "`customResourceState` config, so every `ome_inferenceservice_*` series is absent. "
            "Placement phase is exported, but that is where the ISVC was put, not whether it "
            "serves. | Add `config/monitoring/kube-state-metrics-config.yaml` to that KSM. "
            "Config only, no OME code change. |\n"
            "| Whether quota accounting matches reality | Where Kueue does not gate serving "
            "pods, admitted counts read zero while accelerators are consumed, so a healthy "
            "zero and an unmeasured fleet look identical. | A pod-derived consumption metric "
            "split by whether Kueue governs the pod. |\n"
            "| Where a tenant's share landed across members | `status.budgets[].perCluster` "
            "carries it; no metric exports it. The tenant row sums the members selected, "
            "so an uneven split reads the same as an even one. | Export the per-cluster "
            "breakdown the projection already computes. |\n"
            "| Tree structure — parent, depth, violations by reason | The path label ships "
            "in the budget family but nothing reads it yet, so a tenant is a flat name "
            "rather than a position. | Label the tenant panels by path, and export the "
            "violation reasons the tree check already produces. |\n"
            "| Fan-out latency and sweep lag | Computed by `tests/perf/multicluster` offline "
            "and nowhere in production. The phase gauge shows the outcome, not how long the "
            "race took or how long a loser's derived object survived. | Placement histograms "
            "around fan-out and the loser sweep. |\n"
            "Nothing on this board pages anyone: `prometheusRule.enabled` is false everywhere, "
            "no PrometheusRule object is deployed, and Alertmanager has no route or receiver. "
            "`Failed placements` and `OME control plane errors` are the two panels worth wiring "
            "first.\n\n"
            "*Selector caveat: `ome_workload_cluster_*` and `ome_isvc_placement_candidate*` "
            "carry their own `cluster` label holding the member name, which shadows the "
            "collector's control-plane label, so those panels select on `cluster_id`. Renaming "
            "the metric label to `workload_cluster` would remove the special case.*"
        )},
    })

    return dashboard(
        "ome-mc-control-plane", "OME · Multi-cluster control plane",
        "Fan-out health for the OME multi-cluster control plane: fleet membership, per-ISVC "
        "placement and declared policy, tenant quota on the members, and control-plane "
        "component health. Every query pins endpoint=\"metrics\" because the OME pods are "
        "scraped twice and unfiltered aggregates double-count; gauges collapse per-pod series "
        "with max by (...) so a controller rollout neither splits a line nor inflates a count.",
        # No allValue on any picker: with one, All substitutes `.*` and discards the
        # self-scoping label query, so an aggregate silently spans every cluster in
        # the stack -- including workload clusters running their own
        # ome-controller-manager. The error box read 331 fleet-wide against 0 for the
        # selected control plane. Left unset, All expands to the discovered values
        # joined with `|`, keeping each panel scoped to what its picker lists.
        [var_ds(),
         # Self-scoping: only control planes that actually register a WorkloadCluster appear.
         #
         # No All option, so the board opens on one hub rather than every hub summed.
         # Hubs are independent fleets -- each holds its own tree and makes its own
         # placement decisions -- so a total across them counts unrelated things
         # together, and the number it produces answers nobody's question. Still
         # multi-select for deliberately comparing two.
         var_query("control_plane", "Control plane",
                   'label_values(ome_workload_cluster_ready, cluster_id)',
                   multi=True),
         # Members. Quota panels match this as a suffix of the member's full cluster id.
         var_query("workload_cluster", "Workload cluster",
                   'label_values(ome_workload_cluster_ready, cluster)',
                   multi=True, include_all=True),
         # The namespace label on quota metrics is the quota-manager's own namespace, so the
         # tenant is the quota name.
         var_query("tenant", "Tenant (quota)",
                   'label_values(ome_quota_budget_nominal, quota)',
                   multi=True, include_all=True),
         var_query("isvc", "InferenceService",
                   'label_values(ome_isvc_placement_phase, isvc)',
                   multi=True, include_all=True)],
        # Overview stays open; the drill-downs below it are keyed on variables and
        # are what you open once a number up top sends you looking. Expanded, the
        # board runs every panel's query on load.
        collapse_after(p, expanded=1),
        ["ome", "multicluster", "control-plane", "placement", "quota"])


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for build in (control_plane, scheduler, router, slo, mc_control_plane):
        target.n = 0
        d = build()
        path = OUT / f"{d['uid']}.json"
        path.write_text(json.dumps(d, indent=2) + "\n")
        # Counted through rows: a collapsed row carries its panels inside itself,
        # so a top-level count reports a fraction of the board.
        n = sum(len(q.get("panels", [])) if q["type"] == "row" else 1 for q in d["panels"])
        print(f"wrote {path.relative_to(pathlib.Path(__file__).parent.parent)} "
              f"({n} panels)")


if __name__ == "__main__":
    main()
