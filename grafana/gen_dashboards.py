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

# kube-state-metrics exports the ome_* CustomResourceState series (see
# config/monitoring/kube-state-metrics-config.yaml). They describe OME's
# managed objects, not OME's pods, so they carry the CR's own name/namespace
# and the KSM pod's identity -- never the controller-manager's. Scoping them
# by an OME pod name yields an empty result.
KSM = 'cluster="$cluster",namespace=~"$namespace"'

# Transition states an InferenceService can settle in. InProgress is excluded:
# it is not an outcome, so counting it as a failure would make every rollout
# depress the success rate.
ISVC_SETTLED = "UpToDate|BlockedByFailedLoad|InvalidSpec"

# OME's own long-running components, by pod-name prefix. `redundant` marks a
# leader-elected or multi-replica component where the meaningful availability
# question is "was at least one replica up", as opposed to a DaemonSet where
# every node's agent counts independently.
COMPONENTS = [
    ("Controller manager", "ome-controller-manager.*", True),
    ("Scheduler", "ome-scheduler.*", True),
    ("Model agent", "ome-model-agent-daemonset.*", False),
]

# Multi-cluster control plane. Three independent de-duplications are
# load-bearing on these clusters, because each producer is collected more than
# once and an unqualified sum() is silently 2-3x too high:
#
#   * the manager is scraped twice -- by the ServiceMonitor and by an
#     annotation-driven kubernetes-pods job. Only the ServiceMonitor path
#     carries `endpoint`, so it is the discriminator. Without it the leader
#     gauge reads 2 and looks like split brain.
#   * kube-state-metrics runs two replicas -> max by (...).
#   * the apiserver runs three instances -> max by (...).
MC_MGR = 'cluster=~"$cluster",pod=~"ome-controller-manager.*",endpoint="metrics"'
MC_CTRL = 'controller=~"placement|placement-endpoint|workloadcluster"'

# The quota plane runs as its own Deployment on the same clusters, with its own
# WorkloadCluster manager and status funnel. Same de-duplication applies.
MC_QM = 'cluster=~"$cluster",pod=~"ome-quota-manager.*",endpoint="metrics"'

# The quota tree reconciles whole and resyncs on a 10m tick, so a 5m rate window
# is usually empty and a 5m histogram_quantile is NaN. Rate over a window that
# reliably spans a tick instead.
MC_QM_RATE = "30m"


# kube-state-metrics' view of the two OME deployments. Deliberately not the
# pods' own /metrics: KSM reports desired-vs-available even when a pod is down
# and therefore not exporting anything, which is exactly the case the top row
# has to cover.
MC_KSM = 'cluster=~"$cluster",namespace="ome"'

# The apiserver's own object counter -- the only source of OME custom-resource
# counts on a management cluster, because the platform kube-state-metrics there
# carries no OME customResourceState config.
MC_API = 'cluster=~"$cluster",job="kube-apiserver"'

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
        p, ["ome", "control-plane"])


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

    p.append(row("Outcomes — is work getting placed?", y)); y += 1
    p.append(panel("timeseries", "Attempts by result",
                   [target(f'sum by (result) (rate(scheduler_schedule_attempts_total{{{s}}}[5m]))',
                           "{{result}}")], (0, y, 12, 8), unit="reqps", stacked=True,
                   desc="result = scheduled | unschedulable | error. A rising "
                        "'unschedulable' share is the fleet running out of placeable "
                        "topology, not the scheduler being slow."))
    p.append(panel("timeseries", "Pending pods by queue",
                   [target(f'sum by (queue) (scheduler_pending_pods{{{s}}})', "{{queue}}")],
                   (12, y, 12, 8), stacked=True,
                   desc="active = ready to try; backoff = waiting after a failure; "
                        "unschedulable = nothing fits; gated = held by a scheduling gate "
                        "(how gang admission parks pods)."))
    p.append(panel("timeseries", "Unschedulable rate", [target(
        f'sum(rate(scheduler_schedule_attempts_total{{{s},result="unschedulable"}}[5m]))')],
        (0, y + 8, 12, 7), unit="reqps",
        desc="Sustained non-zero means gangs are repeatedly failing to find a domain."))
    p.append(panel("timeseries", "Attempts per pod (p99)", [target(
        f'histogram_quantile(0.99, sum by (le) (rate(scheduler_pod_scheduling_attempts_bucket{{{s}}}[5m])))')],
        (12, y + 8, 12, 7),
        desc="How many tries a pod needs before it lands. Climbing = churn/contention."))
    y += 15

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

    # OEP-0022 defines gang/topology metrics ("largest gang that still fits" per
    # domain, gate-timeout counts) whose metric names are not yet settled in-tree.
    # Ship a note telling the operator how to discover and add them, rather than
    # shipping panels whose queries are guesses and would read "No data".
    p.append({
        "type": "text", "title": "Gang & topology metrics — not yet wired",
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 6},
        "options": {"mode": "markdown", "content": (
            "**If the scheduling rows are empty, the scheduler is not being scraped.** "
            "Pod-level health above comes from kube-state-metrics, which watches the "
            "pods from outside; the `scheduler_*` rows need the scheduler's own "
            "/metrics endpoint scraped. A Kubernetes scheduler serves it on a secure "
            "port, so the pod template usually needs `prometheus.io/scrape: \"true\"`, "
            "`prometheus.io/scheme: \"https\"`, `prometheus.io/port: \"10259\"`, plus RBAC "
            "for the `/metrics` non-resource URL.\n\n"
            "OEP-0022 describes OME-specific scheduler metrics — the per-domain "
            "**largest gang that still fits**, gate-timeout counts, and the "
            "bookkeeping-drift signal. Their metric names are not defined in-tree yet, "
            "so this dashboard does not guess at them.\n\n"
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

    settled = f'ome_inferenceservice_transition_status{{{KSM},status=~"{ISVC_SETTLED}"}}'
    converged = f'ome_inferenceservice_transition_status{{{KSM},status="UpToDate"}}'
    success_ratio = f"sum({converged}) / sum({settled})"
    # Time-average the ratio across the picker's window rather than reading it
    # instantaneously, so the tile reports the SLI over the stated period.
    success_over_range = f"avg_over_time(({success_ratio})[$__range:])"

    ttr = f'ome_isvc_time_to_ready_seconds_bucket{{{KSM}}}'

    def ttr_quantile(q, extra="", by=""):
        sel = f'ome_isvc_time_to_ready_seconds_bucket{{{KSM}{extra}}}'
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

    # SLO thresholds. Two nines for a deploy path that a human is waiting on,
    # three for the control plane that everything else depends on.
    slo_two_nines = [(None, "red"), (0.95, "yellow"), (0.99, "green")]
    slo_three_nines = [(None, "red"), (0.99, "yellow"), (0.999, "green")]

    p.append(row("SLO summary — over the selected time range", y)); y += 1
    p.append(panel("stat", "Deployment success rate", [target(success_over_range)],
                   (0, y, 6, 4), unit="percentunit", decimals=2,
                   thresholds=slo_two_nines,
                   desc="Share of InferenceServices in a settled state that settled "
                        "healthy: UpToDate over (UpToDate + BlockedByFailedLoad + "
                        "InvalidSpec). InProgress is deliberately excluded — it is "
                        "not an outcome, and counting it would make every rollout "
                        "look like a partial failure. Target 99%."))
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

    p.append(row("SLI 1 — Deployment success rate", y)); y += 1
    p.append(panel("timeseries", "Success rate", [target(success_ratio, "Success rate")],
                   (0, y, 12, 8), unit="percentunit", minv=0, maxv=1,
                   thresholds=slo_two_nines,
                   desc="The ratio over time. Dips are ISVCs entering "
                        "BlockedByFailedLoad or InvalidSpec, not ISVCs mid-rollout."))
    p.append(panel("timeseries", "InferenceServices by transition status", [target(
        f'sum by (status) (ome_inferenceservice_transition_status{{{KSM}}})',
        "{{status}}")], (12, y, 12, 8), stacked=True,
        desc="The whole population, including InProgress. A band that stops "
             "draining is a rollout that stopped making progress."))
    p.append(panel("timeseries", "Stalled — InProgress for 30m without interruption",
                   [target(f'count(min_over_time('
                           f'ome_inferenceservice_transition_status{{{KSM},'
                           f'status="InProgress"}}[30m]) == 1)', "Stalled ISVCs")],
                   (0, y + 8, 12, 7), thresholds=[(None, "green"), (1, "red")],
                   desc="A stall emits nothing new, so it cannot be caught by a rate. "
                        "min_over_time == 1 means the ISVC held InProgress for every "
                        "sample in the window — it never briefly settled."))
    p.append(panel("table", "Not converged, by InferenceService", [target(
        f'ome_inferenceservice_transition_status{{{KSM},'
        f'status=~"InProgress|BlockedByFailedLoad|InvalidSpec"}} == 1',
        instant=True, table=True)], (12, y + 8, 12, 7),
        desc="Who to go look at. BlockedByFailedLoad points at the model import "
             "path; InvalidSpec is a spec the webhook admitted but the reconciler "
             "cannot act on; InProgress is only interesting if it persists."))
    y += 15

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
    p.append(panel("timeseries", "Deployments observed", [target(
        f'sum(rate(ome_isvc_time_to_ready_seconds_count{{{KSM}}}[$__rate_interval])) '
        f'* 3600', "Deploys/hour")], (12, y + 8, 12, 7),
        desc="Sample volume behind the percentiles above. A p99 computed from a "
             "handful of deploys an hour is noise, not a signal — check this before "
             "treating a latency change as a regression."))
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
            "**SLI 1 needs kube-state-metrics** running with "
            "`config/monitoring/kube-state-metrics-config.yaml` applied and "
            "`--custom-resource-state-config-file` set. Without it the "
            "`ome_inferenceservice_transition_status` panels are empty.\n\n"
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
        "Service-level indicators for OME deployments: did the InferenceService the "
        "user asked for converge, how long did it take, and were the components "
        "responsible for it actually running. Requires a long-retention Prometheus "
        "scraping both the OME control plane and kube-state-metrics.",
        [var_ds(),
         var_query("cluster", "Cluster", "label_values(up, cluster)"),
         var_query("namespace", "Namespace",
                   'label_values(ome_inferenceservice_transition_status'
                   '{cluster="$cluster"}, namespace)',
                   multi=True, include_all=True, all_value=".*")],
        p, ["ome", "slo", "on-call"])


# ---------------------------------------------------------------------------
# 5. Multi-cluster control plane — the management cluster running the OEP-0018
#    placer and the quota plane. Everything here is a generic signal:
#    controller-runtime, kube-state-metrics, or the apiserver's object counter.
#    Neither the placement stack nor the quota stack registers a custom metric
#    on main, so this dashboard can show that the control plane is running and
#    never that it is placing correctly. The coverage panel says so explicitly
#    rather than leaving the reader to infer it from absent panels.
# ---------------------------------------------------------------------------
def _mc_component(p, y, sel, ctrl, primary, rate, deploy, container):
    """One controller-runtime process, end to end: health, resources, behaviour.

    The controller manager and the quota plane are the same kind of thing -- a
    controller-runtime process with a webhook, a leader lease and a workqueue --
    so they are built from one function rather than two hand-kept copies that
    drift. Each gets its own row, because an operator asking "is the quota plane
    healthy" should not have to read half the answer from a shared row at the
    top of the page.

    `sel` selects the process, `ctrl` narrows to the controllers worth showing
    (empty for all of them), `primary` is the one controller whose outcome mix
    earns a panel, and `rate` is the window -- a process reconciling on a slow
    tick needs one wide enough to span it, or every rate reads zero and every
    histogram_quantile is NaN.
    """
    both = f"{sel},{ctrl}" if ctrl else sel

    # -- health ------------------------------------------------------------
    p.append(panel(
        "stat", "Ready",
        [target(f'max by (deployment) (kube_deployment_status_replicas_available'
                f'{{{MC_KSM},deployment="{deploy}"}})', "available"),
         target(f'max by (deployment) (kube_deployment_spec_replicas'
                f'{{{MC_KSM},deployment="{deploy}"}})', "desired")],
        (0, y, 8, 4), thresholds=[(None, "red"), (1, "green")],
        desc="From kube-state-metrics, so it still reports when the pod is down and "
             "therefore exporting nothing. available < desired is the signal."))

    p.append(panel(
        "stat", "Leader (must be 1)",
        [target(f'sum(leader_election_master_status{{{sel},name="{deploy}-leader-lock"}})')],
        (8, y, 8, 4), thresholds=[(None, "red"), (1, "green"), (2, "red")],
        desc="The gauge is 1 on the leader and 0 on each follower, so a correct sum is 1 at "
             "any replica count. Reading 2 means the pod is being collected twice, not that "
             "there are two leaders."))

    p.append(panel(
        "stat", "Container restarts (1h)",
        [target(f'sum(increase(kube_pod_container_status_restarts_total'
                f'{{{MC_KSM},container="{container}"}}[1h]))')],
        (16, y, 8, 4), thresholds=[(None, "green"), (1, "yellow"), (3, "red")],
        desc="Neither Deployment carries a PodDisruptionBudget or a priorityClassName, so "
             "eviction and OOM-kill surface here first."))
    y += 4

    # -- resources, per replica -------------------------------------------
    p.append(panel(
        "timeseries", "CPU (cores)",
        [target(f'sum by (pod) (rate(process_cpu_seconds_total{{{sel}}}[5m]))', "{{pod}}")],
        (0, y, 12, 7), decimals=3,
        desc="Absolute cores, one line per replica -- 1 means one core fully used. Taken "
             "from the process rather than cAdvisor because a container-name selector also "
             "matches unrelated operators that call their container \"manager\"."))

    p.append(panel(
        "timeseries", "Memory (RSS)",
        [target(f'sum by (pod) (process_resident_memory_bytes{{{sel}}})', "{{pod}}")],
        (12, y, 12, 7), unit="bytes",
        desc="One line per replica. Growth tracks the informer caches, which on a control "
             "plane scale with members x derived objects. A single replica climbing while "
             "its peers stay flat is the leader doing the work."))
    y += 7

    # -- behaviour ---------------------------------------------------------
    p.append(panel(
        "timeseries", "Reconcile rate by controller",
        [target(f'sum by (controller) (rate(controller_runtime_reconcile_total'
                f'{{{both}}}[{rate}]))', "{{controller}}")],
        (0, y, 8, 7), unit="reqps",
        desc="A controller with no activity in the window drops out rather than drawing a "
             "line at zero."))

    p.append(panel(
        "timeseries", f"Outcome mix ({primary})",
        [target(f'sum by (result) (rate(controller_runtime_reconcile_total'
                f'{{{sel},controller="{primary}"}}[{rate}]))', "{{result}}")],
        (8, y, 8, 7), unit="reqps", stacked=True,
        desc="requeue and requeue_after dominating with no success is retrying without "
             "converging."))

    p.append(panel(
        "timeseries", "Reconcile errors",
        [target(f'sum by (controller) (rate(controller_runtime_reconcile_errors_total'
                f'{{{both}}}[{rate}]))', "{{controller}}")],
        (16, y, 8, 7), unit="reqps",
        desc="There is no error taxonomy: every failure class collapses into this counter. "
             "The class is recoverable only from the Event reason or the logs."))
    y += 7

    p.append(panel(
        "timeseries", "Reconcile latency p99",
        [target('histogram_quantile(0.99, sum by (controller, le) '
                f'(rate(controller_runtime_reconcile_time_seconds_bucket'
                f'{{{both}}}[{rate}])))', "{{controller}}")],
        (0, y, 8, 7), unit="s",
        desc="Bounded by whatever timeout the pass runs under, so a wedged remote dependency "
             "reads as a normal-length pass. Read it with the outcome mix."))

    p.append(panel(
        "timeseries", "Oldest unfinished work",
        [target(f'max by (name) (workqueue_unfinished_work_seconds{{{sel}}})', "{{name}}")],
        (8, y, 8, 7), unit="s",
        desc="The stall detector. A monotonically climbing line is a wedged worker -- the "
             "failure mode the probes cannot catch, because neither checks reconcile "
             "progress."))

    p.append(panel(
        "timeseries", "Queue wait p99",
        [target('histogram_quantile(0.99, sum by (name, le) '
                f'(rate(workqueue_queue_duration_seconds_bucket{{{sel}}}[{rate}])))',
                "{{name}}")],
        (16, y, 8, 7), unit="s",
        desc="Time an item waits for a worker, as distinct from the time the work then "
             "takes. Rising wait with flat work duration is backpressure."))
    y += 7

    p.append(panel(
        "timeseries", "Worker saturation",
        [target(f'sum by (controller) (controller_runtime_active_workers{{{both}}}) '
                f'/ sum by (controller) (controller_runtime_max_concurrent_reconciles'
                f'{{{both}}})', "{{controller}}")],
        (0, y, 8, 7), unit="percentunit", minv=0, maxv=1,
        desc="Busy workers over the configured ceiling. These controllers run at "
             "MaxConcurrentReconciles=1, so one slow pass blocks the queue and this pins "
             "at 1 for as long as it lasts."))

    p.append(panel(
        "timeseries", "Apiserver calls by code",
        [target(f'sum by (code) (rate(rest_client_requests_total{{{sel}}}[{rate}]))',
                "{{code}}")],
        (8, y, 8, 7), unit="reqps", stacked=True,
        desc="409 is a status-write conflict, which is how contention between two writers "
             "of the same object shows up."))

    p.append(panel(
        "timeseries", "Webhook requests by code",
        [target(f'sum by (code) (rate(controller_runtime_webhook_requests_total'
                f'{{{sel}}}[{rate}]))', "{{code}}")],
        (16, y, 8, 7), unit="reqps", stacked=True,
        desc="Flat zero is expected where the role disables its webhooks. Denials are not "
             "visible here either way: a rejection is HTTP 200 with allowed=false, so it "
             "is indistinguishable from an approval."))
    y += 7
    return y


def mc_control_plane():
    p, y = [], 0

    p.append(row("📋 Managed resources", y)); y += 1

    p.append(panel(
        "timeseries", "WorkloadClusters",
        [target(f'max by (cluster) (apiserver_storage_objects'
                f'{{{MC_API},resource="workloadclusters.ome.io"}})', "{{cluster}}")],
        (0, y, 12, 7), decimals=0,
        desc="Members registered with each control plane -- the fleet this plane places "
             "onto. A count only: whether a member is reachable is not exported anywhere. "
             "The Ready condition exists on the CR but no metric carries it, so a member "
             "that has been unreachable for hours still counts here."))

    p.append(panel(
        "timeseries", "InferenceServices (all namespaces)",
        [target(f'max(apiserver_storage_objects'
                f'{{{MC_API},resource="inferenceservices.ome.io"}})', "inferenceservices")],
        (12, y, 12, 7), decimals=0,
        desc="Live object count from the apiserver, across all namespaces. A count, not a "
             "state breakdown: Ready vs failed vs in-progress is not available here."))
    y += 7

    p.append(row("🎛️ ome-controller-manager", y)); y += 1
    y = _mc_component(p, y, MC_MGR, MC_CTRL, "placement", "5m",
                      "ome-controller-manager", "manager")

    p.append(row("🧮 ome-quota-manager", y)); y += 1
    y = _mc_component(p, y, MC_QM, "", "acceleratorquota", MC_QM_RATE,
                      "ome-quota-manager", "quota-manager")

    p.append(row("📖 Coverage limits", y)); y += 1
    p.append({
        "type": "text",
        "title": "What this board cannot tell you, and the fix for each",
        "datasource": DS,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 11},
        "options": {"mode": "markdown", "content": (
            "Every panel above is a generic signal — controller-runtime, kube-state-metrics, "
            "or the apiserver. Neither the placement stack nor the quota stack registers a "
            "custom metric, so this board shows both processes are *running*, never that "
            "either is *correct*.\n\n"
            "| Gap | Why it is missing | Fix |\n"
            "|---|---|---|\n"
            "| InferenceService state — Ready, failed, in-progress, per namespace | The "
            "platform kube-state-metrics on a management cluster carries no OME "
            "`customResourceState` config, so every `ome_inferenceservice_*` series is "
            "absent. Only the raw object count above exists. | Add "
            "`config/monitoring/kube-state-metrics-config.yaml` to that KSM. Config only, "
            "no OME code change. |\n"
            "| Quota **correctness** — projection lag per member, chips nominal vs admitted "
            "vs borrowed, admission decisions by denial reason, tree structural health | The "
            "quota plane is scraped now, but `pkg/quota` registers no metrics of its own, so "
            "its row is entirely controller-runtime: it shows the loop turning, never that "
            "the numbers it produces are right. | Instrument `pkg/quota`. The raw values "
            "largely exist already — `status.clusters[]` carries applied-vs-materialized "
            "generations, and the capacity pass computes an unattributed figure it then "
            "discards. |\n"
            "| Whether quota accounting matches reality | Kueue does not gate serving pods "
            "on this fleet, so admitted counts read zero while accelerators are consumed. "
            "Every usage metric added today would read a healthy zero. | A pod-derived "
            "consumption metric split by whether Kueue governs the pod, so the two cases "
            "stop looking alike. |\n"
            "| The single-active invariant — two members serving one InferenceService | No "
            "metric, no event, no condition. The loser sweep returns void and swallows "
            "per-member failures. | Placement instrumentation. |\n"
            "| Per-member health | `WorkloadCluster.status.conditions[Ready]` is the only "
            "signal, is not exported to Prometheus, and reports True for the whole "
            "connection grace window while the member is already failing its probe. | A KSM "
            "`customResourceState` entry for WorkloadCluster, or placement "
            "instrumentation. |\n"
            "| Fan-out latency, race outcome, sweep lag, derived-object reaping | Computed "
            "by `tests/perf/multicluster` offline and nowhere in production. | "
            "Instrumentation. |\n\n"
            "Nothing on this board can page anyone: `prometheusRule.enabled` is false "
            "everywhere, no PrometheusRule object is deployed, and Alertmanager has no route "
            "or receiver.\n\n"
            "*The `cluster` variable is scoped to control planes running the placer, so a new "
            "tier appears here as soon as it is deployed, with no dashboard change.*"
        )},
    })

    return dashboard(
        "ome-mc-control-plane", "OME · Multi-cluster control plane",
        "Health of the multi-cluster control plane: the placer and the quota plane, each "
        "with its own health, resource and behaviour panels. Generic signals only — see the "
        "coverage-limits panel for what is deliberately absent.",
        [var_ds(),
         # Self-scoping: only clusters actually running the placer appear. Deliberately not
         # label_values(ome_isvc_status_update_total, cluster), which is what the control-plane
         # and scheduler dashboards use — that metric has no series on a management cluster,
         # because --multicluster-role=control-plane disables the reconciler that emits it.
         var_query("cluster", "Control plane",
                   'label_values(controller_runtime_reconcile_total'
                   '{controller="placement"}, cluster)',
                   multi=True, include_all=True, all_value=".*")],
        p, ["ome", "multicluster", "control-plane"])


# ---------------------------------------------------------------------------
# 6. Quota — accelerator capacity and how it is divided.
#
#    Its own dashboard rather than a row on the multi-cluster board, because
#    that board's cluster picker lists only clusters running the placer, and
#    the capacity and tenant usage live on the workload clusters instead.
#
#    Scoped by the quota manager's own Deployment, so a cluster appears here
#    exactly when it runs one, in either mode.
# ---------------------------------------------------------------------------
QUOTA = 'cluster=~"$cluster",endpoint="metrics"'

# Budget series, pinned to the workload plane. Both planes publish budgets and
# mean different things by them -- a management plane's nominal is the authored
# fleet total, a workload plane's is that cluster's share -- so with the picker
# on All, an unpinned sum would add a fleet total to the shares that make it up.
BUDGET = 'cluster=~"$cluster",endpoint="metrics",plane="workload"'



def _by_tenant(inner):
    """Label a budget aggregation by tree path, falling back to the object name.

    Two label_replace passes, and the order is the whole trick: the first always
    writes the quota name, the second overwrites it only where path is
    non-empty. That leaves a readable label for a node the tree cannot reach
    from the root, whose path is empty by construction, and on a cluster whose
    quota manager is older than the path label.
    """
    return (f'label_replace(label_replace({inner}, "tenant", "$1", "quota", "(.*)"), '
            f'"tenant", "$1", "path", "(.+)")')


def quota():
    p, y = [], 0

    p.append(row("🧮 Fleet accelerator capacity", y)); y += 1

    p.append(panel(
        "stat", "Allocatable",
        [target(f'sum by (resource, flavor) (ome_quota_capacity_allocatable{{{QUOTA}}})',
                "{{resource}} · {{flavor}}")],
        (0, y, 5, 4), decimals=0,
        desc="Accelerators on nodes that could accept work. Allocatable, not capacity, so "
             "it will not reconcile against kube_node_status_capacity."))

    p.append(panel(
        "stat", "Unavailable",
        [target(f'sum by (resource, flavor) (ome_quota_capacity_unavailable{{{QUOTA}}})',
                "{{resource}} · {{flavor}}")],
        (5, y, 5, 4), decimals=0,
        desc="Accelerators on nodes that matched a ResourceFlavor but are cordoned or not "
             "Ready. Budgets are sized against allocatable, so this is the parked "
             "remainder."))

    p.append(panel(
        "stat", "Parked",
        [target(f'sum by (resource, flavor) (ome_quota_capacity_unavailable{{{QUOTA}}}) / '
                f'clamp_min(sum by (resource, flavor) (ome_quota_capacity_allocatable{{{QUOTA}}}) '
                f'+ sum by (resource, flavor) (ome_quota_capacity_unavailable{{{QUOTA}}}), 1)',
                "{{resource}} · {{flavor}}")],
        (10, y, 5, 4), unit="percentunit",
        thresholds=[(None, "green"), (0.1, "yellow"), (0.25, "red")],
        desc="Share of matched accelerators that cannot accept work. clamp_min keeps a "
             "cluster with no capacity from dividing by zero."))

    p.append(panel(
        "stat", "Unattributed",
        [target(f'sum by (resource) (ome_quota_capacity_unattributed{{{QUOTA}}}) or vector(0)',
                "{{resource}}")],
        (15, y, 9, 4), decimals=0,
        thresholds=[(None, "green"), (1, "red")],
        desc="Accelerators no ResourceFlavor claims, so they fund no budget at all. Zero is "
             "the healthy reading. Non-zero means a missing or ambiguous ResourceFlavor, "
             "not a shrinking fleet -- and a budget written against that hardware reports "
             "CapacityExceeded with nothing else to say why.\n\n"
             "Broken out by resource but not by flavor: an unattributed accelerator is one "
             "no flavor claimed, so it has no flavor to report."))
    y += 4

    p.append(panel(
        "timeseries", "Allocatable vs unavailable",
        [target(f'sum by (resource, flavor) (ome_quota_capacity_allocatable{{{QUOTA}}})',
                "{{resource}} / {{flavor}} allocatable"),
         target(f'sum by (resource, flavor) (ome_quota_capacity_unavailable{{{QUOTA}}})',
                "{{resource}} / {{flavor}} unavailable")],
        (0, y, 12, 7), decimals=0,
        desc="Per (resource, flavor), which is the granularity Kueue keys quota by."))

    p.append(panel(
        "timeseries", "Nodes by state",
        [target(f'sum by (state) (ome_quota_capacity_nodes{{{QUOTA}}})', "{{state}}")],
        (12, y, 12, 7), decimals=0,
        desc="The node counts behind the accelerator totals. This is what separates a "
             "one-machine shortfall from a rack: the same missing accelerators look "
             "identical until you can see how many machines they came from."))
    y += 7

    p.append(row("📐 Distribution", y)); y += 1

    p.append(panel(
        "stat", "Governed",
        [target(f'sum(ome_quota_budget_nominal{{{BUDGET},role="ClusterQueue"}}) / '
                f'clamp_min(sum(ome_quota_capacity_allocatable{{{QUOTA}}}), 1)')],
        (0, y, 6, 4), unit="percentunit",
        desc="Share of the cluster's allocatable accelerators that any budget covers. Low "
             "means most of the fleet is running outside the quota system entirely, which "
             "is not visible from any per-tenant number."))

    p.append(panel(
        "stat", "Allocated",
        [target(f'sum(ome_quota_budget_nominal{{{BUDGET},role="ClusterQueue"}})')],
        (6, y, 6, 4), decimals=0,
        desc="Accelerators promised to tenants, summed over leaves. Leaves only: a cohort's "
             "nominal contains its children's, so counting both would double."))

    p.append(panel(
        "stat", "Admitted",
        [target(f'sum(ome_quota_budget_admitted{{{BUDGET},role="ClusterQueue"}})')],
        (12, y, 6, 4), decimals=0,
        desc="Accelerators actually admitted against the materialized queues, including "
             "anything borrowed."))

    p.append(panel(
        "stat", "Allocation used",
        [target(f'sum(ome_quota_budget_admitted{{{BUDGET},role="ClusterQueue"}}) / '
                f'clamp_min(sum(ome_quota_budget_nominal{{{BUDGET},role="ClusterQueue"}}), 1)')],
        (18, y, 6, 4), unit="percentunit",
        thresholds=[(None, "green"), (0.85, "yellow"), (0.95, "red")],
        desc="Admitted over allocated, across every tenant. This is headroom inside the "
             "quota system, which is a different question from how much of the fleet the "
             "system governs at all."))
    y += 4

    p.append(panel(
        "timeseries", "Allocated by tenant",
        [target(_by_tenant(
            f'sum by (quota, path) (ome_quota_budget_nominal{{{BUDGET},role="ClusterQueue"}})'),
            "{{tenant}}")],
        (0, y, 24, 7), decimals=0, stacked=True,
        desc="How the tree divides the fleet. Stacked, so the top of the stack is everything "
             "promised and the bands are who it was promised to.\n\n"
             "Labelled by root-to-node path, because a bare object name does not say which "
             "parent a tenant hangs off and two tenants under different parents are "
             "distinguishable only by their ancestry. A band labelled with a plain name "
             "carries no path: either the node is unreachable from the root, or the cluster "
             "reporting it predates the path label."))
    y += 7

    p.append(row("📊 Tenant utilization", y)); y += 1

    p.append(panel(
        "timeseries", "Utilization by tenant",
        [target(f'sum by (quota) (ome_quota_budget_admitted{{{BUDGET},role="ClusterQueue"}}) / '
                f'clamp_min(sum by (quota) (ome_quota_budget_nominal{{{BUDGET},role="ClusterQueue"}}), 1)',
                "{{quota}}")],
        (0, y, 8, 7), unit="percentunit", minv=0,
        desc="Admitted over its own allowance. A tenant walking up to 1 over an hour is the "
             "shape this board exists to make visible; above 1 means it is borrowing."))

    p.append(panel(
        "timeseries", "Reserved but not admitted",
        [target(f'sum by (quota) (ome_quota_budget_reserved{{{BUDGET},role="ClusterQueue"}}) - '
                f'sum by (quota) (ome_quota_budget_admitted{{{BUDGET},role="ClusterQueue"}})',
                "{{quota}}")],
        (8, y, 8, 7), decimals=0,
        desc="Accelerators a tenant owns but has not started using. Without this a tenant "
             "sitting at its ceiling reads as idle, because admitted alone cannot tell "
             "'not running' from 'not entitled'."))

    p.append(panel(
        "timeseries", "Borrowed by tenant",
        [target(f'sum by (quota) (ome_quota_budget_borrowed{{{BUDGET},role="ClusterQueue"}})',
                "{{quota}}")],
        (16, y, 8, 7), decimals=0,
        desc="Admitted above a tenant's own nominal, taken from idle siblings and "
             "reclaimable on contention. One leaf borrowing its whole cohort is the "
             "saturation case that is invisible from any single tenant's numbers."))
    y += 7

    p.append(row("🏷️ Attribution", y)); y += 1

    p.append(panel(
        "timeseries", "Unattributed accelerators by reason",
        [target(f'sum by (reason) (ome_quota_capacity_unattributed{{{QUOTA}}}) or vector(0)',
                "{{reason}}")],
        (0, y, 12, 7), decimals=0,
        desc="A flat zero line is healthy. NoMatchingFlavor means no ResourceFlavor's node "
             "labels matched; AmbiguousFlavor means two matched equally well, so counting "
             "the node into either would be a guess. The two need different fixes, which is "
             "why they are not merged."))

    p.append(panel(
        "timeseries", "Unattributed nodes by reason",
        [target(f'sum by (reason) (ome_quota_capacity_unattributed_nodes{{{QUOTA}}}) or vector(0)',
                "{{reason}}")],
        (12, y, 12, 7), decimals=0,
        desc="Distinct machines, not node-resource pairs: a node advertising two unclaimed "
             "resources counts once. Read with the panel to its left to tell one badly "
             "labelled machine from a whole pool."))
    y += 7

    p.append(row("🔧 Materializer", y)); y += 1

    p.append(panel(
        "timeseries", "Kueue objects written",
        [target(f'sum by (cluster) (rate(ome_quota_backend_applied_total{{{QUOTA}}}[30m]))',
                "{{cluster}}")],
        (0, y, 12, 7), unit="reqps",
        desc="Writes are server-side applies issued unconditionally, so this tracks pass "
             "frequency times render size, not how often the tree changed. A step change "
             "means the tree grew or the resync interval moved."))

    p.append(panel(
        "timeseries", "Kueue objects reaped",
        [target(f'sum by (trigger) (rate(ome_quota_backend_swept_total{{{QUOTA}}}[30m])) or vector(0)',
                "{{trigger}}")],
        (12, y, 12, 7), unit="reqps",
        desc="materialize is routine orphan collection when the tree no longer names an "
             "object; finalize is a node being deleted. Flat zero is the steady state -- a "
             "sweep that finds nothing is not recorded, so this stays at zero rather than "
             "going blank."))
    y += 7

    p.append(row("📖 Coverage limits", y)); y += 1
    p.append({
        "type": "text",
        "title": "What this board cannot tell you yet",
        "datasource": DS,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 8},
        "options": {"mode": "markdown", "content": (
            "Capacity, and how the tree divides it. Cohort rows are absent from the tenant "
            "panels on purpose: a cohort's nominal contains its children's, so a sum over "
            "both would double-count. What is still missing:\n\n"
            "| Gap | Fix |\n"
            "|---|---|\n"
            "| Where a tenant's share landed across members | `status.budgets[].perCluster` "
            "carries it; no metric exports it yet. |\n"
            "| Whether the accounting matches the hardware | Kueue's admitted figure and a "
            "pod-derived count are independent views of the same accelerators; nothing "
            "compares them. |\n"
            "| Tree structure — parent, depth, violations by reason | The path label lands "
            "with the next quota-manager rollout, and until then a tenant band is labelled "
            "with its bare name. The rest has no metric, so a rollup still cannot be shown "
            "complete rather than assumed. |\n\n"
            "Nothing here can page anyone: no PrometheusRule is deployed on any cluster and "
            "Alertmanager has no receiver.\n\n"
            "*The `cluster` picker lists workload clusters only. A management plane holds "
            "the authored fleet tree and has no local silicon, so every panel here would "
            "be empty for one.*"
        )},
    })

    return dashboard(
        "ome-quota", "OME · Quota",
        "Accelerator capacity and how the quota tree divides it. Capacity is measured on "
        "each workload cluster by its own quota manager; a management plane holds the "
        "authored fleet tree and reports no local capacity.",
        [var_ds(),
         # Scoped to clusters that run a capacity pass, which is workload mode only --
         # a management plane holds the authored fleet tree and has no local silicon, so
         # listing it here would offer a selection whose every capacity panel is empty.
         #
         # Matched on the whole ome_quota_capacity_* family rather than on allocatable
         # alone: a cluster with no ResourceFlavor reports everything as unattributed and
         # emits no allocatable series at all, and that is precisely the cluster an
         # operator needs to be able to select.
         var_query("cluster", "Cluster",
                   'label_values({__name__=~"ome_quota_capacity_.*"}, cluster)',
                   multi=True, include_all=True, all_value=".*")],
        p, ["ome", "quota"])


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for build in (control_plane, scheduler, router, slo, mc_control_plane, quota):
        target.n = 0
        d = build()
        path = OUT / f"{d['uid']}.json"
        path.write_text(json.dumps(d, indent=2) + "\n")
        panels = [p for p in d["panels"] if p["type"] != "row"]
        print(f"wrote {path.relative_to(pathlib.Path(__file__).parent.parent)} "
              f"({len(panels)} panels)")


if __name__ == "__main__":
    main()
