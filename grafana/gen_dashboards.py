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

DS = {"type": "prometheus", "uid": "${datasource}"}

# Threshold steps, mirroring MUSE's dashboardPolicy. `None` is the base step.
T_NONE = [(None, "text")]


def steps(pairs):
    return [{"color": c, "value": v} for v, c in pairs]


def target(expr, legend=None, instant=False):
    t = {"datasource": DS, "expr": expr, "refId": chr(65 + target.n % 26)}
    target.n += 1
    if legend:
        t["legendFormat"] = legend
    if instant:
        t["instant"] = True
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
    p.append(panel("timeseries", "Per-revision Services by role", [target(
        f'sum by (role) (ome_omenative_per_revision_service_total{{{job}}})', "{{role}}")],
        (0, y, 8, 7),
        desc="Kubernetes Services OMENative currently generates per revision, split by "
             "role (engine / router / decoder). Step changes track rollouts."))
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

    p.append(row("Managed resources", y)); y += 1
    p.append(panel("timeseries", "InferenceService transition status", [target(
        f'sum by (status) (ome_inferenceservice_transition_status{{{job}}})', "{{status}}")],
        (0, y, 8, 7), desc="ISVCs by transition state. A state that stops draining is a "
                           "stuck rollout."))
    p.append(panel("timeseries", "BaseModel state", [
        target(f'sum by (state) (ome_basemodel_state{{{job}}})', "namespaced/{{state}}"),
        target(f'sum by (state) (ome_clusterbasemodel_state{{{job}}})', "cluster/{{state}}"),
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


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for build in (control_plane, scheduler, router):
        target.n = 0
        d = build()
        path = OUT / f"{d['uid']}.json"
        path.write_text(json.dumps(d, indent=2) + "\n")
        panels = [p for p in d["panels"] if p["type"] != "row"]
        print(f"wrote {path.relative_to(pathlib.Path(__file__).parent.parent)} "
              f"({len(panels)} panels)")


if __name__ == "__main__":
    main()
