# OME Monitoring Assets

Standalone monitoring artifacts for OME. These are not shipped via Helm because they target cluster-wide platform components (Grafana, kube-state-metrics) that are typically managed by a separate platform/SRE team rather than co-deployed with the operator.

## Files

| File | Purpose |
|---|---|
| `ome-controller-health-dashboard.json` | Minimal Grafana dashboard — controller-runtime metrics only, no KSM dependency. Install first. |
| `ome-overview-dashboard.json` | Full Grafana dashboard — adds Resource Inventory panels that consume `ome_*` metrics from kube-state-metrics. |
| `kube-state-metrics-config.yaml` | `CustomResourceStateMetrics` ConfigMap that surfaces OME CR state as `ome_*` metrics |

## Which dashboard to install when

| Stage | Install |
|---|---|
| ServiceMonitor / PodMonitor scraping the controller | `ome-controller-health-dashboard.json` — works immediately, 7 panels of controller health + API/runtime data |
| Above + kube-state-metrics wired with the CR config | `ome-overview-dashboard.json` — adds 4 Resource Inventory stat panels (ISVC/BaseModel/ServingRuntime state) |

Both can coexist in Grafana — they have distinct UIDs (`ome-controller-health` vs `ome-overview`).

## Prerequisites

To get the most out of the dashboard, the cluster needs:

1. **Prometheus scraping the OME controller** — see `charts/ome-resources/templates/ome-controller/servicemonitor.yaml` (gated on `.Values.serviceMonitor.enabled`) and `charts/ome-resources/templates/model-agent-daemonset/podmonitor.yaml` (`.Values.podMonitor.enabled`). Without these (or equivalent pod-annotation scraping), the controller health panels show "No data."
2. **kube-state-metrics with the config below mounted** — without this, the Resource Inventory panels (ISVC count by state, BaseModel count, etc.) show "No data."

## Installing the dashboard

Pick one path depending on how your Grafana is managed:

**Grafana UI import:**
1. Grafana → Dashboards → New → Import
2. Upload one (or both) of the dashboard JSON files or paste its contents
3. Select your Prometheus datasource when prompted

**Grafana provisioning (file-based):**
Copy the JSON into the directory referenced by your Grafana provisioning config (`/etc/grafana/provisioning/dashboards/`).

**Grafana sidecar (`kiwigrid/k8s-sidecar`) or grafana-operator:**
Wrap the JSON in a ConfigMap with the discovery label (`grafana_dashboard: "1"` is the default):

```sh
# Per-dashboard ConfigMap. Repeat for each JSON file you want loaded.
kubectl create configmap ome-dashboard-controller-health \
  --from-file=ome-controller-health-dashboard.json \
  --dry-run=client -o yaml \
  | kubectl label -f- --local -o yaml --dry-run=client grafana_dashboard=1 \
  | kubectl apply -n <grafana-namespace> -f -

kubectl create configmap ome-dashboard-overview \
  --from-file=ome-overview-dashboard.json \
  --dry-run=client -o yaml \
  | kubectl label -f- --local -o yaml --dry-run=client grafana_dashboard=1 \
  | kubectl apply -n <grafana-namespace> -f -
```

## Installing the kube-state-metrics CR config

Most clusters run a single cluster-wide KSM. Wire OME's CR config into it:

```sh
# 1. Apply the ConfigMap (uncomment the namespace: line first, or pass -n)
kubectl apply -n <ksm-namespace> -f kube-state-metrics-config.yaml

# 2. Mount the ConfigMap on the KSM pod. With the prometheus-operator
#    kube-prometheus-stack chart, set:
#
#    kube-state-metrics:
#      extraArgs:
#        - "--custom-resource-state-config-file=/etc/ksm-custom-config/config.yaml"
#      volumes:
#        - name: ome-ksm-config
#          configMap:
#            name: ome-kube-state-metrics-config
#      volumeMounts:
#        - name: ome-ksm-config
#          mountPath: /etc/ksm-custom-config
#          readOnly: true
#
# 3. KSM needs read access to ome.io CRs — add the ome-monitoring
#    ClusterRole bindings (not shipped in this dir; create matching
#    your KSM's RBAC convention).
```

## What's emitted

See the file headers of each YAML/JSON file for the full metric list. Summary:

**From OME controller (via `ServiceMonitor`):**
- `controller_runtime_reconcile_total{controller, result}`
- `controller_runtime_reconcile_errors_total{controller}`
- `controller_runtime_reconcile_time_seconds_bucket{controller, le}`
- `workqueue_depth{name}`, `workqueue_retries_total{name}`, `workqueue_adds_total{name}`
- `rest_client_requests_total{code, method, host}`
- `leader_election_master_status`
- `go_*`, `process_*` (Go runtime + process)

**From kube-state-metrics (via this directory's `kube-state-metrics-config.yaml`):**
- `ome_inferenceservice_{info,created,transition_status,active_model_state,target_model_state}`
- `ome_basemodel_{info,created,state}`
- `ome_clusterbasemodel_{info,created,state}`
- `ome_servingruntime_{info,created}`
- `ome_clusterservingruntime_{info,created}`

**Not emitted yet (declared in OEPs, not yet in code):**
- `ome_omenative_rollout_group_*` — see [OEP-0007.1](../../oeps/0007.1-omenative-coordinated-rollout/oep.yaml)
- `ome_traffic_*`, `ome_rollout_*`, `ome_backend_policy_*` — see [OEP-0011](../../oeps/0011-inference-traffic-policy/oep.yaml)

When those metrics ship in code, extend `ome-overview-dashboard.json` or add new dashboards alongside it.
