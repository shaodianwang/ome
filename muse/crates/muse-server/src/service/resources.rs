//! The generic resource endpoints: `GET /v1/{kind}` (projected overview rows) and
//! `GET /v1/{kind}/{name}` (projected detail, or the full object with `?raw=true`).
//!
//! Every OME kind is declared once in the [`ome_kinds!`] table — its URL plural,
//! its `ClusterState` lister, and its list/detail projections — and both endpoints
//! dispatch through it, so a new kind is wired up in exactly one place. Two kinds
//! are handled separately because their views join other caches: Nodes need
//! accelerator use joined from the Pods, and InferenceServices need their runtime
//! identity resolved against the runtime caches (and, on detail, the Pods).
//! Both endpoints fan out across the selected clusters, tagging each row with its
//! owning cluster; an unknown kind is a 404.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    Json,
};
use kube::Resource;
use ome_client::{k8s_openapi::api::core::v1::Pod, listers::Lister};
use serde::Serialize;
use serde_json::Value;

use super::{error::ApiError, runtime_ref::resolved_runtime_ref};
use crate::{
    app_context::AppState,
    informers::ClusterState,
    protocol::{
        isvc_owner, pod_accelerators, sum_accel, tagged, BaseModelDetail, BaseModelView,
        FineTunedWeightView, InferenceServiceDetail, InferenceServiceView, ListParams, Listing,
        NodeDetail, NodePod, NodeView, ResourceSummary, RuntimeDetail, RuntimeView, Topology,
        ViewParams,
    },
    rollup,
};

/// Declare the served OME kinds once, and generate the two per-cluster dispatchers
/// (`list_for` / `detail_for`) from that single table. Each row is:
/// `"<url-plural>" => <ClusterState lister field> : <list view>, <detail view>;`
macro_rules! ome_kinds {
    ($($plural:literal => $field:ident : $list:expr, $detail:expr;)*) => {
        /// Projected list rows for one kind on one cluster; `None` if `kind` is not
        /// a table-served OME kind (the caller handles the special-cased kinds
        /// before consulting the table, else 404s).
        fn list_for(kind: &str, c: &ClusterState) -> Option<Vec<Value>> {
            match kind {
                $($plural => Some(map_view(&c.name, c.$field.list(), $list)),)*
                _ => None,
            }
        }

        /// Projected detail (or raw object) for one named kind on one cluster. Outer
        /// `None` = not a served OME kind; inner `None` = kind served but not found.
        fn detail_for(
            kind: &str,
            name: &str,
            namespace: Option<&str>,
            raw: bool,
            c: &ClusterState,
        ) -> Option<Option<Value>> {
            match kind {
                $($plural => Some(one(
                    &c.name,
                    lookup_resource(&c.$field, name, namespace),
                    raw,
                    $detail,
                )),)*
                _ => None,
            }
        }
    };
}

ome_kinds! {
    "basemodels"             => base_models              : BaseModelView::of,          BaseModelDetail::of;
    "servingruntimes"        => serving_runtimes         : RuntimeView::of,            RuntimeDetail::of;
    "inferencereplicas"      => inference_replicas       : ResourceSummary::of,        ResourceSummary::of;
    "clusterbasemodels"      => cluster_base_models      : BaseModelView::of_cluster,  BaseModelDetail::of_cluster;
    "clusterservingruntimes" => cluster_serving_runtimes : RuntimeView::of_cluster,    RuntimeDetail::of_cluster;
    "finetunedweights"       => fine_tuned_weights       : FineTunedWeightView::of,    FineTunedWeightView::of;
    "workloadclusters"       => workload_clusters        : ResourceSummary::of,        ResourceSummary::of;
}

/// `GET /v1/{kind}` — projected overview rows from the informer caches, across
/// all clusters (or one via `?cluster=`); each row carries its `cluster`.
pub async fn list_kind(
    Path(kind): Path<String>,
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Listing>, ApiError> {
    let selected = st.select(params.cluster.as_deref())?;
    let mut items: Vec<Value> = Vec::new();
    for c in selected {
        let rows = match kind.as_str() {
            "nodes" => node_rows(c, &st.topology),
            "inferenceservices" => isvc_rows(c, &st),
            _ => list_for(&kind, c)
                .ok_or_else(|| ApiError::NotFound(format!("unknown kind: {kind}")))?,
        };
        items.extend(rows);
    }
    Ok(Json(Listing::of_kind(kind, items)))
}

/// `GET /v1/{kind}/{name}` — projected detail from cache; `?raw=true` for the
/// full object, `?cluster=` and `?namespace=` to disambiguate. Namespace-less
/// requests retain the legacy first-name-match behavior.
pub async fn get_kind(
    Path((kind, name)): Path<(String, String)>,
    Query(params): Query<ViewParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let raw = params.raw;
    let selected = st.select(params.cluster.as_deref())?;
    for c in selected {
        let found = match kind.as_str() {
            "nodes" => node_detail(c, &name, raw, &st.topology),
            "inferenceservices" => isvc_detail(c, &name, params.namespace.as_deref(), raw, &st),
            _ => detail_for(&kind, &name, params.namespace.as_deref(), raw, c)
                .ok_or_else(|| ApiError::NotFound(format!("unknown kind: {kind}")))?,
        };
        if let Some(value) = found {
            return Ok(Json(value));
        }
    }
    Err(ApiError::NotFound(format!("{kind}/{name} not found")))
}

/// Resolve a cached object by full namespaced identity when supplied. Falling
/// back to name-only keeps existing API URLs working while new console links are
/// exact. Cluster-scoped resources naturally use the name-only path.
pub(crate) fn lookup_resource<K>(
    lister: &Lister<K>,
    name: &str,
    namespace: Option<&str>,
) -> Option<Arc<K>>
where
    K: Resource<DynamicType = ()> + Clone + 'static,
{
    match namespace {
        Some(namespace) => lister.get_namespaced(namespace, name),
        None => lister.get(name),
    }
}

/// List rows for one cluster's InferenceServices, each with its runtime identity
/// resolved against the runtime caches and the accelerator chips its pods hold
/// (one pod-cache scan for the whole listing). Runtime identity resolves from the
/// spec only — the pod-label fallback would cost a pod scan per row, so it is
/// reserved for the single-object detail view. Callers select synchronized
/// clusters, so a missing pod entry means zero occupancy.
fn isvc_rows(c: &ClusterState, state: &AppState) -> Vec<Value> {
    let held = accel_held_by_isvc(&c.pods.list());
    c.inference_services
        .list()
        .iter()
        .map(|isvc| {
            let mut view = InferenceServiceView::of(isvc);
            view.resolved_runtime_ref =
                resolved_runtime_ref(isvc, &c.serving_runtimes, &c.cluster_serving_runtimes, None);
            view.accelerators = Some(
                held.get(&(
                    isvc.meta().namespace.clone().unwrap_or_default(),
                    view.name.clone(),
                ))
                .copied()
                .unwrap_or(0),
            );
            view.delivery = state
                .argo()
                .and_then(|argo| argo.delivery_for(isvc.as_ref(), "ome.io", "InferenceService"));
            tagged(view, &c.name)
        })
        .collect()
}

/// Accelerator chips held per (namespace, InferenceService) by scheduled,
/// non-terminal pods carrying the ownership label.
fn accel_held_by_isvc(pods: &[Arc<Pod>]) -> BTreeMap<(String, String), i64> {
    let mut out: BTreeMap<(String, String), i64> = BTreeMap::new();
    for pod in pods {
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
        if matches!(phase, Some("Succeeded" | "Failed")) {
            continue;
        }
        if pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .is_none()
        {
            continue;
        }
        let Some(owner) = isvc_owner(pod) else {
            continue;
        };
        let chips = pod_accelerators(pod);
        if chips > 0 {
            *out.entry((
                pod.metadata.namespace.clone().unwrap_or_default(),
                owner.to_owned(),
            ))
            .or_insert(0) += chips;
        }
    }
    out
}

/// Detail for one named InferenceService (or the raw object when `raw`), with its
/// runtime identity resolved from the spec or — for auto-selected runtimes — from
/// the labels of the service's cached pods.
fn isvc_detail(
    c: &ClusterState,
    name: &str,
    namespace: Option<&str>,
    raw: bool,
    state: &AppState,
) -> Option<Value> {
    lookup_resource(&c.inference_services, name, namespace).map(|isvc| {
        if raw {
            return tagged(&*isvc, &c.name);
        }
        let mut detail = InferenceServiceDetail::of(&isvc);
        detail.view.resolved_runtime_ref = resolved_runtime_ref(
            &isvc,
            &c.serving_runtimes,
            &c.cluster_serving_runtimes,
            Some(&c.pods),
        );
        if let Some(ns) = isvc.meta().namespace.as_deref() {
            detail.view.accelerators = Some(
                accel_held_by_isvc(&c.pods.list_namespaced(ns))
                    .get(&(ns.to_owned(), detail.view.name.clone()))
                    .copied()
                    .unwrap_or(0),
            );
        }
        detail.view.delivery = state
            .argo()
            .and_then(|argo| argo.delivery_for(isvc.as_ref(), "ome.io", "InferenceService"));
        tagged(detail, &c.name)
    })
}

/// List rows for one cluster's Nodes, each tagged with the accelerator count
/// currently claimed by Pods scheduled on it.
fn node_rows(c: &ClusterState, topo: &Topology) -> Vec<Value> {
    let allocated = rollup::accel_allocated_by_node(&c.pods.list());
    c.nodes
        .list()
        .iter()
        .map(|n| {
            let used = n
                .meta()
                .name
                .as_deref()
                .and_then(|name| allocated.get(name))
                .copied()
                .unwrap_or(0);
            tagged(NodeView::of(n.as_ref(), used, topo), &c.name)
        })
        .collect()
}

/// Detail for one named Node on one cluster (or the raw object when `raw`): the
/// overview plus conditions, taints, and the pods scheduled on it.
fn node_detail(c: &ClusterState, name: &str, raw: bool, topo: &Topology) -> Option<Value> {
    c.nodes.get(name).map(|n| {
        if raw {
            return tagged(&*n, &c.name);
        }
        let pods = c.pods.list();
        let used = rollup::accel_allocated_by_node(&pods)
            .get(name)
            .copied()
            .unwrap_or(0);
        let mut on_node: Vec<NodePod> = pods
            .iter()
            .filter(|p| {
                p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(name)
                    && !matches!(
                        p.status.as_ref().and_then(|s| s.phase.as_deref()),
                        Some("Succeeded" | "Failed")
                    )
            })
            .map(|p| {
                let accel: i64 = p
                    .spec
                    .as_ref()
                    .map(|s| {
                        s.containers
                            .iter()
                            .filter_map(|ctr| ctr.resources.as_ref())
                            .filter_map(|r| r.requests.as_ref())
                            .map(|r| sum_accel(r, "gpu") + sum_accel(r, "tpu"))
                            .sum()
                    })
                    .unwrap_or(0);
                NodePod {
                    namespace: p.metadata.namespace.clone().unwrap_or_default(),
                    name: p.metadata.name.clone().unwrap_or_default(),
                    accelerators: (accel > 0).then_some(accel),
                    deployment: isvc_owner(p).map(str::to_owned),
                }
            })
            .collect();
        on_node.sort_by(|a, b| {
            b.accelerators
                .unwrap_or(0)
                .cmp(&a.accelerators.unwrap_or(0))
                .then(a.namespace.cmp(&b.namespace))
                .then(a.name.cmp(&b.name))
        });
        tagged(NodeDetail::of(n.as_ref(), used, on_node, topo), &c.name)
    })
}

/// Project a cached list through `view`, tagging each row with its cluster.
fn map_view<K, V, F>(cluster: &str, items: Vec<Arc<K>>, view: F) -> Vec<Value>
where
    V: Serialize,
    F: Fn(&K) -> V,
{
    items
        .iter()
        .map(|o| tagged(view(o.as_ref()), cluster))
        .collect()
}

/// Project a single cached object (or the full object when `raw`), tagged with cluster.
fn one<K, V, F>(cluster: &str, obj: Option<Arc<K>>, raw: bool, view: F) -> Option<Value>
where
    K: Serialize,
    V: Serialize,
    F: Fn(&K) -> V,
{
    obj.map(|o| {
        if raw {
            tagged(&*o, cluster)
        } else {
            tagged(view(o.as_ref()), cluster)
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kube::runtime::{reflector, watcher};
    use ome_client::InferenceService;
    use serde_json::json;

    use super::*;
    use crate::{
        app_context::AppStateConfig,
        informers::{CacheState, CapacitySignal, Feed, Invalidations},
        protocol::INFERENCE_SERVICE_POD_LABEL,
    };

    fn cached<K: Resource<DynamicType = ()> + Clone + 'static>(items: Vec<K>) -> Lister<K> {
        let (store, mut writer) = reflector::store::<K>();
        for item in items {
            writer.apply_watcher_event(&watcher::Event::Apply(item));
        }
        Lister::new(store)
    }

    fn state(services: Vec<InferenceService>, pods: Vec<Pod>) -> AppState {
        let cluster = ClusterState {
            name: "test".to_owned(),
            version: String::new(),
            client: kube::Client::try_from(kube::Config::new(
                "http://127.0.0.1:1".parse().unwrap(),
            ))
            .unwrap(),
            inference_services: cached(services),
            base_models: cached(vec![]),
            serving_runtimes: cached(vec![]),
            inference_replicas: cached(vec![]),
            cluster_base_models: cached(vec![]),
            cluster_serving_runtimes: cached(vec![]),
            fine_tuned_weights: cached(vec![]),
            workload_clusters: cached(vec![]),
            nodes: cached(vec![]),
            pods: cached(pods),
            deployments: cached(vec![]),
            daemon_sets: cached(vec![]),
            cache: CacheState::default(),
        };
        AppState::new(
            vec![cluster],
            Topology {
                island_labels: vec![],
                nvlink_labels: vec![],
            },
            Feed::new(1),
            Invalidations::new(None),
            CapacitySignal::new(),
            None,
            AppStateConfig {
                metrics: None,
                pod_inspect: None,
                pod_log_stream: None,
                refresh: Duration::from_secs(1),
                sse_keep_alive: None,
                attention_stuck_step_secs: None,
                capacity_limits: rollup::CapacityLimits {
                    max_fragments: 1,
                    max_consumers: 1,
                },
                dashboard_policy: None,
                metrics_time_ranges: None,
                system_components: Arc::new(vec![]),
                argo: None,
            },
        )
    }

    fn pod(namespace: &str, name: &str, phase: &str, scheduled: bool) -> Pod {
        let mut pod: Pod = serde_json::from_value(json!({
            "metadata": {
                "name": name, "namespace": namespace,
                "labels": { (INFERENCE_SERVICE_POD_LABEL): "shared" }
            },
            "spec": { "containers": [{
                "name": "engine", "resources": { "requests": { "google.com/tpu": "4" } }
            }] },
            "status": { "phase": phase }
        }))
        .unwrap();
        if scheduled {
            pod.spec.as_mut().unwrap().node_name = Some("worker".to_owned());
        }
        pod
    }

    async fn projected(state: &AppState) -> (Value, Value) {
        let list = list_kind(
            Path("inferenceservices".to_owned()),
            Query(ListParams::default()),
            State(state.clone()),
        )
        .await
        .unwrap();
        let detail = get_kind(
            Path(("inferenceservices".to_owned(), "shared".to_owned())),
            Query(ViewParams {
                namespace: Some("team-a".to_owned()),
                ..Default::default()
            }),
            State(state.clone()),
        )
        .await
        .unwrap();
        (
            serde_json::to_value(list.0).unwrap()["items"][0].clone(),
            detail.0,
        )
    }

    #[tokio::test]
    async fn terminating_service_stays_visible_with_zero_occupancy_and_raw_status() {
        let service: InferenceService = serde_json::from_value(json!({
            "metadata": {
                "name": "shared", "namespace": "team-a",
                "deletionTimestamp": "2026-09-01T00:00:00Z",
                "finalizers": ["foregroundDeletion"]
            },
            "spec": {},
            "status": {
                "conditions": [{
                    "type": "Ready", "status": "True", "reason": "Ready", "message": "",
                    "lastTransitionTime": "2026-08-01T00:00:00Z"
                }],
                "components": { "engine": { "autoscaler": { "currentReplicas": 62 } } }
            }
        }))
        .unwrap();
        let state = state(vec![service], vec![]);
        state.clusters[0].cache.mark_ready();
        let (list, detail) = projected(&state).await;
        for view in [list, detail] {
            assert_eq!(view["deletionTimestamp"], "2026-09-01T00:00:00Z");
            assert_eq!(view["ready"], false);
            assert_eq!(view["reason"], "Terminating");
            assert_eq!(view["accelerators"], 0);
        }
        let raw = get_kind(
            Path(("inferenceservices".to_owned(), "shared".to_owned())),
            Query(ViewParams {
                raw: true,
                namespace: Some("team-a".to_owned()),
                ..Default::default()
            }),
            State(state),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(raw["status"]["conditions"][0]["status"], "True");
        assert_eq!(
            raw["status"]["components"]["engine"]["autoscaler"]["currentReplicas"],
            62
        );
        assert_eq!(raw["metadata"]["finalizers"], json!(["foregroundDeletion"]));
    }

    #[tokio::test]
    async fn occupancy_counts_draining_pods_but_excludes_terminal_unscheduled_and_other_owners() {
        let mut service = isvc("team-a");
        let timestamp = serde_json::from_value(json!("2026-09-01T00:00:00Z")).unwrap();
        service.metadata.deletion_timestamp = Some(timestamp);
        let mut draining = pod("team-a", "draining", "Running", true);
        draining.metadata.deletion_timestamp = service.metadata.deletion_timestamp.clone();
        let mut other_owner = pod("team-a", "other", "Running", true);
        other_owner.metadata.labels = None;
        let state = state(
            vec![service],
            vec![
                draining,
                pod("team-a", "starting", "Pending", true),
                pod("team-a", "finished", "Succeeded", true),
                pod("team-a", "failed", "Failed", true),
                pod("team-a", "queued", "Pending", false),
                pod("team-b", "same-owner-name", "Running", true),
                other_owner,
            ],
        );
        state.clusters[0].cache.mark_ready();
        let (list, detail) = projected(&state).await;
        assert_eq!(list["accelerators"], 8);
        assert_eq!(detail["accelerators"], 8);
    }

    #[tokio::test]
    async fn unavailable_inventory_is_not_reported_as_zero() {
        let state = state(vec![isvc("team-a")], vec![]);
        assert!(matches!(
            list_kind(
                Path("inferenceservices".to_owned()),
                Query(ListParams::default()),
                State(state.clone()),
            )
            .await,
            Err(ApiError::Unavailable(_))
        ));
        assert!(matches!(
            get_kind(
                Path(("inferenceservices".to_owned(), "shared".to_owned())),
                Query(ViewParams {
                    cluster: Some("test".to_owned()),
                    ..Default::default()
                }),
                State(state.clone()),
            )
            .await,
            Err(ApiError::Unavailable(_))
        ));
        state.clusters[0].cache.mark_ready();
        let (list, detail) = projected(&state).await;
        for view in [list, detail] {
            assert_eq!(view["accelerators"], 0);
            assert!(view.get("deletionTimestamp").is_none());
        }
    }

    fn isvc(namespace: &str) -> InferenceService {
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "shared", "namespace": namespace },
            "spec": {}
        }))
        .expect("minimal InferenceService should deserialize")
    }

    #[test]
    fn namespaced_lookup_disambiguates_duplicate_names() {
        let (store, mut writer) = reflector::store::<InferenceService>();
        writer.apply_watcher_event(&watcher::Event::Apply(isvc("team-a")));
        writer.apply_watcher_event(&watcher::Event::Apply(isvc("team-b")));
        let lister = Lister::new(store);

        let selected = lookup_resource(&lister, "shared", Some("team-b"))
            .expect("team-b resource should exist");
        assert_eq!(selected.meta().namespace.as_deref(), Some("team-b"));
        assert!(lookup_resource(&lister, "shared", Some("missing")).is_none());
        assert!(lookup_resource(&lister, "shared", None).is_some());
    }
}
