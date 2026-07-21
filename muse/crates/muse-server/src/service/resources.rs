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

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use kube::Resource;
use ome_client::{
    k8s_openapi::api::core::v1::Pod, listers::Lister, ClusterServingRuntime, InferenceService,
    ServingRuntime,
};
use serde::Serialize;
use serde_json::Value;

use super::error::ApiError;
use crate::{
    app_context::AppState,
    informers::ClusterState,
    protocol::{
        sum_accel, tagged, BaseModelDetail, BaseModelView, FineTunedWeightView,
        InferenceServiceDetail, InferenceServiceView, ListParams, Listing, NodeDetail, NodePod,
        NodeView, ResolvedRuntimeRefView, ResourceSummary, RuntimeDetail, RuntimeView, Topology,
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
            "inferenceservices" => isvc_rows(c),
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
            "inferenceservices" => isvc_detail(c, &name, params.namespace.as_deref(), raw),
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

/// Pod label carrying the owning InferenceService's name; the operator stamps it
/// on every serving pod it creates.
const INFERENCE_SERVICE_POD_LABEL: &str = "ome.io/inferenceservice";

/// Pod label carrying the resolved runtime's name. For an auto-selected runtime
/// (no `spec.runtime`) this label is the only record of the operator's choice —
/// nothing on the InferenceService itself names it.
const SERVING_RUNTIME_POD_LABEL: &str = "serving-runtime";

/// List rows for one cluster's InferenceServices, each with its runtime identity
/// resolved against the runtime caches. Rows resolve from the spec only — the
/// pod-label fallback would cost a pod-cache scan per row, so it is reserved for
/// the single-object detail view.
fn isvc_rows(c: &ClusterState) -> Vec<Value> {
    c.inference_services
        .list()
        .iter()
        .map(|isvc| {
            let mut view = InferenceServiceView::of(isvc);
            view.resolved_runtime_ref =
                resolved_runtime_ref(isvc, &c.serving_runtimes, &c.cluster_serving_runtimes, None);
            tagged(view, &c.name)
        })
        .collect()
}

/// Detail for one named InferenceService (or the raw object when `raw`), with its
/// runtime identity resolved from the spec or — for auto-selected runtimes — from
/// the labels of the service's cached pods.
fn isvc_detail(c: &ClusterState, name: &str, namespace: Option<&str>, raw: bool) -> Option<Value> {
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
        tagged(detail, &c.name)
    })
}

/// The runtime name an InferenceService uses, with where it came from: the spec
/// when declared there (`"spec"`), else the runtime label on any of the service's
/// cached pods (`"pods"`, when a pod lister is supplied).
fn runtime_name(
    isvc: &InferenceService,
    pods: Option<&Lister<Pod>>,
) -> Option<(String, &'static str)> {
    let declared = isvc
        .spec
        .runtime
        .as_ref()
        .map(|r| r.name.clone())
        .filter(|n| !n.is_empty());
    if let Some(name) = declared {
        return Some((name, "spec"));
    }
    let namespace = isvc.meta().namespace.as_deref()?;
    let isvc_name = isvc.meta().name.as_deref()?;
    pods?
        .list_namespaced(namespace)
        .iter()
        .filter(|p| {
            p.meta()
                .labels
                .as_ref()
                .and_then(|l| l.get(INFERENCE_SERVICE_POD_LABEL))
                .map(String::as_str)
                == Some(isvc_name)
        })
        .find_map(|p| {
            p.meta()
                .labels
                .as_ref()?
                .get(SERVING_RUNTIME_POD_LABEL)
                .cloned()
                .filter(|n| !n.is_empty())
        })
        .map(|name| (name, "pods"))
}

/// Resolve which runtime object an InferenceService's runtime name denotes, the
/// way the operator does: a namespaced ServingRuntime wins over a
/// ClusterServingRuntime with the same name, and the kind declared on the spec is
/// never consulted (the operator ignores it too). `None` when the name matches
/// neither cache — the spec-declared `runtime`/`runtimeRef` fields still carry
/// the declaration for such rows.
fn resolved_runtime_ref(
    isvc: &InferenceService,
    runtimes: &Lister<ServingRuntime>,
    cluster_runtimes: &Lister<ClusterServingRuntime>,
    pods: Option<&Lister<Pod>>,
) -> Option<ResolvedRuntimeRefView> {
    let (name, source) = runtime_name(isvc, pods)?;
    if let Some(ns) = isvc.meta().namespace.as_deref() {
        if runtimes.get_namespaced(ns, &name).is_some() {
            return Some(ResolvedRuntimeRefView {
                kind: "ServingRuntime",
                name,
                namespace: Some(ns.to_owned()),
                source,
            });
        }
    }
    cluster_runtimes
        .get(&name)
        .is_some()
        .then_some(ResolvedRuntimeRefView {
            kind: "ClusterServingRuntime",
            name,
            namespace: None,
            source,
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
    use kube::runtime::{reflector, reflector::Lookup, watcher};
    use ome_client::InferenceService;
    use serde_json::json;

    use super::*;

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

    fn lister_of<K>(objects: Vec<K>) -> Lister<K>
    where
        K: Lookup<DynamicType = ()> + Resource<DynamicType = ()> + Clone + 'static,
    {
        let (store, mut writer) = reflector::store::<K>();
        for object in objects {
            writer.apply_watcher_event(&watcher::Event::Apply(object));
        }
        Lister::new(store)
    }

    /// An ISVC whose spec declares `runtime` (with a deliberately wrong kind, to
    /// prove the declared kind is never consulted), or omits it entirely.
    fn isvc_with_runtime(namespace: &str, name: &str, runtime: Option<&str>) -> InferenceService {
        let spec = match runtime {
            Some(r) => json!({ "runtime": { "name": r, "kind": "ClusterServingRuntime" } }),
            None => json!({}),
        };
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": name, "namespace": namespace },
            "spec": spec
        }))
        .expect("InferenceService fixture should deserialize")
    }

    fn serving_runtime(namespace: &str, name: &str) -> ServingRuntime {
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "ServingRuntime",
            "metadata": { "name": name, "namespace": namespace },
            "spec": {}
        }))
        .expect("ServingRuntime fixture should deserialize")
    }

    fn cluster_serving_runtime(name: &str) -> ClusterServingRuntime {
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "ClusterServingRuntime",
            "metadata": { "name": name },
            "spec": {}
        }))
        .expect("ClusterServingRuntime fixture should deserialize")
    }

    /// A serving pod as the operator labels it: owner ISVC + resolved runtime.
    fn serving_pod(namespace: &str, name: &str, isvc: &str, runtime: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    INFERENCE_SERVICE_POD_LABEL: isvc,
                    SERVING_RUNTIME_POD_LABEL: runtime,
                }
            }
        }))
        .expect("Pod fixture should deserialize")
    }

    #[test]
    fn spec_runtime_name_collision_resolves_to_namespaced_runtime() {
        let runtimes = lister_of(vec![serving_runtime("team-a", "vllm")]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let isvc = isvc_with_runtime("team-a", "svc", Some("vllm"));

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, None)
            .expect("collision should resolve");
        assert_eq!(resolved.kind, "ServingRuntime");
        assert_eq!(resolved.name, "vllm");
        assert_eq!(resolved.namespace.as_deref(), Some("team-a"));
        assert_eq!(resolved.source, "spec");
    }

    #[test]
    fn spec_runtime_name_without_namespaced_match_resolves_to_cluster_runtime() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let isvc = isvc_with_runtime("team-a", "svc", Some("vllm"));

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, None)
            .expect("cluster-scoped runtime should resolve");
        assert_eq!(resolved.kind, "ClusterServingRuntime");
        assert!(resolved.namespace.is_none());
        assert_eq!(resolved.source, "spec");
    }

    #[test]
    fn auto_selected_runtime_resolves_from_the_owned_pod_labels() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let pods = lister_of(vec![
            // Another service's pod in the same namespace must not win.
            serving_pod("team-a", "other-0", "other", "sglang"),
            serving_pod("team-a", "svc-0", "svc", "vllm"),
        ]);
        let isvc = isvc_with_runtime("team-a", "svc", None);

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, Some(&pods))
            .expect("pod-labelled runtime should resolve");
        assert_eq!(resolved.kind, "ClusterServingRuntime");
        assert_eq!(resolved.name, "vllm");
        assert_eq!(resolved.source, "pods");
    }

    #[test]
    fn unresolvable_runtime_identity_is_absent() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of::<ClusterServingRuntime>(vec![]);

        // No spec.runtime and no pods to read a label from.
        let auto = isvc_with_runtime("team-a", "svc", None);
        let empty_pods = lister_of::<Pod>(vec![]);
        assert!(
            resolved_runtime_ref(&auto, &runtimes, &cluster_runtimes, Some(&empty_pods)).is_none()
        );

        // A declared name matching neither runtime cache.
        let named = isvc_with_runtime("team-a", "svc", Some("missing"));
        assert!(resolved_runtime_ref(&named, &runtimes, &cluster_runtimes, None).is_none());
    }

    #[test]
    fn list_rows_never_resolve_from_pods() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let isvc = isvc_with_runtime("team-a", "svc", None);

        // Without a pod lister — the list-row path — an auto-selected runtime
        // stays unresolved even though the runtime it would name is cached.
        assert!(resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, None).is_none());
    }
}
