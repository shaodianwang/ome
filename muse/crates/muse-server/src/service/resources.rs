//! The generic resource endpoints: `GET /v1/{kind}` (projected overview rows) and
//! `GET /v1/{kind}/{name}` (projected detail, or the full object with `?raw=true`).
//!
//! Every OME kind is declared once in the [`ome_kinds!`] table — its URL plural,
//! its `ClusterState` lister, and its list/detail projections — and both endpoints
//! dispatch through it, so a new kind is wired up in exactly one place. Nodes are
//! handled separately: their views need accelerator use joined from the Pods.
//! Both endpoints fan out across the selected clusters, tagging each row with its
//! owning cluster; an unknown kind is a 404.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use kube::Resource;
use serde::Serialize;
use serde_json::Value;

use super::error::ApiError;
use crate::{
    app_context::AppState,
    informers::ClusterState,
    protocol::{
        sum_accel, tagged, BaseModelDetail, BaseModelView, FineTunedWeightView,
        InferenceServiceDetail, InferenceServiceView, ListParams, Listing, NodeDetail, NodePod,
        NodeView, ResourceSummary, RuntimeDetail, RuntimeView, Topology, ViewParams,
    },
    rollup,
};

/// Declare the served OME kinds once, and generate the two per-cluster dispatchers
/// (`list_for` / `detail_for`) from that single table. Each row is:
/// `"<url-plural>" => <ClusterState lister field> : <list view>, <detail view>;`
macro_rules! ome_kinds {
    ($($plural:literal => $field:ident : $list:expr, $detail:expr;)*) => {
        /// Projected list rows for one kind on one cluster; `None` if `kind` is not
        /// a served OME kind (the caller then tries Nodes, else 404s).
        fn list_for(kind: &str, c: &ClusterState) -> Option<Vec<Value>> {
            match kind {
                $($plural => Some(map_view(&c.name, c.$field.list(), $list)),)*
                _ => None,
            }
        }

        /// Projected detail (or raw object) for one named kind on one cluster. Outer
        /// `None` = not a served OME kind; inner `None` = kind served but not found.
        fn detail_for(kind: &str, name: &str, raw: bool, c: &ClusterState) -> Option<Option<Value>> {
            match kind {
                $($plural => Some(one(&c.name, c.$field.get(name), raw, $detail)),)*
                _ => None,
            }
        }
    };
}

ome_kinds! {
    "inferenceservices"      => inference_services       : InferenceServiceView::of,  InferenceServiceDetail::of;
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
            _ => list_for(&kind, c)
                .ok_or_else(|| ApiError::NotFound(format!("unknown kind: {kind}")))?,
        };
        items.extend(rows);
    }
    Ok(Json(Listing::of_kind(kind, items)))
}

/// `GET /v1/{kind}/{name}` — projected detail from cache; `?raw=true` for the
/// full object, `?cluster=` to disambiguate. First match across clusters wins.
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
            _ => detail_for(&kind, &name, raw, c)
                .ok_or_else(|| ApiError::NotFound(format!("unknown kind: {kind}")))?,
        };
        if let Some(value) = found {
            return Ok(Json(value));
        }
    }
    Err(ApiError::NotFound(format!("{kind}/{name} not found")))
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
