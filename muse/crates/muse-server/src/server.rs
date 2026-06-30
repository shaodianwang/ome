//! API router, shared state, the middleware stack, and the server lifecycle.
//!
//! Multi-cluster: state holds one `ClusterState` (a set of listers) per workload
//! cluster. Reads aggregate across all clusters by default, or one cluster via
//! `?cluster=<name>`; every item carries a `cluster` field.

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    routing::get,
    Json, Router,
};
use kube::Resource;
use ome_client::{
    informers::SharedInformerFactory,
    k8s_openapi::api::core::v1::{Node, Pod},
    listers::Lister,
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, WorkloadCluster,
};
use serde::Serialize;
use serde_json::{json, Value};
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    request_id::{PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use crate::{
    capacity,
    config::Config,
    error::ApiError,
    health,
    observe::{make_span, track_metrics, MakeRequestUuidV7},
    views::{sum_gpu, InferenceServiceView, NodeView, ResourceSummary, Topology},
};

/// One workload cluster's informer-backed listers.
pub struct ClusterState {
    pub name: String,
    inference_services: Lister<InferenceService>,
    base_models: Lister<BaseModel>,
    serving_runtimes: Lister<ServingRuntime>,
    inference_replicas: Lister<InferenceReplica>,
    cluster_base_models: Lister<ClusterBaseModel>,
    cluster_serving_runtimes: Lister<ClusterServingRuntime>,
    fine_tuned_weights: Lister<FineTunedWeight>,
    workload_clusters: Lister<WorkloadCluster>,
    nodes: Lister<Node>,
    pods: Lister<Pod>,
}

/// Build every informer for one cluster (awaiting initial sync).
pub async fn build_cluster_state(name: String, factory: &SharedInformerFactory) -> ClusterState {
    ClusterState {
        name,
        inference_services: factory.inference_services().await,
        base_models: factory.base_models().await,
        serving_runtimes: factory.serving_runtimes().await,
        inference_replicas: factory.inference_replicas().await,
        cluster_base_models: factory.cluster_base_models().await,
        cluster_serving_runtimes: factory.cluster_serving_runtimes().await,
        fine_tuned_weights: factory.fine_tuned_weights().await,
        workload_clusters: factory.workload_clusters().await,
        nodes: factory.lister::<Node>().await,
        pods: factory.lister::<Pod>().await,
    }
}

/// Shared state: every cluster's listers plus the topology labels. Cheap to clone
/// (Arc + small struct).
#[derive(Clone)]
pub struct AppState {
    clusters: Arc<Vec<ClusterState>>,
    topology: Topology,
}

impl AppState {
    pub fn new(clusters: Vec<ClusterState>, topology: Topology) -> Self {
        Self {
            clusters: Arc::new(clusters),
            topology,
        }
    }

    /// Clusters matching `cluster` (one named cluster, or all when None).
    fn select(&self, cluster: Option<&str>) -> Result<Vec<&ClusterState>, ApiError> {
        match cluster {
            Some(name) => self
                .clusters
                .iter()
                .find(|c| c.name == name)
                .map(|c| vec![c])
                .ok_or_else(|| ApiError::NotFound(format!("unknown cluster: {name}"))),
            None => Ok(self.clusters.iter().collect()),
        }
    }
}

/// Assemble the router and its middleware stack. The last `.layer()` is the
/// outermost at request time, so the effective order is:
/// CORS → compression → timeout → request-id → trace → metrics → handler.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/livez", get(health::livez))
        .route("/readyz", get(health::readyz))
        .route("/v1/clusters", get(list_clusters))
        .route("/v1/capacity", get(capacity_report))
        .route("/v1/{kind}", get(list_kind))
        .route("/v1/{kind}/{name}", get(get_kind))
        .with_state(state)
        .layer(middleware::from_fn(track_metrics))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(make_span)
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuidV7))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
}

/// Bind the API listener and serve until a shutdown signal.
pub async fn serve(config: &Config, state: AppState) -> anyhow::Result<()> {
    let app = build_app(state);
    let addr = config.api_addr();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "muse read-plane listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Resolve on Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => tracing::warn!(error = %err, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => tracing::info!("received Ctrl-C, shutting down"),
        () = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}

/// Query params common to the aggregating endpoints.
#[derive(Debug, Default, serde::Deserialize)]
struct ListParams {
    /// Restrict to a single cluster by name.
    cluster: Option<String>,
}

/// Query params for `GET /v1/{kind}/{name}`.
#[derive(Debug, Default, serde::Deserialize)]
struct ViewParams {
    /// Return the full underlying object instead of the projected view.
    #[serde(default)]
    raw: bool,
    /// Restrict the lookup to a single cluster by name.
    cluster: Option<String>,
}

/// `GET /v1/clusters` — the registered clusters and a per-cluster summary.
async fn list_clusters(State(st): State<AppState>) -> Json<Value> {
    let items: Vec<Value> = st
        .clusters
        .iter()
        .map(|c| {
            let nodes = c.nodes.list();
            let gpu_nodes = nodes
                .iter()
                .filter(|n| {
                    n.status
                        .as_ref()
                        .and_then(|s| s.allocatable.as_ref())
                        .map_or(0, sum_gpu)
                        > 0
                })
                .count();
            json!({
                "name": c.name,
                "nodes": nodes.len(),
                "gpuNodes": gpu_nodes,
                "inferenceServices": c.inference_services.list().len(),
            })
        })
        .collect();
    Json(json!({ "count": items.len(), "items": items }))
}

/// `GET /v1/capacity` — fleet GPU capacity (allocatable/allocated/free) grouped
/// by cluster, island, NVLink domain, instance type. `?cluster=` narrows it.
async fn capacity_report(
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let selected = st.select(params.cluster.as_deref())?;
    let data: Vec<capacity::ClusterData> = selected
        .iter()
        .map(|c| capacity::ClusterData {
            name: c.name.clone(),
            nodes: c.nodes.list(),
            pods: c.pods.list(),
        })
        .collect();
    Ok(Json(to_val(capacity::build(&data, &st.topology))))
}

/// `GET /v1/{kind}` — projected overview rows from the informer caches, across
/// all clusters (or one via `?cluster=`); each row carries its `cluster`.
async fn list_kind(
    Path(kind): Path<String>,
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let selected = st.select(params.cluster.as_deref())?;
    let mut items: Vec<Value> = Vec::new();
    for c in selected {
        let part: Vec<Value> = match kind.as_str() {
            "inferenceservices" => map_view(
                &c.name,
                c.inference_services.list(),
                InferenceServiceView::of,
            ),
            "basemodels" => map_view(&c.name, c.base_models.list(), ResourceSummary::of),
            "servingruntimes" => map_view(&c.name, c.serving_runtimes.list(), ResourceSummary::of),
            "inferencereplicas" => {
                map_view(&c.name, c.inference_replicas.list(), ResourceSummary::of)
            }
            "clusterbasemodels" => {
                map_view(&c.name, c.cluster_base_models.list(), ResourceSummary::of)
            }
            "clusterservingruntimes" => map_view(
                &c.name,
                c.cluster_serving_runtimes.list(),
                ResourceSummary::of,
            ),
            "finetunedweights" => {
                map_view(&c.name, c.fine_tuned_weights.list(), ResourceSummary::of)
            }
            "workloadclusters" => {
                map_view(&c.name, c.workload_clusters.list(), ResourceSummary::of)
            }
            "nodes" => {
                let allocated = capacity::gpu_allocated_by_node(&c.pods.list());
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
                        tagged(NodeView::of(n.as_ref(), used, &st.topology), &c.name)
                    })
                    .collect()
            }
            other => return Err(ApiError::NotFound(format!("unknown kind: {other}"))),
        };
        items.extend(part);
    }
    Ok(Json(
        json!({ "kind": kind, "count": items.len(), "items": items }),
    ))
}

/// `GET /v1/{kind}/{name}` — projected detail from cache; `?raw=true` for the
/// full object, `?cluster=` to disambiguate. First match across clusters wins.
async fn get_kind(
    Path((kind, name)): Path<(String, String)>,
    Query(params): Query<ViewParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let raw = params.raw;
    let selected = st.select(params.cluster.as_deref())?;
    for c in selected {
        let found = match kind.as_str() {
            "inferenceservices" => one(
                &c.name,
                c.inference_services.get(&name),
                raw,
                InferenceServiceView::of,
            ),
            "basemodels" => one(&c.name, c.base_models.get(&name), raw, ResourceSummary::of),
            "servingruntimes" => one(
                &c.name,
                c.serving_runtimes.get(&name),
                raw,
                ResourceSummary::of,
            ),
            "inferencereplicas" => one(
                &c.name,
                c.inference_replicas.get(&name),
                raw,
                ResourceSummary::of,
            ),
            "clusterbasemodels" => one(
                &c.name,
                c.cluster_base_models.get(&name),
                raw,
                ResourceSummary::of,
            ),
            "clusterservingruntimes" => one(
                &c.name,
                c.cluster_serving_runtimes.get(&name),
                raw,
                ResourceSummary::of,
            ),
            "finetunedweights" => one(
                &c.name,
                c.fine_tuned_weights.get(&name),
                raw,
                ResourceSummary::of,
            ),
            "workloadclusters" => one(
                &c.name,
                c.workload_clusters.get(&name),
                raw,
                ResourceSummary::of,
            ),
            "nodes" => c.nodes.get(&name).map(|n| {
                if raw {
                    tagged(&*n, &c.name)
                } else {
                    let used = capacity::gpu_allocated_by_node(&c.pods.list())
                        .get(&name)
                        .copied()
                        .unwrap_or(0);
                    tagged(NodeView::of(n.as_ref(), used, &st.topology), &c.name)
                }
            }),
            other => return Err(ApiError::NotFound(format!("unknown kind: {other}"))),
        };
        if let Some(value) = found {
            return Ok(Json(value));
        }
    }
    Err(ApiError::NotFound(format!("{kind}/{name} not found")))
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

/// Serialize `view` to JSON and inject the owning `cluster` field.
fn tagged<T: Serialize>(view: T, cluster: &str) -> Value {
    let mut value = to_val(view);
    if let Value::Object(map) = &mut value {
        map.insert("cluster".to_owned(), Value::String(cluster.to_owned()));
    }
    value
}

fn to_val<T: Serialize>(value: T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}
