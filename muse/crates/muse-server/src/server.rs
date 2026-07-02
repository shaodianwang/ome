//! API router, shared state, the middleware stack, and the server lifecycle.
//!
//! Multi-cluster: state holds one `ClusterState` (a set of listers) per workload
//! cluster. Reads aggregate across all clusters by default, or one cluster via
//! `?cluster=<name>`; every item carries a `cluster` field.

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    fmt::Debug,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use kube::Resource;
use ome_client::{
    informers::{InformerEvent, Prune, SharedInformerFactory},
    k8s_openapi::api::core::v1::{Node, Pod},
    listers::Lister,
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, WorkloadCluster,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    request_id::{PropagateRequestIdLayer, SetRequestIdLayer},
    services::{ServeDir, ServeFile},
    timeout::TimeoutLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use crate::{
    capacity,
    config::Config,
    error::ApiError,
    feed::{Feed, FeedEvent},
    health,
    observe::{make_span, track_metrics, MakeRequestUuidV7},
    views::{
        node_accel, BaseModelDetail, BaseModelView, FineTunedWeightView, InferenceServiceDetail,
        InferenceServiceView, NodeView, ResourceSummary, RolloutView, Topology,
    },
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
    /// Flipped to `true` once every informer for this cluster has finished its
    /// first sync. Listers serve (empty, then filling) before this is set, so the
    /// console can render a "syncing" cluster immediately.
    pub ready: Arc<AtomicBool>,
}

/// A future that resolves when one informer's cache has completed its first sync.
type ReadyFut = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Build every informer for one cluster, returning immediately (caches warm in
/// the background) along with a readiness future per started informer.
///
/// `ome` is the set of `ome.io/v1beta1` resource names this cluster actually
/// serves (from discovery). A kind whose CRD is absent gets an empty lister and
/// no informer — otherwise it would 404-loop on its initial list forever. This is
/// the common case for member clusters, which serve the workload CRDs
/// (InferenceService, BaseModel, …) but not WorkloadCluster, the registry CRD
/// that lives only on the hub. Nodes/pods (core/v1, always present) are watched
/// unconditionally, so the cluster still contributes capacity with no OME CRDs.
///
/// Nothing here blocks on initial sync: the returned [`ClusterState`] serves
/// (empty, then filling) right away, and the caller awaits the returned futures
/// in the background to learn when the cluster is fully warm.
pub fn build_cluster_state(
    name: String,
    factory: &SharedInformerFactory,
    ome: &HashSet<String>,
    feed: &Feed,
    signal: &CapacitySignal,
) -> (ClusterState, Vec<ReadyFut>) {
    let mut readies = Vec::new();
    let cluster = name.clone();
    // OME kinds feed the activity stream. Nodes/pods drive capacity, so they
    // signal the recompute task on change instead (never the high-volume feed).
    let f = |kind| FeedSub {
        feed: feed.clone(),
        cluster: cluster.clone(),
        kind,
    };
    let state = ClusterState {
        inference_services: warm(
            factory,
            present::<InferenceService>(ome),
            &mut readies,
            Some(f("InferenceService")),
            None,
        ),
        base_models: warm(
            factory,
            present::<BaseModel>(ome),
            &mut readies,
            Some(f("BaseModel")),
            None,
        ),
        serving_runtimes: warm(
            factory,
            present::<ServingRuntime>(ome),
            &mut readies,
            Some(f("ServingRuntime")),
            None,
        ),
        inference_replicas: warm(
            factory,
            present::<InferenceReplica>(ome),
            &mut readies,
            Some(f("InferenceReplica")),
            None,
        ),
        cluster_base_models: warm(
            factory,
            present::<ClusterBaseModel>(ome),
            &mut readies,
            Some(f("ClusterBaseModel")),
            None,
        ),
        cluster_serving_runtimes: warm(
            factory,
            present::<ClusterServingRuntime>(ome),
            &mut readies,
            Some(f("ClusterServingRuntime")),
            None,
        ),
        fine_tuned_weights: warm(
            factory,
            present::<FineTunedWeight>(ome),
            &mut readies,
            Some(f("FineTunedWeight")),
            None,
        ),
        workload_clusters: warm(
            factory,
            present::<WorkloadCluster>(ome),
            &mut readies,
            Some(f("WorkloadCluster")),
            None,
        ),
        nodes: warm::<Node>(factory, true, &mut readies, None, Some(signal.clone())),
        pods: warm::<Pod>(factory, true, &mut readies, None, Some(signal.clone())),
        name,
        ready: Arc::new(AtomicBool::new(false)),
    };
    (state, readies)
}

/// Wiring for a kind's activity-feed subscriber: where to push, and how to label.
struct FeedSub {
    feed: Feed,
    cluster: String,
    kind: &'static str,
}

/// Start (lazily) the informer for kind `K` if the cluster serves it, returning
/// its live-but-warming lister and pushing a readiness future onto `readies`. A
/// kind the cluster doesn't serve gets an empty lister and starts no informer.
/// When `feed` is set, a background task forwards the informer's change events
/// into the activity feed. When `on_change` is set (nodes/pods), each event marks
/// the capacity rollup stale so the recompute task reacts to real changes.
fn warm<K>(
    factory: &SharedInformerFactory,
    present: bool,
    readies: &mut Vec<ReadyFut>,
    feed: Option<FeedSub>,
    on_change: Option<CapacitySignal>,
) -> Lister<K>
where
    K: Resource<DynamicType = ()>
        + Clone
        + DeserializeOwned
        + Debug
        + Send
        + Sync
        + Prune
        + 'static,
{
    if !present {
        return empty_lister::<K>();
    }
    let informer = factory.informer_lazy::<K>();
    if let Some(signal) = on_change {
        let mut rx = informer.subscribe();
        tokio::spawn(async move {
            // Any add/update/delete (or a lag) means capacity may have changed;
            // mark stale (coalesced by the recompute task). Loop ends when the
            // channel closes (pattern stops matching).
            while let Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) =
                rx.recv().await
            {
                signal.mark();
            }
        });
    }
    if let Some(sub) = feed {
        let mut rx = informer.subscribe();
        tokio::spawn(async move {
            // uid -> last-seen generation. We surface creates, deletes, and spec
            // changes (generation bumps) only. A repeated Apply whose generation is
            // unchanged is status churn (heartbeats, conditions, replica counts) —
            // dropping it keeps the feed and the SSE stream quiet when nothing
            // meaningful happens, so clients don't refetch on every status write.
            let mut seen: HashMap<String, Option<i64>> = HashMap::new();
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let (action, obj) = match &event {
                            InformerEvent::Applied(o) => ("applied", o),
                            InformerEvent::Deleted(o) => ("deleted", o),
                        };
                        let meta = obj.meta();
                        let uid = meta.uid.clone().unwrap_or_else(|| {
                            format!(
                                "{}/{}",
                                meta.namespace.clone().unwrap_or_default(),
                                meta.name.clone().unwrap_or_default()
                            )
                        });
                        let emit = if action == "deleted" {
                            seen.remove(&uid);
                            true
                        } else {
                            match seen.get(&uid) {
                                Some(prev) if *prev == meta.generation => false,
                                _ => {
                                    seen.insert(uid, meta.generation);
                                    true
                                }
                            }
                        };
                        if !emit {
                            continue;
                        }
                        sub.feed.push(FeedEvent {
                            cluster: sub.cluster.clone(),
                            kind: sub.kind.to_owned(),
                            namespace: meta.namespace.clone(),
                            name: meta.name.clone().unwrap_or_default(),
                            action,
                            at: crate::feed::now_secs(),
                        });
                    }
                    // A lagging subscriber skips ahead; a closed channel ends the task.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    let store = informer.store();
    readies.push(Box::pin(async move {
        let _ = store.wait_until_ready().await;
    }));
    informer.lister()
}

/// Whether the cluster serves kind `K`, matched by its plural resource name
/// against the discovered `ome.io` resource set.
fn present<K: Resource<DynamicType = ()>>(ome: &HashSet<String>) -> bool {
    ome.contains(K::plural(&()).as_ref())
}

/// An empty, never-updated lister for a kind a cluster doesn't serve. No watcher
/// is started, so it stays empty rather than 404-looping on a missing CRD.
fn empty_lister<K>() -> Lister<K>
where
    K: Resource + Clone + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
{
    let (store, _writer) = kube::runtime::reflector::store::<K>();
    Lister::new(store)
}

/// Shared state: every cluster's listers plus the topology labels. Cheap to clone
/// (Arc + small struct).
/// A coalescing "capacity is stale" signal, fired by the Node/Pod informers when
/// they observe a change and awaited by the background recompute task. Multiple
/// marks between waits collapse into one (a burst of pod churn ⇒ one recompute).
#[derive(Clone)]
pub struct CapacitySignal {
    notify: Arc<tokio::sync::Notify>,
}

impl CapacitySignal {
    pub fn new() -> Self {
        Self {
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
    /// Mark capacity stale (cheap; safe to call on every informer event).
    pub fn mark(&self) {
        self.notify.notify_one();
    }
    async fn wait(&self) {
        self.notify.notified().await;
    }
}

impl Default for CapacitySignal {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct AppState {
    clusters: Arc<Vec<ClusterState>>,
    topology: Topology,
    feed: Feed,
    /// Materialized capacity: the fleet rollup and each per-cluster rollup, kept
    /// current by a background task that recomputes on Node/Pod changes. Serving
    /// from this keeps `/v1/capacity` latency flat as the fleet grows, instead of
    /// re-walking every Node × Pod on each request.
    capacity: Arc<RwLock<Arc<CapacitySnapshot>>>,
    signal: CapacitySignal,
    /// Whether the inference test proxy (`POST …/chat`) is enabled.
    chat_enabled: bool,
    /// HTTP client for the inference test proxy (timeout baked in). Cheap to clone.
    http: reqwest::Client,
}

/// Pre-computed capacity responses (already serialized to JSON).
struct CapacitySnapshot {
    fleet: Value,
    per_cluster: HashMap<String, Value>,
}

impl AppState {
    pub fn new(
        clusters: Vec<ClusterState>,
        topology: Topology,
        feed: Feed,
        signal: CapacitySignal,
        chat_enabled: bool,
        chat_timeout: Duration,
    ) -> Self {
        // Client for the inference test proxy. `unwrap_or_default` (not unwrap):
        // a builder failure falls back to a default client rather than panicking.
        let http = reqwest::Client::builder()
            .timeout(chat_timeout)
            .build()
            .unwrap_or_default();
        let state = Self {
            clusters: Arc::new(clusters),
            topology,
            feed,
            capacity: Arc::new(RwLock::new(Arc::new(CapacitySnapshot {
                fleet: Value::Null,
                per_cluster: HashMap::new(),
            }))),
            signal,
            chat_enabled,
            http,
        };
        // Warm the cache once so the first request is served, not computed.
        state.refresh_capacity();
        state
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

    /// Recompute the capacity rollups (fleet + per-cluster) from the current
    /// informer caches and publish them. Called on a timer by the background
    /// refresher; walking the caches is CPU-only (no apiserver traffic).
    fn refresh_capacity(&self) {
        let all: Vec<capacity::ClusterData> = self
            .clusters
            .iter()
            .map(|c| capacity::ClusterData {
                name: c.name.clone(),
                nodes: c.nodes.list(),
                pods: c.pods.list(),
            })
            .collect();
        let fleet = to_val(capacity::build(&all, &self.topology));
        let per_cluster = all
            .iter()
            .map(|cd| {
                let report = capacity::build(std::slice::from_ref(cd), &self.topology);
                (cd.name.clone(), to_val(report))
            })
            .collect();
        *self.capacity.write().unwrap_or_else(|e| e.into_inner()) =
            Arc::new(CapacitySnapshot { fleet, per_cluster });
    }

    /// The cached capacity response for one cluster, or the fleet rollup.
    fn capacity_snapshot(&self, cluster: Option<&str>) -> Result<Value, ApiError> {
        let snap = self
            .capacity
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        match cluster {
            None => Ok(snap.fleet.clone()),
            Some(name) => snap
                .per_cluster
                .get(name)
                .cloned()
                .ok_or_else(|| ApiError::NotFound(format!("unknown cluster: {name}"))),
        }
    }
}

/// Assemble the router and its middleware stack. The last `.layer()` is the
/// outermost at request time, so the effective order is:
/// CORS → compression → timeout → request-id → trace → metrics → handler.
pub fn build_app(state: AppState, web_dir: Option<PathBuf>) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/livez", get(health::livez))
        .route("/readyz", get(health::readyz))
        .route("/v1/clusters", get(list_clusters))
        .route("/v1/capacity", get(capacity_report))
        .route("/v1/events", get(list_events))
        .route("/v1/stream", get(stream_events))
        .route("/v1/rollouts", get(list_rollouts))
        .route("/v1/config", get(get_config))
        .route("/v1/inferenceservices/{name}/chat", post(chat_proxy))
        .route("/v1/{kind}", get(list_kind))
        .route("/v1/{kind}/{name}", get(get_kind));
    // When a built console bundle is provided, serve it at `/` with SPA fallback
    // to index.html. The explicit `/v1` + health routes above take precedence, so
    // only non-API paths (index, assets) hit the static server. Absent a bundle,
    // the server is API-only.
    if let Some(dir) = web_dir {
        let index = dir.join("index.html");
        router = router.fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)));
    }
    router
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
    // Recompute the capacity rollups when the Node/Pod informers observe a change
    // — not on a blind timer or per request. A burst of changes is coalesced into
    // at most one recompute per `capacity_refresh_secs`; when nothing changes,
    // nothing is recomputed (a long backstop re-syncs in case an event is missed).
    let min_interval = Duration::from_secs(config.capacity_refresh_secs.max(1));
    const BACKSTOP: Duration = Duration::from_secs(120);
    let bg = state.clone();
    tokio::spawn(async move {
        loop {
            // Wake on a change, or at the backstop.
            let _ = tokio::time::timeout(BACKSTOP, bg.signal.wait()).await;
            let s = bg.clone();
            let _ = tokio::task::spawn_blocking(move || s.refresh_capacity()).await;
            // Pace: coalesce further changes for one interval before recomputing.
            tokio::time::sleep(min_interval).await;
        }
    });

    let app = build_app(state, config.web_dir.clone());
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
            let accelerator_nodes = nodes
                .iter()
                .filter(|n| {
                    n.status
                        .as_ref()
                        .and_then(|s| s.allocatable.as_ref())
                        .and_then(node_accel)
                        .is_some()
                })
                .count();
            let ready = c.ready.load(Ordering::Relaxed);
            json!({
                "name": c.name,
                "ready": ready,
                "status": if ready { "ready" } else { "syncing" },
                "nodes": nodes.len(),
                "acceleratorNodes": accelerator_nodes,
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
    // Served from the background-refreshed snapshot — O(1), no per-request walk.
    st.capacity_snapshot(params.cluster.as_deref()).map(Json)
}

/// `GET /v1/events` — recent resource changes observed across clusters (newest
/// first), from the in-memory activity feed. `?cluster=` narrows it.
async fn list_events(Query(params): Query<ListParams>, State(st): State<AppState>) -> Json<Value> {
    let mut items = st.feed.recent();
    if let Some(cluster) = params.cluster.as_deref() {
        items.retain(|e| e.cluster == cluster);
    }
    Json(json!({ "count": items.len(), "items": items }))
}

/// `GET /v1/stream` — Server-Sent Events of live resource changes (the activity
/// feed as it happens), so the UI updates on change instead of polling. Each
/// message is a `FeedEvent` as JSON; `?cluster=` is intentionally not supported
/// (the client filters), and a keep-alive comment holds idle connections open.
async fn stream_events(
    State(st): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>> {
    let live = BroadcastStream::new(st.feed.subscribe())
        // Drop lagged-subscriber errors; the next event resyncs the client.
        .filter_map(|res| res.ok())
        .map(|event| {
            let sse = SseEvent::default()
                .json_data(&event)
                .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
            Ok::<_, Infallible>(sse)
        });
    // Emit one comment immediately so a buffering proxy in front (e.g. the Vite
    // dev proxy) flushes the response headers and the client's `onopen` fires
    // right away, instead of waiting for the first change or the keep-alive.
    let hello = tokio_stream::once(Ok::<_, Infallible>(
        SseEvent::default().comment("connected"),
    ));
    Sse::new(hello.chain(live)).keep_alive(KeepAlive::default())
}

/// `GET /v1/rollouts` — InferenceServices currently mid-rollout (not yet Ready,
/// or spec generation unobserved), across all clusters or one via `?cluster=`.
async fn list_rollouts(
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let selected = st.select(params.cluster.as_deref())?;
    let mut items: Vec<Value> = Vec::new();
    for c in selected {
        for isvc in c.inference_services.list() {
            if let Some(view) = RolloutView::progressing(isvc.as_ref()) {
                items.push(tagged(view, &c.name));
            }
        }
    }
    Ok(Json(json!({ "count": items.len(), "items": items })))
}

/// `GET /v1/config` — feature flags the console reads once at load (currently just
/// whether the inference test proxy is enabled).
async fn get_config(State(st): State<AppState>) -> Json<Value> {
    Json(json!({ "chat": st.chat_enabled }))
}

/// Query for the inference test proxy: which cluster the ISVC lives on.
#[derive(Debug, Default, serde::Deserialize)]
struct ChatParams {
    cluster: Option<String>,
}

/// `POST /v1/inferenceservices/{name}/chat` — proxy one OpenAI-style chat request
/// to the InferenceService's own endpoint (a console test tool, off by default).
/// The target URL is read from the ISVC's cached `status.url` — never supplied by
/// the caller — so this can't be turned into an open relay (SSRF-safe). The request
/// body (model, messages, …) is forwarded verbatim to `<url>/v1/chat/completions`.
async fn chat_proxy(
    Path(name): Path<String>,
    Query(params): Query<ChatParams>,
    State(st): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    if !st.chat_enabled {
        return Err(ApiError::Unavailable(
            "inference test proxy is disabled".to_owned(),
        ));
    }
    let url = st
        .select(params.cluster.as_deref())?
        .iter()
        .find_map(|c| c.inference_services.get(&name))
        .and_then(|isvc| isvc.status.as_ref().and_then(|s| s.url.clone()))
        .filter(|u| !u.is_empty())
        .ok_or_else(|| ApiError::Unavailable(format!("{name} has no ready endpoint")))?;
    let target = format!("{}/v1/chat/completions", url.trim_end_matches('/'));
    let resp = st
        .http
        .post(&target)
        .json(&body)
        .send()
        .await
        .map_err(|e| ApiError::BadGateway(format!("request to {target} failed: {e}")))?;
    let status = resp.status();
    let payload = resp
        .json::<Value>()
        .await
        .map_err(|e| ApiError::BadGateway(format!("bad response from {target}: {e}")))?;
    if !status.is_success() {
        return Err(ApiError::BadGateway(format!(
            "endpoint returned {status}: {payload}"
        )));
    }
    Ok(Json(payload))
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
            "basemodels" => map_view(&c.name, c.base_models.list(), BaseModelView::of),
            "servingruntimes" => map_view(&c.name, c.serving_runtimes.list(), ResourceSummary::of),
            "inferencereplicas" => {
                map_view(&c.name, c.inference_replicas.list(), ResourceSummary::of)
            }
            "clusterbasemodels" => map_view(
                &c.name,
                c.cluster_base_models.list(),
                BaseModelView::of_cluster,
            ),
            "clusterservingruntimes" => map_view(
                &c.name,
                c.cluster_serving_runtimes.list(),
                ResourceSummary::of,
            ),
            "finetunedweights" => map_view(
                &c.name,
                c.fine_tuned_weights.list(),
                FineTunedWeightView::of,
            ),
            "workloadclusters" => {
                map_view(&c.name, c.workload_clusters.list(), ResourceSummary::of)
            }
            "nodes" => {
                let allocated = capacity::accel_allocated_by_node(&c.pods.list());
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
                InferenceServiceDetail::of,
            ),
            "basemodels" => one(&c.name, c.base_models.get(&name), raw, BaseModelDetail::of),
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
                BaseModelDetail::of_cluster,
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
                FineTunedWeightView::of,
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
                    let used = capacity::accel_allocated_by_node(&c.pods.list())
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
