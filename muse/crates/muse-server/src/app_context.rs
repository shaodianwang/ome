//! The application context: `AppState`, the handle every request runs against.
//!
//! It holds one [`ClusterState`] per workload cluster, the topology labels, the
//! activity feed, independent live-query invalidations, a background-refreshed
//! capacity snapshot, and a background-refreshed metrics snapshot (pushed to SSE
//! subscribers). Cheap to clone (an `Arc` and a few small handles), so Axum clones
//! it per request. Reads aggregate across all clusters by default, or one via
//! [`AppState::select`].

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde_json::{json, Value};
use tokio::sync::watch;

use crate::{
    chat_proxy::ChatProxy,
    config::{PromTarget, QueryDef},
    informers::{CapacitySignal, ClusterState, Feed, Invalidations},
    protocol::{to_val, DashboardPolicy, Topology},
    rollup,
    service::error::ApiError,
};

#[derive(Clone)]
pub struct AppState {
    pub clusters: Arc<Vec<ClusterState>>,
    pub topology: Topology,
    pub feed: Feed,
    pub invalidations: Invalidations,
    /// Materialized capacity: the fleet rollup and each per-cluster rollup, kept
    /// current by a background task that recomputes on Node/Pod changes. Serving
    /// from this keeps `/v1/capacity` latency flat as the fleet grows, instead of
    /// re-walking every Node × Pod on each request.
    capacity: Arc<RwLock<Arc<CapacitySnapshot>>>,
    capacity_limits: rollup::CapacityLimits,
    pub signal: CapacitySignal,
    /// Optional inference-test proxy. `None` means disabled or invalidly configured;
    /// the rest of the read plane remains available.
    chat: Option<Arc<ChatProxy>>,
    /// The Prometheus to proxy PromQL to (per cluster, via the apiserver
    /// `services/proxy`). `None` ⇒ metrics are unconfigured and the endpoints are off.
    pub prom: Option<PromTarget>,
    /// The curated dashboard query set the metrics refresher runs each interval.
    metrics_queries: Arc<Vec<QueryDef>>,
    metrics_refresh: Duration,
    dashboard_policy: Option<DashboardPolicy>,
    /// Latest metrics snapshot, updated by the background refresher and pushed to
    /// SSE subscribers. `watch` retains only the latest value (new subscribers get
    /// it immediately) — exactly the "current fleet metrics" semantics.
    metrics_tx: watch::Sender<Arc<Value>>,
}

/// Pre-computed capacity responses (already serialized to JSON).
struct CapacitySnapshot {
    fleet: Value,
    per_cluster: HashMap<String, Value>,
}

/// Cross-cutting report, metrics, and display inputs, grouped so AppState's
/// constructor stays readable as services are added.
pub struct AppStateConfig {
    pub prom: Option<PromTarget>,
    pub queries: Arc<Vec<QueryDef>>,
    pub refresh: Duration,
    pub capacity_limits: rollup::CapacityLimits,
    pub dashboard_policy: Option<DashboardPolicy>,
}

impl AppState {
    pub fn new(
        clusters: Vec<ClusterState>,
        topology: Topology,
        feed: Feed,
        invalidations: Invalidations,
        signal: CapacitySignal,
        chat: Option<ChatProxy>,
        config: AppStateConfig,
    ) -> Self {
        let (metrics_tx, _) = watch::channel(empty_metrics());
        let state = Self {
            clusters: Arc::new(clusters),
            topology,
            feed,
            invalidations,
            capacity: Arc::new(RwLock::new(Arc::new(CapacitySnapshot {
                fleet: Value::Null,
                per_cluster: HashMap::new(),
            }))),
            capacity_limits: config.capacity_limits,
            signal,
            chat: chat.map(Arc::new),
            prom: config.prom,
            metrics_queries: config.queries,
            metrics_refresh: config.refresh,
            dashboard_policy: config.dashboard_policy,
            metrics_tx,
        };
        // Warm the cache once so the first request is served, not computed.
        state.refresh_capacity();
        state
    }

    /// Clusters matching `cluster` (one named cluster, or all when None).
    pub fn select(&self, cluster: Option<&str>) -> Result<Vec<&ClusterState>, ApiError> {
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

    /// Configured inference-test proxy, or `None` when the feature is unavailable.
    pub fn chat_proxy(&self) -> Option<&ChatProxy> {
        self.chat.as_deref()
    }

    /// Recompute the capacity rollups (fleet + per-cluster) from the current
    /// informer caches and publish them. Called on a timer by the background
    /// refresher; walking the caches is CPU-only (no apiserver traffic).
    pub fn refresh_capacity(&self) {
        let all: Vec<rollup::ClusterData> = self
            .clusters
            .iter()
            .map(|c| rollup::ClusterData {
                name: c.name.clone(),
                nodes: c.nodes.list(),
                pods: c.pods.list(),
            })
            .collect();
        let fleet = to_val(rollup::build(&all, &self.topology, self.capacity_limits));
        let per_cluster = all
            .iter()
            .map(|cd| {
                let report = rollup::build(
                    std::slice::from_ref(cd),
                    &self.topology,
                    self.capacity_limits,
                );
                (cd.name.clone(), to_val(report))
            })
            .collect();
        *self.capacity.write().unwrap_or_else(|e| e.into_inner()) =
            Arc::new(CapacitySnapshot { fleet, per_cluster });
        // Publish only after replacing the snapshot. A client reacting to this
        // signal therefore cannot refetch the old capacity calculation.
        self.invalidations.mark("Capacity");
    }

    /// The cached capacity response for one cluster, or the fleet rollup.
    pub fn capacity_snapshot(&self, cluster: Option<&str>) -> Result<Value, ApiError> {
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

    /// Whether the metrics proxy is configured (a `--prom-service` was supplied).
    pub fn metrics_enabled(&self) -> bool {
        self.prom.is_some()
    }

    /// The curated dashboard query set (shared; cheap to clone the Arc).
    pub fn metrics_queries(&self) -> Arc<Vec<QueryDef>> {
        self.metrics_queries.clone()
    }

    /// How often the background refresher re-polls each cluster's Prometheus.
    pub fn metrics_refresh_interval(&self) -> Duration {
        self.metrics_refresh
    }

    /// Optional UI health classification policy. Missing categories are neutral.
    pub fn dashboard_policy(&self) -> Option<&DashboardPolicy> {
        self.dashboard_policy.as_ref()
    }

    /// Publish a freshly-built metrics snapshot; new + existing SSE subscribers see
    /// it, and it's retained for the next `GET /v1/metrics`. `send_replace` never
    /// fails on zero subscribers (unlike `send`), so the latest value always sticks.
    pub fn publish_metrics(&self, snapshot: Arc<Value>) {
        let _ = self.metrics_tx.send_replace(snapshot);
    }

    /// The latest metrics snapshot (for `GET /v1/metrics`).
    pub fn metrics_snapshot(&self) -> Arc<Value> {
        self.metrics_tx.borrow().clone()
    }

    /// A receiver yielding the current snapshot then each subsequent one (for SSE).
    pub fn metrics_subscribe(&self) -> watch::Receiver<Arc<Value>> {
        self.metrics_tx.subscribe()
    }
}

/// The initial (empty) metrics snapshot, before the first refresh completes.
fn empty_metrics() -> Arc<Value> {
    Arc::new(json!({ "updatedAt": 0, "clusters": {} }))
}
