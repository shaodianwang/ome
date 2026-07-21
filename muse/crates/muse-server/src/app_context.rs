//! The application context: `AppState`, the handle every request runs against.
//!
//! It holds one [`ClusterState`] per workload cluster, the topology labels, the
//! activity feed, independent live-query invalidations, a background-refreshed
//! capacity snapshot, and a background-refreshed metrics snapshot (pushed to SSE
//! subscribers). Cheap to clone (an `Arc` and a few small handles), so Axum clones
//! it per request. Reads aggregate across all clusters by default, or one via
//! [`AppState::select`].

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde_json::{json, Value};
use tokio::sync::watch;

use crate::{
    chat_proxy::ChatProxy,
    config::{PromTarget, QueryDef},
    informers::{CapacitySignal, ClusterState, Feed, Invalidations},
    protocol::{to_val, DashboardPolicy, MetricsTimeRanges, Topology},
    rollup,
    service::{
        error::ApiError,
        logs::{LogsConfig, LogsState},
    },
};

#[derive(Clone)]
pub struct AppState {
    pub clusters: Arc<Vec<ClusterState>>,
    pub topology: Topology,
    pub feed: Feed,
    pub invalidations: Invalidations,
    /// Materialized capacity for ready clusters: the fleet rollup and each
    /// per-cluster rollup, kept current by a background task that recomputes on
    /// Node/Pod or cache-state changes. Serving from this keeps `/v1/capacity`
    /// latency flat as the fleet grows instead of re-walking every Node × Pod.
    capacity: Arc<RwLock<Arc<CapacitySnapshot>>>,
    capacity_limits: rollup::CapacityLimits,
    pub signal: CapacitySignal,
    /// Optional inference-test proxy. `None` means disabled or invalidly configured;
    /// the rest of the read plane remains available.
    chat: Option<Arc<ChatProxy>>,
    /// Optional pod log streaming. `None` means disabled or invalidly
    /// configured; the rest of the read plane remains available.
    logs: Option<Arc<LogsState>>,
    /// The Prometheus to proxy PromQL to (per cluster, via the apiserver
    /// `services/proxy`). `None` ⇒ metrics are unconfigured and the endpoints are off.
    pub prom: Option<PromTarget>,
    /// The curated dashboard query set the metrics refresher runs each interval.
    metrics_queries: Arc<Vec<QueryDef>>,
    metrics_refresh: Duration,
    /// Optional heartbeat interval shared by every SSE endpoint. Deployment config
    /// owns this value; `None` leaves an otherwise valid stream event-driven.
    sse_keep_alive: Option<Duration>,
    /// Optional per-replica request parallelism. `None` preserves serial polling.
    metrics_query_concurrency: Option<NonZeroUsize>,
    /// Optional deadline for each discovery/query request. `None` waits normally.
    metrics_request_timeout: Option<Duration>,
    dashboard_policy: Option<DashboardPolicy>,
    metrics_time_ranges: Option<MetricsTimeRanges>,
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
    pub sse_keep_alive: Option<Duration>,
    pub query_concurrency: Option<NonZeroUsize>,
    pub request_timeout: Option<Duration>,
    pub capacity_limits: rollup::CapacityLimits,
    pub dashboard_policy: Option<DashboardPolicy>,
    pub metrics_time_ranges: Option<MetricsTimeRanges>,
    /// Optional log-streaming settings; `None` keeps the feature off.
    pub logs: Option<LogsConfig>,
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
        // The per-cluster tail gates cover exactly the registered clusters —
        // the same set every request selects from.
        let logs = config.logs.map(|logs_config| {
            Arc::new(LogsState::new(
                logs_config,
                clusters.iter().map(|cluster| cluster.name.clone()),
            ))
        });
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
            logs,
            prom: config.prom,
            metrics_queries: config.queries,
            metrics_refresh: config.refresh,
            sse_keep_alive: config.sse_keep_alive,
            metrics_query_concurrency: config.query_concurrency,
            metrics_request_timeout: config.request_timeout,
            dashboard_policy: config.dashboard_policy,
            metrics_time_ranges: config.metrics_time_ranges,
            metrics_tx,
        };
        // Warm the cache once so the first request is served, not computed.
        state.refresh_capacity();
        state
    }

    /// Ready clusters matching `cluster` (one named cluster, or all when None).
    /// Explicitly selecting a known non-ready cluster is unavailable rather than
    /// exposing a partial informer cache as authoritative data. Unscoped fleet
    /// reads wait until every cluster is settled and at least one is ready.
    pub fn select(&self, cluster: Option<&str>) -> Result<Vec<&ClusterState>, ApiError> {
        match cluster {
            Some(name) => self.ready_cluster(name).map(|cluster| vec![cluster]),
            None => {
                self.ensure_fleet_ready()?;
                Ok(self
                    .clusters
                    .iter()
                    .filter(|cluster| cluster.cache.snapshot().ready())
                    .collect())
            }
        }
    }

    fn ensure_fleet_ready(&self) -> Result<(), ApiError> {
        if self.caches_ready() {
            Ok(())
        } else {
            Err(ApiError::Unavailable(
                "fleet caches have not reached a settled usable state".to_owned(),
            ))
        }
    }

    fn ready_cluster(&self, name: &str) -> Result<&ClusterState, ApiError> {
        let cluster = self
            .clusters
            .iter()
            .find(|cluster| cluster.name == name)
            .ok_or_else(|| ApiError::NotFound(format!("unknown cluster: {name}")))?;
        let cache = cluster.cache.snapshot();
        if cache.ready() {
            Ok(cluster)
        } else {
            Err(ApiError::Unavailable(format!(
                "cluster {name} cache is {}",
                cache.status()
            )))
        }
    }

    /// Configured inference-test proxy, or `None` when the feature is unavailable.
    pub fn chat_proxy(&self) -> Option<&ChatProxy> {
        self.chat.as_deref()
    }

    /// Configured log streaming, or `None` when the feature is unavailable.
    pub fn logs(&self) -> Option<&LogsState> {
        self.logs.as_deref()
    }

    /// Whether every registered cluster has reached a settled initial-sync state
    /// and at least one cluster is ready. A completed partial fleet remains usable,
    /// but pending or wholly failed fleets must not serve authoritative-looking data.
    pub fn caches_ready(&self) -> bool {
        cache_fleet_ready(self.clusters.iter().map(|cluster| &cluster.cache))
    }

    /// Recompute the capacity rollups (fleet + per-cluster) from ready informer
    /// caches and publish them. Called by the background refresher; walking the
    /// caches is CPU-only (no apiserver traffic).
    pub fn refresh_capacity(&self) {
        let all: Vec<rollup::ClusterData> = self
            .clusters
            .iter()
            .filter(|cluster| cluster.cache.snapshot().ready())
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
        match cluster {
            Some(name) => {
                self.ready_cluster(name)?;
            }
            None => self.ensure_fleet_ready()?,
        }
        let snap = loop {
            let snap = self
                .capacity
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let ready = self
                .clusters
                .iter()
                .filter(|cluster| cluster.cache.snapshot().ready())
                .map(|cluster| cluster.name.as_str());
            if capacity_membership_matches(&snap, ready) {
                break snap;
            }
            // Cache-state transitions normally wake the background refresher.
            // Close the publication race synchronously before serving capacity.
            self.refresh_capacity();
        };
        match cluster {
            None => Ok(snap.fleet.clone()),
            Some(name) => snap.per_cluster.get(name).cloned().ok_or_else(|| {
                ApiError::Unavailable(format!("capacity snapshot for cluster {name} is not ready"))
            }),
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

    /// Configured interval between SSE keep-alive comments, if any.
    pub fn sse_keep_alive_interval(&self) -> Option<Duration> {
        self.sse_keep_alive
    }

    /// Optional global in-flight limit for one refresh; absent means serial.
    pub fn metrics_query_concurrency(&self) -> Option<NonZeroUsize> {
        self.metrics_query_concurrency
    }

    /// Optional per-request deadline; absent waits without a Muse deadline.
    pub fn metrics_request_timeout(&self) -> Option<Duration> {
        self.metrics_request_timeout
    }

    /// Optional UI health classification policy. Missing categories are neutral.
    pub fn dashboard_policy(&self) -> Option<&DashboardPolicy> {
        self.dashboard_policy.as_ref()
    }

    /// Optional operator-configured range choices for Deployment Monitoring.
    pub fn metrics_time_ranges(&self) -> Option<&MetricsTimeRanges> {
        self.metrics_time_ranges.as_ref()
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

fn cache_fleet_ready<'a>(
    states: impl IntoIterator<Item = &'a crate::informers::CacheState>,
) -> bool {
    let mut any_ready = false;
    for cache in states {
        let state = cache.snapshot();
        if !state.settled() {
            return false;
        }
        any_ready |= state.ready();
    }
    any_ready
}

fn capacity_membership_matches<'a>(
    snapshot: &CapacitySnapshot,
    ready_clusters: impl IntoIterator<Item = &'a str>,
) -> bool {
    let ready: HashSet<&str> = ready_clusters.into_iter().collect();
    ready.len() == snapshot.per_cluster.len()
        && ready
            .iter()
            .all(|name| snapshot.per_cluster.contains_key(*name))
}

/// The initial (empty) metrics snapshot, before the first refresh completes.
fn empty_metrics() -> Arc<Value> {
    Arc::new(json!({ "updatedAt": 0, "evaluatedAt": 0, "clusters": {}, "sources": {} }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::Value;

    use super::{cache_fleet_ready, capacity_membership_matches, CapacitySnapshot};
    use crate::informers::CacheState;

    #[test]
    fn cache_readiness_requires_a_ready_cluster_and_no_pending_clusters() {
        let ready = CacheState::default();
        ready.mark_ready();
        let pending = CacheState::default();

        assert!(!cache_fleet_ready(std::iter::empty()));
        assert!(!cache_fleet_ready([&pending]));
        assert!(!cache_fleet_ready([&ready, &pending]));
    }

    #[test]
    fn cache_readiness_accepts_a_settled_partial_fleet() {
        let ready = CacheState::default();
        ready.mark_ready();
        let failed = CacheState::default();
        failed.mark_failed("watch failed".to_owned());
        let timed_out = CacheState::default();
        timed_out.mark_timed_out("sync deadline exceeded".to_owned());

        assert!(cache_fleet_ready([&ready, &failed, &timed_out]));
        assert!(!cache_fleet_ready([&failed, &timed_out]));
    }

    #[test]
    fn capacity_membership_detects_cache_state_transitions() {
        let snapshot = CapacitySnapshot {
            fleet: Value::Null,
            per_cluster: HashMap::from([("ready-a".to_owned(), Value::Null)]),
        };

        assert!(capacity_membership_matches(&snapshot, ["ready-a"]));
        assert!(!capacity_membership_matches(
            &snapshot,
            ["ready-a", "ready-b"]
        ));
        assert!(!capacity_membership_matches(&snapshot, std::iter::empty()));
    }
}
