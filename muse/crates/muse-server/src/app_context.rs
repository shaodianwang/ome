//! The application context: `AppState`, the handle every request runs against.
//!
//! It holds one [`ClusterState`] per workload cluster, the topology labels, the
//! activity feed, independent live-query invalidations, a background-refreshed
//! capacity snapshot, and the on-demand Query Gateway metrics backend. Cheap to
//! clone (an `Arc` and a few small handles), so Axum clones it per request. Reads
//! aggregate across all clusters by default, or one via [`AppState::select`].

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use serde_json::Value;

use crate::{
    chat_proxy::ChatProxy,
    informers::{CapacitySignal, ClusterState, Feed, Invalidations},
    protocol::{to_val, DashboardPolicy, MetricsTimeRanges, SystemComponent, Topology},
    rollup,
    service::{error::ApiError, metrics::MetricsBackend},
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
    /// Optional Query Gateway client/cache. `None` leaves console metrics off.
    metrics: Option<Arc<MetricsBackend>>,
    /// Optional on-demand pod-inspection policy. `None` leaves the endpoints off;
    /// inventory keeps serving from the informer caches either way.
    pod_inspect: Option<crate::config::PodInspectConfig>,
    /// Optional live log-streaming policy (requires pod inspection).
    pod_log_stream: Option<crate::config::PodLogStreamConfig>,
    metrics_refresh: Duration,
    /// Optional heartbeat interval shared by every SSE endpoint. Deployment config
    /// owns this value; `None` leaves an otherwise valid stream event-driven.
    sse_keep_alive: Option<Duration>,
    dashboard_policy: Option<DashboardPolicy>,
    metrics_time_ranges: Option<MetricsTimeRanges>,
    /// OME control-plane components the System panel reports on. Empty ⇒ the panel
    /// is empty (no baked-in set; see [`SystemComponent`]).
    system_components: Arc<Vec<SystemComponent>>,
}

/// Pre-computed capacity responses (already serialized to JSON).
struct CapacitySnapshot {
    fleet: Value,
    per_cluster: HashMap<String, Value>,
}

/// Cross-cutting report, metrics, and display inputs, grouped so AppState's
/// constructor stays readable as services are added.
pub struct AppStateConfig {
    pub metrics: Option<MetricsBackend>,
    pub pod_inspect: Option<crate::config::PodInspectConfig>,
    pub pod_log_stream: Option<crate::config::PodLogStreamConfig>,
    pub refresh: Duration,
    pub sse_keep_alive: Option<Duration>,
    pub capacity_limits: rollup::CapacityLimits,
    pub dashboard_policy: Option<DashboardPolicy>,
    pub metrics_time_ranges: Option<MetricsTimeRanges>,
    pub system_components: Arc<Vec<SystemComponent>>,
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
            metrics: config.metrics.map(Arc::new),
            pod_inspect: config.pod_inspect,
            pod_log_stream: config.pod_log_stream,
            metrics_refresh: config.refresh,
            sse_keep_alive: config.sse_keep_alive,
            dashboard_policy: config.dashboard_policy,
            metrics_time_ranges: config.metrics_time_ranges,
            system_components: config.system_components,
        };
        // Warm the cache once so the first request is served, not computed.
        state.refresh_capacity();
        state
    }

    /// Ready clusters matching `cluster` (one named cluster, or all when None).
    /// Explicitly selecting a known non-ready cluster is unavailable rather than
    /// exposing a partial informer cache as authoritative data. Unscoped fleet
    /// reads serve every ready cluster as soon as one exists — the console
    /// surfaces which clusters are still syncing or failed instead of holding
    /// the whole fleet offline behind its slowest cluster.
    pub fn select(&self, cluster: Option<&str>) -> Result<Vec<&ClusterState>, ApiError> {
        match cluster {
            Some(name) => self.ready_cluster(name).map(|cluster| vec![cluster]),
            None => {
                let ready: Vec<&ClusterState> = self
                    .clusters
                    .iter()
                    .filter(|cluster| cluster.cache.snapshot().ready())
                    .collect();
                if ready.is_empty() {
                    return Err(ApiError::Unavailable(self.no_ready_cluster_detail()));
                }
                Ok(ready)
            }
        }
    }

    /// Validate that a named cluster is configured, regardless of its cache
    /// state — for endpoints that report on non-ready clusters too.
    pub fn validate_cluster(&self, name: &str) -> Result<(), ApiError> {
        if self.clusters.iter().any(|cluster| cluster.name == name) {
            Ok(())
        } else {
            Err(ApiError::NotFound(format!("unknown cluster: {name}")))
        }
    }

    /// Why an unscoped read cannot be served: every cluster's sync state, so the
    /// operator sees which cluster is stuck and why instead of a generic 503.
    fn no_ready_cluster_detail(&self) -> String {
        if self.clusters.is_empty() {
            return "no clusters are configured".to_owned();
        }
        let parts: Vec<String> = self
            .clusters
            .iter()
            .map(|cluster| {
                let cache = cluster.cache.snapshot();
                match cache.error() {
                    Some(error) => format!("{}: {} ({error})", cluster.name, cache.status()),
                    None => format!("{}: {}", cluster.name, cache.status()),
                }
            })
            .collect();
        format!("no cluster cache is ready — {}", parts.join(", "))
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

    /// Whether at least one cluster's informer cache is ready — the point at
    /// which the read plane serves useful (possibly partial) fleet data. The
    /// console reports the remaining clusters' sync state alongside the data.
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
            None => {
                if !self.caches_ready() {
                    return Err(ApiError::Unavailable(self.no_ready_cluster_detail()));
                }
            }
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

    /// The on-demand pod-inspection policy, when configured.
    pub fn pod_inspect(&self) -> Option<crate::config::PodInspectConfig> {
        self.pod_inspect
    }

    /// The live log-streaming policy, when configured.
    pub fn pod_log_stream(&self) -> Option<crate::config::PodLogStreamConfig> {
        self.pod_log_stream
    }

    /// Whether the Query Gateway client is configured.
    pub fn metrics_enabled(&self) -> bool {
        self.metrics.is_some()
    }

    /// Whether the configured catalog can safely attribute fleet-wide results.
    pub fn fleet_metrics_enabled(&self) -> bool {
        self.metrics
            .as_deref()
            .is_some_and(MetricsBackend::supports_fleet_scope)
    }

    pub fn metrics_backend(&self) -> Option<Arc<MetricsBackend>> {
        self.metrics.clone()
    }

    /// How often an active browser view should refresh its cached metrics scope.
    pub fn metrics_refresh_interval(&self) -> Duration {
        self.metrics_refresh
    }

    /// Configured interval between SSE keep-alive comments, if any.
    pub fn sse_keep_alive_interval(&self) -> Option<Duration> {
        self.sse_keep_alive
    }

    /// Optional UI health classification policy. Missing categories are neutral.
    pub fn dashboard_policy(&self) -> Option<&DashboardPolicy> {
        self.dashboard_policy.as_ref()
    }

    /// Optional operator-configured range choices for Deployment Monitoring.
    pub fn metrics_time_ranges(&self) -> Option<&MetricsTimeRanges> {
        self.metrics_time_ranges.as_ref()
    }

    /// The configured OME control-plane components for the System panel. Empty
    /// when unconfigured — the endpoint then returns an empty listing.
    pub fn system_components(&self) -> &[SystemComponent] {
        &self.system_components
    }
}

fn cache_fleet_ready<'a>(
    states: impl IntoIterator<Item = &'a crate::informers::CacheState>,
) -> bool {
    states.into_iter().any(|cache| cache.snapshot().ready())
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::Value;

    use super::{cache_fleet_ready, capacity_membership_matches, CapacitySnapshot};
    use crate::informers::CacheState;

    #[test]
    fn cache_readiness_requires_at_least_one_ready_cluster() {
        let ready = CacheState::default();
        ready.mark_ready();
        let pending = CacheState::default();

        assert!(!cache_fleet_ready(std::iter::empty()));
        assert!(!cache_fleet_ready([&pending]));
        assert!(cache_fleet_ready([&ready, &pending]));
    }

    #[test]
    fn cache_readiness_accepts_a_partial_fleet() {
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
