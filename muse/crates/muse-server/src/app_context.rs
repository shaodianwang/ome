//! The application context: `AppState`, the handle every request runs against.
//!
//! It holds one [`ClusterState`] per workload cluster, the topology labels, the
//! activity feed, and a background-refreshed capacity snapshot. Cheap to clone
//! (an `Arc` and a few small handles), so Axum clones it per request. Reads
//! aggregate across all clusters by default, or one via [`AppState::select`].

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde_json::Value;

use crate::{
    informers::{CapacitySignal, ClusterState, Feed},
    protocol::{to_val, Topology},
    rollup,
    service::error::ApiError,
};

#[derive(Clone)]
pub struct AppState {
    pub clusters: Arc<Vec<ClusterState>>,
    pub topology: Topology,
    pub feed: Feed,
    /// Materialized capacity: the fleet rollup and each per-cluster rollup, kept
    /// current by a background task that recomputes on Node/Pod changes. Serving
    /// from this keeps `/v1/capacity` latency flat as the fleet grows, instead of
    /// re-walking every Node × Pod on each request.
    capacity: Arc<RwLock<Arc<CapacitySnapshot>>>,
    pub signal: CapacitySignal,
    /// Whether the inference test proxy (`POST …/chat`) is enabled.
    pub chat_enabled: bool,
    /// Total budget (seconds) for one proxied chat request. Surfaced via
    /// `/v1/config` so the console can bound its own request a little above this,
    /// so a stalled endpoint can't leave the UI "Sending…" indefinitely.
    pub chat_timeout_secs: u64,
    /// HTTP client for the inference test proxy (timeouts baked in). Cheap to clone.
    pub http: reqwest::Client,
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
        chat_connect_timeout: Duration,
    ) -> Self {
        // Client for the inference test proxy. A short connect timeout bounds the
        // TCP+TLS handshake so an unreachable endpoint fails fast, while the total
        // timeout still allows for generation. `unwrap_or_default` (not unwrap): a
        // builder failure falls back to a default client rather than panicking.
        let http = reqwest::Client::builder()
            .timeout(chat_timeout)
            .connect_timeout(chat_connect_timeout)
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
            chat_timeout_secs: chat_timeout.as_secs(),
            http,
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
        let fleet = to_val(rollup::build(&all, &self.topology));
        let per_cluster = all
            .iter()
            .map(|cd| {
                let report = rollup::build(std::slice::from_ref(cd), &self.topology);
                (cd.name.clone(), to_val(report))
            })
            .collect();
        *self.capacity.write().unwrap_or_else(|e| e.into_inner()) =
            Arc::new(CapacitySnapshot { fleet, per_cluster });
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
}
