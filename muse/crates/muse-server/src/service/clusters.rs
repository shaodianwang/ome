use std::sync::atomic::Ordering;

use axum::{extract::State, Json};
use serde_json::Value;

use crate::{
    app_context::AppState,
    protocol::{node_accel, to_val, ClusterSummary, Listing},
};

/// `GET /v1/clusters` — the registered clusters and a per-cluster summary
/// (readiness, node and accelerator-node counts, InferenceService count).
pub async fn list(State(st): State<AppState>) -> Json<Listing> {
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
            // Cloud region from the first node carrying the standard label — a
            // well-known Kubernetes key, uniform across a cluster.
            let region = nodes.iter().find_map(|n| {
                n.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("topology.kubernetes.io/region"))
                    .filter(|v| !v.is_empty())
                    .cloned()
            });
            let ready = c.ready.load(Ordering::Relaxed);
            to_val(ClusterSummary {
                name: c.name.clone(),
                ready,
                status: if ready { "ready" } else { "syncing" },
                version: c.version.clone(),
                region,
                nodes: nodes.len(),
                accelerator_nodes,
                inference_services: c.inference_services.list().len(),
            })
        })
        .collect();
    Json(Listing::new(items))
}
