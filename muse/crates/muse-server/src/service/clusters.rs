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
            let ready = c.ready.load(Ordering::Relaxed);
            to_val(ClusterSummary {
                name: c.name.clone(),
                ready,
                status: if ready { "ready" } else { "syncing" },
                nodes: nodes.len(),
                accelerator_nodes,
                inference_services: c.inference_services.list().len(),
            })
        })
        .collect();
    Json(Listing::new(items))
}
