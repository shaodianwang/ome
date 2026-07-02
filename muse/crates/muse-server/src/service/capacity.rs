use axum::{
    extract::{Query, State},
    Json,
};
use serde_json::Value;

use super::error::ApiError;
use crate::{app_context::AppState, protocol::ListParams};

/// `GET /v1/capacity` — fleet GPU capacity (allocatable/allocated/free) grouped
/// by cluster, island, NVLink domain, instance type. `?cluster=` narrows it.
pub async fn report(
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    // Served from the background-refreshed snapshot (computed in `rollup`, cached
    // on AppState) — O(1), no per-request walk.
    st.capacity_snapshot(params.cluster.as_deref()).map(Json)
}
