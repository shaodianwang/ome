use axum::{
    extract::{Query, State},
    Json,
};
use serde_json::Value;

use super::error::ApiError;
use crate::{
    app_context::AppState,
    protocol::{tagged, ListParams, Listing, RolloutView},
};

/// `GET /v1/rollouts` — InferenceServices currently mid-rollout (not yet Ready,
/// or spec generation unobserved), across all clusters or one via `?cluster=`.
pub async fn list(
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Listing>, ApiError> {
    let selected = st.select(params.cluster.as_deref())?;
    let mut items: Vec<Value> = Vec::new();
    for c in selected {
        for isvc in c.inference_services.list() {
            if let Some(view) = RolloutView::progressing(isvc.as_ref()) {
                items.push(tagged(view, &c.name));
            }
        }
    }
    Ok(Json(Listing::new(items)))
}
