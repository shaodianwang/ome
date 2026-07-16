use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde_json::Value;

use super::error::ApiError;
use crate::{app_context::AppState, protocol::ChatParams};

/// `POST /v1/inferenceservices/{name}/chat` — proxy one OpenAI-style chat request
/// to the InferenceService's own endpoint (a console test tool, off by default).
/// The cached `status.url` is the trusted default. A caller-selected external base
/// must pass the proxy's explicit host policy; redirects are never followed.
pub async fn proxy(
    Path(name): Path<String>,
    Query(params): Query<ChatParams>,
    State(st): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let chat = st
        .chat_proxy()
        .ok_or_else(|| ApiError::Unavailable("inference test proxy is disabled".to_owned()))?;
    let url = st
        .select(params.cluster.as_deref())?
        .iter()
        .find_map(|c| c.inference_services.get(&name))
        .and_then(|isvc| isvc.status.as_ref().and_then(|s| s.url.clone()))
        .filter(|u| !u.is_empty())
        .ok_or_else(|| ApiError::Unavailable(format!("{name} has no ready endpoint")))?;
    let path = params
        .path
        .as_deref()
        .ok_or_else(|| ApiError::BadRequest("endpoint path is required".to_owned()))?;
    let target = chat
        .target_url(&url, params.base.as_deref(), path)
        .map_err(ApiError::BadRequest)?;
    let resp = chat
        .client()
        .post(target.clone())
        .json(&body)
        .send()
        .await
        .map_err(|e| ApiError::BadGateway(format!("request to {target} failed: {e}")))?;
    let status = resp.status();
    if status.is_redirection() {
        return Err(ApiError::BadGateway(format!(
            "endpoint returned redirect {status}; redirects are not allowed"
        )));
    }
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
