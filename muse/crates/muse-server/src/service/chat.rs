use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde_json::Value;

use super::error::ApiError;
use crate::{app_context::AppState, protocol::ChatParams};

/// `POST /v1/inferenceservices/{name}/chat` — proxy one OpenAI-style chat request
/// to the InferenceService's own endpoint (a console test tool, off by default).
/// The target URL is read from the ISVC's cached `status.url` — never the caller —
/// so it can't be an open relay (SSRF-safe); the body (model, messages, …) is
/// forwarded verbatim to `<status.url>/v1/chat/completions`.
pub async fn proxy(
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
    // The endpoint path is caller-chosen (engines differ — vLLM
    // `/v1/chat/completions`, sglang `/generate`) but must be an absolute path with
    // no host/scheme/`..`, so the proxy stays pinned to the ISVC's own URL (the base
    // is always `status.url`, never the caller) — it can't be redirected elsewhere.
    let path = params.path.as_deref().unwrap_or("/v1/chat/completions");
    if !path.starts_with('/') || path.starts_with("//") || path.contains("..") {
        return Err(ApiError::Unavailable(format!(
            "invalid endpoint path: {path}"
        )));
    }
    let target = format!("{}{}", url.trim_end_matches('/'), path);
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
