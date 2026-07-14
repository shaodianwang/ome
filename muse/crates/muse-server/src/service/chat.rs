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
    // Target base: `status.url` by default, or the console-supplied `base` when its
    // host is within the deployment's own cluster domain — lets the user select the
    // external gateway (status.url's internal one is often unreachable, OEP-0023)
    // without the proxy becoming an open relay to an arbitrary host.
    let base = choose_base(&url, params.base.as_deref())?;
    // The endpoint path is caller-chosen (engines differ — vLLM
    // `/v1/chat/completions`, sglang `/generate`) but must be an absolute path with
    // no host/scheme/`..`, so it can't escape the chosen base.
    let path = params.path.as_deref().unwrap_or("/v1/chat/completions");
    if !path.starts_with('/') || path.starts_with("//") || path.contains("..") {
        return Err(ApiError::Unavailable(format!(
            "invalid endpoint path: {path}"
        )));
    }
    let target = format!("{}{}", base.trim_end_matches('/'), path);
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

/// Pick the proxy target base. Defaults to the ISVC's `status.url`; accepts a
/// console-supplied `requested` base only when its host shares the deployment's
/// cluster domain (the status host with its `<isvc>.<ns>.<gateway>` labels
/// stripped), so the console can select the external gateway without turning the
/// proxy into an open relay to arbitrary hosts.
fn choose_base(status_url: &str, requested: Option<&str>) -> Result<String, ApiError> {
    let Some(req) = requested.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(status_url.to_owned());
    };
    let host_of = |u: &str| {
        reqwest::Url::parse(u)
            .ok()
            .and_then(|p| p.host_str().map(str::to_owned))
    };
    let (Some(sh), Some(rh)) = (host_of(status_url), host_of(req)) else {
        return Err(ApiError::Unavailable("invalid target URL".to_owned()));
    };
    // e.g. `gpt-oss.meta-ai.int-gw-https.CLUSTER.dom` -> allowed domain `CLUSTER.dom`.
    let domain = sh.splitn(4, '.').nth(3).unwrap_or(sh.as_str());
    if rh == sh || rh == domain || rh.ends_with(&format!(".{domain}")) {
        Ok(req.to_owned())
    } else {
        Err(ApiError::Unavailable(format!(
            "target host {rh} is not in the deployment domain {domain}"
        )))
    }
}
