use axum::{extract::State, Json};

use crate::{app_context::AppState, protocol::ConfigResp};

/// `GET /v1/config` — feature flags the console reads once at load: whether the
/// inference test proxy is enabled, and its total request budget (which the
/// console uses to bound its own chat request).
pub async fn get(State(st): State<AppState>) -> Json<ConfigResp> {
    Json(ConfigResp {
        chat: st.chat_proxy().is_some(),
        chat_timeout_secs: st.chat_proxy().map(|chat| chat.timeout_secs()),
        chat_client_grace_secs: st.chat_proxy().and_then(|chat| chat.client_grace_secs()),
        metrics: st.metrics_enabled(),
        metrics_refresh_secs: st.metrics_refresh_interval().as_secs(),
        dashboard_policy: st.dashboard_policy().cloned(),
    })
}
