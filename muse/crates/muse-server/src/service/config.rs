use axum::{extract::State, Json};

use crate::{app_context::AppState, protocol::ConfigResp};

/// `GET /v1/config` — feature flags the console reads once at load: whether the
/// inference test proxy is enabled (and its request budget), and whether metrics
/// are available.
pub async fn get(State(st): State<AppState>) -> Json<ConfigResp> {
    Json(ConfigResp {
        chat: st.chat_proxy().is_some(),
        chat_timeout_secs: st.chat_proxy().map(|chat| chat.timeout_secs()),
        chat_client_grace_secs: st.chat_proxy().and_then(|chat| chat.client_grace_secs()),
        pod_inspect: st.pod_inspect().is_some(),
        pod_log_tail_lines: st.pod_inspect().map(|policy| policy.log_tail_lines),
        pod_log_stream: st.pod_log_stream().is_some(),
        pod_log_stream_max_pods: st.pod_log_stream().map(|policy| policy.max_pods),
        metrics: st.metrics_enabled(),
        fleet_metrics: st.fleet_metrics_enabled(),
        metrics_refresh_secs: st.metrics_refresh_interval().as_secs(),
        dashboard_policy: st.dashboard_policy().cloned(),
        metrics_time_ranges: st.metrics_time_ranges().cloned(),
    })
}
