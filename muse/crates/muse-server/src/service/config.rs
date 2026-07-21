use axum::{extract::State, Json};

use crate::{app_context::AppState, protocol::ConfigResp};

/// `GET /v1/config` — feature flags the console reads once at load: whether the
/// inference test proxy is enabled (and its request budget), whether metrics
/// and log streaming are available, and the log viewer's operational limits.
pub async fn get(State(st): State<AppState>) -> Json<ConfigResp> {
    let logs = st.logs().map(|logs| &logs.config);
    Json(ConfigResp {
        chat: st.chat_proxy().is_some(),
        chat_timeout_secs: st.chat_proxy().map(|chat| chat.timeout_secs()),
        chat_client_grace_secs: st.chat_proxy().and_then(|chat| chat.client_grace_secs()),
        metrics: st.metrics_enabled(),
        metrics_refresh_secs: st.metrics_refresh_interval().as_secs(),
        dashboard_policy: st.dashboard_policy().cloned(),
        metrics_time_ranges: st.metrics_time_ranges().cloned(),
        logs: logs.is_some(),
        logs_max_pods_per_stream: logs.map(|logs| logs.max_pods_per_session),
        logs_tail_lines_max: logs.map(|logs| logs.tail_lines_max),
        logs_session_max_secs: logs.map(|logs| logs.session_max.as_secs()),
        logs_client_buffer_lines: logs.map(|logs| logs.client_buffer_lines),
        logs_since_options: logs.and_then(|logs| logs.since_options.clone()),
    })
}
