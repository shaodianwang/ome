use axum::{extract::State, Json};

use crate::{
    app_context::AppState,
    protocol::{ConfigResp, LogsCaps},
};

/// `GET /v1/config` — feature flags the console reads once at load: whether the
/// inference test proxy is enabled (and its request budget), whether metrics
/// and log streaming are available, which clusters serve logs, and the log
/// viewer's operational limits.
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
        logs_clusters: logs.map(|logs| logs.clusters.iter().cloned().collect()),
        logs_caps: logs.map(|logs| LogsCaps {
            max_pods_per_session: logs.max_pods_per_session,
            tail_lines_max: logs.tail_lines_max,
            max_lines_per_sec: logs.max_lines_per_sec,
        }),
        logs_since_options: logs.and_then(|logs| logs.since_options.clone()),
        logs_client_buffer_lines: logs.map(|logs| logs.client_buffer_lines),
    })
}
