use axum::{extract::State, Json};

use crate::{app_context::AppState, protocol::ConfigResp};

/// `GET /v1/config` — feature flags the console reads once at load (currently just
/// whether the inference test proxy is enabled).
pub async fn get(State(st): State<AppState>) -> Json<ConfigResp> {
    Json(ConfigResp {
        chat: st.chat_enabled,
    })
}
