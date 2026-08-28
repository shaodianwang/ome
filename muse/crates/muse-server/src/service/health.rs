//! Liveness / readiness / health endpoints.
//!
//! Liveness reports that the process can serve HTTP. Readiness flips as soon as
//! one cluster's informer cache is ready: the read plane then serves useful
//! (possibly partial) fleet data, and the console reports the remaining
//! clusters' sync state alongside it. A slow or failed remote cluster therefore
//! never holds the whole console offline.

use axum::{extract::State, http::StatusCode};

use crate::app_context::AppState;

/// Liveness: the process is up.
pub async fn livez() -> &'static str {
    "ok"
}

/// Readiness: at least one cluster's informer cache is ready.
pub async fn readyz(State(st): State<AppState>) -> (StatusCode, &'static str) {
    if st.caches_ready() {
        (StatusCode::OK, "ok")
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no informer cache is ready",
        )
    }
}

/// Combined health (alias of liveness).
pub async fn healthz() -> &'static str {
    "ok"
}
