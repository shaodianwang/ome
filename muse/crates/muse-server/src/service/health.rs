//! Liveness / readiness / health endpoints.
//!
//! Liveness reports that the process can serve HTTP. Readiness waits until every
//! registered cluster has reached a settled initial-sync state, then requires at
//! least one ready cluster. A failed remote cluster therefore does not take a
//! completed partial fleet offline.

use axum::{extract::State, http::StatusCode};

use crate::app_context::AppState;

/// Liveness: the process is up.
pub async fn livez() -> &'static str {
    "ok"
}

/// Readiness: all registered clusters are terminal and at least one is ready.
pub async fn readyz(State(st): State<AppState>) -> (StatusCode, &'static str) {
    if st.caches_ready() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "informer caches not ready")
    }
}

/// Combined health (alias of liveness).
pub async fn healthz() -> &'static str {
    "ok"
}
