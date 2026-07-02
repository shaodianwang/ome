//! Liveness / readiness / health endpoints.
//!
//! muse builds every informer cache (initial list+watch) before it binds the API
//! listener, so by the time these are reachable the read plane is ready. Liveness
//! is therefore always 200 (the process is up); readiness mirrors it.

use axum::http::StatusCode;

/// Liveness: the process is up.
pub async fn livez() -> &'static str {
    "ok"
}

/// Readiness: muse only serves after the informer caches sync, so once reachable
/// it is ready.
pub async fn readyz() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}

/// Combined health (alias of liveness).
pub async fn healthz() -> &'static str {
    "ok"
}
