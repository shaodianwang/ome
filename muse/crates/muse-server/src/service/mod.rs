//! The HTTP service: one module per endpoint (closely-related routes share a
//! module), plus the [`routes`] table that maps paths to them.
//!
//! Endpoints project the cached resources into the `protocol` wire types. The
//! caches, capacity rollup, and activity feed they read live outside this module
//! (the data layer + `capacity` + `feed`), reached through `AppState`. The
//! middleware stack and static-console fallback are added by `server::build_app`.

use axum::{
    routing::{get, post},
    Router,
};

use crate::app_context::AppState;

pub mod capacity;
pub mod chat;
pub mod clusters;
pub mod config;
pub mod error;
pub mod events;
pub mod health;
pub mod metrics;
pub mod resources;
pub mod rollouts;
mod sse;

/// The complete route table, still awaiting its state. `server::build_app` applies
/// the state, the static-console fallback, and the middleware layers.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/livez", get(health::livez))
        .route("/readyz", get(health::readyz))
        .route("/v1/clusters", get(clusters::list))
        .route("/v1/capacity", get(capacity::report))
        .route("/v1/events", get(events::list))
        .route("/v1/stream", get(events::stream))
        .route("/v1/rollouts", get(rollouts::list))
        .route("/v1/config", get(config::get))
        // Static metrics paths coexist with the `/v1/{kind}/{name}` catch-all below
        // (matchit prefers the static match), mirroring the `.../chat` route.
        .route("/v1/metrics", get(metrics::snapshot))
        .route("/v1/inferenceservices/{name}/chat", post(chat::proxy))
        .route("/v1/{kind}", get(resources::list_kind))
        .route("/v1/{kind}/{name}", get(resources::get_kind))
}
