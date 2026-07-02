//! Router assembly, the middleware stack, and the server lifecycle.
//!
//! The route table and its handlers live in `service`; the shared state
//! ([`AppState`]) in `app_context`, built from the `informers` layer; the wire
//! types in `protocol`. This module only wires the router to its state and
//! middleware, then binds and runs the listener.

use std::{path::PathBuf, time::Duration};

use axum::{http::StatusCode, Router};
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    request_id::{PropagateRequestIdLayer, SetRequestIdLayer},
    services::{ServeDir, ServeFile},
    timeout::TimeoutLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use crate::{
    app_context::AppState,
    config::Config,
    service,
    telemetry::{make_span, track_metrics, MakeRequestUuidV7},
};

/// Assemble the router and its middleware stack. The last `.layer()` is the
/// outermost at request time, so the effective order is:
/// CORS → compression → timeout → request-id → trace → metrics → handler.
pub fn build_app(state: AppState, web_dir: Option<PathBuf>) -> Router {
    let mut router = service::routes();
    // When a built console bundle is provided, serve it at `/` with SPA fallback
    // to index.html. The explicit `/v1` + health routes above take precedence, so
    // only non-API paths (index, assets) hit the static server. Absent a bundle,
    // the server is API-only.
    if let Some(dir) = web_dir {
        let index = dir.join("index.html");
        router = router.fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)));
    }
    router
        .with_state(state)
        .layer(axum::middleware::from_fn(track_metrics))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(make_span)
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuidV7))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
}

/// Bind the API listener and serve until a shutdown signal.
pub async fn serve(config: &Config, state: AppState) -> anyhow::Result<()> {
    // Recompute the capacity rollups when the Node/Pod informers observe a change
    // — not on a blind timer or per request. A burst of changes is coalesced into
    // at most one recompute per `capacity_refresh_secs`; when nothing changes,
    // nothing is recomputed (a long backstop re-syncs in case an event is missed).
    let min_interval = Duration::from_secs(config.capacity_refresh_secs.max(1));
    const BACKSTOP: Duration = Duration::from_secs(120);
    let bg = state.clone();
    tokio::spawn(async move {
        loop {
            // Wake on a change, or at the backstop.
            let _ = tokio::time::timeout(BACKSTOP, bg.signal.wait()).await;
            let s = bg.clone();
            let _ = tokio::task::spawn_blocking(move || s.refresh_capacity()).await;
            // Pace: coalesce further changes for one interval before recomputing.
            tokio::time::sleep(min_interval).await;
        }
    });

    let app = build_app(state, config.web_dir.clone());
    let addr = config.api_addr();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "muse read-plane listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Resolve on Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => tracing::warn!(error = %err, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => tracing::info!("received Ctrl-C, shutting down"),
        () = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
