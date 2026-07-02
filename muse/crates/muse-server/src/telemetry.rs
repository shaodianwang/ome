//! Telemetry: logging + Prometheus metrics setup, and the per-request HTTP
//! middleware (request-id, tracing span, metrics recording) that feeds them.
//!
//! Setup (`init_logging`, `init_metrics`, `serve_metrics`) runs once at boot; the
//! middleware runs on every request. `server::build_app` installs the middleware;
//! `serve_metrics` exposes `/metrics` on its own listener, off the API surface.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::Context;
use axum::{
    extract::{MatchedPath, Request},
    http::HeaderValue,
    middleware::Next,
    response::Response,
    routing::get,
    Router,
};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tower_http::request_id::{MakeRequestId, RequestId};
use tracing::Span;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

// ===========================================================================
// Setup — installed once at boot.
// ===========================================================================

/// Initialise tracing: honour `RUST_LOG`, else a sane default; fmt or JSON.
pub fn init_logging(json: bool) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,muse=debug"));
    let fmt = tracing_subscriber::fmt::layer().with_target(true);
    let fmt = if json {
        fmt.json().boxed()
    } else {
        fmt.boxed()
    };
    tracing_subscriber::registry().with(filter).with(fmt).init();
}

/// Install the Prometheus recorder (latency buckets on any `*_duration_seconds`
/// metric), describe the HTTP metrics, and start the histogram upkeep task.
pub fn init_metrics() -> anyhow::Result<PrometheusHandle> {
    const BUCKETS: &[f64] = &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("duration_seconds".to_owned()), BUCKETS)
        .context("set metric buckets")?
        .install_recorder()
        .context("install prometheus recorder")?;

    metrics::describe_counter!("muse_http_requests_total", "Total HTTP requests received");
    metrics::describe_counter!(
        "muse_http_responses_total",
        "Total HTTP responses by status"
    );
    metrics::describe_gauge!(
        "muse_http_requests_in_flight",
        "HTTP requests currently in flight"
    );
    metrics::describe_histogram!(
        "muse_http_request_duration_seconds",
        "HTTP request latency in seconds"
    );

    // Histogram bucket rotation — required when using install_recorder.
    let upkeep = handle.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            upkeep.run_upkeep();
        }
    });
    Ok(handle)
}

/// Serve `/metrics` on its own listener (kept off the public API surface).
pub async fn serve_metrics(handle: PrometheusHandle, addr: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    );
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind metrics listener {addr}"))?;
    tracing::info!(%addr, "metrics endpoint listening");
    axum::serve(listener, app).await.context("metrics server")?;
    Ok(())
}

// ===========================================================================
// Per-request middleware — installed by `server::build_app`.
// ===========================================================================

/// Generate time-sortable v7 UUIDs for `x-request-id` when a client didn't send one.
#[derive(Clone, Default)]
pub struct MakeRequestUuidV7;

impl MakeRequestId for MakeRequestUuidV7 {
    fn make_request_id<B>(&mut self, _request: &axum::http::Request<B>) -> Option<RequestId> {
        let id = uuid::Uuid::now_v7().to_string();
        HeaderValue::from_str(&id).ok().map(RequestId::new)
    }
}

/// Per-request tracing span carrying method, route template, and request id. The
/// id is already set on the request by `SetRequestIdLayer` (applied outer to this).
pub fn make_span(request: &axum::http::Request<axum::body::Body>) -> Span {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| request.uri().path(), MatchedPath::as_str);
    tracing::info_span!(
        "http",
        method = %request.method(),
        path = %path,
        request_id = %request_id,
    )
}

/// axum middleware recording request/response counters, an in-flight gauge, and a
/// latency histogram — labelled by the matched route template (cardinality-safe).
pub async fn track_metrics(request: Request, next: Next) -> Response {
    let method = request.method().to_string();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unknown".to_owned(), |m| m.as_str().to_owned());
    let start = Instant::now();

    metrics::counter!("muse_http_requests_total", "method" => method.clone(), "path" => path.clone())
        .increment(1);
    metrics::gauge!("muse_http_requests_in_flight").increment(1.0);

    let response = next.run(request).await;

    metrics::gauge!("muse_http_requests_in_flight").decrement(1.0);
    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    metrics::histogram!("muse_http_request_duration_seconds", "method" => method, "path" => path.clone())
        .record(latency);
    metrics::counter!("muse_http_responses_total", "path" => path, "status" => status).increment(1);

    response
}
