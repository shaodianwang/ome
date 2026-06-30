//! Logging + Prometheus metrics wiring.

use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use axum::{routing::get, Router};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

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
