//! HTTP middleware: request-id generation, per-request metrics, span shaping.

use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request},
    http::HeaderValue,
    middleware::Next,
    response::Response,
};
use tower_http::request_id::{MakeRequestId, RequestId};
use tracing::Span;

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
