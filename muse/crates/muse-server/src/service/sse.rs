//! Shared response policy for long-lived Server-Sent Event endpoints.

use std::{convert::Infallible, time::Duration};

use axum::{
    http::header::{HeaderName, HeaderValue, CACHE_CONTROL},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use tokio_stream::{Stream, StreamExt};

/// Apply the deployment-configured heartbeat without falling back to Axum's
/// built-in interval. A missing interval still returns a valid event-driven SSE
/// response; reconnect policy remains the browser's responsibility.
///
/// Every stream opens with one `connected` comment so a buffering proxy in
/// front (a gateway, the Vite dev proxy) flushes the response headers and the
/// client's `onopen` fires right away, instead of waiting for the first event
/// or the keep-alive.
pub fn response<S>(stream: S, keep_alive: Option<Duration>) -> Response
where
    S: Stream<Item = Result<Event, Infallible>> + Send + 'static,
{
    let hello = tokio_stream::once(Ok::<_, Infallible>(Event::default().comment("connected")));
    let stream = hello.chain(stream);
    let mut response = match keep_alive {
        Some(interval) => Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(interval))
            .into_response(),
        None => Sse::new(stream).into_response(),
    };
    // A buffering proxy between the browser and this server holds an SSE body
    // until a size threshold a trickle of events never reaches, which reads as
    // a hung stream. `no-transform` forbids intermediaries from recoding or
    // aggregating the body, and `X-Accel-Buffering: no` disables per-response
    // buffering in nginx-family proxies. Harmless where no proxy is involved.
    let headers = response.headers_mut();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}
