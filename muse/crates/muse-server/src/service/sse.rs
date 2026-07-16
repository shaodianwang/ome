//! Shared response policy for long-lived Server-Sent Event endpoints.

use std::{convert::Infallible, time::Duration};

use axum::response::{
    sse::{Event, KeepAlive, Sse},
    IntoResponse, Response,
};
use tokio_stream::Stream;

/// Apply the deployment-configured heartbeat without falling back to Axum's
/// built-in interval. A missing interval still returns a valid event-driven SSE
/// response; reconnect policy remains the browser's responsibility.
pub fn response<S>(stream: S, keep_alive: Option<Duration>) -> Response
where
    S: Stream<Item = Result<Event, Infallible>> + Send + 'static,
{
    match keep_alive {
        Some(interval) => Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(interval))
            .into_response(),
        None => Sse::new(stream).into_response(),
    }
}
