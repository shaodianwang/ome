//! Live resource changes as HTTP: `GET /v1/events` returns human-facing activity,
//! while `GET /v1/stream` carries separately named `activity` and `invalidation`
//! Server-Sent Events. Invalidations include status-only and high-volume resource
//! changes that activity intentionally suppresses, without coupling cache refresh
//! to how an operator-facing feed is formatted.

use std::convert::Infallible;

use axum::{
    extract::{Query, State},
    response::{sse::Event as SseEvent, Response},
    Json,
};
use tokio_stream::{
    wrappers::{BroadcastStream, WatchStream},
    StreamExt,
};

use crate::{
    app_context::AppState,
    protocol::{to_val, ListParams, Listing},
    service::{error::ApiError, sse},
};

/// `GET /v1/events` — recent resource changes observed across clusters (newest
/// first), from the in-memory activity feed. `?cluster=` narrows it; an unknown
/// cluster is a 404.
pub async fn list(
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Listing>, ApiError> {
    let mut items = st.feed.recent();
    if let Some(cluster) = params.cluster.as_deref() {
        st.validate_cluster(cluster)?;
        items.retain(|e| e.cluster == cluster);
    }
    Ok(Json(Listing::new(items.into_iter().map(to_val).collect())))
}

/// `GET /v1/stream` — named Server-Sent Events for human-facing activity and
/// query-cache invalidation. `?cluster=` is intentionally not supported (the
/// client scopes fetched data); deployment-configured keep-alive comments hold
/// idle connections open.
pub async fn stream(State(st): State<AppState>) -> Response {
    let activity = BroadcastStream::new(st.feed.subscribe())
        // Drop lagged-subscriber errors; the next event resyncs the client.
        .filter_map(|res| res.ok())
        .map(|event| {
            let sse = SseEvent::default()
                .event("activity")
                .json_data(&event)
                .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
            Ok::<_, Infallible>(sse)
        });
    let invalidations = WatchStream::new(st.invalidations.subscribe())
        .filter_map(|event| event)
        .map(|event| {
            let sse = SseEvent::default()
                .event("invalidation")
                .json_data(event.as_ref())
                .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
            Ok::<_, Infallible>(sse)
        });
    let live = activity.merge(invalidations);
    let keep_alive = st.sse_keep_alive_interval();
    sse::response(live, keep_alive)
}
