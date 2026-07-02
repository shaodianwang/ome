//! The activity feed as HTTP: `GET /v1/events` (recent changes, newest first) and
//! `GET /v1/stream` (the same feed as live Server-Sent Events). Both read the
//! in-memory `informers::Feed` on `AppState`; the stream lets the console update on
//! change instead of polling. Two routes, one concern — kept together.

use std::convert::Infallible;

use axum::{
    extract::{Query, State},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    Json,
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::{
    app_context::AppState,
    protocol::{to_val, ListParams, Listing},
};

/// `GET /v1/events` — recent resource changes observed across clusters (newest
/// first), from the in-memory activity feed. `?cluster=` narrows it.
pub async fn list(Query(params): Query<ListParams>, State(st): State<AppState>) -> Json<Listing> {
    let mut items = st.feed.recent();
    if let Some(cluster) = params.cluster.as_deref() {
        items.retain(|e| e.cluster == cluster);
    }
    Json(Listing::new(items.into_iter().map(to_val).collect()))
}

/// `GET /v1/stream` — Server-Sent Events of live resource changes (the activity
/// feed as it happens), so the UI updates on change instead of polling. Each
/// message is a `FeedEvent` as JSON; `?cluster=` is intentionally not supported
/// (the client filters), and a keep-alive comment holds idle connections open.
pub async fn stream(
    State(st): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>> {
    let live = BroadcastStream::new(st.feed.subscribe())
        // Drop lagged-subscriber errors; the next event resyncs the client.
        .filter_map(|res| res.ok())
        .map(|event| {
            let sse = SseEvent::default()
                .json_data(&event)
                .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
            Ok::<_, Infallible>(sse)
        });
    // Emit one comment immediately so a buffering proxy in front (e.g. the Vite
    // dev proxy) flushes the response headers and the client's `onopen` fires
    // right away, instead of waiting for the first change or the keep-alive.
    let hello = tokio_stream::once(Ok::<_, Infallible>(
        SseEvent::default().comment("connected"),
    ));
    Sse::new(hello.chain(live)).keep_alive(KeepAlive::default())
}
