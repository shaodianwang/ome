//! Recent-activity feed — a small in-memory ring buffer of resource changes
//! observed across clusters, plus a live broadcast for streaming clients.
//!
//! Each cluster's OME informers broadcast `Applied`/`Deleted` events (the SDK's
//! `Informer::subscribe`); a background task per kind pushes them here. The buffer
//! is bounded (oldest dropped), so `GET /v1/events` returns a rolling window of
//! recent activity newest-first; `GET /v1/stream` (SSE) subscribes to the live
//! broadcast so the UI updates on change instead of polling. Nodes/pods are not
//! fed in (far too high-volume); only the OME kinds are.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::sync::broadcast;

/// One observed change to a watched resource.
#[derive(Clone, Serialize)]
pub struct FeedEvent {
    pub cluster: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub name: String,
    /// `"applied"` (add or update) or `"deleted"`.
    pub action: &'static str,
    /// Wall-clock seconds since the Unix epoch when muse observed the change.
    pub at: u64,
}

/// A bounded, shared activity buffer with a live broadcast. Cheap to clone (Arc +
/// broadcast sender inside).
#[derive(Clone)]
pub struct Feed {
    inner: Arc<Mutex<VecDeque<FeedEvent>>>,
    tx: broadcast::Sender<FeedEvent>,
    cap: usize,
}

impl Feed {
    pub fn new(cap: usize) -> Self {
        let (tx, _) = broadcast::channel(cap.max(16));
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(cap))),
            tx,
            cap,
        }
    }

    /// Record an event: append to the backlog (evicting the oldest if full) and
    /// publish it to live subscribers. A poisoned lock is recovered (the buffer is
    /// non-critical); a send with no subscribers is a no-op.
    pub fn push(&self, event: FeedEvent) {
        {
            let mut q = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if q.len() == self.cap {
                q.pop_front();
            }
            q.push_back(event.clone());
        }
        let _ = self.tx.send(event);
    }

    /// A snapshot of the backlog, newest first.
    pub fn recent(&self) -> Vec<FeedEvent> {
        let q = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        q.iter().rev().cloned().collect()
    }

    /// A receiver for live events as they're recorded (for the SSE stream).
    pub fn subscribe(&self) -> broadcast::Receiver<FeedEvent> {
        self.tx.subscribe()
    }
}

/// Wall-clock seconds since the Unix epoch (0 if the clock is before the epoch).
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
