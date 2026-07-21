//! Pod log fan-in: `GET /v1/logs/stream` follows the logs of every pod behind
//! one InferenceService on one cluster and multiplexes them into a single SSE
//! response — the console never opens one connection per pod.
//!
//! The split: this module owns the endpoint (parameter validation, admission,
//! response assembly); `roster` decides *which* (pod, container) streams a
//! session tails from the cached pod lister; `session` runs them (one task per
//! stream feeding a bounded channel, one aggregator batching frames out); and
//! `throttle` bounds the egress rate. Every failure after the stream starts is
//! reported as a `notice` event — only pre-stream failures are HTTP errors.

mod roster;
mod session;
mod throttle;

use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};

use axum::{
    extract::{Query, State},
    response::{sse::Event as SseEvent, Response},
};
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::StreamExt;

use super::{error::ApiError, sse};
use crate::{app_context::AppState, protocol::MetricsTimeRanges};

/// Operational settings for the log fan-in. Every value is deployment
/// configuration (see `Config::logs_config`); nothing here has a baked
/// default.
#[derive(Clone, Debug)]
pub struct LogsConfig {
    /// Concurrent sessions per replica; admission returns 503 beyond this.
    pub max_sessions: usize,
    /// Pods tailed by one session; excess pods are reported, not tailed.
    pub max_pods_per_session: usize,
    /// Concurrent pod log streams per workload cluster, across all sessions.
    pub max_tails_per_cluster: usize,
    /// Bounded per-session line channel between the tails and the egress.
    pub buffer_lines: usize,
    /// Byte cap for one log line; longer lines are truncated and flagged.
    pub line_bytes_max: usize,
    /// Per-session egress allowance; lines above it are dropped and counted.
    pub max_lines_per_sec: usize,
    /// Upper bound for the caller's `?tail=` request (clamped, not rejected).
    pub tail_lines_max: i64,
    /// History depth for mid-session tail starts and stream retries.
    pub catchup_lines: i64,
    /// Cadence of roster resyncs against the pod cache.
    pub pod_refresh: Duration,
    /// Hard session lifetime; the stream closes cleanly when it elapses.
    pub session_max: Duration,
    /// Delay between consecutive tail starts within one batch.
    pub start_stagger: Duration,
    /// Minimum interval between dropped-line notices.
    pub notice_interval: Duration,
    /// Wait before reopening a pod stream that ended or errored.
    pub tail_retry_backoff: Duration,
    /// Egress flush cadence; buffered lines are framed on this tick.
    pub flush_interval: Duration,
    /// Maximum lines carried by one SSE frame.
    pub batch_max_lines: usize,
    /// Client-side scrollback advice, surfaced through `GET /v1/config`.
    pub client_buffer_lines: usize,
    /// Optional since-selector policy for the console (same shape as the
    /// metrics range selector); the stream honors `?sinceSecs=` regardless.
    pub since_options: Option<MetricsTimeRanges>,
}

/// Shared runtime state: the settings plus the two admission gates — one
/// session gate per replica, one tail gate per configured cluster.
pub struct LogsState {
    pub config: LogsConfig,
    sessions: Arc<Semaphore>,
    cluster_tails: HashMap<String, Arc<Semaphore>>,
}

impl LogsState {
    pub fn new(config: LogsConfig, clusters: impl IntoIterator<Item = String>) -> Self {
        let sessions = Arc::new(Semaphore::new(config.max_sessions));
        let cluster_tails = clusters
            .into_iter()
            .map(|name| (name, Arc::new(Semaphore::new(config.max_tails_per_cluster))))
            .collect();
        Self {
            config,
            sessions,
            cluster_tails,
        }
    }

    /// Take one session slot, or `None` when the replica is at capacity.
    fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        self.sessions.clone().try_acquire_owned().ok()
    }

    /// The per-cluster tail gate. Every cluster registered at startup has one;
    /// `None` only for a name outside that set.
    fn tail_gate(&self, cluster: &str) -> Option<Arc<Semaphore>> {
        self.cluster_tails.get(cluster).cloned()
    }
}

/// Query parameters for `GET /v1/logs/stream`. Numeric fields arrive as
/// strings so a malformed value returns the structured JSON error envelope
/// instead of the extractor's plaintext rejection.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogStreamParams {
    /// Cluster to stream from (required — log fan-in is per cluster).
    pub cluster: Option<String>,
    /// Namespace of the InferenceService (required).
    pub namespace: Option<String>,
    /// InferenceService name (required); pods match its selector label.
    pub isvc: Option<String>,
    /// Narrow to one component (engine / decoder / router).
    pub component: Option<String>,
    /// Narrow to one exact pod name.
    pub pod: Option<String>,
    /// Narrow to one container name in every tailed pod.
    pub container: Option<String>,
    /// History lines per stream at session start; clamped to the configured cap.
    pub tail: Option<String>,
    /// Only lines newer than this many seconds at session start.
    pub since_secs: Option<String>,
}

/// `GET /v1/logs/stream` — SSE fan-in of the matching pods' follow-mode logs:
/// `pods` roster snapshots, batched `log` frames, and `notice` events for
/// truncation, drops, per-stream errors, and the session-lifetime close.
pub async fn stream(
    Query(params): Query<LogStreamParams>,
    State(st): State<AppState>,
) -> Result<Response, ApiError> {
    let logs = st
        .logs()
        .ok_or_else(|| ApiError::Unavailable("log streaming is disabled".to_owned()))?;
    let cluster_name = require_param(params.cluster.as_deref(), "cluster")?.to_owned();
    let request = session::SessionRequest {
        namespace: require_param(params.namespace.as_deref(), "namespace")?.to_owned(),
        isvc: require_param(params.isvc.as_deref(), "isvc")?.to_owned(),
        component: params.component,
        pod: params.pod,
        container: params.container,
        tail_lines: parse_tail(params.tail.as_deref(), logs.config.tail_lines_max)?,
        since_seconds: parse_since(params.since_secs.as_deref())?,
    };

    let selected = st.select(Some(&cluster_name))?;
    let cluster = selected
        .first()
        .ok_or_else(|| ApiError::Unavailable(format!("cluster {cluster_name} is not available")))?;
    let tail_gate = logs.tail_gate(&cluster.name).ok_or_else(|| {
        ApiError::Unavailable(format!(
            "cluster {cluster_name} has no log-stream capacity gate"
        ))
    })?;
    let permit = logs.try_admit().ok_or_else(|| {
        ApiError::Unavailable(format!(
            "log streaming session capacity ({}) reached; retry shortly",
            logs.config.max_sessions
        ))
    })?;

    let live = session::spawn(
        session::SessionContext {
            config: logs.config.clone(),
            pods: cluster.pods.clone(),
            client: cluster.client.clone(),
            tail_gate,
            request,
        },
        permit,
    );
    // Emit one comment immediately so a buffering proxy in front flushes the
    // response headers and the client's `onopen` fires right away — mirrors
    // the activity and metrics streams.
    let hello = tokio_stream::once(Ok::<_, Infallible>(
        SseEvent::default().comment("connected"),
    ));
    Ok(sse::response(
        hello.chain(live),
        st.sse_keep_alive_interval(),
    ))
}

fn require_param<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str, ApiError> {
    match value.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(ApiError::BadRequest(format!("{name} is required"))),
    }
}

/// Parse `?tail=` and clamp it to the configured cap — an oversized request
/// streams the maximum rather than erroring.
fn parse_tail(spec: Option<&str>, tail_lines_max: i64) -> Result<Option<i64>, ApiError> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let value = spec
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ApiError::BadRequest(format!("tail must be a positive integer, got '{spec}'"))
        })?;
    Ok(Some(value.min(tail_lines_max)))
}

fn parse_since(spec: Option<&str>) -> Result<Option<i64>, ApiError> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let value = spec
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "sinceSecs must be a positive integer, got '{spec}'"
            ))
        })?;
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::{parse_since, parse_tail, require_param};

    #[test]
    fn required_params_reject_missing_and_blank_values() {
        assert_eq!(
            require_param(Some("team-a"), "namespace").unwrap(),
            "team-a"
        );
        assert_eq!(
            require_param(Some(" team-a "), "namespace").unwrap(),
            "team-a"
        );
        assert!(require_param(None, "namespace").is_err());
        assert!(require_param(Some("  "), "namespace").is_err());
    }

    #[test]
    fn tail_is_clamped_to_the_configured_cap_not_rejected() {
        assert_eq!(parse_tail(None, 5000).unwrap(), None);
        assert_eq!(parse_tail(Some("200"), 5000).unwrap(), Some(200));
        assert_eq!(parse_tail(Some("99999"), 5000).unwrap(), Some(5000));
        assert!(parse_tail(Some("0"), 5000).is_err());
        assert!(parse_tail(Some("-3"), 5000).is_err());
        assert!(parse_tail(Some("many"), 5000).is_err());
    }

    #[test]
    fn since_secs_must_be_a_positive_integer() {
        assert_eq!(parse_since(None).unwrap(), None);
        assert_eq!(parse_since(Some("900")).unwrap(), Some(900));
        assert!(parse_since(Some("0")).is_err());
        assert!(parse_since(Some("soon")).is_err());
    }
}
