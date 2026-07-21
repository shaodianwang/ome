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

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use axum::{
    extract::{Query, State},
    response::Response,
};
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{error::ApiError, sse};
use crate::{app_context::AppState, protocol::MetricsTimeRanges};

/// Operational settings for the log fan-in. Every value is deployment
/// configuration (see `Config::logs_config`); nothing here has a baked
/// default.
#[derive(Clone, Debug)]
pub struct LogsConfig {
    /// Per-cluster allowlist: only these clusters serve log streams. A
    /// request for any other cluster is refused before a stream opens, and
    /// the console hides the Logs tab for it. Empty means the feature is
    /// enabled but no cluster serves logs yet.
    pub clusters: BTreeSet<String>,
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
        // Gates exist only for allowlisted clusters — the same set the
        // endpoint admits — so capacity is never reserved for a cluster that
        // cannot serve logs.
        let cluster_tails = clusters
            .into_iter()
            .filter(|name| config.clusters.contains(name))
            .map(|name| (name, Arc::new(Semaphore::new(config.max_tails_per_cluster))))
            .collect();
        Self {
            config,
            sessions,
            cluster_tails,
        }
    }

    /// Whether a cluster is allowlisted for log streaming.
    fn cluster_enabled(&self, cluster: &str) -> bool {
        self.config.clusters.contains(cluster)
    }

    /// Take one session slot, or `None` when the replica is at capacity.
    fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        self.sessions.clone().try_acquire_owned().ok()
    }

    /// The per-cluster tail gate. Every allowlisted cluster registered at
    /// startup has one; `None` only for a name outside that set.
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
    /// Reserved for server-side filtering: accepted and ignored, so filtering
    /// can move server-side later without a protocol change.
    pub grep: Option<String>,
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
    // Per-cluster gate: only allowlisted clusters serve logs, so a cluster
    // whose log permissions haven't landed is refused fast instead of
    // streaming per-pod permission notices.
    if !logs.cluster_enabled(&cluster_name) {
        return Err(ApiError::BadRequest(format!(
            "log streaming is not enabled for cluster {cluster_name}"
        )));
    }
    // The reserved filter param is deliberately accepted and ignored.
    let _ = params.grep;
    let request = session::SessionRequest {
        namespace: require_param(params.namespace.as_deref(), "namespace")?.to_owned(),
        isvc: require_param(params.isvc.as_deref(), "isvc")?.to_owned(),
        component: params.component,
        pod: params.pod,
        container: params.container,
        tail_lines: parse_tail(params.tail.as_deref(), logs.config.tail_lines_max)?,
        since_seconds: parse_positive_i64(params.since_secs.as_deref(), "sinceSecs")?,
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
    Ok(sse::response(live, st.sse_keep_alive_interval()))
}

fn require_param<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str, ApiError> {
    match value.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(ApiError::BadRequest(format!("{name} is required"))),
    }
}

/// Parse an optional positive-integer query parameter; `name` labels the
/// error for the caller.
fn parse_positive_i64(spec: Option<&str>, name: &str) -> Result<Option<i64>, ApiError> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let value = spec
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ApiError::BadRequest(format!("{name} must be a positive integer, got '{spec}'"))
        })?;
    Ok(Some(value))
}

/// Parse `?tail=` and clamp it to the configured cap — an oversized request
/// streams the maximum rather than erroring.
fn parse_tail(spec: Option<&str>, tail_lines_max: i64) -> Result<Option<i64>, ApiError> {
    let value = parse_positive_i64(spec, "tail")?;
    Ok(value.map(|value| value.min(tail_lines_max)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        parse_positive_i64, parse_tail, require_param, LogStreamParams, LogsConfig, LogsState,
    };

    fn config_with_allowlist(clusters: &[&str]) -> LogsConfig {
        LogsConfig {
            clusters: clusters.iter().map(|name| (*name).to_owned()).collect(),
            max_sessions: 2,
            max_pods_per_session: 4,
            max_tails_per_cluster: 8,
            buffer_lines: 16,
            line_bytes_max: 512,
            max_lines_per_sec: 100,
            tail_lines_max: 500,
            catchup_lines: 10,
            pod_refresh: Duration::from_secs(5),
            session_max: Duration::from_secs(60),
            start_stagger: Duration::from_millis(1),
            notice_interval: Duration::from_secs(5),
            tail_retry_backoff: Duration::from_secs(1),
            flush_interval: Duration::from_millis(50),
            batch_max_lines: 10,
            client_buffer_lines: 100,
            since_options: None,
        }
    }

    #[test]
    fn only_allowlisted_clusters_are_admitted_and_gated() {
        let registered = ["cw".to_owned(), "gcp".to_owned()];
        let state = LogsState::new(config_with_allowlist(&["cw"]), registered);

        // Admission refuses the registered-but-unlisted cluster before any
        // stream machinery is touched; only the allowlisted cluster holds a
        // tail-capacity gate.
        assert!(state.cluster_enabled("cw"));
        assert!(!state.cluster_enabled("gcp"));
        assert!(!state.cluster_enabled("unknown"));
        assert!(state.tail_gate("cw").is_some());
        assert!(state.tail_gate("gcp").is_none());

        // Ships-dark posture: enabled feature, empty allowlist, no cluster
        // admitted.
        let dark = LogsState::new(config_with_allowlist(&[]), ["cw".to_owned()]);
        assert!(!dark.cluster_enabled("cw"));
        assert!(dark.tail_gate("cw").is_none());
    }

    #[test]
    fn reserved_grep_param_is_accepted_not_rejected() {
        let uri: axum::http::Uri =
            "/v1/logs/stream?cluster=cw&namespace=team-a&isvc=llama&grep=error"
                .parse()
                .unwrap();
        let axum::extract::Query(params) =
            axum::extract::Query::<LogStreamParams>::try_from_uri(&uri)
                .expect("the reserved grep param must not reject the request");
        assert_eq!(params.grep.as_deref(), Some("error"));
        assert_eq!(params.cluster.as_deref(), Some("cw"));
        assert_eq!(params.isvc.as_deref(), Some("llama"));
    }

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
        assert_eq!(parse_positive_i64(None, "sinceSecs").unwrap(), None);
        assert_eq!(
            parse_positive_i64(Some("900"), "sinceSecs").unwrap(),
            Some(900)
        );
        assert!(parse_positive_i64(Some("0"), "sinceSecs").is_err());
        assert!(parse_positive_i64(Some("soon"), "sinceSecs").is_err());
    }
}
