//! Command-line / env configuration (clap).
//!
//! Behavioral values come from flags or env. Production defaults live in the
//! Helm chart; local-development values live in `muse/config/dev.env`.

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::Parser;

use crate::{
    chat_proxy::ChatProxyConfig,
    protocol::{DashboardPolicy, MetricsTimeRanges, SystemComponent, SystemComponents},
};

/// A workload cluster to aggregate: a name + an optional explicit kubeconfig
/// (None = use the ambient kube config).
#[derive(Debug, Clone)]
pub struct ClusterRef {
    pub name: String,
    pub kubeconfig: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct MetricsClusterId {
    cluster: String,
    id: String,
}

fn parse_metrics_cluster_id(spec: &str) -> Result<MetricsClusterId, String> {
    let (cluster, id) = spec
        .split_once('=')
        .ok_or_else(|| format!("expected cluster=ods-id, got '{spec}'"))?;
    if cluster.trim().is_empty() || id.trim().is_empty() {
        return Err(format!("expected cluster=ods-id, got '{spec}'"));
    }
    Ok(MetricsClusterId {
        cluster: cluster.to_owned(),
        id: id.to_owned(),
    })
}

fn parse_metrics_gateway_url(spec: &str) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(spec).map_err(|error| format!("invalid gateway URL: {error}"))?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err("metrics gateway URL must be an absolute HTTPS URL".to_owned());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("metrics gateway URL must not contain a query or fragment".to_owned());
    }
    if !url.path().ends_with('/') {
        return Err("metrics gateway URL path must end with '/'".to_owned());
    }
    Ok(url)
}

#[derive(Debug, Clone)]
pub struct MetricsGatewayConfig {
    pub url: reqwest::Url,
    pub ca_cert: Option<PathBuf>,
    pub query_routing: u8,
    pub cluster_ids: HashMap<String, String>,
    pub query_concurrency: NonZeroUsize,
    pub request_timeout: std::time::Duration,
    pub cache_ttl: std::time::Duration,
    pub cache_max_entries: NonZeroUsize,
    pub max_response_bytes: usize,
    pub max_series: NonZeroUsize,
    pub max_scope_pods: NonZeroUsize,
}

fn parse_cluster(spec: &str) -> Result<ClusterRef, String> {
    let (name, path) = spec
        .split_once('=')
        .ok_or_else(|| format!("expected name=path, got '{spec}'"))?;
    if name.is_empty() || path.is_empty() {
        return Err(format!("expected name=path, got '{spec}'"));
    }
    Ok(ClusterRef {
        name: name.to_owned(),
        kubeconfig: Some(PathBuf::from(path)),
    })
}

fn parse_positive_u64(spec: &str, unit: &str) -> Result<u64, String> {
    match spec.parse::<u64>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!(
            "expected a positive number of {unit}, got '{spec}'"
        )),
    }
}

// clap `value_parser` needs fn items, so each unit keeps a named wrapper.
fn parse_positive_secs(spec: &str) -> Result<u64, String> {
    parse_positive_u64(spec, "seconds")
}

fn parse_positive_bytes(spec: &str) -> Result<u64, String> {
    parse_positive_u64(spec, "bytes")
}

fn parse_positive_usize(spec: &str) -> Result<usize, String> {
    let value = spec
        .parse::<usize>()
        .map_err(|_| format!("expected a positive integer, got '{spec}'"))?;
    if value == 0 {
        return Err(format!("expected a positive integer, got '{spec}'"));
    }
    Ok(value)
}

fn parse_positive_nonzero_usize(spec: &str) -> Result<NonZeroUsize, String> {
    let value = parse_positive_usize(spec)?;
    NonZeroUsize::new(value).ok_or_else(|| format!("expected a positive integer, got '{spec}'"))
}

fn parse_query_routing(spec: &str) -> Result<u8, String> {
    let value = spec
        .parse::<u8>()
        .map_err(|_| format!("expected a Query Gateway routing value from 0 to 4, got '{spec}'"))?;
    if value > 4 {
        return Err(format!(
            "expected a Query Gateway routing value from 0 to 4, got '{spec}'"
        ));
    }
    Ok(value)
}

fn parse_positive_float(spec: &str) -> Result<f64, String> {
    let value = spec
        .parse::<f64>()
        .map_err(|_| format!("expected a positive number, got '{spec}'"))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("expected a positive number, got '{spec}'"));
    }
    Ok(value)
}

fn validate_threshold_pair(
    name: &str,
    warning: f64,
    critical: f64,
    max: f64,
    larger_is_worse: bool,
) -> Result<(), String> {
    if !warning.is_finite()
        || !critical.is_finite()
        || !(0.0..=max).contains(&warning)
        || !(0.0..=max).contains(&critical)
    {
        return Err(format!(
            "{name} thresholds must be finite values from 0 to {max}"
        ));
    }
    if (larger_is_worse && warning >= critical) || (!larger_is_worse && critical >= warning) {
        return Err(format!(
            "{name} warning and critical thresholds are reversed"
        ));
    }
    Ok(())
}

fn validate_dashboard_policy(policy: &DashboardPolicy) -> Result<(), String> {
    if let Some(value) = &policy.utilization {
        validate_threshold_pair(
            "utilization",
            value.warning_at,
            value.critical_at,
            100.0,
            true,
        )?;
    }
    if let Some(value) = &policy.availability {
        validate_threshold_pair(
            "availability",
            value.warning_below,
            value.critical_below,
            100.0,
            false,
        )?;
    }
    if let Some(value) = &policy.error_rate {
        validate_threshold_pair(
            "error rate",
            value.warning_at,
            value.critical_at,
            100.0,
            true,
        )?;
    }
    if let Some(value) = &policy.kv_cache {
        validate_threshold_pair("KV cache", value.warning_at, value.critical_at, 1.0, true)?;
    }
    if let Some(value) = &policy.prefix_cache {
        validate_threshold_pair(
            "prefix cache",
            value.warning_below,
            value.critical_below,
            1.0,
            false,
        )?;
    }
    Ok(())
}

fn parse_dashboard_policy(spec: &str) -> Result<DashboardPolicy, String> {
    let policy = serde_json::from_str(spec)
        .map_err(|error| format!("invalid dashboard policy JSON: {error}"))?;
    validate_dashboard_policy(&policy)?;
    Ok(policy)
}

/// Validate a configured metrics range selector, including its per-range
/// Query Gateway evaluation cadence.
fn validate_time_options(context: &str, ranges: &MetricsTimeRanges) -> Result<(), String> {
    if ranges.default.trim().is_empty() {
        return Err(format!("{context} default must not be empty"));
    }
    if ranges.options.is_empty() {
        return Err(format!("{context} options must not be empty"));
    }

    let mut ids = HashSet::new();
    for option in &ranges.options {
        if option.id.trim().is_empty() {
            return Err(format!("{context} option id must not be empty"));
        }
        if option.label.trim().is_empty() {
            return Err(format!(
                "{context} option '{}' must have a label",
                option.id
            ));
        }
        if option.seconds == 0 {
            return Err(format!(
                "{context} option '{}' must use positive seconds",
                option.id
            ));
        }
        if option.step_seconds == 0 {
            return Err(format!(
                "{context} option '{}' must use positive stepSeconds",
                option.id
            ));
        }
        if option.step_seconds > option.seconds {
            return Err(format!(
                "{context} option '{}' stepSeconds must not exceed its window",
                option.id
            ));
        }
        if !ids.insert(option.id.as_str()) {
            return Err(format!("{context} option id '{}' is duplicated", option.id));
        }
    }
    if !ids.contains(ranges.default.as_str()) {
        return Err(format!(
            "{context} default '{}' does not match an option id",
            ranges.default
        ));
    }
    Ok(())
}

fn validate_metrics_time_ranges(ranges: &MetricsTimeRanges) -> Result<(), String> {
    validate_time_options("metrics time-range", ranges)
}

fn parse_metrics_time_ranges(spec: &str) -> Result<MetricsTimeRanges, String> {
    let ranges = serde_json::from_str(spec)
        .map_err(|error| format!("invalid metrics time-range JSON: {error}"))?;
    validate_metrics_time_ranges(&ranges)?;
    Ok(ranges)
}

fn validate_system_component(component: &SystemComponent) -> Result<(), String> {
    if component.name.trim().is_empty() {
        return Err("system component name must not be empty".to_owned());
    }
    if component.namespace.trim().is_empty() {
        return Err(format!(
            "system component '{}' must set a namespace",
            component.name
        ));
    }
    let has_name = component
        .resource_name
        .as_deref()
        .is_some_and(|name| !name.trim().is_empty());
    if !has_name && component.selector.is_empty() {
        return Err(format!(
            "system component '{}' must set resourceName or a non-empty selector",
            component.name
        ));
    }
    Ok(())
}

fn parse_system_components(spec: &str) -> Result<SystemComponents, String> {
    let mut components: SystemComponents = serde_json::from_str(spec)
        .map_err(|error| format!("invalid system components JSON: {error}"))?;
    for component in &mut components.0 {
        // Normalize resourceName: trim, and treat a blank string as absent. Without
        // this, `resourceName: " "` plus a valid selector would pass validation but
        // then make matching look for an object literally named " " (ignoring the
        // selector) — a misleading `missing`.
        component.resource_name = component
            .resource_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        validate_system_component(component)?;
    }
    Ok(components)
}

fn parse_non_empty(spec: &str) -> Result<String, String> {
    let value = spec.trim();
    if value.is_empty() {
        return Err("value must not be empty".to_owned());
    }
    Ok(value.to_owned())
}

/// Whether a dashboard query is an instant (single value per series) or range
/// (time series) PromQL query.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
    Instant,
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryScope {
    Fleet,
    Deployment,
}

/// One dashboard metric: an id the console renders by, the PromQL to run, and — for
/// range queries — the maximum lookback and minimum permitted step, in seconds.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryDef {
    pub id: String,
    pub promql: String,
    pub kind: QueryType,
    pub window: Option<u64>,
    pub step: Option<u64>,
    pub scopes: Vec<QueryScope>,
}

/// Load the dashboard query set from a JSON file (`{ "queries": [ … ] }`).
pub fn load_queries(path: &Path) -> anyhow::Result<Vec<QueryDef>> {
    #[derive(serde::Deserialize)]
    struct QueriesFile {
        queries: Vec<QueryDef>,
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read metrics queries file {}", path.display()))?;
    let file: QueriesFile = serde_json::from_str(&text)
        .with_context(|| format!("parse metrics queries file {}", path.display()))?;
    let mut ids = HashSet::new();
    for query in &file.queries {
        if query.id.trim().is_empty() || !ids.insert(query.id.as_str()) {
            anyhow::bail!(
                "metrics query IDs must be non-empty and unique: '{}'",
                query.id
            );
        }
        if query.scopes.is_empty() {
            anyhow::bail!(
                "metrics query '{}' must configure at least one scope",
                query.id
            );
        }
        if !query.promql.contains("{{cluster_id}}") {
            anyhow::bail!("metrics query '{}' is missing {{cluster_id}}", query.id);
        }
        if !query.promql.contains("{{pod_matcher}}") {
            anyhow::bail!("metrics query '{}' is missing {{pod_matcher}}", query.id);
        }
        if matches!(query.kind, QueryType::Range)
            && (!matches!(query.window, Some(value) if value > 0)
                || !matches!(query.step, Some(value) if value > 0))
        {
            anyhow::bail!(
                "range metrics query '{}' must configure positive window and step values",
                query.id
            );
        }
    }
    Ok(file.queries)
}

/// Ensure every configured UI range is fully represented by every range query
/// and does not request a cadence finer than the catalog permits.
/// The selector is optional, so callers can disable only it on an incompatible
/// custom query catalog while retaining the full fetched metrics snapshot.
pub fn validate_metrics_time_ranges_against_queries(
    ranges: &MetricsTimeRanges,
    queries: &[QueryDef],
) -> Result<(), String> {
    validate_metrics_time_ranges(ranges)?;

    // load_queries already rejects range queries without positive window/step values.
    let minimum_window = queries
        .iter()
        .filter(|query| matches!(query.kind, QueryType::Range))
        .filter_map(|query| query.window)
        .min();
    let maximum_step = queries
        .iter()
        .filter(|query| matches!(query.kind, QueryType::Range))
        .filter_map(|query| query.step)
        .max();

    let (Some(minimum_window), Some(maximum_step)) = (minimum_window, maximum_step) else {
        return Err("metrics time ranges require at least one range query".to_owned());
    };
    for option in &ranges.options {
        if option.step_seconds < maximum_step {
            return Err(format!(
                "metrics time-range option '{}' step ({}s) is shorter than the largest query minimum step ({}s)",
                option.id, option.step_seconds, maximum_step
            ));
        }
        if option.seconds > minimum_window {
            return Err(format!(
                "metrics time-range option '{}' ({}s) exceeds the smallest query window ({}s)",
                option.id, option.seconds, minimum_window
            ));
        }
    }
    Ok(())
}

/// How an informer fetches its initial state from an apiserver.
///
/// Bandwidth note: the apiserver gzip-compresses large classic LIST responses
/// but never streaming (WatchList) responses. On a bandwidth-constrained link,
/// `paginated` therefore syncs large caches far faster; `streaming` trades that
/// for lower apiserver memory pressure and is preferred in-cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ListStrategy {
    /// Stream the initial list (WatchList) on apiservers new enough to support it,
    /// else paginate — decided per cluster from its reported version. WatchList is
    /// beta and on by default since Kubernetes 1.32.
    Auto,
    /// Always stream the initial list (WatchList). Will fail to sync on apiservers
    /// without WatchList support.
    Streaming,
    /// Always use paginated list calls. Works on every apiserver.
    Paginated,
}

impl ListStrategy {
    /// Whether to use streaming for an apiserver at this minor version. `Auto`
    /// enables it for Kubernetes >= 1.32 (WatchList beta, on by default).
    pub fn use_streaming(self, server_minor: Option<u32>) -> bool {
        match self {
            ListStrategy::Streaming => true,
            ListStrategy::Paginated => false,
            ListStrategy::Auto => server_minor.is_some_and(|m| m >= 32),
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[command(name = "muse", about = "MUSE read-plane API server (OEP-0021)")]
pub struct Config {
    /// Address to bind to (API and metrics).
    #[arg(long, env = "MUSE_HOST")]
    pub host: IpAddr,

    /// Port for the read-plane API.
    #[arg(long, env = "MUSE_PORT")]
    pub port: u16,

    /// Port for the Prometheus `/metrics` endpoint (served off the API surface).
    #[arg(long, env = "MUSE_METRICS_PORT")]
    pub metrics_port: u16,

    /// Emit logs as JSON instead of human-readable text.
    #[arg(
        long,
        env = "MUSE_LOG_JSON",
        action = clap::ArgAction::Set,
        required = true
    )]
    pub log_json: bool,

    /// Tracing filter expression (for example `info,muse=debug`).
    #[arg(long, env = "MUSE_LOG_FILTER", value_parser = parse_non_empty)]
    pub log_filter: String,

    /// Tokio worker-thread stack size in bytes. Large generated Kubernetes types
    /// may require more than Tokio's standard worker stack.
    #[arg(
        long,
        env = "MUSE_WORKER_THREAD_STACK_BYTES",
        value_parser = parse_positive_usize
    )]
    pub worker_thread_stack_bytes: usize,

    /// Workload clusters to aggregate, as `name=/path/to/kubeconfig` (repeatable).
    /// If none are given, the ambient kube config uses `--local-cluster-name`.
    #[arg(long = "cluster", value_parser = parse_cluster)]
    pub clusters: Vec<ClusterRef>,

    /// Display identity for the ambient in-cluster kubeconfig when no explicit
    /// `--cluster` entries are configured. Required only for that ambient mode.
    #[arg(
        long = "local-cluster-name",
        env = "MUSE_LOCAL_CLUSTER_NAME",
        value_parser = parse_non_empty
    )]
    pub local_cluster_name: Option<String>,

    /// Informer initial-list strategy: `auto` (stream on Kubernetes >= 1.32, else
    /// paginate), `streaming`, or `paginated`. Streaming (WatchList) is far cheaper
    /// on large clusters; `auto` picks it per cluster from the apiserver version.
    #[arg(long = "list-strategy", env = "MUSE_LIST_STRATEGY", value_enum)]
    pub list_strategy: ListStrategy,

    /// Maximum seconds allowed for all informers on one connected cluster to
    /// complete their initial list. A cluster that exceeds the deadline becomes
    /// `timed_out`; readiness can then serve a completed partial fleet when at
    /// least one other cluster is ready. No binary default: unset waits without a
    /// deadline, while the deployment chart supplies its operational policy.
    #[arg(long = "cache-sync-timeout-secs", env = "MUSE_CACHE_SYNC_TIMEOUT_SECS")]
    pub cache_sync_timeout_secs: Option<NonZeroU64>,

    /// Minimum seconds between capacity recomputes. The rollup is recomputed when
    /// the Node/Pod informers observe a change (coalesced to at most once per this
    /// interval) — not on a timer or per request — so it's reactive without
    /// storming under pod churn, and served from a cached snapshot (flat request
    /// latency). Lower = fresher, higher = cheaper.
    #[arg(
        long = "capacity-refresh-secs",
        env = "MUSE_CAPACITY_REFRESH_SECS",
        value_parser = parse_positive_secs
    )]
    pub capacity_refresh_secs: u64,

    /// Milliseconds to batch Kubernetes informer changes before publishing one
    /// live-query invalidation. This bounds SSE and refetch churn while retaining
    /// every changed resource domain. No binary default: deployment config owns
    /// the responsiveness/load tradeoff; absent means no time-based delay.
    #[arg(
        long = "live-invalidation-batch-ms",
        env = "MUSE_LIVE_INVALIDATION_BATCH_MS"
    )]
    pub live_invalidation_batch_ms: Option<NonZeroU64>,

    /// Seconds between SSE keep-alive comments on the activity and metrics streams.
    /// No binary default: absent config leaves the streams event-driven, while the
    /// deployment chart supplies the heartbeat policy needed by its intermediaries.
    #[arg(long = "sse-keep-alive-secs", env = "MUSE_SSE_KEEP_ALIVE_SECS")]
    pub sse_keep_alive_secs: Option<NonZeroU64>,

    /// Age at which a canary sitting in one step becomes a fleet-triage finding
    /// ("canary stuck at step N"). No binary default: absent config disables
    /// step-age detection; the deployment chart supplies the fleet's policy.
    #[arg(
        long = "attention-stuck-step-secs",
        env = "MUSE_ATTENTION_STUCK_STEP_SECS"
    )]
    pub attention_stuck_step_secs: Option<NonZeroU64>,

    /// Backstop interval for a capacity recompute when no informer signal arrives.
    #[arg(
        long = "capacity-backstop-secs",
        env = "MUSE_CAPACITY_BACKSTOP_SECS",
        value_parser = parse_positive_secs
    )]
    pub capacity_backstop_secs: u64,

    /// Maximum fragmented placement units returned per size bucket.
    #[arg(
        long = "capacity-max-fragments",
        env = "MUSE_CAPACITY_MAX_FRAGMENTS",
        value_parser = parse_positive_usize
    )]
    pub capacity_max_fragments: usize,

    /// Maximum rows returned in each ranked capacity list.
    #[arg(
        long = "capacity-max-consumers",
        env = "MUSE_CAPACITY_MAX_CONSUMERS",
        value_parser = parse_positive_usize
    )]
    pub capacity_max_consumers: usize,

    /// Maximum time to wait for the initial apiserver version preflight.
    #[arg(
        long = "apiserver-preflight-timeout-secs",
        env = "MUSE_APISERVER_PREFLIGHT_TIMEOUT_SECS",
        value_parser = parse_positive_secs
    )]
    pub apiserver_preflight_timeout_secs: u64,

    /// Number of recent activity events retained in memory for new subscribers.
    #[arg(
        long = "event-history-limit",
        env = "MUSE_EVENT_HISTORY_LIMIT",
        value_parser = parse_positive_usize
    )]
    pub event_history_limit: usize,

    /// Grace period for in-flight requests after SIGTERM before forced exit.
    #[arg(
        long = "shutdown-grace-secs",
        env = "MUSE_SHUTDOWN_GRACE_SECS",
        value_parser = parse_positive_secs
    )]
    pub shutdown_grace_secs: u64,

    /// Prometheus histogram upkeep cadence.
    #[arg(
        long = "metrics-upkeep-secs",
        env = "MUSE_METRICS_UPKEEP_SECS",
        value_parser = parse_positive_secs
    )]
    pub metrics_upkeep_secs: u64,

    /// HTTP request-duration histogram buckets, in seconds, strictly increasing.
    #[arg(
        long = "http-latency-buckets",
        env = "MUSE_HTTP_LATENCY_BUCKETS",
        value_delimiter = ',',
        value_parser = parse_positive_float,
        required = true
    )]
    pub http_latency_buckets: Vec<f64>,

    /// Dashboard health thresholds as one JSON object. The deployment config owns
    /// every value; absent or `{}` renders health tones neutrally.
    #[arg(
        long = "dashboard-policy-json",
        env = "MUSE_DASHBOARD_POLICY_JSON",
        value_parser = parse_dashboard_policy
    )]
    pub dashboard_policy: Option<DashboardPolicy>,

    /// OME's own control-plane components to surface in the System panel, as one
    /// JSON array of descriptors — each `{ name, group?, kind:
    /// "Deployment"|"DaemonSet", namespace, selector, clusters? }`. No baked-in
    /// set: the namespaces and label selectors that identify the controller /
    /// scheduler / webhook / model-agent are install-specific, so the deployment
    /// config owns them (flag / env / GitOps). Absent or `[]` ⇒ the System panel
    /// is empty rather than guessing at a component.
    #[arg(
        long = "system-components-json",
        env = "MUSE_SYSTEM_COMPONENTS_JSON",
        value_parser = parse_system_components
    )]
    pub system_components: Option<SystemComponents>,

    /// Namespace containing Argo CD Application resources. When unset, delivery
    /// observation is disabled; when set, a separate ambient-cluster informer is
    /// started and failures do not affect workload-cluster readiness.
    #[arg(
        long = "argo-namespace",
        env = "MUSE_ARGO_NAMESPACE",
        value_parser = parse_non_empty
    )]
    pub argo_namespace: Option<String>,

    /// Node label(s) naming the accelerator's outer topology domain — the RDMA
    /// island / network fabric for GPUs, or the physical block for TPUs — used to
    /// group capacity. Comma-separated; the first label present on a node wins, so
    /// one setting spans providers that label topology differently. No baked
    /// default: supply the label key(s) for your environment via deployment config
    /// (flag / env / GitOps). A node carrying none of these labels groups as
    /// "unassigned" and is excluded from placement rather than guessed at.
    #[arg(
        long = "rdma-island-label",
        env = "MUSE_RDMA_ISLAND_LABEL",
        value_delimiter = ','
    )]
    pub rdma_island_labels: Vec<String>,

    /// Node label(s) naming the accelerator's inner topology domain — the NVLink
    /// domain for GPUs (e.g. an NVL72 rack), or the ICI sub-block for TPUs — within
    /// one island. Comma-separated, first-present wins. No baked default: supply the
    /// label key(s) for your environment via deployment config (flag / env /
    /// GitOps). A node carrying none of these labels groups as "unassigned" and is
    /// excluded from placement.
    #[arg(
        long = "nvlink-domain-label",
        env = "MUSE_NVLINK_DOMAIN_LABEL",
        value_delimiter = ','
    )]
    pub nvlink_domain_labels: Vec<String>,

    /// Enable the console's inference test proxy: `POST /v1/inferenceservices/
    /// {name}/chat` forwards one OpenAI-style request to that model's own endpoint
    /// and returns the reply. Off by default — it is a human-driven debug tool, not
    /// a serving path, so it is opt-in. When off, the console shows a copyable curl
    /// command instead of a live chat box.
    #[arg(
        long = "chat-enabled",
        env = "MUSE_CHAT_ENABLED",
        action = clap::ArgAction::Set,
        required = true
    )]
    pub chat_enabled: bool,

    /// Total timeout (seconds) for one proxied inference test request — the whole
    /// budget, which must allow for the model's generation time. No binary default:
    /// when chat is enabled without this value, the proxy stays disabled and the
    /// rest of MUSE continues serving.
    #[arg(
        long = "chat-timeout-secs",
        env = "MUSE_CHAT_TIMEOUT_SECS",
        value_parser = parse_positive_secs
    )]
    pub chat_timeout_secs: Option<u64>,

    /// Maximum upstream response body size (bytes) for one proxied inference test.
    /// No binary default: when chat is enabled without this value, the proxy stays
    /// disabled. The limit is enforced while streaming, including when the upstream
    /// omits or lies about Content-Length.
    #[arg(
        long = "chat-max-response-bytes",
        env = "MUSE_CHAT_MAX_RESPONSE_BYTES",
        value_parser = parse_positive_bytes
    )]
    pub chat_max_response_bytes: Option<u64>,

    /// Connect timeout (seconds) for a proxied inference test request — bounds just
    /// the TCP+TLS connect, so an unreachable model endpoint fails fast instead of
    /// stalling until the total `--chat-timeout-secs` elapses. Optional; the total
    /// timeout still bounds the request when this is absent.
    #[arg(
        long = "chat-connect-timeout-secs",
        env = "MUSE_CHAT_CONNECT_TIMEOUT_SECS",
        value_parser = parse_positive_secs
    )]
    pub chat_connect_timeout_secs: Option<u64>,

    /// Additional browser-side seconds beyond the server request budget. This lets
    /// the server's specific timeout response reach the console before its own
    /// AbortController fires. Optional; no browser deadline is added when absent.
    #[arg(
        long = "chat-client-grace-secs",
        env = "MUSE_CHAT_CLIENT_GRACE_SECS",
        value_parser = parse_positive_secs
    )]
    pub chat_client_grace_secs: Option<u64>,

    /// Exact external gateway hosts the caller may select instead of the cached
    /// InferenceService status URL. Entries may include a non-default port
    /// (`gateway.example:8443`). Empty means only the exact status origin is valid.
    #[arg(
        long = "chat-allowed-host",
        env = "MUSE_CHAT_ALLOWED_HOSTS",
        value_delimiter = ','
    )]
    pub chat_allowed_hosts: Vec<String>,

    /// Explicit external gateway DNS suffixes. Matching is label-anchored; no suffix
    /// is inferred from the InferenceService hostname. Non-default ports require an
    /// exact `--chat-allowed-host` entry.
    #[arg(
        long = "chat-allowed-host-suffix",
        env = "MUSE_CHAT_ALLOWED_HOST_SUFFIXES",
        value_delimiter = ','
    )]
    pub chat_allowed_host_suffixes: Vec<String>,

    /// Permit plaintext HTTP chat targets. Enable only for a controlled
    /// development environment.
    #[arg(
        long = "chat-allow-http",
        env = "MUSE_CHAT_ALLOW_HTTP",
        action = clap::ArgAction::Set,
        required = true
    )]
    pub chat_allow_http: bool,

    /// Client certificate (PEM) the chat proxy presents for mTLS to the model's
    /// gateway. The muse pod mounts its service identity here; all of
    /// `--chat-client-cert`, `--chat-client-key`, and `--chat-ca-cert` are required
    /// together. No baked default: leave all three unset for a plain TLS request.
    #[arg(long = "chat-client-cert", env = "MUSE_CHAT_CLIENT_CERT")]
    pub chat_client_cert: Option<String>,

    /// Private key (PEM) paired with `--chat-client-cert`. Supplying only part of
    /// the cert/key/CA tuple disables the proxy.
    #[arg(long = "chat-client-key", env = "MUSE_CHAT_CLIENT_KEY")]
    pub chat_client_key: Option<String>,

    /// CA bundle (PEM) verifying the gateway's server cert for the chat proxy.
    /// Supply it with both client identity files, or leave all three unset to use
    /// system roots without client authentication.
    #[arg(long = "chat-ca-cert", env = "MUSE_CHAT_CA_CERT")]
    pub chat_ca_cert: Option<String>,

    /// PromQL HTTP base URL, including its configured API prefix and a trailing
    /// slash. No binary default; absent leaves external metrics disabled.
    #[arg(
        long = "metrics-gateway-url",
        env = "MUSE_METRICS_GATEWAY_URL",
        value_parser = parse_metrics_gateway_url
    )]
    pub metrics_gateway_url: Option<reqwest::Url>,

    /// Optional PEM CA bundle used to verify the Query Gateway server identity.
    #[arg(long = "metrics-gateway-ca-cert", env = "MUSE_METRICS_GATEWAY_CA_CERT")]
    pub metrics_gateway_ca_cert: Option<PathBuf>,

    /// Query Gateway backend routing value applied to every metrics request.
    /// No binary default; the deployment must choose its metrics backend.
    #[arg(
        long = "metrics-query-routing",
        env = "MUSE_METRICS_QUERY_ROUTING",
        value_parser = parse_query_routing
    )]
    pub metrics_query_routing: Option<u8>,

    /// Explicit MUSE cluster-name to metrics-backend cluster-ID mappings.
    #[arg(
        long = "metrics-cluster-id",
        env = "MUSE_METRICS_CLUSTER_IDS",
        value_delimiter = ',',
        value_parser = parse_metrics_cluster_id
    )]
    pub metrics_cluster_ids: Vec<MetricsClusterId>,

    /// How often (seconds) an active console view refetches its scoped metrics
    /// snapshot. The server cache coalesces identical requests within its TTL.
    #[arg(
        long = "metrics-refresh-secs",
        env = "MUSE_METRICS_REFRESH_SECS",
        value_parser = parse_positive_secs
    )]
    pub metrics_refresh_secs: u64,

    /// Maximum number of in-flight Query Gateway requests per MUSE replica,
    /// shared across every active scope and workload cluster.
    #[arg(
        long = "metrics-query-concurrency",
        env = "MUSE_METRICS_QUERY_CONCURRENCY",
        value_parser = parse_positive_nonzero_usize
    )]
    pub metrics_query_concurrency: Option<NonZeroUsize>,

    /// Maximum seconds for one Query Gateway request. A timeout becomes a
    /// per-query error and does not fail inventory or successful queries.
    #[arg(
        long = "metrics-request-timeout-secs",
        env = "MUSE_METRICS_REQUEST_TIMEOUT_SECS"
    )]
    pub metrics_request_timeout_secs: Option<NonZeroU64>,

    /// Seconds a completed scoped snapshot may be reused by this replica.
    #[arg(long = "metrics-cache-ttl-secs", env = "MUSE_METRICS_CACHE_TTL_SECS")]
    pub metrics_cache_ttl_secs: Option<NonZeroU64>,

    /// Maximum distinct scoped snapshots retained by this replica.
    #[arg(
        long = "metrics-cache-max-entries",
        env = "MUSE_METRICS_CACHE_MAX_ENTRIES"
    )]
    pub metrics_cache_max_entries: Option<NonZeroUsize>,

    /// Maximum bytes accepted from one Query Gateway response.
    #[arg(
        long = "metrics-max-response-bytes",
        env = "MUSE_METRICS_MAX_RESPONSE_BYTES"
    )]
    pub metrics_max_response_bytes: Option<NonZeroU64>,

    /// Maximum result series accepted from one PromQL response.
    #[arg(long = "metrics-max-series", env = "MUSE_METRICS_MAX_SERIES")]
    pub metrics_max_series: Option<NonZeroUsize>,

    /// Maximum unambiguous deployment pods admitted into one scoped query.
    #[arg(long = "metrics-max-scope-pods", env = "MUSE_METRICS_MAX_SCOPE_PODS")]
    pub metrics_max_scope_pods: Option<NonZeroUsize>,

    /// Deployment Monitoring range selector configuration as one JSON object:
    /// `{ "default": "15m", "options": [{ "id", "label", "seconds", "stepSeconds" }] }`.
    /// The deployment owns every option and the default. Unset keeps the full
    /// cached query range visible and omits the selector.
    #[arg(
        long = "metrics-time-ranges-json",
        env = "MUSE_METRICS_TIME_RANGES_JSON",
        value_parser = parse_metrics_time_ranges
    )]
    pub metrics_time_ranges: Option<MetricsTimeRanges>,

    /// Path to the dashboard query set. Every entry declares its allowed
    /// scopes and reserved cluster/pod placeholders. Engine and table
    /// names remain deployment data rather than Rust constants.
    #[arg(long = "metrics-queries-file", env = "MUSE_METRICS_QUERIES_FILE")]
    pub metrics_queries_file: Option<PathBuf>,

    /// Maximum seconds for one on-demand pod-inspection apiserver call (live pod
    /// status, events, or a log tail). Pod inspection is a deliberate exception
    /// to the informer-only rule — strictly per-click, one object, bounded — and
    /// stays disabled unless all three `pod-*` values are configured.
    #[arg(
        long = "pod-inspect-timeout-secs",
        env = "MUSE_POD_INSPECT_TIMEOUT_SECS"
    )]
    pub pod_inspect_timeout_secs: Option<NonZeroU64>,

    /// Log-tail line cap for pod inspection: requests are clamped to this many
    /// trailing lines.
    #[arg(long = "pod-log-tail-lines", env = "MUSE_POD_LOG_TAIL_LINES")]
    pub pod_log_tail_lines: Option<NonZeroUsize>,

    /// Maximum bytes accepted from one pod log fetch.
    #[arg(long = "pod-log-max-bytes", env = "MUSE_POD_LOG_MAX_BYTES")]
    pub pod_log_max_bytes: Option<NonZeroU64>,

    /// Maximum pods followed by one live log-stream session (a component can be
    /// hundreds of pods; a session follows at most this many). Streaming also
    /// requires the `pod-*` inspection values and stays disabled without them.
    #[arg(long = "pod-log-stream-max-pods", env = "MUSE_POD_LOG_STREAM_MAX_PODS")]
    pub pod_log_stream_max_pods: Option<NonZeroUsize>,

    /// Hard wall-clock cap (seconds) on one live log-stream session.
    #[arg(long = "pod-log-stream-max-secs", env = "MUSE_POD_LOG_STREAM_MAX_SECS")]
    pub pod_log_stream_max_secs: Option<NonZeroU64>,

    /// Directory of the built web console (the SPA) to serve at `/`, with the
    /// API under `/v1`. Unset ⇒ API-only (the console is served elsewhere). The
    /// container image bakes the bundle in and sets this; override to point at
    /// your own build.
    #[arg(long = "web-dir", env = "MUSE_WEB_DIR")]
    pub web_dir: Option<PathBuf>,
}

/// Effective on-demand pod-inspection policy; `None` leaves the feature off and
/// the rest of the read plane untouched.
#[derive(Clone, Copy, Debug)]
pub struct PodInspectConfig {
    pub timeout: std::time::Duration,
    pub log_tail_lines: usize,
    pub log_max_bytes: u64,
}

/// Effective live log-streaming policy (on top of [`PodInspectConfig`]);
/// `None` leaves the streaming endpoint off.
#[derive(Clone, Copy, Debug)]
pub struct PodLogStreamConfig {
    pub max_pods: usize,
    pub max_duration: std::time::Duration,
}

impl Config {
    /// Validate cross-field constraints that clap cannot express.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.http_latency_buckets.is_empty()
            || !self
                .http_latency_buckets
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            anyhow::bail!("MUSE_HTTP_LATENCY_BUCKETS must be strictly increasing");
        }
        if self.clusters.is_empty() && self.local_cluster_name.is_none() {
            anyhow::bail!(
                "MUSE_LOCAL_CLUSTER_NAME is required when no explicit clusters are configured"
            );
        }
        Ok(())
    }

    pub fn api_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }

    pub fn metrics_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.metrics_port)
    }

    /// Build the effective Query Gateway policy. Supplying an endpoint opts in;
    /// every load-control value must then be explicit so the binary never invents
    /// an operational limit.
    pub fn metrics_gateway_config(&self) -> Result<Option<MetricsGatewayConfig>, String> {
        let Some(url) = self.metrics_gateway_url.clone() else {
            return Ok(None);
        };
        let required = |name: &str| {
            format!("metrics gateway enabled without {name}; metrics will remain disabled")
        };
        let query_concurrency = self
            .metrics_query_concurrency
            .ok_or_else(|| required("MUSE_METRICS_QUERY_CONCURRENCY"))?;
        let query_routing = self
            .metrics_query_routing
            .ok_or_else(|| required("MUSE_METRICS_QUERY_ROUTING"))?;
        let request_timeout = self
            .metrics_request_timeout_secs
            .ok_or_else(|| required("MUSE_METRICS_REQUEST_TIMEOUT_SECS"))?;
        let cache_ttl = self
            .metrics_cache_ttl_secs
            .ok_or_else(|| required("MUSE_METRICS_CACHE_TTL_SECS"))?;
        if cache_ttl.get() < request_timeout.get() {
            return Err(
                "MUSE_METRICS_CACHE_TTL_SECS must be at least MUSE_METRICS_REQUEST_TIMEOUT_SECS"
                    .to_owned(),
            );
        }
        let cache_max_entries = self
            .metrics_cache_max_entries
            .ok_or_else(|| required("MUSE_METRICS_CACHE_MAX_ENTRIES"))?;
        let max_response_bytes = self
            .metrics_max_response_bytes
            .ok_or_else(|| required("MUSE_METRICS_MAX_RESPONSE_BYTES"))?;
        let max_response_bytes = usize::try_from(max_response_bytes.get())
            .map_err(|_| required("a platform-sized MUSE_METRICS_MAX_RESPONSE_BYTES"))?;
        let max_series = self
            .metrics_max_series
            .ok_or_else(|| required("MUSE_METRICS_MAX_SERIES"))?;
        let max_scope_pods = self
            .metrics_max_scope_pods
            .ok_or_else(|| required("MUSE_METRICS_MAX_SCOPE_PODS"))?;

        let mut cluster_ids = HashMap::new();
        for mapping in &self.metrics_cluster_ids {
            if cluster_ids
                .insert(mapping.cluster.clone(), mapping.id.clone())
                .is_some()
            {
                return Err(format!(
                    "duplicate metrics cluster mapping for '{}'",
                    mapping.cluster
                ));
            }
        }

        Ok(Some(MetricsGatewayConfig {
            url,
            ca_cert: self.metrics_gateway_ca_cert.clone(),
            query_routing,
            cluster_ids,
            query_concurrency,
            request_timeout: std::time::Duration::from_secs(request_timeout.get()),
            cache_ttl: std::time::Duration::from_secs(cache_ttl.get()),
            cache_max_entries,
            max_response_bytes,
            max_series,
            max_scope_pods,
        }))
    }

    /// Build the effective pod-inspection policy. Enabling it without every
    /// bound explicit is a configuration error the caller reports by leaving the
    /// feature disabled; no limit is invented here.
    pub fn pod_inspect_config(&self) -> Option<PodInspectConfig> {
        Some(PodInspectConfig {
            timeout: std::time::Duration::from_secs(self.pod_inspect_timeout_secs?.get()),
            log_tail_lines: self.pod_log_tail_lines?.get(),
            log_max_bytes: self.pod_log_max_bytes?.get(),
        })
    }

    /// Build the effective log-streaming policy; requires the base inspection
    /// policy plus both stream bounds.
    pub fn pod_log_stream_config(&self) -> Option<PodLogStreamConfig> {
        self.pod_inspect_config()?;
        Some(PodLogStreamConfig {
            max_pods: self.pod_log_stream_max_pods?.get(),
            max_duration: std::time::Duration::from_secs(self.pod_log_stream_max_secs?.get()),
        })
    }

    /// The clusters to watch — the configured list, or the ambient config under
    /// its explicitly configured display name.
    pub fn cluster_refs(&self) -> anyhow::Result<Vec<ClusterRef>> {
        if self.clusters.is_empty() {
            Ok(vec![ClusterRef {
                name: self
                    .local_cluster_name
                    .clone()
                    .context("local cluster name was not configured")?,
                kubeconfig: None,
            }])
        } else {
            Ok(self.clusters.clone())
        }
    }

    /// Build the optional chat proxy's effective settings. Enabling the feature
    /// without an explicit total timeout and response cap is a configuration error
    /// that the caller handles by leaving chat disabled; no value is invented here.
    pub fn chat_proxy_config(&self) -> Result<Option<ChatProxyConfig>, String> {
        if !self.chat_enabled {
            return Ok(None);
        }
        let timeout_secs = self.chat_timeout_secs.ok_or_else(|| {
            "chat proxy enabled without MUSE_CHAT_TIMEOUT_SECS; proxy will remain disabled"
                .to_owned()
        })?;
        let max_response_bytes = self.chat_max_response_bytes.ok_or_else(|| {
            "chat proxy enabled without MUSE_CHAT_MAX_RESPONSE_BYTES; proxy will remain disabled"
                .to_owned()
        })?;
        let max_response_bytes = usize::try_from(max_response_bytes).map_err(|_| {
            "MUSE_CHAT_MAX_RESPONSE_BYTES exceeds this platform's addressable size; proxy will remain disabled"
                .to_owned()
        })?;
        Ok(Some(ChatProxyConfig {
            timeout: std::time::Duration::from_secs(timeout_secs),
            max_response_bytes,
            connect_timeout: self
                .chat_connect_timeout_secs
                .map(std::time::Duration::from_secs),
            client_grace: self
                .chat_client_grace_secs
                .map(std::time::Duration::from_secs),
            allowed_hosts: self.chat_allowed_hosts.clone(),
            allowed_host_suffixes: self.chat_allowed_host_suffixes.clone(),
            allow_http: self.chat_allow_http,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    use clap::Parser;

    use super::{
        load_queries, parse_system_components, validate_metrics_time_ranges_against_queries,
        Config, QueryDef, QueryScope, QueryType,
    };

    #[test]
    fn system_components_parse_and_validate() {
        let ok = parse_system_components(
            r#"[
              { "name": "Controller manager", "group": "control-plane",
                "kind": "Deployment", "namespace": "ome",
                "selector": { "control-plane": "ome-controller-manager" },
                "clusters": ["hub"] },
              { "name": "Model agent", "kind": "DaemonSet", "namespace": "ome",
                "resourceName": "ome-model-agent-daemonset" }
            ]"#,
        )
        .expect("valid descriptors parse")
        .0;
        assert_eq!(ok.len(), 2);
        assert!(ok[0].applies_to("hub"));
        assert!(!ok[0].applies_to("member"));
        // No `clusters` ⇒ expected on every aggregated cluster.
        assert!(ok[1].applies_to("member"));
        // Matching by object name alone (no selector) is valid.
        assert_eq!(
            ok[1].resource_name.as_deref(),
            Some("ome-model-agent-daemonset")
        );

        // Neither resourceName nor selector, an unknown kind, and a blank namespace
        // are each rejected.
        assert!(parse_system_components(
            r#"[{ "name": "x", "kind": "Deployment", "namespace": "ome", "selector": {} }]"#
        )
        .is_err());
        assert!(parse_system_components(
            r#"[{ "name": "x", "kind": "StatefulSet", "namespace": "ome",
                 "selector": { "a": "b" } }]"#
        )
        .is_err());
        assert!(parse_system_components(
            r#"[{ "name": "x", "kind": "Deployment", "namespace": "",
                 "selector": { "a": "b" } }]"#
        )
        .is_err());

        // A present-but-blank resourceName normalizes to None, so matching falls
        // back to the selector rather than looking for an object named " ".
        let blank = parse_system_components(
            r#"[{ "name": "x", "kind": "Deployment", "namespace": "ome",
                 "resourceName": "  ", "selector": { "a": "b" } }]"#,
        )
        .expect("blank resourceName with a selector is valid")
        .0;
        assert!(blank[0].resource_name.is_none());
        // Blank resourceName with no selector is still rejected.
        assert!(parse_system_components(
            r#"[{ "name": "x", "kind": "Deployment", "namespace": "ome", "resourceName": " " }]"#
        )
        .is_err());
    }

    fn configured_args(chat_enabled: bool, allow_http: bool) -> Vec<String> {
        [
            "muse",
            "--host=127.0.0.1",
            "--port=18080",
            "--metrics-port=19090",
            "--log-json=false",
            "--log-filter=info,muse=debug",
            "--worker-thread-stack-bytes=1048576",
            "--local-cluster-name=test-cluster",
            "--list-strategy=paginated",
            "--capacity-refresh-secs=3",
            "--capacity-backstop-secs=30",
            "--capacity-max-fragments=4",
            "--capacity-max-consumers=3",
            "--apiserver-preflight-timeout-secs=2",
            "--event-history-limit=10",
            "--shutdown-grace-secs=1",
            "--metrics-upkeep-secs=4",
            "--http-latency-buckets=0.001,0.1,1",
            "--metrics-refresh-secs=7",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain([
            format!("--chat-enabled={chat_enabled}"),
            format!("--chat-allow-http={allow_http}"),
        ])
        .collect()
    }

    fn parse_with(
        chat_enabled: bool,
        allow_http: bool,
        extra: &[&str],
    ) -> Result<Config, clap::Error> {
        let mut args = configured_args(chat_enabled, allow_http);
        args.extend(extra.iter().map(|value| (*value).to_owned()));
        Config::try_parse_from(args)
    }

    #[test]
    fn enabled_chat_without_timeout_degrades_to_disabled_config() {
        let config = parse_with(true, false, &[]).unwrap();
        assert!(config.chat_proxy_config().is_err());
    }

    #[test]
    fn enabled_chat_without_response_cap_degrades_to_disabled_config() {
        let config = parse_with(true, false, &["--chat-timeout-secs=75"]).unwrap();
        let err = config.chat_proxy_config().unwrap_err();
        assert!(err.contains("MUSE_CHAT_MAX_RESPONSE_BYTES"));
    }

    #[test]
    fn chat_timeout_and_target_policy_are_config_driven() {
        let config = parse_with(
            true,
            true,
            &[
                "--chat-timeout-secs=75",
                "--chat-max-response-bytes=1048576",
                "--chat-connect-timeout-secs=4",
                "--chat-client-grace-secs=6",
                "--chat-allowed-host=gateway.example:8443",
                "--chat-allowed-host-suffix=serving.example",
            ],
        )
        .unwrap();
        let chat = config.chat_proxy_config().unwrap().unwrap();

        assert_eq!(chat.timeout, Duration::from_secs(75));
        assert_eq!(chat.max_response_bytes, 1_048_576);
        assert_eq!(chat.connect_timeout, Some(Duration::from_secs(4)));
        assert_eq!(chat.client_grace, Some(Duration::from_secs(6)));
        assert_eq!(chat.allowed_hosts, ["gateway.example:8443"]);
        assert_eq!(chat.allowed_host_suffixes, ["serving.example"]);
        assert!(chat.allow_http);
    }

    #[test]
    fn zero_chat_timeout_is_rejected_at_config_parse_time() {
        let result = parse_with(false, false, &["--chat-timeout-secs=0"]);
        assert!(result.is_err());
    }

    #[test]
    fn runtime_values_have_no_binary_fallbacks() {
        assert!(Config::try_parse_from(["muse"]).is_err());

        let config = parse_with(false, false, &[]).unwrap();
        config.validate().unwrap();
        assert_eq!(config.port, 18080);
        assert_eq!(config.metrics_port, 19090);
        assert_eq!(config.worker_thread_stack_bytes, 1_048_576);
        assert_eq!(config.event_history_limit, 10);
        assert_eq!(config.capacity_max_fragments, 4);
        assert_eq!(config.capacity_max_consumers, 3);
        assert_eq!(config.http_latency_buckets, [0.001, 0.1, 1.0]);
        assert_eq!(config.metrics_query_concurrency, None);
        assert_eq!(config.metrics_request_timeout_secs, None);
        assert_eq!(config.metrics_gateway_url, None);
        assert_eq!(config.metrics_gateway_ca_cert, None);
        assert_eq!(config.metrics_query_routing, None);
        assert!(config.metrics_cluster_ids.is_empty());
        assert_eq!(config.metrics_cache_ttl_secs, None);
        assert_eq!(config.metrics_cache_max_entries, None);
        assert_eq!(config.metrics_max_response_bytes, None);
        assert_eq!(config.metrics_max_series, None);
        assert_eq!(config.metrics_max_scope_pods, None);
        assert_eq!(config.metrics_time_ranges, None);
        assert_eq!(config.cluster_refs().unwrap()[0].name, "test-cluster");
        assert!(config.dashboard_policy.is_none());
    }

    #[test]
    fn metrics_time_ranges_are_config_driven_and_validated() {
        let config = parse_with(
            false,
            false,
            &[r#"--metrics-time-ranges-json={"default":"15m","options":[{"id":"15m","label":"15m","seconds":900,"stepSeconds":60},{"id":"1h","label":"1h","seconds":3600,"stepSeconds":60}]}"#],
        )
        .unwrap();
        let ranges = config.metrics_time_ranges.unwrap();
        assert_eq!(ranges.default, "15m");
        assert_eq!(ranges.options[0].seconds, 900);
        assert_eq!(ranges.options[0].step_seconds, 60);

        for invalid in [
            r#"{"default":"15m","options":[]}"#,
            r#"{"default":"missing","options":[{"id":"15m","label":"15m","seconds":900,"stepSeconds":60}]}"#,
            r#"{"default":"15m","options":[{"id":"15m","label":"15m","seconds":900}]}"#,
            r#"{"default":"15m","options":[{"id":"15m","label":"15m","seconds":0,"stepSeconds":60}]}"#,
            r#"{"default":"15m","options":[{"id":"15m","label":"15m","seconds":900,"stepSeconds":0}]}"#,
            r#"{"default":"15m","options":[{"id":"15m","label":"15m","seconds":60,"stepSeconds":120}]}"#,
            r#"{"default":"15m","options":[{"id":"15m","label":"15m","seconds":900,"stepSeconds":60},{"id":"15m","label":"other","seconds":3600,"stepSeconds":60}]}"#,
        ] {
            let arg = format!("--metrics-time-ranges-json={invalid}");
            assert!(
                parse_with(false, false, &[&arg]).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn metrics_time_ranges_must_fit_every_range_query() {
        let config = parse_with(
            false,
            false,
            &[r#"--metrics-time-ranges-json={"default":"15m","options":[{"id":"15m","label":"15m","seconds":900,"stepSeconds":60},{"id":"6h","label":"6h","seconds":21600,"stepSeconds":300}]}"#],
        )
        .unwrap();
        let ranges = config.metrics_time_ranges.unwrap();
        let query = |id: &str, window: u64, step: u64| QueryDef {
            id: id.to_owned(),
            promql: "up{t=\"test\",cluster_id=\"{{cluster_id}}\"{{pod_matcher}}}".to_owned(),
            kind: QueryType::Range,
            window: Some(window),
            step: Some(step),
            scopes: vec![QueryScope::Fleet, QueryScope::Deployment],
        };

        assert!(validate_metrics_time_ranges_against_queries(
            &ranges,
            &[query("wide", 21_600, 60), query("dense", 43_200, 60)],
        )
        .is_ok());
        assert!(validate_metrics_time_ranges_against_queries(
            &ranges,
            &[query("short", 3_600, 300)],
        )
        .is_err());
        assert!(validate_metrics_time_ranges_against_queries(
            &ranges,
            &[query("sparse", 21_600, 1_800)],
        )
        .is_err());
        assert!(validate_metrics_time_ranges_against_queries(&ranges, &[]).is_err());
    }

    #[test]
    fn dashboard_health_policy_is_config_driven() {
        let config = parse_with(
            false,
            false,
            &[r#"--dashboard-policy-json={"utilization":{"warningAt":72,"criticalAt":91},"availability":{"warningBelow":99.9,"criticalBelow":99},"errorRate":{"warningAt":0.2,"criticalAt":2},"kvCache":{"warningAt":0.65,"criticalAt":0.88},"prefixCache":{"warningBelow":0.75,"criticalBelow":0.45}}"#],
        )
        .unwrap();
        config.validate().unwrap();
        let policy = config.dashboard_policy.unwrap();
        let wire = serde_json::to_value(&policy).unwrap();

        assert_eq!(wire["errorRate"]["criticalAt"], 2.0);
        assert_eq!(wire["prefixCache"]["warningBelow"], 0.75);
        assert_eq!(policy.utilization.unwrap().warning_at, 72.0);
        assert_eq!(policy.availability.unwrap().critical_below, 99.0);
        assert_eq!(policy.error_rate.unwrap().critical_at, 2.0);
        assert_eq!(policy.kv_cache.unwrap().warning_at, 0.65);
        assert_eq!(policy.prefix_cache.unwrap().warning_below, 0.75);
    }

    #[test]
    fn dashboard_health_policy_requires_complete_threshold_pairs() {
        assert!(parse_with(
            false,
            false,
            &[r#"--dashboard-policy-json={"errorRate":{"warningAt":0.2}}"#],
        )
        .is_err());
    }

    #[test]
    fn dashboard_health_policy_rejects_noncanonical_threshold_keys() {
        assert!(parse_with(
            false,
            false,
            &[r#"--dashboard-policy-json={"utilization":{"warningAtPct":75,"criticalAtPct":90}}"#],
        )
        .is_err());
    }

    #[test]
    fn dashboard_health_policy_rejects_reversed_warning_and_critical_order() {
        assert!(parse_with(
            false,
            false,
            &[r#"--dashboard-policy-json={"kvCache":{"warningAt":0.9,"criticalAt":0.7}}"#],
        )
        .is_err());
    }

    #[test]
    fn dashboard_health_policy_rejects_values_outside_the_metric_domain() {
        assert!(parse_with(
            false,
            false,
            &[r#"--dashboard-policy-json={"prefixCache":{"warningBelow":1.1,"criticalBelow":0.4}}"#],
        )
        .is_err());
    }

    #[test]
    fn capacity_limits_must_be_positive() {
        for flag in ["capacity-max-fragments", "capacity-max-consumers"] {
            let mut args = configured_args(false, false);
            args.retain(|arg| !arg.starts_with(&format!("--{flag}=")));
            args.push(format!("--{flag}=0"));
            assert!(Config::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn metrics_request_limits_are_optional_and_must_be_positive() {
        let config = parse_with(
            false,
            false,
            &[
                "--metrics-query-concurrency=3",
                "--metrics-request-timeout-secs=15",
            ],
        )
        .unwrap();
        assert_eq!(
            config.metrics_query_concurrency.map(NonZeroUsize::get),
            Some(3)
        );
        assert_eq!(
            config.metrics_request_timeout_secs.map(NonZeroU64::get),
            Some(15)
        );

        for flag in ["metrics-query-concurrency", "metrics-request-timeout-secs"] {
            assert!(parse_with(false, false, &[&format!("--{flag}=0")]).is_err());
        }
    }

    #[test]
    fn metrics_gateway_configuration_is_explicit_and_bounded() {
        let config = parse_with(
            false,
            false,
            &[
                "--metrics-gateway-url=https://query.example/api/v1/",
                "--metrics-gateway-ca-cert=/etc/muse/query-ca.pem",
                "--metrics-query-routing=2",
                "--metrics-cluster-id=cluster-a=123/us-east-2/cluster-a",
                "--metrics-cluster-id=cluster-b=456/us-west-2/cluster-b",
                "--metrics-query-concurrency=4",
                "--metrics-request-timeout-secs=12",
                "--metrics-cache-ttl-secs=20",
                "--metrics-cache-max-entries=128",
                "--metrics-max-response-bytes=8388608",
                "--metrics-max-series=2000",
                "--metrics-max-scope-pods=1000",
            ],
        )
        .unwrap();
        let gateway = config.metrics_gateway_config().unwrap().unwrap();

        assert_eq!(gateway.url.as_str(), "https://query.example/api/v1/");
        assert_eq!(gateway.query_routing, 2);
        assert_eq!(
            gateway.ca_cert.as_deref(),
            Some(std::path::Path::new("/etc/muse/query-ca.pem"))
        );
        assert_eq!(gateway.cluster_ids["cluster-a"], "123/us-east-2/cluster-a");
        assert_eq!(gateway.query_concurrency.get(), 4);
        assert_eq!(gateway.request_timeout, Duration::from_secs(12));
        assert_eq!(gateway.cache_ttl, Duration::from_secs(20));
        assert_eq!(gateway.cache_max_entries.get(), 128);
        assert_eq!(gateway.max_response_bytes, 8_388_608);
        assert_eq!(gateway.max_series.get(), 2000);
        assert_eq!(gateway.max_scope_pods.get(), 1000);
    }

    #[test]
    fn metrics_gateway_requires_every_load_limit() {
        let base = [
            "--metrics-gateway-url=https://query.example/api/v1/",
            "--metrics-query-routing=2",
            "--metrics-query-concurrency=4",
            "--metrics-request-timeout-secs=12",
            "--metrics-cache-ttl-secs=20",
            "--metrics-cache-max-entries=128",
            "--metrics-max-response-bytes=8388608",
            "--metrics-max-series=2000",
            "--metrics-max-scope-pods=1000",
        ];
        for omitted in &base[1..] {
            let args = base
                .iter()
                .copied()
                .filter(|value| value != omitted)
                .collect::<Vec<_>>();
            let config = parse_with(false, false, &args).unwrap();
            assert!(
                config.metrics_gateway_config().is_err(),
                "accepted missing {omitted}"
            );
        }

        for flag in [
            "metrics-cache-ttl-secs",
            "metrics-cache-max-entries",
            "metrics-max-response-bytes",
            "metrics-max-series",
            "metrics-max-scope-pods",
        ] {
            assert!(parse_with(false, false, &[&format!("--{flag}=0")]).is_err());
        }
        assert!(parse_with(
            false,
            false,
            &["--metrics-gateway-url=http://query.example/api/v1/"]
        )
        .is_err());
        let short_cache = parse_with(
            false,
            false,
            &[
                "--metrics-gateway-url=https://query.example/api/v1/",
                "--metrics-query-routing=2",
                "--metrics-query-concurrency=4",
                "--metrics-request-timeout-secs=12",
                "--metrics-cache-ttl-secs=5",
                "--metrics-cache-max-entries=128",
                "--metrics-max-response-bytes=8388608",
                "--metrics-max-series=2000",
                "--metrics-max-scope-pods=1000",
            ],
        )
        .unwrap();
        assert!(short_cache.metrics_gateway_config().is_err());
    }

    #[test]
    fn histogram_buckets_must_be_strictly_increasing() {
        let mut config = parse_with(false, false, &[]).unwrap();
        config.http_latency_buckets = vec![0.1, 0.1];
        assert!(config.validate().is_err());
    }

    #[test]
    fn ambient_cluster_requires_an_explicit_name() {
        let mut config = parse_with(false, false, &[]).unwrap();
        config.local_cluster_name = None;
        assert!(config.validate().is_err());
        assert!(config.cluster_refs().is_err());
    }

    #[test]
    fn metrics_query_timing_is_required_only_for_range_queries() {
        let path = std::env::temp_dir().join(format!(
            "muse-query-config-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::write(
            &path,
            r#"{"queries":[{"id":"instant","promql":"up{t=\"test\",cluster_id=\"{{cluster_id}}\"{{pod_matcher}}}","kind":"instant","scopes":["fleet","deployment"]}]}"#,
        )
        .unwrap();
        let instant = load_queries(&path).unwrap();
        assert_eq!(instant.len(), 1);
        assert_eq!(instant[0].window, None);
        assert_eq!(instant[0].step, None);

        fs::write(
            &path,
            r#"{"queries":[{"id":"range","promql":"up{t=\"test\",cluster_id=\"{{cluster_id}}\"{{pod_matcher}}}","kind":"range","window":0,"step":1,"scopes":["fleet"]}]}"#,
        )
        .unwrap();
        let range = load_queries(&path);
        let _ = fs::remove_file(path);
        assert!(range.is_err());
    }

    #[test]
    fn metrics_queries_require_scope_and_reserved_placeholders() {
        let path = std::env::temp_dir().join(format!(
            "muse-query-scope-config-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));

        for invalid in [
            r#"{"queries":[{"id":"missing-scopes","promql":"up{t=\"test\",cluster_id=\"{{cluster_id}}\"{{pod_matcher}}}","kind":"instant","scopes":[]}]}"#,
            r#"{"queries":[{"id":"missing-cluster","promql":"up{t=\"test\"{{pod_matcher}}}","kind":"instant","scopes":["fleet"]}]}"#,
            r#"{"queries":[{"id":"missing-deployment","promql":"up{t=\"test\",cluster_id=\"{{cluster_id}}\"}","kind":"instant","scopes":["deployment"]}]}"#,
        ] {
            fs::write(&path, invalid).unwrap();
            assert!(load_queries(&path).is_err(), "accepted {invalid}");
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn bundled_grafana_catalog_is_deployment_only_and_fully_scoped() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../charts/muse/files/metrics-queries.json");
        let queries = load_queries(&path).unwrap();

        assert_eq!(queries.len(), 25);
        for query in queries {
            assert!(
                !query.promql.contains("t=\""),
                "{} still contains an ODS table selector",
                query.id
            );
            assert!(
                query.promql.contains("{{cluster_id}}"),
                "{} has no cluster scope",
                query.id
            );
            assert!(
                query.promql.contains("{{pod_matcher}}"),
                "{} has no deployment scope",
                query.id
            );
            assert_eq!(
                query.promql.matches("{{cluster_id}}").count(),
                query.promql.matches("cluster=\"{{cluster_id}}\"").count(),
                "{} has a cluster placeholder outside a Grafana cluster selector",
                query.id
            );
            assert_eq!(query.scopes, [QueryScope::Deployment]);
        }
    }

    #[test]
    fn zero_chat_response_cap_is_rejected_at_config_parse_time() {
        let result = parse_with(false, false, &["--chat-max-response-bytes=0"]);
        assert!(result.is_err());
    }

    #[test]
    fn cache_sync_timeout_is_optional_and_requires_positive_seconds() {
        let config = parse_with(false, false, &[]).unwrap();
        assert_eq!(config.cache_sync_timeout_secs, None);

        let config = parse_with(false, false, &["--cache-sync-timeout-secs=45"]).unwrap();
        assert_eq!(
            config.cache_sync_timeout_secs.map(|value| value.get()),
            Some(45)
        );
        assert!(parse_with(false, false, &["--cache-sync-timeout-secs=0"]).is_err());
    }

    #[test]
    fn sse_keep_alive_is_optional_and_requires_positive_seconds() {
        let config = parse_with(false, false, &[]).unwrap();
        assert_eq!(config.sse_keep_alive_secs, None);

        let config = parse_with(false, false, &["--sse-keep-alive-secs=20"]).unwrap();
        assert_eq!(
            config.sse_keep_alive_secs.map(|value| value.get()),
            Some(20)
        );
        assert!(parse_with(false, false, &["--sse-keep-alive-secs=0"]).is_err());
    }
}
