//! Command-line / env configuration (clap).
//!
//! No behavioural value is a code literal — host, ports, and log format come
//! from flags or env (defaults live in the flag definitions, the config surface).

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::Parser;

use crate::chat_proxy::ChatProxyConfig;

/// A workload cluster to aggregate: a name + an optional explicit kubeconfig
/// (None = use the ambient kube config).
#[derive(Debug, Clone)]
pub struct ClusterRef {
    pub name: String,
    pub kubeconfig: Option<PathBuf>,
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

/// A Prometheus the apiserver can proxy PromQL to: `namespace/name:port` (e.g.
/// `ome/ome-prometheus:9090`). The metrics endpoint reaches it through the
/// cluster's own kube client via the `services/proxy` subresource, so MUSE holds
/// no Prometheus credential of its own.
#[derive(Debug, Clone)]
pub struct PromTarget {
    pub namespace: String,
    pub service: String,
    pub port: u16,
}

fn parse_prom_target(spec: &str) -> Result<PromTarget, String> {
    let err = || format!("expected namespace/name:port, got '{spec}'");
    let (namespace, rest) = spec.split_once('/').ok_or_else(err)?;
    let (service, port) = rest.split_once(':').ok_or_else(err)?;
    if namespace.is_empty() || service.is_empty() {
        return Err(err());
    }
    let port = port.parse().map_err(|_| format!("bad port in '{spec}'"))?;
    Ok(PromTarget {
        namespace: namespace.to_owned(),
        service: service.to_owned(),
        port,
    })
}

fn parse_positive_secs(spec: &str) -> Result<u64, String> {
    let value = spec
        .parse::<u64>()
        .map_err(|_| format!("expected a positive number of seconds, got '{spec}'"))?;
    if value == 0 {
        return Err(format!(
            "expected a positive number of seconds, got '{spec}'"
        ));
    }
    Ok(value)
}

/// Whether a dashboard query is an instant (single value per series) or range
/// (time series) PromQL query.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
    Instant,
    #[default]
    Range,
}

/// One dashboard metric: an id the console renders by, the PromQL to run, and — for
/// range queries — the lookback window and step, both in seconds.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryDef {
    pub id: String,
    pub promql: String,
    #[serde(default)]
    pub kind: QueryType,
    #[serde(default = "default_window")]
    pub window: u64,
    #[serde(default = "default_step")]
    pub step: u64,
}

fn default_window() -> u64 {
    21600 // 6h, matching the dashboard's default time range
}
fn default_step() -> u64 {
    300 // 5m resolution
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
    Ok(file.queries)
}

/// How an informer fetches its initial state from an apiserver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ListStrategy {
    /// Stream the initial list (WatchList) on apiservers new enough to support it,
    /// else paginate — decided per cluster from its reported version. WatchList is
    /// beta and on by default since Kubernetes 1.32.
    #[default]
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
    #[arg(long, env = "MUSE_HOST", default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    pub host: IpAddr,

    /// Port for the read-plane API.
    #[arg(long, env = "MUSE_PORT", default_value_t = 8080)]
    pub port: u16,

    /// Port for the Prometheus `/metrics` endpoint (served off the API surface).
    #[arg(long, env = "MUSE_METRICS_PORT", default_value_t = 9090)]
    pub metrics_port: u16,

    /// Emit logs as JSON instead of human-readable text.
    #[arg(long, env = "MUSE_LOG_JSON", default_value_t = false)]
    pub log_json: bool,

    /// Workload clusters to aggregate, as `name=/path/to/kubeconfig` (repeatable).
    /// If none are given, the ambient kube config is used as one cluster "default".
    #[arg(long = "cluster", value_parser = parse_cluster)]
    pub clusters: Vec<ClusterRef>,

    /// Informer initial-list strategy: `auto` (stream on Kubernetes >= 1.32, else
    /// paginate), `streaming`, or `paginated`. Streaming (WatchList) is far cheaper
    /// on large clusters; `auto` picks it per cluster from the apiserver version.
    #[arg(long = "list-strategy", env = "MUSE_LIST_STRATEGY", value_enum, default_value_t = ListStrategy::Auto)]
    pub list_strategy: ListStrategy,

    /// Minimum seconds between capacity recomputes. The rollup is recomputed when
    /// the Node/Pod informers observe a change (coalesced to at most once per this
    /// interval) — not on a timer or per request — so it's reactive without
    /// storming under pod churn, and served from a cached snapshot (flat request
    /// latency). Lower = fresher, higher = cheaper.
    #[arg(
        long = "capacity-refresh-secs",
        env = "MUSE_CAPACITY_REFRESH_SECS",
        default_value_t = 10
    )]
    pub capacity_refresh_secs: u64,

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
    #[arg(long = "chat-enabled", env = "MUSE_CHAT_ENABLED")]
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

    /// Permit plaintext HTTP chat targets. Off when absent; enable only for a
    /// controlled development environment.
    #[arg(long = "chat-allow-http", env = "MUSE_CHAT_ALLOW_HTTP")]
    pub chat_allow_http: bool,

    /// Client certificate (PEM) the chat proxy presents for mTLS to the model's
    /// gateway. The muse pod mounts its service identity here; with
    /// `--chat-client-key` + `--chat-ca-cert` it lets the in-console test reach an
    /// mTLS-gated endpoint. No baked default: unset ⇒ a plain TLS request.
    #[arg(long = "chat-client-cert", env = "MUSE_CHAT_CLIENT_CERT")]
    pub chat_client_cert: Option<String>,

    /// Private key (PEM) paired with `--chat-client-cert`; omit if the cert file
    /// already carries the key.
    #[arg(long = "chat-client-key", env = "MUSE_CHAT_CLIENT_KEY")]
    pub chat_client_key: Option<String>,

    /// CA bundle (PEM) verifying the gateway's server cert for the chat proxy.
    /// No baked default: unset ⇒ the system roots.
    #[arg(long = "chat-ca-cert", env = "MUSE_CHAT_CA_CERT")]
    pub chat_ca_cert: Option<String>,

    /// Prometheus to proxy PromQL to, per cluster, as `namespace/name:port`
    /// (e.g. `ome/ome-prometheus:9090`). Reached through each cluster's own kube
    /// client via the apiserver `services/proxy` subresource — no separate
    /// Prometheus credential. No baked default: unset ⇒ the `/v1/metrics/*`
    /// endpoints are disabled and the console hides its metric panels. Supply it
    /// via deployment config (flag / env / GitOps).
    #[arg(long = "prom-service", env = "MUSE_PROM_SERVICE", value_parser = parse_prom_target)]
    pub prom_service: Option<PromTarget>,

    /// How often (seconds) the background refresher re-polls each cluster's
    /// Prometheus for the dashboard query set and pushes a new snapshot to SSE
    /// subscribers. One poll per interval regardless of how many consoles are open.
    #[arg(
        long = "metrics-refresh-secs",
        env = "MUSE_METRICS_REFRESH_SECS",
        default_value_t = 15
    )]
    pub metrics_refresh_secs: u64,

    /// Path to the dashboard metrics query set: JSON `{ "queries": [ … ] }`, each
    /// `{ id, promql, kind: "instant"|"range", window?, step? }`. No baked-in
    /// queries — engine-specific metric names (e.g. sglang vs vllm) come from this
    /// file, supplied per environment via deployment config. Unset ⇒ empty snapshot.
    #[arg(long = "metrics-queries-file", env = "MUSE_METRICS_QUERIES_FILE")]
    pub metrics_queries_file: Option<PathBuf>,

    /// Directory of the built web console (the SPA) to serve at `/`, with the
    /// API under `/v1`. Unset ⇒ API-only (the console is served elsewhere). The
    /// container image bakes the bundle in and sets this; override to point at
    /// your own build.
    #[arg(long = "web-dir", env = "MUSE_WEB_DIR")]
    pub web_dir: Option<PathBuf>,
}

impl Config {
    pub fn api_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }

    pub fn metrics_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.metrics_port)
    }

    /// The clusters to watch — the configured list, or the ambient config as a
    /// single "default" cluster.
    pub fn cluster_refs(&self) -> Vec<ClusterRef> {
        if self.clusters.is_empty() {
            vec![ClusterRef {
                name: "default".to_owned(),
                kubeconfig: None,
            }]
        } else {
            self.clusters.clone()
        }
    }

    /// Build the optional chat proxy's effective settings. Enabling the feature
    /// without an explicit total timeout is a configuration error that the caller
    /// handles by leaving chat disabled; no timeout is silently invented here.
    pub fn chat_proxy_config(&self) -> Result<Option<ChatProxyConfig>, String> {
        if !self.chat_enabled {
            return Ok(None);
        }
        let timeout_secs = self.chat_timeout_secs.ok_or_else(|| {
            "chat proxy enabled without MUSE_CHAT_TIMEOUT_SECS; proxy will remain disabled"
                .to_owned()
        })?;
        Ok(Some(ChatProxyConfig {
            timeout: std::time::Duration::from_secs(timeout_secs),
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
    use std::time::Duration;

    use clap::Parser;

    use super::Config;

    #[test]
    fn enabled_chat_without_timeout_degrades_to_disabled_config() {
        let config = Config::try_parse_from(["muse", "--chat-enabled"]).unwrap();
        assert!(config.chat_proxy_config().is_err());
    }

    #[test]
    fn chat_timeout_and_target_policy_are_config_driven() {
        let config = Config::try_parse_from([
            "muse",
            "--chat-enabled",
            "--chat-timeout-secs=75",
            "--chat-connect-timeout-secs=4",
            "--chat-client-grace-secs=6",
            "--chat-allowed-host=gateway.example:8443",
            "--chat-allowed-host-suffix=serving.example",
            "--chat-allow-http",
        ])
        .unwrap();
        let chat = config.chat_proxy_config().unwrap().unwrap();

        assert_eq!(chat.timeout, Duration::from_secs(75));
        assert_eq!(chat.connect_timeout, Some(Duration::from_secs(4)));
        assert_eq!(chat.client_grace, Some(Duration::from_secs(6)));
        assert_eq!(chat.allowed_hosts, ["gateway.example:8443"]);
        assert_eq!(chat.allowed_host_suffixes, ["serving.example"]);
        assert!(chat.allow_http);
    }

    #[test]
    fn zero_chat_timeout_is_rejected_at_config_parse_time() {
        let result = Config::try_parse_from(["muse", "--chat-timeout-secs=0"]);
        assert!(result.is_err());
    }
}
