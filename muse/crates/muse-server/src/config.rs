//! Command-line / env configuration (clap).
//!
//! No behavioural value is a code literal — host, ports, and log format come
//! from flags or env (defaults live in the flag definitions, the config surface).

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use clap::Parser;

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
    #[arg(
        long = "chat-enabled",
        env = "MUSE_CHAT_ENABLED",
        default_value_t = false
    )]
    pub chat_enabled: bool,

    /// Timeout (seconds) for one proxied inference test request.
    #[arg(
        long = "chat-timeout-secs",
        env = "MUSE_CHAT_TIMEOUT_SECS",
        default_value_t = 60
    )]
    pub chat_timeout_secs: u64,

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
}
