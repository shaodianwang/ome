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

    /// Node label(s) whose value names the RDMA island (outer topology level — the
    /// network fabric), for capacity grouping. Comma-separated; the first label
    /// present on a node wins, so one flag covers clusters that label topology
    /// differently (e.g. network.meta.com/ai-zone,backend.coreweave.cloud/fabric).
    /// Default is the standard k8s zone.
    #[arg(
        long = "rdma-island-label",
        env = "MUSE_RDMA_ISLAND_LABEL",
        value_delimiter = ',',
        default_value = "topology.kubernetes.io/zone"
    )]
    pub rdma_island_labels: Vec<String>,

    /// Node label(s) whose value names the NVLink domain (inner topology level —
    /// e.g. a GB300 NVL72's 72-GPU rack, within one island). Comma-separated,
    /// first-present wins (e.g. network.meta.com/domain,nvidia.com/gpu.clique).
    /// Default is NVIDIA's standard clique label.
    #[arg(
        long = "nvlink-domain-label",
        env = "MUSE_NVLINK_DOMAIN_LABEL",
        value_delimiter = ',',
        default_value = "nvidia.com/gpu.clique"
    )]
    pub nvlink_domain_labels: Vec<String>,
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
