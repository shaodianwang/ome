//! MUSE read-plane server (OEP-0021): read-only OME + Node views aggregated
//! across workload clusters, every response served from informer caches — never
//! a per-request apiserver round-trip.
//!
//! Built on the OME Rust SDK (`ome_client`): one `SharedInformerFactory` per
//! cluster, each backing a `Lister` per kind (the factory's generic
//! `lister::<K>()` also covers built-ins like `Node`/`Pod`). See `server` for the
//! request path + aggregation, `capacity` for the Node×Pod GPU rollup,
//! `telemetry` for logging/metrics, `observe` for middleware.
//!
//! Clusters come from `--cluster name=/path/to/kubeconfig` (repeatable); with
//! none given, the ambient kube config is used as a single cluster "default". A
//! remote cluster reachable only via a proxy works through HTTP(S)_PROXY /
//! NO_PROXY (the kube `http-proxy` feature is enabled). Example:
//!   cargo run -p muse-server -- \
//!     --cluster cw=~/.kube/msl-cw-use2-dev-2.conf \
//!     --rdma-island-label network.meta.com/ai-zone \
//!     --nvlink-domain-label network.meta.com/domain

mod capacity;
mod config;
mod error;
mod health;
mod observe;
mod server;
mod telemetry;
mod views;

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use kube::{
    config::{Config as KubeConfig, KubeConfigOptions, Kubeconfig},
    Client,
};
use ome_client::informers::SharedInformerFactory;

fn main() -> anyhow::Result<()> {
    let config = config::Config::parse();
    // InferenceService / ServingRuntime are enormous generated types (the
    // pod-template schema is inlined); the reflector deserializes them on a
    // worker thread, which overflows tokio's default 2 MiB stack. Give workers a
    // large stack. (RUST_MIN_STACK does NOT override tokio's explicit worker size.)
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(64 * 1024 * 1024)
        .build()?
        .block_on(run(config))
}

async fn run(config: config::Config) -> anyhow::Result<()> {
    telemetry::init_logging(config.log_json);

    let metrics = telemetry::init_metrics()?;
    let metrics_addr = config.metrics_addr();
    tokio::spawn(async move {
        if let Err(err) = telemetry::serve_metrics(metrics, metrics_addr).await {
            tracing::error!(error = %err, "metrics server exited");
        }
    });

    let topology = views::Topology {
        island_labels: config.rdma_island_labels.clone(),
        nvlink_labels: config.nvlink_domain_labels.clone(),
    };

    // Fan out: one informer set per cluster. An unreachable cluster is skipped
    // (logged), not fatal — the fleet view degrades rather than failing.
    let mut clusters = Vec::new();
    for cluster_ref in config.cluster_refs() {
        match build_cluster(&cluster_ref).await {
            Ok(state) => clusters.push(state),
            Err(err) => {
                tracing::error!(cluster = %cluster_ref.name, error = %err, "skipping unreachable cluster");
            }
        }
    }
    if clusters.is_empty() {
        anyhow::bail!("no clusters reachable");
    }
    tracing::info!(
        clusters = clusters.len(),
        "informer caches synced; serving read plane"
    );

    let state = server::AppState::new(clusters, topology);
    server::serve(&config, state).await
}

/// Connect to one cluster, preflight it, and build its informer set.
async fn build_cluster(cluster_ref: &config::ClusterRef) -> anyhow::Result<server::ClusterState> {
    let client = client_for(cluster_ref).await?;
    // Preflight so an unreachable cluster fails fast (and gets skipped) rather
    // than hanging on the first informer's initial sync.
    let info = tokio::time::timeout(Duration::from_secs(15), client.apiserver_version())
        .await
        .with_context(|| format!("cluster {} unreachable within 15s", cluster_ref.name))?
        .with_context(|| format!("cluster {} version query failed", cluster_ref.name))?;
    tracing::info!(cluster = %cluster_ref.name, version = %info.git_version, "connected to apiserver");
    let factory = SharedInformerFactory::new(client);
    Ok(server::build_cluster_state(cluster_ref.name.clone(), &factory).await)
}

/// Build a kube client from an explicit kubeconfig, or the ambient config.
async fn client_for(cluster_ref: &config::ClusterRef) -> anyhow::Result<Client> {
    match &cluster_ref.kubeconfig {
        Some(path) => {
            let kc = Kubeconfig::read_from(path)
                .with_context(|| format!("read kubeconfig {}", path.display()))?;
            let cfg = KubeConfig::from_custom_kubeconfig(kc, &KubeConfigOptions::default())
                .await
                .with_context(|| format!("kube config for {}", cluster_ref.name))?;
            Client::try_from(cfg).with_context(|| format!("kube client for {}", cluster_ref.name))
        }
        None => Client::try_default().await.context("ambient kube client"),
    }
}
