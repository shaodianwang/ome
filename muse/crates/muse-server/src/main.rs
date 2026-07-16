//! MUSE read-plane server (OEP-0021): read-only OME + Node views aggregated
//! across workload clusters, every response served from informer caches — never
//! a per-request apiserver round-trip.
//!
//! Built on the OME Rust SDK (`ome_client`): one `SharedInformerFactory` per
//! cluster, each backing a `Lister` per kind (the factory's generic
//! `lister::<K>()` also covers built-ins like `Node`/`Pod`). `informers` builds a
//! cluster's watches (listers + activity + invalidation + capacity signals),
//! `app_context` holds the shared state, `service` has the HTTP endpoints (one
//! module per endpoint) and the route table, `protocol` defines the wire types,
//! `rollup` does the Node×Pod GPU capacity aggregation, `server` runs the listener,
//! and `telemetry` handles logging, metrics, and the per-request middleware.
//!
//! Clusters come from `--cluster name=/path/to/kubeconfig` (repeatable); with
//! none given, the ambient kube config is used as a single cluster "default". A
//! remote cluster reachable only via a proxy works through HTTP(S)_PROXY /
//! NO_PROXY (the kube `http-proxy` feature is enabled). Example:
//!   cargo run -p muse-server -- \
//!     --cluster cw=~/.kube/msl-cw-use2-dev-2.conf \
//!     --rdma-island-label network.meta.com/ai-zone \
//!     --nvlink-domain-label network.meta.com/domain

mod app_context;
mod chat_proxy;
mod config;
mod informers;
mod protocol;
mod rollup;
mod server;
mod service;
mod telemetry;

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use kube::{
    config::{Config as KubeConfig, KubeConfigOptions, Kubeconfig},
    Client,
};
use ome_client::informers::SharedInformerFactory;

// Route allocations through jemalloc: warming large informer caches churns
// transient allocations the system allocator holds onto, inflating RSS. jemalloc
// returns freed pages to the OS — aggressively when MALLOC_CONF enables a
// background purge thread (set in the chart). MSVC isn't a target.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The `ome.io/v1beta1` resource plurals MUSE watches — used only to report which
/// are absent on a given cluster. Discovery (not this list) gates each informer.
const OME_PLURALS: [&str; 8] = [
    "inferenceservices",
    "basemodels",
    "servingruntimes",
    "inferencereplicas",
    "clusterbasemodels",
    "clusterservingruntimes",
    "finetunedweights",
    "workloadclusters",
];

fn main() -> anyhow::Result<()> {
    let config = config::Config::parse();
    config.validate()?;
    // InferenceService / ServingRuntime are enormous generated types (the
    // pod-template schema is inlined); the reflector deserializes them on a
    // worker thread, which overflows tokio's default 2 MiB stack. Give workers a
    // large stack. (RUST_MIN_STACK does NOT override tokio's explicit worker size.)
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(config.worker_thread_stack_bytes)
        .build()?
        .block_on(run(config))
}

async fn run(config: config::Config) -> anyhow::Result<()> {
    telemetry::init_logging(config.log_json, &config.log_filter)?;
    let dashboard_policy = config
        .dashboard_policy
        .clone()
        .filter(|policy| !policy.is_empty());

    // Chat is optional and must fail closed. Missing policy/timeout config or bad
    // TLS material disables only the proxy; the read plane remains available.
    let chat = match config.chat_proxy_config() {
        Ok(Some(chat_config)) => match chat_proxy::ChatProxy::new(
            chat_config,
            chat_proxy::ChatTls {
                client_cert: config.chat_client_cert.clone(),
                client_key: config.chat_client_key.clone(),
                ca_cert: config.chat_ca_cert.clone(),
            },
        ) {
            Ok(proxy) => Some(proxy),
            Err(err) => {
                tracing::error!(error = %err, "chat proxy configuration invalid; proxy disabled");
                None
            }
        },
        Ok(None) => None,
        Err(err) => {
            tracing::error!(error = %err, "chat proxy configuration incomplete; proxy disabled");
            None
        }
    };

    let metrics = telemetry::init_metrics(
        &config.http_latency_buckets,
        Duration::from_secs(config.metrics_upkeep_secs),
    )?;
    let metrics_addr = config.metrics_addr();
    tokio::spawn(async move {
        if let Err(err) = telemetry::serve_metrics(metrics, metrics_addr).await {
            tracing::error!(error = %err, "metrics server exited");
        }
    });

    let topology = protocol::Topology {
        island_labels: config.rdma_island_labels.clone(),
        nvlink_labels: config.nvlink_domain_labels.clone(),
    };

    // Shared activity feed: curated, human-facing OME changes flow here.
    let feed = informers::Feed::new(config.event_history_limit);
    // Query invalidations are independent from activity and cover every informer
    // update. Deployment config owns their batching/load tradeoff.
    let invalidations = informers::Invalidations::new(
        config
            .live_invalidation_batch_ms
            .map(|millis| Duration::from_millis(millis.get())),
    );
    // Shared signal: Node/Pod and cache-eligibility changes mark capacity stale.
    let signal = informers::CapacitySignal::new();

    // Fan out: one informer set per cluster. An unreachable cluster is skipped
    // (logged), not fatal — the fleet view degrades rather than failing.
    let mut clusters = Vec::new();
    let cache_sync_timeout = config
        .cache_sync_timeout_secs
        .map(|seconds| Duration::from_secs(seconds.get()));
    for cluster_ref in config.cluster_refs()? {
        match build_cluster(
            &cluster_ref,
            config.list_strategy,
            cache_sync_timeout,
            &feed,
            &invalidations,
            &signal,
            Duration::from_secs(config.apiserver_preflight_timeout_secs),
        )
        .await
        {
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
        "serving read plane; caches warming in background"
    );

    // Load the curated dashboard query set (if configured). A read/parse failure
    // degrades to an empty snapshot rather than failing startup.
    let metrics_queries = std::sync::Arc::new(match config.metrics_queries_file.as_deref() {
        Some(path) => config::load_queries(path).unwrap_or_else(|err| {
            tracing::error!(error = %err, "failed to load metrics queries; snapshot will be empty");
            Vec::new()
        }),
        None => Vec::new(),
    });

    let state = app_context::AppState::new(
        clusters,
        topology,
        feed,
        invalidations,
        signal,
        chat,
        app_context::AppStateConfig {
            prom: config.prom_service.clone(),
            queries: metrics_queries,
            refresh: Duration::from_secs(config.metrics_refresh_secs),
            sse_keep_alive: config
                .sse_keep_alive_secs
                .map(|seconds| Duration::from_secs(seconds.get())),
            query_concurrency: config.metrics_query_concurrency,
            request_timeout: config
                .metrics_request_timeout_secs
                .map(|seconds| Duration::from_secs(seconds.get())),
            capacity_limits: rollup::CapacityLimits {
                max_fragments: config.capacity_max_fragments,
                max_consumers: config.capacity_max_consumers,
            },
            dashboard_policy,
        },
    );
    server::serve(&config, state).await
}

/// Connect to one cluster, preflight it, build its informer set, and kick off the
/// background cache warm-up. Returns as soon as the listers exist (initially
/// empty), so a slow-syncing giant cluster never blocks the console from serving.
async fn build_cluster(
    cluster_ref: &config::ClusterRef,
    list_strategy: config::ListStrategy,
    cache_sync_timeout: Option<Duration>,
    feed: &informers::Feed,
    invalidations: &informers::Invalidations,
    signal: &informers::CapacitySignal,
    preflight_timeout: Duration,
) -> anyhow::Result<informers::ClusterState> {
    let client = client_for(cluster_ref).await?;
    // Preflight so an unreachable cluster fails fast (and gets skipped) rather
    // than hanging on the first informer's initial sync.
    let info = tokio::time::timeout(preflight_timeout, client.apiserver_version())
        .await
        .with_context(|| {
            format!(
                "cluster {} unreachable within {}s",
                cluster_ref.name,
                preflight_timeout.as_secs()
            )
        })?
        .with_context(|| format!("cluster {} version query failed", cluster_ref.name))?;
    // Decide the initial-list strategy from the configured policy and the
    // apiserver version (WatchList is beta/on by default only since 1.32).
    let streaming = list_strategy.use_streaming(server_minor(&info.git_version));
    tracing::info!(cluster = %cluster_ref.name, version = %info.git_version, streaming, "connected to apiserver");

    // Which ome.io/v1beta1 resources does this cluster actually serve? Gate each
    // informer on its CRD being present: a missing CRD 404-loops on the initial
    // list and blocks startup. Member clusters typically serve the workload CRDs
    // but not WorkloadCluster (hub-only), and a cluster with no OME at all yields
    // an empty set — capacity (nodes/pods) still works in every case.
    let ome: std::collections::HashSet<String> = client
        .list_api_group_resources("ome.io/v1beta1")
        .await
        .map(|list| list.resources.into_iter().map(|r| r.name).collect())
        .unwrap_or_default();
    let missing: Vec<&str> = OME_PLURALS
        .iter()
        .copied()
        .filter(|p| !ome.contains(*p))
        .collect();
    if !missing.is_empty() {
        tracing::warn!(cluster = %cluster_ref.name, ?missing, "ome.io CRDs absent on this cluster — those kinds will be empty");
    }

    let factory = SharedInformerFactory::with_streaming_lists(client.clone(), streaming);
    let (state, readies) = informers::build_cluster_state(
        cluster_ref.name.clone(),
        info.git_version.clone(),
        client,
        &factory,
        &ome,
        informers::ChangeSinks {
            feed,
            invalidations,
            capacity: signal,
        },
    );
    // Serve immediately and warm the caches in the background. Every readiness
    // future is observed concurrently so one dropped watcher fails the cluster
    // promptly even when another watcher is still pending. A timed-out sync keeps
    // running so the cluster can recover when its initial list eventually lands.
    let cache = state.cache.clone();
    let invalidations = invalidations.clone();
    let capacity = signal.clone();
    let name = cluster_ref.name.clone();
    tokio::spawn(async move {
        observe_cache_sync(readies, cache_sync_timeout, |outcome| {
            match outcome {
                CacheSyncOutcome::Ready => {
                    let recovered =
                        matches!(cache.snapshot(), informers::CacheSyncState::TimedOut(_));
                    cache.mark_ready();
                    tracing::info!(cluster = %name, recovered, "cache sync complete");
                }
                CacheSyncOutcome::Failed(error) => {
                    cache.mark_failed(error.clone());
                    tracing::error!(cluster = %name, %error, "cache sync failed");
                }
                CacheSyncOutcome::TimedOut(deadline) => {
                    let seconds = deadline.as_secs();
                    let error =
                        format!("initial informer sync exceeded configured timeout of {seconds}s");
                    cache.mark_timed_out(error.clone());
                    tracing::error!(cluster = %name, %error, "cache sync timed out; continuing in background");
                }
            }
            invalidations.mark("Cluster");
            capacity.mark();
        })
        .await;
    });
    Ok(state)
}

#[derive(Debug, PartialEq, Eq)]
enum CacheSyncOutcome {
    Ready,
    Failed(String),
    TimedOut(Duration),
}

async fn observe_cache_sync(
    readies: Vec<informers::ReadyFut>,
    deadline: Option<Duration>,
    mut observe: impl FnMut(CacheSyncOutcome),
) {
    let mut sync = Box::pin(wait_for_cache_sync(readies));
    match deadline {
        Some(deadline) => match tokio::time::timeout(deadline, &mut sync).await {
            Ok(result) => observe(settled_cache_sync_outcome(result)),
            Err(_) => {
                observe(CacheSyncOutcome::TimedOut(deadline));
                observe(settled_cache_sync_outcome(sync.await));
            }
        },
        None => observe(settled_cache_sync_outcome(sync.await)),
    }
}

fn settled_cache_sync_outcome(result: Result<(), String>) -> CacheSyncOutcome {
    match result {
        Ok(()) => CacheSyncOutcome::Ready,
        Err(error) => CacheSyncOutcome::Failed(error),
    }
}

async fn wait_for_cache_sync(readies: Vec<informers::ReadyFut>) -> Result<(), String> {
    let mut pending = tokio::task::JoinSet::new();
    for ready in readies {
        pending.spawn(ready);
    }
    while let Some(result) = pending.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(format!("informer sync task failed: {error}")),
        }
    }
    Ok(())
}

/// Parse the minor version from an apiserver `gitVersion` string, e.g.
/// `"v1.35.6"` → `35` (tolerates suffixes like `"v1.32.0-gke.1"`). `None` if it
/// doesn't parse, in which case `ListStrategy::Auto` conservatively paginates.
fn server_minor(git_version: &str) -> Option<u32> {
    git_version
        .trim_start_matches('v')
        .split('.')
        .nth(1)?
        .parse()
        .ok()
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

#[cfg(test)]
mod tests {
    use std::{future, time::Duration};

    use super::{observe_cache_sync, CacheSyncOutcome};
    use crate::informers::ReadyFut;
    use tokio::sync::{mpsc, oneshot};

    fn ready(result: Result<(), &str>) -> ReadyFut {
        let result = result.map_err(ToOwned::to_owned);
        Box::pin(async move { result })
    }

    #[tokio::test]
    async fn cache_sync_observes_failures_concurrently() {
        let pending: ReadyFut = Box::pin(future::pending());
        let mut outcomes = Vec::new();
        observe_cache_sync(
            vec![pending, ready(Err("watcher dropped"))],
            Some(Duration::from_secs(1)),
            |outcome| outcomes.push(outcome),
        )
        .await;
        assert_eq!(
            outcomes,
            [CacheSyncOutcome::Failed("watcher dropped".to_owned())]
        );
    }

    #[tokio::test]
    async fn cache_sync_reports_timeout_then_late_recovery() {
        let (release, released) = oneshot::channel();
        let pending: ReadyFut =
            Box::pin(async move { released.await.map_err(|error| error.to_string()) });
        let (outcomes, mut observed) = mpsc::unbounded_channel();
        let monitor = tokio::spawn(observe_cache_sync(
            vec![pending],
            Some(Duration::from_millis(1)),
            move |outcome| {
                outcomes.send(outcome).unwrap();
            },
        ));

        assert_eq!(
            observed.recv().await,
            Some(CacheSyncOutcome::TimedOut(Duration::from_millis(1)))
        );
        release.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), observed.recv())
                .await
                .unwrap(),
            Some(CacheSyncOutcome::Ready)
        );
        monitor.await.unwrap();
    }
}
