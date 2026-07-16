//! The informer layer: everything driven by the per-cluster watches.
//!
//! For each workload cluster, [`build_cluster_state`] starts one informer per kind
//! and returns a [`ClusterState`] — the bundle of listers every read serves from.
//! The same informers drive three side-channels: curated OME changes are pushed
//! into the human-facing [`Feed`], every change marks [`Invalidations`] for live
//! queries, and Node/Pod changes fire the [`CapacitySignal`] so the cached
//! capacity rollup recomputes.
//!
//! A kind whose CRD is absent on a cluster gets an empty lister and no informer,
//! so a missing CRD never 404-loops on its initial list. Nothing here blocks on
//! the first sync: the state serves (empty, then filling) immediately, and the
//! caller awaits the returned [`ReadyFut`]s in the background to learn when a
//! cluster is fully warm.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kube::{Client, Resource};
use ome_client::{
    informers::{InformerEvent, Prune, SharedInformerFactory},
    k8s_openapi::api::core::v1::{Node, Pod},
    listers::Lister,
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, WorkloadCluster,
};
use serde::de::DeserializeOwned;
use tokio::sync::{broadcast, mpsc, watch};

use crate::protocol::{FeedEvent, InvalidationEvent};

// ===========================================================================
// Per-cluster listers
// ===========================================================================

/// One workload cluster's informer-backed listers.
pub struct ClusterState {
    pub name: String,
    /// Apiserver `gitVersion` (e.g. "v1.35.6"), captured at connect time.
    pub version: String,
    /// The cluster's kube client, retained past informer setup for the metrics
    /// proxy (PromQL over the apiserver `services/proxy`). Cheap to clone (Arc).
    pub client: Client,
    pub inference_services: Lister<InferenceService>,
    pub base_models: Lister<BaseModel>,
    pub serving_runtimes: Lister<ServingRuntime>,
    pub inference_replicas: Lister<InferenceReplica>,
    pub cluster_base_models: Lister<ClusterBaseModel>,
    pub cluster_serving_runtimes: Lister<ClusterServingRuntime>,
    pub fine_tuned_weights: Lister<FineTunedWeight>,
    pub workload_clusters: Lister<WorkloadCluster>,
    pub nodes: Lister<Node>,
    pub pods: Lister<Pod>,
    /// Flipped to `true` once every informer for this cluster has finished its
    /// first sync. Listers serve (empty, then filling) before this is set, so the
    /// console can render a "syncing" cluster immediately.
    pub ready: Arc<AtomicBool>,
}

/// A future that resolves when one informer's cache has completed its first sync.
pub type ReadyFut = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Side channels driven by informer events, grouped so cache construction does
/// not grow one parameter per downstream projection.
pub struct ChangeSinks<'a> {
    pub feed: &'a Feed,
    pub invalidations: &'a Invalidations,
    pub capacity: &'a CapacitySignal,
}

/// Build every informer for one cluster, returning immediately (caches warm in
/// the background) along with a readiness future per started informer.
///
/// `ome` is the set of `ome.io/v1beta1` resource names this cluster actually
/// serves (from discovery). A kind whose CRD is absent gets an empty lister and
/// no informer — otherwise it would 404-loop on its initial list forever. This is
/// the common case for member clusters, which serve the workload CRDs
/// (InferenceService, BaseModel, …) but not WorkloadCluster, the registry CRD
/// that lives only on the hub. Nodes/pods (core/v1, always present) are watched
/// unconditionally, so the cluster still contributes capacity with no OME CRDs.
///
/// Nothing here blocks on initial sync: the returned [`ClusterState`] serves
/// (empty, then filling) right away, and the caller awaits the returned futures
/// in the background to learn when the cluster is fully warm.
pub fn build_cluster_state(
    name: String,
    version: String,
    client: Client,
    factory: &SharedInformerFactory,
    ome: &HashSet<String>,
    changes: ChangeSinks<'_>,
) -> (ClusterState, Vec<ReadyFut>) {
    let mut readies = Vec::new();
    let cluster = name.clone();
    // OME kinds feed the activity stream. Nodes/pods drive capacity, so they
    // signal the recompute task on change instead (never the high-volume feed).
    let f = |kind| FeedSub {
        feed: changes.feed.clone(),
        cluster: cluster.clone(),
        kind,
    };
    let i = |resource| InvalidationSub {
        invalidations: changes.invalidations.clone(),
        resource,
    };
    let state = ClusterState {
        inference_services: warm(
            factory,
            present::<InferenceService>(ome),
            &mut readies,
            Some(f("InferenceService")),
            Some(i("InferenceService")),
            None,
        ),
        base_models: warm(
            factory,
            present::<BaseModel>(ome),
            &mut readies,
            Some(f("BaseModel")),
            Some(i("BaseModel")),
            None,
        ),
        serving_runtimes: warm(
            factory,
            present::<ServingRuntime>(ome),
            &mut readies,
            Some(f("ServingRuntime")),
            Some(i("ServingRuntime")),
            None,
        ),
        inference_replicas: warm(
            factory,
            present::<InferenceReplica>(ome),
            &mut readies,
            Some(f("InferenceReplica")),
            Some(i("InferenceReplica")),
            None,
        ),
        cluster_base_models: warm(
            factory,
            present::<ClusterBaseModel>(ome),
            &mut readies,
            Some(f("ClusterBaseModel")),
            Some(i("ClusterBaseModel")),
            None,
        ),
        cluster_serving_runtimes: warm(
            factory,
            present::<ClusterServingRuntime>(ome),
            &mut readies,
            Some(f("ClusterServingRuntime")),
            Some(i("ClusterServingRuntime")),
            None,
        ),
        fine_tuned_weights: warm(
            factory,
            present::<FineTunedWeight>(ome),
            &mut readies,
            Some(f("FineTunedWeight")),
            Some(i("FineTunedWeight")),
            None,
        ),
        workload_clusters: warm(
            factory,
            present::<WorkloadCluster>(ome),
            &mut readies,
            Some(f("WorkloadCluster")),
            Some(i("WorkloadCluster")),
            None,
        ),
        nodes: warm::<Node>(
            factory,
            true,
            &mut readies,
            None,
            Some(i("Node")),
            Some(changes.capacity.clone()),
        ),
        pods: warm::<Pod>(
            factory,
            true,
            &mut readies,
            None,
            Some(i("Pod")),
            Some(changes.capacity.clone()),
        ),
        name,
        version,
        client,
        ready: Arc::new(AtomicBool::new(false)),
    };
    (state, readies)
}

/// Wiring for a kind's activity-feed subscriber: where to push, and how to label.
struct FeedSub {
    feed: Feed,
    cluster: String,
    kind: &'static str,
}

/// Wiring for a kind's cache-invalidation subscriber.
struct InvalidationSub {
    invalidations: Invalidations,
    resource: &'static str,
}

/// Start (lazily) the informer for kind `K` if the cluster serves it, returning
/// its live-but-warming lister and pushing a readiness future onto `readies`. A
/// kind the cluster doesn't serve gets an empty lister and starts no informer.
/// When `feed` is set, a background task forwards the informer's change events
/// into the human-facing activity feed. `invalidate` observes every update,
/// including status-only changes filtered out of activity. When `on_change` is
/// set (nodes/pods), each event also marks the capacity rollup stale.
fn warm<K>(
    factory: &SharedInformerFactory,
    present: bool,
    readies: &mut Vec<ReadyFut>,
    feed: Option<FeedSub>,
    invalidate: Option<InvalidationSub>,
    on_change: Option<CapacitySignal>,
) -> Lister<K>
where
    K: Resource<DynamicType = ()>
        + Clone
        + DeserializeOwned
        + Debug
        + Send
        + Sync
        + Prune
        + 'static,
{
    if !present {
        return empty_lister::<K>();
    }
    let informer = factory.informer_lazy::<K>();
    if let Some(sub) = invalidate {
        let mut rx = informer.subscribe();
        tokio::spawn(async move {
            while let Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) = rx.recv().await {
                // A lag still means at least one projection changed. The next
                // HTTP fetch reads the latest informer snapshot.
                sub.invalidations.mark(sub.resource);
            }
        });
    }
    if let Some(signal) = on_change {
        let mut rx = informer.subscribe();
        tokio::spawn(async move {
            // Any add/update/delete (or a lag) means capacity may have changed;
            // mark stale (coalesced by the recompute task). Loop ends when the
            // channel closes (pattern stops matching).
            while let Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) = rx.recv().await {
                signal.mark();
            }
        });
    }
    if let Some(sub) = feed {
        let mut rx = informer.subscribe();
        tokio::spawn(async move {
            // uid -> last-seen generation. We surface creates, deletes, and spec
            // changes (generation bumps) only. A repeated Apply whose generation is
            // unchanged is status churn (heartbeats, conditions, replica counts) —
            // dropping it keeps the feed and the SSE stream quiet when nothing
            // meaningful happens, so clients don't refetch on every status write.
            let mut seen: HashMap<String, Option<i64>> = HashMap::new();
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let (action, obj) = match &event {
                            InformerEvent::Applied(o) => ("applied", o),
                            InformerEvent::Deleted(o) => ("deleted", o),
                        };
                        let meta = obj.meta();
                        let uid = meta.uid.clone().unwrap_or_else(|| {
                            format!(
                                "{}/{}",
                                meta.namespace.clone().unwrap_or_default(),
                                meta.name.clone().unwrap_or_default()
                            )
                        });
                        let emit = if action == "deleted" {
                            seen.remove(&uid);
                            true
                        } else {
                            match seen.get(&uid) {
                                Some(prev) if *prev == meta.generation => false,
                                _ => {
                                    seen.insert(uid, meta.generation);
                                    true
                                }
                            }
                        };
                        if !emit {
                            continue;
                        }
                        sub.feed.push(FeedEvent {
                            cluster: sub.cluster.clone(),
                            kind: sub.kind.to_owned(),
                            namespace: meta.namespace.clone(),
                            name: meta.name.clone().unwrap_or_default(),
                            action,
                            at: now_secs(),
                        });
                    }
                    // A lagging subscriber skips ahead; a closed channel ends the task.
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    let store = informer.store();
    readies.push(Box::pin(async move {
        let _ = store.wait_until_ready().await;
    }));
    informer.lister()
}

/// Whether the cluster serves kind `K`, matched by its plural resource name
/// against the discovered `ome.io` resource set.
fn present<K: Resource<DynamicType = ()>>(ome: &HashSet<String>) -> bool {
    ome.contains(K::plural(&()).as_ref())
}

/// An empty, never-updated lister for a kind a cluster doesn't serve. No watcher
/// is started, so it stays empty rather than 404-looping on a missing CRD.
fn empty_lister<K>() -> Lister<K>
where
    K: Resource + Clone + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
{
    let (store, _writer) = kube::runtime::reflector::store::<K>();
    Lister::new(store)
}

// ===========================================================================
// Activity feed — a bounded ring buffer + live broadcast of resource changes.
// ===========================================================================

/// A bounded, shared activity buffer with a live broadcast. Cheap to clone (Arc +
/// broadcast sender inside). Fed by the OME-kind informers above; read by the
/// `/v1/events` (backlog) and `/v1/stream` (SSE) endpoints. Nodes/pods are not
/// fed in (far too high-volume); only the OME kinds are.
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

// ===========================================================================
// Live query invalidations — independent from human-facing activity.
// ===========================================================================

/// Coalesces informer changes into compact resource-domain invalidations.
///
/// The optional batch window is deployment configuration. When absent, marks
/// already queued in the same scheduler turn are drained into one event; this
/// preserves live updates without silently choosing a timing policy in code.
/// `watch` also collapses updates for a slow SSE consumer to the latest batch.
#[derive(Clone)]
pub struct Invalidations {
    marks: mpsc::UnboundedSender<&'static str>,
    updates: watch::Sender<Option<Arc<InvalidationEvent>>>,
}

impl Invalidations {
    pub fn new(batch_window: Option<Duration>) -> Self {
        let (marks, mut pending) = mpsc::unbounded_channel();
        let (updates, _) = watch::channel(None);
        let publisher = updates.clone();
        tokio::spawn(async move {
            while let Some(first) = pending.recv().await {
                let mut resources = BTreeSet::from([first]);
                if let Some(window) = batch_window {
                    let deadline = tokio::time::sleep(window);
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            () = &mut deadline => break,
                            next = pending.recv() => match next {
                                Some(resource) => { resources.insert(resource); }
                                None => break,
                            },
                        }
                    }
                } else {
                    while let Ok(resource) = pending.try_recv() {
                        resources.insert(resource);
                    }
                }
                publisher.send_replace(Some(Arc::new(InvalidationEvent {
                    resources: resources.into_iter().map(str::to_owned).collect(),
                })));
            }
        });
        Self { marks, updates }
    }

    /// Mark one resource or derived query domain stale. Marks are cheap and the
    /// publisher deduplicates them within the configured batch window.
    pub fn mark(&self, resource: &'static str) {
        let _ = self.marks.send(resource);
    }

    /// Subscribe to future invalidation batches. The retained latest batch lets
    /// a reconnecting SSE consumer immediately converge again.
    pub fn subscribe(&self) -> watch::Receiver<Option<Arc<InvalidationEvent>>> {
        self.updates.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Invalidations;

    #[tokio::test]
    async fn invalidations_batch_and_deduplicate_resources() {
        let invalidations = Invalidations::new(Some(Duration::from_millis(5)));
        let mut updates = invalidations.subscribe();

        invalidations.mark("Pod");
        invalidations.mark("InferenceService");
        invalidations.mark("Pod");

        updates.changed().await.unwrap();
        let update = updates.borrow().clone().unwrap();
        assert_eq!(update.resources, ["InferenceService", "Pod"]);
    }

    #[tokio::test]
    async fn invalidations_work_without_a_timing_policy() {
        let invalidations = Invalidations::new(None);
        let mut updates = invalidations.subscribe();

        invalidations.mark("Capacity");

        updates.changed().await.unwrap();
        let update = updates.borrow().clone().unwrap();
        assert_eq!(update.resources, ["Capacity"]);
    }
}

/// Wall-clock seconds since the Unix epoch (0 if the clock is before the epoch).
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ===========================================================================
// Capacity-stale signal
// ===========================================================================

/// A coalescing "capacity is stale" signal, fired by the Node/Pod informers when
/// they observe a change and awaited by the background recompute task. Multiple
/// marks between waits collapse into one (a burst of pod churn ⇒ one recompute).
#[derive(Clone)]
pub struct CapacitySignal {
    notify: Arc<tokio::sync::Notify>,
}

impl CapacitySignal {
    pub fn new() -> Self {
        Self {
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
    /// Mark capacity stale (cheap; safe to call on every informer event).
    pub fn mark(&self) {
        self.notify.notify_one();
    }
    /// Wait until the next `mark` (used by the background recompute task).
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

impl Default for CapacitySignal {
    fn default() -> Self {
        Self::new()
    }
}
