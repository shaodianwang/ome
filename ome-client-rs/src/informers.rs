//! Informers + a shared factory — the `pkg/client/informers` analog, generic
//! rather than generated per type.
//!
//! - [`shared_informer`] is the low-level primitive: one watcher+reflector that
//!   keeps a [`Store`] (the lister cache) current AND fans every change out to
//!   [`Informer::subscribe`]rs (the `AddEventHandler` analog).
//! - [`SharedInformerFactory`] shares one cluster-wide informer per kind (like
//!   client-go's SharedInformerFactory) and hands out typed [`Lister`]s and
//!   [`Informer`]s. One generic impl covers every kind — including built-ins
//!   like `Node` — plus typed per-kind accessors for the OME kinds.
//!
//! Codegen note: the generated OME types keep their inlined pod-template fields
//! opaque (`serde_json::Value`; see ome-client-rs's `hack/slim_crd.py`), so they
//! stay small and deserialize cheaply. If you regenerate WITHOUT that slimming,
//! the inlined PodSpec/Container trees are enormous and can overflow the
//! reflector task's default 2 MiB stack — give consumers a large tokio
//! `thread_stack_size` then.
//!
//! Gated behind the `runtime` feature.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use kube::runtime::watcher::Event;
use kube::runtime::{reflector, watcher};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;

pub use kube::runtime::reflector::Store;

use k8s_openapi::api::core::v1::{Container, Node, Pod, PodSpec, PodStatus};

use crate::listers::Lister;
use crate::{
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, WorkloadCluster,
};

/// Cache pruning: drop fields no reader needs *before* an object enters the
/// reflector store. On large clusters this is a big memory and deserialize-cost
/// saving — `Node.status.images` is the single largest field on a node (the full
/// image inventory, tens of KB each), and `managedFields` is dead weight on every
/// object. `Pod` is trimmed hardest — kept to just the fields the capacity rollup
/// reads (node name, container resource requests, phase) — because at ~10^5 pods
/// it dominates the cache. Implemented per watched kind so the generic informer
/// can prune without knowing the concrete type.
pub trait Prune {
    /// Strip unneeded fields in place.
    fn prune(&mut self);
}

/// Drop `metadata.managedFields` — server-side-apply bookkeeping no reader uses.
fn drop_managed_fields<K: Resource>(obj: &mut K) {
    obj.meta_mut().managed_fields = None;
}

macro_rules! prune_managed_fields_only {
    ($($t:ty),+ $(,)?) => {
        $(impl Prune for $t {
            fn prune(&mut self) {
                drop_managed_fields(self);
            }
        })+
    };
}

// Most kinds only need managedFields stripped; their specs/status are small.
prune_managed_fields_only!(
    InferenceService,
    BaseModel,
    ServingRuntime,
    InferenceReplica,
    ClusterBaseModel,
    ClusterServingRuntime,
    FineTunedWeight,
    WorkloadCluster,
);

impl Prune for Node {
    fn prune(&mut self) {
        drop_managed_fields(self);
        // The image inventory feeds no view (capacity reads allocatable/capacity)
        // and is by far the heaviest part of a Node object — drop it.
        if let Some(status) = self.status.as_mut() {
            status.images = None;
        }
    }
}

impl Prune for Pod {
    fn prune(&mut self) {
        drop_managed_fields(self);
        // Cached Pods have two readers: the capacity rollup (node name, container
        // resource requests, phase) and InferenceService runtime attribution
        // (labels — an auto-selected runtime is recorded only there). Keep those
        // and drop everything else — env, volumes, probes, ports, init containers,
        // status conditions/containerStatuses, annotations — which is the bulk of a
        // Pod and, at ~10^5 pods, the bulk of the whole cache. A future Pod reader
        // must widen this prune to keep the fields it needs.
        self.metadata.annotations = None;
        if let Some(spec) = self.spec.take() {
            self.spec = Some(PodSpec {
                node_name: spec.node_name,
                containers: spec
                    .containers
                    .into_iter()
                    .map(|c| Container {
                        name: c.name,
                        resources: c.resources,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            });
        }
        if let Some(status) = self.status.take() {
            self.status = Some(PodStatus {
                phase: status.phase,
                ..Default::default()
            });
        }
    }
}

/// Change-event broadcast buffer per informer. A subscriber that lags past this
/// many events receives `RecvError::Lagged` and should resync from the lister.
const EVENT_BUFFER: usize = 256;

/// A change observed by an informer. The cache (`Store`/`Lister`) is already
/// updated to reflect this event by the time a subscriber receives it.
/// `Applied` covers both add and update; `Deleted` is removal.
#[derive(Clone, Debug)]
pub enum InformerEvent<K> {
    Applied(Arc<K>),
    Deleted(Arc<K>),
}

/// A running informer: a background watcher+reflector keeps `store` current and
/// fans every change out to `subscribe`rs. Cheap to clone (shares both inside).
pub struct Informer<K>
where
    K: Resource + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    store: Store<K>,
    events: broadcast::Sender<InformerEvent<K>>,
}

impl<K> Clone for Informer<K>
where
    K: Resource + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            events: self.events.clone(),
        }
    }
}

impl<K> Informer<K>
where
    K: Resource + Clone + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    /// The reflector cache (clone is cheap; it shares the backing store).
    pub fn store(&self) -> Store<K> {
        self.store.clone()
    }

    /// A typed `Lister` over this informer's cache.
    pub fn lister(&self) -> Lister<K> {
        Lister::new(self.store.clone())
    }

    /// Subscribe to change events (the `AddEventHandler` analog). Each subscriber
    /// gets its own receiver; a slow one that lags past the buffer receives
    /// `RecvError::Lagged` and should resync from [`Informer::lister`].
    pub fn subscribe(&self) -> broadcast::Receiver<InformerEvent<K>> {
        self.events.subscribe()
    }
}

/// Start a shared informer for `api` *without* awaiting initial sync: spawns the
/// background watcher → reflector task and returns immediately. The [`Store`]
/// (and any [`Lister`] over it) is live at once and fills as the cache syncs;
/// await [`Store::wait_until_ready`] to know when the first sync completes. Use
/// this to serve while caches warm; use [`shared_informer`] when you need a
/// fully-populated cache before proceeding.
///
/// `config` selects the initial-list strategy. On large clusters the streaming
/// `WatchList` strategy (`watcher::Config::default().streaming_lists()`) is far
/// cheaper than the default paginated `LIST` and avoids the "too old resource
/// version" expiry a slow paginated list can hit — but it needs apiserver
/// WatchList support (beta, on by default since Kubernetes 1.32), so the caller
/// chooses. Every object is [`Prune`]d before entering the cache regardless.
/// Generic over any watched kind; must run inside a tokio runtime.
pub fn shared_informer_lazy<K>(api: Api<K>, config: watcher::Config) -> Informer<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + Prune + 'static,
    K::DynamicType: Clone + Eq + Hash + Default,
{
    let (reader, writer) = reflector::store();
    let (events, _) = broadcast::channel(EVENT_BUFFER);
    let tx = events.clone();
    let pruned = watcher(api, config).map(|event| event.map(|ev| ev.modify(|obj| obj.prune())));
    let stream = reflector(writer, pruned);
    tokio::spawn(async move {
        futures::pin_mut!(stream);
        while let Some(event) = stream.next().await {
            match event {
                // The reflector writes the object to the store before yielding,
                // so subscribers observe a cache already consistent with the
                // event. Init/InitDone are list bookkeeping, not object changes.
                Ok(Event::Apply(obj)) | Ok(Event::InitApply(obj)) => {
                    let _ = tx.send(InformerEvent::Applied(Arc::new(obj)));
                }
                Ok(Event::Delete(obj)) => {
                    let _ = tx.send(InformerEvent::Deleted(Arc::new(obj)));
                }
                Ok(Event::Init) | Ok(Event::InitDone) => {}
                Err(err) => tracing::warn!(error = %err, "informer watch error"),
            }
        }
    });
    Informer {
        store: reader,
        events,
    }
}

/// Start a shared informer and await its initial sync before returning — the
/// [`Lister`] is fully populated when this resolves. See [`shared_informer_lazy`]
/// for the non-blocking variant, the `config` strategy, and the efficiency notes.
pub async fn shared_informer<K>(api: Api<K>, config: watcher::Config) -> Informer<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + Prune + 'static,
    K::DynamicType: Clone + Eq + Hash + Default,
{
    let informer = shared_informer_lazy(api, config);
    informer
        .store
        .wait_until_ready()
        .await
        .expect("informer initial sync failed (watcher task dropped)");
    informer
}

/// Shares one cluster-wide informer per kind and hands out typed `Lister`s and
/// `Informer`s over their caches — the SharedInformerFactory analog. Cheap to
/// clone (Arc inside).
#[derive(Clone)]
pub struct SharedInformerFactory {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    // Initial-list strategy applied to every informer this factory starts.
    list_config: watcher::Config,
    // One shared Informer per kind, type-erased by TypeId.
    informers: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl SharedInformerFactory {
    /// A factory whose informers use the default paginated initial-list strategy
    /// (`ListWatch`), which every apiserver supports.
    pub fn new(client: Client) -> Self {
        Self::with_streaming_lists(client, false)
    }

    /// A factory whose informers stream the initial list (`WatchList`) when
    /// `streaming` is set — far cheaper on large clusters, but requires apiserver
    /// WatchList support (beta, on by default since Kubernetes 1.32). When unset,
    /// uses the paginated strategy that works everywhere.
    pub fn with_streaming_lists(client: Client, streaming: bool) -> Self {
        let list_config = if streaming {
            watcher::Config::default().streaming_lists()
        } else {
            watcher::Config::default()
        };
        Self {
            inner: Arc::new(Inner {
                client,
                list_config,
                informers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Get-or-create the shared `Informer` for `K` (cluster-wide), awaiting
    /// initial sync the first time `K` is requested; later calls share it.
    pub async fn informer<K>(&self) -> Informer<K>
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
        let tid = TypeId::of::<K>();
        if let Some(existing) = self
            .inner
            .informers
            .lock()
            .unwrap()
            .get(&tid)
            .and_then(|b| b.downcast_ref::<Informer<K>>())
            .cloned()
        {
            return existing;
        }
        // Build outside the lock (it awaits initial sync); dedup on insert.
        let informer = shared_informer(
            Api::<K>::all(self.inner.client.clone()),
            self.inner.list_config.clone(),
        )
        .await;
        self.inner
            .informers
            .lock()
            .unwrap()
            .entry(tid)
            .or_insert_with(|| Box::new(informer.clone()));
        informer
    }

    /// Like [`informer`](Self::informer) but returns *without* awaiting initial
    /// sync — the cache warms in the background. Use for serve-while-warming
    /// startup, then await the lister's `store().wait_until_ready()` to track
    /// readiness. Shares the same per-kind informer as [`informer`](Self::informer).
    pub fn informer_lazy<K>(&self) -> Informer<K>
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
        let tid = TypeId::of::<K>();
        if let Some(existing) = self
            .inner
            .informers
            .lock()
            .unwrap()
            .get(&tid)
            .and_then(|b| b.downcast_ref::<Informer<K>>())
            .cloned()
        {
            return existing;
        }
        let informer = shared_informer_lazy(
            Api::<K>::all(self.inner.client.clone()),
            self.inner.list_config.clone(),
        );
        self.inner
            .informers
            .lock()
            .unwrap()
            .entry(tid)
            .or_insert_with(|| Box::new(informer.clone()));
        informer
    }

    /// The shared `Store` for `K`, creating the informer if needed.
    pub async fn store<K>(&self) -> Store<K>
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
        self.informer::<K>().await.store()
    }

    /// A typed `Lister` for any kind `K`, creating the informer if needed.
    pub async fn lister<K>(&self) -> Lister<K>
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
        self.informer::<K>().await.lister()
    }

    // ---- typed per-kind listers (the client-go factory.X().Lister() feel) ----
    pub async fn inference_services(&self) -> Lister<InferenceService> {
        self.lister().await
    }
    pub async fn base_models(&self) -> Lister<BaseModel> {
        self.lister().await
    }
    pub async fn serving_runtimes(&self) -> Lister<ServingRuntime> {
        self.lister().await
    }
    pub async fn inference_replicas(&self) -> Lister<InferenceReplica> {
        self.lister().await
    }
    pub async fn cluster_base_models(&self) -> Lister<ClusterBaseModel> {
        self.lister().await
    }
    pub async fn cluster_serving_runtimes(&self) -> Lister<ClusterServingRuntime> {
        self.lister().await
    }
    pub async fn fine_tuned_weights(&self) -> Lister<FineTunedWeight> {
        self.lister().await
    }
    pub async fn workload_clusters(&self) -> Lister<WorkloadCluster> {
        self.lister().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerImage, EnvVar, NodeStatus, ResourceRequirements, Volume,
    };
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ManagedFieldsEntry, ObjectMeta};
    use std::collections::BTreeMap;

    // The capacity rollup reads only pod node_name, container resource requests,
    // and phase. Prune must keep exactly those and drop the rest — this test is the
    // contract, so a future prune change can't silently break capacity numbers.
    #[test]
    fn pod_prune_keeps_capacity_fields_drops_the_rest() {
        let requests = BTreeMap::from([("nvidia.com/gpu".to_string(), Quantity("8".to_string()))]);
        let mut pod = Pod {
            metadata: ObjectMeta {
                name: Some("p".into()),
                namespace: Some("ns".into()),
                annotations: Some(BTreeMap::from([("big".into(), "x".repeat(10_000))])),
                managed_fields: Some(vec![ManagedFieldsEntry::default()]),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some("node-1".into()),
                volumes: Some(vec![Volume {
                    name: "v".into(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "c".into(),
                    env: Some(vec![EnvVar {
                        name: "E".into(),
                        value: Some("v".into()),
                        ..Default::default()
                    }]),
                    resources: Some(ResourceRequirements {
                        requests: Some(requests),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".into()),
                conditions: Some(vec![Default::default()]),
                ..Default::default()
            }),
        };

        pod.prune();

        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.node_name.as_deref(), Some("node-1"));
        assert_eq!(
            pod.status.as_ref().unwrap().phase.as_deref(),
            Some("Running")
        );
        let c = &spec.containers[0];
        assert_eq!(
            c.resources.as_ref().unwrap().requests.as_ref().unwrap()["nvidia.com/gpu"].0,
            "8"
        );
        // Heavy fields no reader needs are gone.
        assert!(pod.metadata.annotations.is_none());
        assert!(pod.metadata.managed_fields.is_none());
        assert!(spec.volumes.is_none());
        assert!(c.env.is_none());
        assert!(pod.status.as_ref().unwrap().conditions.is_none());
    }

    #[test]
    fn node_prune_drops_images_and_managed_fields() {
        let mut node = Node {
            metadata: ObjectMeta {
                name: Some("n".into()),
                managed_fields: Some(vec![ManagedFieldsEntry::default()]),
                ..Default::default()
            },
            status: Some(NodeStatus {
                images: Some(vec![ContainerImage {
                    names: Some(vec!["img".into()]),
                    size_bytes: Some(1),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        node.prune();

        assert!(node.metadata.managed_fields.is_none());
        assert!(node.status.as_ref().unwrap().images.is_none());
    }
}
