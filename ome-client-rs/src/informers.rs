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
//! Large-CRD note: types generated from CRDs that inline the pod-template schema
//! (`InferenceService`, `ServingRuntime`) are huge, and the reflector
//! deserializes them on its task's thread. Give consumers a generous stack — a
//! tokio runtime built with a large `thread_stack_size`, or `RUST_MIN_STACK` for
//! a current-thread runtime — or the watch loop can overflow the default 2 MiB
//! stack. The integration tests set `RUST_MIN_STACK` (see the Makefile).
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

use crate::listers::Lister;
use crate::{
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, WorkloadCluster,
};

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

/// Start a shared informer for `api`: a background task drives the
/// watcher → reflector so the [`Store`] stays current and every change is
/// broadcast to subscribers. Returns once the initial sync completes. Generic
/// over any kube resource; must run inside a tokio runtime.
pub async fn shared_informer<K>(api: Api<K>) -> Informer<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Clone + Eq + Hash + Default,
{
    let (reader, writer) = reflector::store();
    let (events, _) = broadcast::channel(EVENT_BUFFER);
    let tx = events.clone();
    let stream = reflector(writer, watcher(api, watcher::Config::default()));
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
    reader
        .wait_until_ready()
        .await
        .expect("informer initial sync failed (watcher task dropped)");
    Informer {
        store: reader,
        events,
    }
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
    // One shared Informer per kind, type-erased by TypeId.
    informers: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl SharedInformerFactory {
    pub fn new(client: Client) -> Self {
        Self {
            inner: Arc::new(Inner {
                client,
                informers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Get-or-create the shared `Informer` for `K` (cluster-wide), awaiting
    /// initial sync the first time `K` is requested; later calls share it.
    pub async fn informer<K>(&self) -> Informer<K>
    where
        K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
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
        let informer = shared_informer(Api::<K>::all(self.inner.client.clone())).await;
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
        K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    {
        self.informer::<K>().await.store()
    }

    /// A typed `Lister` for any kind `K`, creating the informer if needed.
    pub async fn lister<K>(&self) -> Lister<K>
    where
        K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
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
