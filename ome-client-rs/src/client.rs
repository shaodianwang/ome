//! Clientset-style handle for the OME kinds (the `pkg/client/clientset` analog).
//!
//! Each accessor is a one-line constructor over the generic `kube::Api<T>` — not
//! a generated typed wrapper. Namespaced kinds take a namespace (with a `*_all`
//! variant for every namespace); cluster-scoped kinds take none.
//!
//! Gated behind the `client` feature so a types-only consumer pays nothing.

use kube::{Api, Client};

use crate::{
    BaseModel, ClusterBaseModel, ClusterServingRuntime, FineTunedWeight, InferenceReplica,
    InferenceService, ServingRuntime, TrafficMap, WorkloadCluster,
};

/// A `kube::Client` plus typed `Api` accessors for every OME kind.
#[derive(Clone)]
pub struct OmeClient {
    client: Client,
}

impl OmeClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Build from the ambient kubeconfig / in-cluster config.
    pub async fn try_default() -> kube::Result<Self> {
        Ok(Self::new(Client::try_default().await?))
    }

    /// The underlying client, for kinds/objects not covered here.
    pub fn raw(&self) -> &Client {
        &self.client
    }

    // ---- namespaced kinds ----
    pub fn inference_services(&self, ns: &str) -> Api<InferenceService> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn inference_services_all(&self) -> Api<InferenceService> {
        Api::all(self.client.clone())
    }
    pub fn base_models(&self, ns: &str) -> Api<BaseModel> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn base_models_all(&self) -> Api<BaseModel> {
        Api::all(self.client.clone())
    }
    pub fn serving_runtimes(&self, ns: &str) -> Api<ServingRuntime> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn serving_runtimes_all(&self) -> Api<ServingRuntime> {
        Api::all(self.client.clone())
    }
    pub fn inference_replicas(&self, ns: &str) -> Api<InferenceReplica> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn inference_replicas_all(&self) -> Api<InferenceReplica> {
        Api::all(self.client.clone())
    }

    pub fn traffic_maps(&self, ns: &str) -> Api<TrafficMap> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn traffic_maps_all(&self) -> Api<TrafficMap> {
        Api::all(self.client.clone())
    }

    // ---- cluster-scoped kinds ----
    pub fn cluster_base_models(&self) -> Api<ClusterBaseModel> {
        Api::all(self.client.clone())
    }
    pub fn cluster_serving_runtimes(&self) -> Api<ClusterServingRuntime> {
        Api::all(self.client.clone())
    }
    pub fn fine_tuned_weights(&self) -> Api<FineTunedWeight> {
        Api::all(self.client.clone())
    }
    pub fn workload_clusters(&self) -> Api<WorkloadCluster> {
        Api::all(self.client.clone())
    }
}
