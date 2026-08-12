//! Integration tests for the OME Rust client against a real kube-apiserver.
//!
//! Run with:
//!   KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)" \
//!     cargo test -p ome-client --features runtime --test integration -- --nocapture
//! or just `make rust-integration-test` from `ome-client-rs/`.
//!
//! Gated on the `runtime` feature (the informer/lister layer); without it the
//! whole crate compiles to nothing here.
#![cfg(feature = "runtime")]

mod common;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use kube::api::{DeleteParams, PostParams};
use serde_json::json;
use tokio::sync::broadcast;

use ome_client::client::OmeClient;
use ome_client::inferenceservices::InferenceServiceSpec;
use ome_client::informers::{InformerEvent, SharedInformerFactory};
use ome_client::k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    LabelSelector, LabelSelectorRequirement,
};
use ome_client::workloadclusters::WorkloadClusterSpec;
use ome_client::{InferenceService, WorkloadCluster};

/// Poll `cond` until it holds or `timeout` elapses.
async fn eventually(timeout: Duration, mut cond: impl FnMut() -> bool) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!("condition not satisfied within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// End-to-end exercise of all three SDK layers against a live apiserver:
///   clientset  — create / get / list / delete round-trips,
///   informer   — watcher → reflector keeps a Store current,
///   lister     — typed reads off that Store converge on create *and* delete.
#[tokio::test]
async fn clientset_informer_lister_roundtrip() -> anyhow::Result<()> {
    let cluster = common::start().await?;

    // ---- clientset ----
    let ome = OmeClient::new(cluster.client.clone());
    let api = ome.workload_clusters(); // Api<WorkloadCluster>, cluster-scoped

    // ---- informer + lister (started before the create, so we observe the add) ----
    let factory = SharedInformerFactory::new(cluster.client.clone());
    let lister = factory.workload_clusters().await; // Lister<WorkloadCluster>
    assert!(lister.list().is_empty(), "store should start empty");

    // Build the spec from JSON (avoids hand-assembling the nested source structs);
    // clusterSource.kubeConfig.secretRef.name is the only required field.
    let spec: WorkloadClusterSpec = serde_json::from_value(json!({
        "clusterSource": { "kubeConfig": { "secretRef": { "name": "dummy" } } }
    }))?;
    let wc = WorkloadCluster::new("test-wc", spec);

    // clientset CREATE
    let created = api.create(&PostParams::default(), &wc).await?;
    assert_eq!(created.metadata.name.as_deref(), Some("test-wc"));

    // clientset GET + LIST
    let got = api.get("test-wc").await?;
    assert_eq!(got.metadata.name.as_deref(), Some("test-wc"));
    let listed = api.list(&Default::default()).await?;
    assert_eq!(listed.items.len(), 1);

    // informer/lister converge to see the object (watch → reflector → Store)
    eventually(Duration::from_secs(10), || lister.get("test-wc").is_some()).await?;
    assert_eq!(
        lister.list().len(),
        1,
        "lister should see exactly one object"
    );

    // clientset DELETE, then the lister cache converges back to empty
    api.delete("test-wc", &DeleteParams::default()).await?;
    eventually(Duration::from_secs(10), || lister.get("test-wc").is_none()).await?;
    assert!(
        lister.list().is_empty(),
        "lister should be empty after delete"
    );

    Ok(())
}

/// Same three layers against a *namespaced* kind (InferenceService), exercising
/// the parts the cluster-scoped test doesn't: the `x(ns)` clientset accessor and
/// the lister's namespace + label-selector filters.
#[tokio::test]
async fn namespaced_clientset_informer_lister() -> anyhow::Result<()> {
    let cluster = common::start().await?;
    let ns = "default";

    // ---- clientset (namespaced accessor) ----
    let ome = OmeClient::new(cluster.client.clone());
    let api = ome.inference_services(ns); // Api<InferenceService>, namespaced

    // ---- informer + lister (all-namespaces) ----
    let factory = SharedInformerFactory::new(cluster.client.clone());
    let lister = factory.inference_services().await; // Lister<InferenceService>

    // Empty spec is valid (every InferenceServiceSpec field is optional); tag it
    // with a label so list_matching has something to filter on.
    let spec: InferenceServiceSpec = serde_json::from_value(json!({}))?;
    let mut isvc = InferenceService::new("test-isvc", spec);
    isvc.metadata.labels = Some(BTreeMap::from([("app".to_string(), "muse".to_string())]));

    let created = api.create(&PostParams::default(), &isvc).await?;
    assert_eq!(created.metadata.namespace.as_deref(), Some(ns));

    // clientset namespaced GET
    let got = api.get("test-isvc").await?;
    assert_eq!(got.metadata.name.as_deref(), Some("test-isvc"));

    // lister converges, then namespace + selector filters resolve it
    eventually(Duration::from_secs(10), || {
        lister.get_namespaced(ns, "test-isvc").is_some()
    })
    .await?;
    assert_eq!(lister.list_namespaced(ns).len(), 1);
    assert!(lister.list_namespaced("kube-system").is_empty());

    let hit = BTreeMap::from([("app".to_string(), "muse".to_string())]);
    let miss = BTreeMap::from([("app".to_string(), "nope".to_string())]);
    assert_eq!(lister.list_matching(&hit).len(), 1);
    assert!(lister.list_matching(&miss).is_empty());

    // set-based selectors (the labels.Selector parity surface)
    let req = |op: &str, values: Option<Vec<String>>| LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "app".into(),
            operator: op.into(),
            values,
        }]),
    };
    assert_eq!(lister.list_by_selector(&req("Exists", None)).len(), 1);
    assert_eq!(
        lister
            .list_namespaced_by_selector(ns, &req("In", Some(vec!["muse".into(), "other".into()])))
            .len(),
        1
    );
    assert!(lister
        .list_by_selector(&req("NotIn", Some(vec!["muse".into()])))
        .is_empty());
    assert!(lister
        .list_by_selector(&req("DoesNotExist", None))
        .is_empty());

    // delete + converge gone
    api.delete("test-isvc", &DeleteParams::default()).await?;
    eventually(Duration::from_secs(10), || {
        lister.get_namespaced(ns, "test-isvc").is_none()
    })
    .await?;

    Ok(())
}

/// The informer's event-subscription path (the `AddEventHandler` analog): an
/// `Applied` event on create and a `Deleted` event on delete, each delivered
/// after the cache is already updated.
#[tokio::test]
async fn informer_event_subscription() -> anyhow::Result<()> {
    let cluster = common::start().await?;
    let ome = OmeClient::new(cluster.client.clone());
    let api = ome.workload_clusters();

    let factory = SharedInformerFactory::new(cluster.client.clone());
    let informer = factory.informer::<WorkloadCluster>().await;
    let mut rx = informer.subscribe();

    // create -> Applied
    let spec: WorkloadClusterSpec = serde_json::from_value(json!({
        "clusterSource": { "kubeConfig": { "secretRef": { "name": "dummy" } } }
    }))?;
    api.create(
        &PostParams::default(),
        &WorkloadCluster::new("evt-wc", spec),
    )
    .await?;
    match next_event(&mut rx, Duration::from_secs(10)).await? {
        InformerEvent::Applied(o) => assert_eq!(o.metadata.name.as_deref(), Some("evt-wc")),
        other => anyhow::bail!("expected Applied, got {other:?}"),
    }

    // delete -> Deleted
    api.delete("evt-wc", &DeleteParams::default()).await?;
    match next_event(&mut rx, Duration::from_secs(10)).await? {
        InformerEvent::Deleted(o) => assert_eq!(o.metadata.name.as_deref(), Some("evt-wc")),
        other => anyhow::bail!("expected Deleted, got {other:?}"),
    }

    Ok(())
}

/// Receive the next informer event, tolerating broadcast lag, within `timeout`.
async fn next_event(
    rx: &mut broadcast::Receiver<InformerEvent<WorkloadCluster>>,
    timeout: Duration,
) -> anyhow::Result<InformerEvent<WorkloadCluster>> {
    tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Ok(ev) => return Ok(ev),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(e) => anyhow::bail!("subscription closed: {e}"),
            }
        }
    })
    .await?
}
