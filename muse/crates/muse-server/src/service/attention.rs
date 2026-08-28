//! `GET /v1/attention` — the fleet-wide triage list.
//!
//! One aggregation over the informer caches answering "what needs an operator
//! right now": deployments that are not ready, rollout components wedged in a
//! failed or paused phase, pods stuck with a concrete failure reason (surfaced
//! even when their service reports Ready — e.g. a broken canary instance),
//! models whose download failed, and clusters whose cache is unhealthy. Most
//! severe first. Transient pod states (image still pulling, containers
//! starting) are deliberately excluded — they belong to the per-deployment
//! diagnosis view, not fleet triage.

use std::collections::BTreeMap;

use axum::{
    extract::{Query, State},
    Json,
};
use ome_client::{k8s_openapi::api::core::v1::Pod, BaseModel, ClusterBaseModel, InferenceService};

use super::error::ApiError;
use crate::{
    app_context::AppState,
    informers::CacheSyncState,
    protocol::{
        pod_problem_reason, to_val, AttentionItem, BaseModelDetail, InferenceServiceView, Listing,
        ViewParams, INFERENCE_SERVICE_POD_LABEL,
    },
};

/// Pod reasons that mean "definitively broken" rather than "degraded".
const CRITICAL_POD_REASONS: [&str; 5] = [
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "OOMKilled",
    "CreateContainerConfigError",
];

/// Pod reasons that are normal transitional states during startup or scale-up;
/// fleet triage skips them (the per-deployment pods view still shows them).
const TRANSIENT_POD_REASONS: [&str; 4] = [
    "ContainerCreating",
    "PodInitializing",
    "ContainersNotReady",
    "Pending",
];

pub async fn list(
    Query(params): Query<ViewParams>,
    State(st): State<AppState>,
) -> Result<Json<Listing>, ApiError> {
    let mut items = Vec::new();
    let mut matched = false;
    for c in st.clusters.iter() {
        if let Some(want) = params.cluster.as_deref() {
            if c.name != want {
                continue;
            }
        }
        matched = true;
        let cache = c.cache.snapshot();
        if let Some(item) = cluster_item(&c.name, &cache) {
            items.push(item);
        }
        // Data-level findings come only from a ready cache — a partial cache
        // would report phantom problems (e.g. "0 ready replicas") mid-sync.
        if !cache.ready() {
            continue;
        }
        for isvc in c.inference_services.list() {
            deployment_items(&c.name, &isvc, &mut items);
        }
        pod_items(&c.name, &c.pods.list(), &mut items);
        for m in c.base_models.list() {
            model_item(&c.name, "BaseModel", base_model_facts(&m), &mut items);
        }
        for m in c.cluster_base_models.list() {
            model_item(
                &c.name,
                "ClusterBaseModel",
                cluster_base_model_facts(&m),
                &mut items,
            );
        }
    }
    if params.cluster.is_some() && !matched {
        return Err(ApiError::NotFound(format!(
            "unknown cluster: {}",
            params.cluster.as_deref().unwrap_or_default()
        )));
    }
    sort_items(&mut items);
    Ok(Json(Listing::new(items.into_iter().map(to_val).collect())))
}

/// Most severe first, then by domain (cluster-level problems outrank workload
/// ones), then stably by cluster and name so the list doesn't jitter.
fn sort_items(items: &mut [AttentionItem]) {
    fn rank(item: &AttentionItem) -> (u8, u8) {
        let severity = match item.severity {
            "critical" => 0,
            _ => 1,
        };
        let category = match item.category {
            "cluster" => 0,
            "deployment" => 1,
            "rollout" => 2,
            "pods" => 3,
            _ => 4,
        };
        (severity, category)
    }
    items.sort_by(|a, b| {
        rank(a)
            .cmp(&rank(b))
            .then_with(|| a.cluster.cmp(&b.cluster))
            .then_with(|| a.name.cmp(&b.name))
    });
}

fn cluster_item(name: &str, cache: &CacheSyncState) -> Option<AttentionItem> {
    let (severity, summary) = match cache {
        CacheSyncState::Failed(error) => ("critical", format!("cluster cache failed: {error}")),
        CacheSyncState::TimedOut(error) => {
            ("warning", format!("cluster cache sync timed out: {error}"))
        }
        CacheSyncState::Pending | CacheSyncState::Ready => return None,
    };
    Some(AttentionItem {
        severity,
        category: "cluster",
        cluster: name.to_owned(),
        namespace: None,
        kind: "Cluster",
        name: name.to_owned(),
        summary,
        detail: None,
    })
}

fn deployment_items(cluster: &str, isvc: &InferenceService, out: &mut Vec<AttentionItem>) {
    let view = InferenceServiceView::of(isvc);
    if view.ready == Some(false) {
        out.push(AttentionItem {
            severity: "critical",
            category: "deployment",
            cluster: cluster.to_owned(),
            namespace: view.namespace.clone(),
            kind: "InferenceService",
            name: view.name.clone(),
            summary: view
                .reason
                .clone()
                .unwrap_or_else(|| "not ready".to_owned()),
            detail: view.message.clone(),
        });
    }
    for component in &view.components {
        let Some(phase) = component.rollout_phase.as_deref() else {
            continue;
        };
        let (severity, why) = match phase {
            "Failed" => ("critical", "rollout failed"),
            "Paused" => ("warning", "rollout paused — awaiting promotion"),
            "RollingBack" => ("warning", "rolling back"),
            _ => continue,
        };
        out.push(AttentionItem {
            severity,
            category: "rollout",
            cluster: cluster.to_owned(),
            namespace: view.namespace.clone(),
            kind: "InferenceService",
            name: view.name.clone(),
            summary: format!("{}: {why}", component.name),
            detail: None,
        });
    }
}

/// Aggregate stuck pods per owning deployment: one item per deployment naming
/// the top failure reasons, so a 500-pod engine with 3 crash-looping instances
/// is one row, not three.
fn pod_items(cluster: &str, pods: &[std::sync::Arc<Pod>], out: &mut Vec<AttentionItem>) {
    let mut per: BTreeMap<(String, String), BTreeMap<String, usize>> = BTreeMap::new();
    for pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(owner) = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(INFERENCE_SERVICE_POD_LABEL))
        else {
            continue;
        };
        if pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Succeeded") {
            continue;
        }
        let Some(reason) = pod_problem_reason(pod) else {
            continue;
        };
        if TRANSIENT_POD_REASONS.contains(&reason.as_str()) {
            continue;
        }
        let namespace = pod.metadata.namespace.clone().unwrap_or_default();
        *per.entry((namespace, owner.clone()))
            .or_default()
            .entry(reason)
            .or_default() += 1;
    }
    for ((namespace, name), reasons) in per {
        let total: usize = reasons.values().sum();
        let critical = reasons
            .keys()
            .any(|reason| CRITICAL_POD_REASONS.contains(&reason.as_str()));
        let mut counts: Vec<(&String, &usize)> = reasons.iter().collect();
        counts.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        let top: Vec<String> = counts
            .iter()
            .take(3)
            .map(|(reason, count)| format!("{count} {reason}"))
            .collect();
        out.push(AttentionItem {
            severity: if critical { "critical" } else { "warning" },
            category: "pods",
            cluster: cluster.to_owned(),
            namespace: Some(namespace),
            kind: "InferenceService",
            name,
            summary: format!(
                "{total} pod{} stuck: {}",
                if total == 1 { "" } else { "s" },
                top.join(", ")
            ),
            detail: None,
        });
    }
}

/// The model facts attention needs, extracted once per model kind.
struct ModelFacts {
    namespace: Option<String>,
    name: String,
    state: Option<String>,
    nodes_failed: usize,
    failure: Option<String>,
}

fn base_model_facts(m: &BaseModel) -> ModelFacts {
    let detail = BaseModelDetail::of(m);
    ModelFacts {
        namespace: detail.view.namespace.clone(),
        name: detail.view.name.clone(),
        state: detail.view.state.clone(),
        nodes_failed: detail.nodes_failed.unwrap_or(0),
        failure: failing_condition_message(&detail),
    }
}

fn cluster_base_model_facts(m: &ClusterBaseModel) -> ModelFacts {
    let detail = BaseModelDetail::of_cluster(m);
    ModelFacts {
        namespace: detail.view.namespace.clone(),
        name: detail.view.name.clone(),
        state: detail.view.state.clone(),
        nodes_failed: detail.nodes_failed.unwrap_or(0),
        failure: failing_condition_message(&detail),
    }
}

fn failing_condition_message(detail: &BaseModelDetail) -> Option<String> {
    detail
        .conditions
        .iter()
        .find(|c| c.status == "False")
        .and_then(|c| c.message.clone().or_else(|| c.reason.clone()))
}

fn model_item(cluster: &str, kind: &'static str, facts: ModelFacts, out: &mut Vec<AttentionItem>) {
    let (severity, summary) = if facts.state.as_deref() == Some("Failed") {
        ("critical", "model download failed".to_owned())
    } else if facts.nodes_failed > 0 {
        (
            "warning",
            format!(
                "model download failed on {} node{}",
                facts.nodes_failed,
                if facts.nodes_failed == 1 { "" } else { "s" }
            ),
        )
    } else {
        return;
    };
    out.push(AttentionItem {
        severity,
        category: "model",
        cluster: cluster.to_owned(),
        namespace: facts.namespace,
        kind,
        name: facts.name,
        summary,
        detail: facts.failure,
    });
}

#[cfg(test)]
mod tests {
    use ome_client::k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateWaiting, ContainerStatus, Pod, PodCondition, PodStatus,
    };

    use super::*;

    fn labelled_pod(name: &str, owner: &str, status: PodStatus) -> std::sync::Arc<Pod> {
        let mut pod = Pod::default();
        pod.metadata.name = Some(name.to_owned());
        pod.metadata.namespace = Some("ns".to_owned());
        pod.metadata.labels = Some(
            [(INFERENCE_SERVICE_POD_LABEL.to_owned(), owner.to_owned())]
                .into_iter()
                .collect(),
        );
        pod.status = Some(status);
        std::sync::Arc::new(pod)
    }

    fn waiting(reason: &str) -> PodStatus {
        PodStatus {
            phase: Some("Pending".to_owned()),
            container_statuses: Some(vec![ContainerStatus {
                name: "c".to_owned(),
                state: Some(ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some(reason.to_owned()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn pod_items_aggregate_per_deployment_and_skip_transient_states() {
        let pods = vec![
            labelled_pod("a-0", "svc-a", waiting("CrashLoopBackOff")),
            labelled_pod("a-1", "svc-a", waiting("CrashLoopBackOff")),
            labelled_pod("a-2", "svc-a", waiting("ContainerCreating")),
            labelled_pod(
                "b-0",
                "svc-b",
                PodStatus {
                    phase: Some("Pending".to_owned()),
                    conditions: Some(vec![PodCondition {
                        type_: "PodScheduled".to_owned(),
                        status: "False".to_owned(),
                        reason: Some("Unschedulable".to_owned()),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            ),
        ];
        let mut items = Vec::new();
        pod_items("c1", &pods, &mut items);
        assert_eq!(items.len(), 2);
        let a = items.iter().find(|i| i.name == "svc-a").unwrap();
        assert_eq!(a.severity, "critical");
        assert_eq!(a.summary, "2 pods stuck: 2 CrashLoopBackOff");
        let b = items.iter().find(|i| i.name == "svc-b").unwrap();
        assert_eq!(b.severity, "warning");
        assert_eq!(b.summary, "1 pod stuck: 1 Unschedulable");
    }

    #[test]
    fn cluster_items_report_failed_and_timed_out_only() {
        assert!(cluster_item("c1", &CacheSyncState::Pending).is_none());
        assert!(cluster_item("c1", &CacheSyncState::Ready).is_none());
        let failed = cluster_item("c1", &CacheSyncState::Failed("watch dropped".to_owned()))
            .expect("failed cache is a finding");
        assert_eq!(failed.severity, "critical");
        let timed_out = cluster_item(
            "c1",
            &CacheSyncState::TimedOut("deadline exceeded".to_owned()),
        )
        .expect("timed-out cache is a finding");
        assert_eq!(timed_out.severity, "warning");
    }

    #[test]
    fn items_sort_most_severe_first_then_by_domain() {
        let item = |severity: &'static str, category: &'static str, name: &str| AttentionItem {
            severity,
            category,
            cluster: "c1".to_owned(),
            namespace: None,
            kind: "InferenceService",
            name: name.to_owned(),
            summary: String::new(),
            detail: None,
        };
        let mut items = vec![
            item("warning", "model", "m"),
            item("critical", "pods", "p"),
            item("warning", "cluster", "c"),
            item("critical", "deployment", "d"),
        ];
        sort_items(&mut items);
        let order: Vec<&str> = items.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(order, ["d", "p", "c", "m"]);
    }
}
