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
use ome_client::{
    inferenceservices::InferenceServiceStatusCanary, k8s_openapi::api::core::v1::Pod,
    InferenceService,
};

use super::error::ApiError;
use crate::{
    app_context::AppState,
    informers::CacheSyncState,
    protocol::{
        failing_checks, isvc_owner, pod_problem_reason, secs_since_status_time, since_status_time,
        to_val, AttentionItem, BaseModelDetail, CanaryCheckView, Category, InferenceServiceView,
        Listing, Severity, ViewParams,
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
    if let Some(cluster) = params.cluster.as_deref() {
        st.validate_cluster(cluster)?;
    }
    let mut items = Vec::new();
    for c in st.clusters.iter() {
        if let Some(want) = params.cluster.as_deref() {
            if c.name != want {
                continue;
            }
        }
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
            deployment_items(&c.name, &isvc, st.attention_stuck_step_secs(), &mut items);
        }
        pod_items(&c.name, &c.pods.list(), &mut items);
        for m in c.base_models.list() {
            model_item(
                &c.name,
                "BaseModel",
                model_facts(&BaseModelDetail::of(&m)),
                &mut items,
            );
        }
        for m in c.cluster_base_models.list() {
            model_item(
                &c.name,
                "ClusterBaseModel",
                model_facts(&BaseModelDetail::of_cluster(&m)),
                &mut items,
            );
        }
    }
    sort_items(&mut items);
    Ok(Json(Listing::new(items.into_iter().map(to_val).collect())))
}

/// Most severe first, then by domain (cluster-level problems outrank workload
/// ones), then stably by cluster and name so the list doesn't jitter.
fn sort_items(items: &mut [AttentionItem]) {
    items.sort_by(|a, b| {
        (a.severity, a.category)
            .cmp(&(b.severity, b.category))
            .then_with(|| a.cluster.cmp(&b.cluster))
            .then_with(|| a.name.cmp(&b.name))
    });
}

fn cluster_item(name: &str, cache: &CacheSyncState) -> Option<AttentionItem> {
    let (severity, summary) = match cache {
        CacheSyncState::Failed(error) => {
            (Severity::Critical, format!("cluster cache failed: {error}"))
        }
        CacheSyncState::TimedOut(error) => (
            Severity::Warning,
            format!("cluster cache sync timed out: {error}"),
        ),
        CacheSyncState::Pending | CacheSyncState::Ready => return None,
    };
    Some(AttentionItem {
        severity,
        category: Category::Cluster,
        cluster: name.to_owned(),
        namespace: None,
        kind: "Cluster",
        name: name.to_owned(),
        summary,
        detail: None,
    })
}

fn deployment_items(
    cluster: &str,
    isvc: &InferenceService,
    stuck_step_secs: Option<u64>,
    out: &mut Vec<AttentionItem>,
) {
    let view = InferenceServiceView::of(isvc);
    if view.ready == Some(false) {
        out.push(AttentionItem {
            severity: Severity::Critical,
            category: Category::Deployment,
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
    let mut phase_reported = false;
    for component in &view.components {
        let Some(phase) = component.rollout_phase.as_deref() else {
            continue;
        };
        let (severity, why) = match phase {
            "Failed" => (Severity::Critical, "rollout failed"),
            "Paused" => (Severity::Warning, "rollout paused — awaiting promotion"),
            "RollingBack" => (Severity::Warning, "rolling back"),
            _ => continue,
        };
        phase_reported = true;
        out.push(AttentionItem {
            severity,
            category: Category::Rollout,
            cluster: cluster.to_owned(),
            namespace: view.namespace.clone(),
            kind: "InferenceService",
            name: view.name.clone(),
            summary: format!("{}: {why}", component.name),
            detail: None,
        });
    }
    if let Some(canary) = isvc.status.as_ref().and_then(|s| s.canary.as_ref()) {
        canary_items(cluster, &view, canary, stuck_step_secs, phase_reported, out);
    }
}

/// Canary wedge findings. The phase findings above catch Failed / Paused /
/// RollingBack; this catches the two ways an *active* canary stops making
/// progress — its analysis checks are failing, or its step has sat unchanged
/// past `stuck_step_secs` (None disables age detection). A canary that is both
/// stuck and failing checks cannot advance without an operator, so it
/// escalates to critical.
fn canary_items(
    cluster: &str,
    view: &InferenceServiceView,
    canary: &InferenceServiceStatusCanary,
    stuck_step_secs: Option<u64>,
    phase_reported: bool,
    out: &mut Vec<AttentionItem>,
) {
    let failing = failing_checks(canary);
    // The counter is authoritative even when per-check results are pruned.
    let failing_count = failing
        .len()
        .max(canary.analysis_failed_checks.unwrap_or(0).max(0) as usize);
    let step_age = canary.step_entered_time.as_deref();
    let stuck = !phase_reported
        && matches!(
            (stuck_step_secs, step_age.and_then(secs_since_status_time)),
            (Some(threshold), Some(secs))
                if secs >= i64::try_from(threshold).unwrap_or(i64::MAX)
        );
    if !stuck && failing_count == 0 {
        return;
    }
    let checks = |n: usize| format!("{n} check{} failing", if n == 1 { "" } else { "s" });
    let (severity, summary) = if stuck {
        let age = step_age
            .and_then(since_status_time)
            .unwrap_or_else(|| "a while".to_owned());
        let stuck_at = format!("canary stuck at step {} for {age}", canary.current_step);
        if failing_count > 0 {
            (
                Severity::Critical,
                format!("{stuck_at} — {}", checks(failing_count)),
            )
        } else {
            (Severity::Warning, stuck_at)
        }
    } else {
        (
            Severity::Warning,
            format!("canary: {}", checks(failing_count)),
        )
    };
    out.push(AttentionItem {
        severity,
        category: Category::Rollout,
        cluster: cluster.to_owned(),
        namespace: view.namespace.clone(),
        kind: "InferenceService",
        name: view.name.clone(),
        summary,
        detail: (!failing.is_empty()).then(|| {
            failing
                .iter()
                .map(describe_check)
                .collect::<Vec<_>>()
                .join("; ")
        }),
    });
}

/// One failing check as a sentence fragment: "gpu_error_rate (want <= 0.5, got
/// 2.1): error budget exhausted".
fn describe_check(c: &CanaryCheckView) -> String {
    let mut s = c.name.clone();
    match (&c.threshold, &c.value) {
        (Some(t), Some(v)) => s.push_str(&format!(" (want {t}, got {v})")),
        (Some(t), None) => s.push_str(&format!(" (want {t})")),
        (None, Some(v)) => s.push_str(&format!(" (got {v})")),
        (None, None) => {}
    }
    if let Some(m) = &c.message {
        s.push_str(&format!(": {m}"));
    }
    s
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
        let Some(owner) = isvc_owner(pod) else {
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
        *per.entry((namespace, owner.to_owned()))
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
            severity: if critical {
                Severity::Critical
            } else {
                Severity::Warning
            },
            category: Category::Pods,
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

/// Extract the facts from a projected model detail (BaseModel and
/// ClusterBaseModel share the [`BaseModelDetail`] shape).
fn model_facts(detail: &BaseModelDetail) -> ModelFacts {
    ModelFacts {
        namespace: detail.view.namespace.clone(),
        name: detail.view.name.clone(),
        state: detail.view.state.clone(),
        nodes_failed: detail.nodes_failed.unwrap_or(0),
        failure: failing_condition_message(detail),
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
        (Severity::Critical, "model download failed".to_owned())
    } else if facts.nodes_failed > 0 {
        (
            Severity::Warning,
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
        category: Category::Model,
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

    use serde_json::{json, Value};

    use super::*;
    use crate::protocol::INFERENCE_SERVICE_POD_LABEL;

    /// A minimal InferenceService whose status is the given JSON.
    fn isvc_with_status(status: Value) -> InferenceService {
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": "svc", "namespace": "ns" },
            "spec": {},
            "status": status,
        }))
        .expect("fixture deserializes")
    }

    /// A canary far past any reasonable step age (entered in 2020).
    fn old_canary(failed_checks: i32) -> Value {
        json!({
            "currentStep": 2,
            "observedTrafficWeight": 25,
            "stepEnteredTime": "2020-01-01T00:00:00Z",
            "analysisFailedChecks": failed_checks,
            "metricResults": (0..failed_checks).map(|i| json!({
                "name": format!("check-{i}"),
                "passed": false,
                "operator": "<=",
                "threshold": "0.5",
                "value": "2.1",
                "message": "over budget"
            })).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn stuck_canary_step_warns_and_escalates_with_failing_checks() {
        // Old step, threshold armed, nothing failing → warning.
        let isvc = isvc_with_status(json!({ "canary": old_canary(0) }));
        let mut items = Vec::new();
        deployment_items("c1", &isvc, Some(60), &mut items);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].severity, Severity::Warning);
        assert_eq!(items[0].category, Category::Rollout);
        assert!(items[0].summary.starts_with("canary stuck at step 2 for "));
        assert!(items[0].detail.is_none());

        // Stuck *and* failing checks → cannot advance on its own → critical.
        let isvc = isvc_with_status(json!({ "canary": old_canary(2) }));
        let mut items = Vec::new();
        deployment_items("c1", &isvc, Some(60), &mut items);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].severity, Severity::Critical);
        assert!(items[0].summary.contains("2 checks failing"));
        let detail = items[0].detail.as_deref().expect("failing checks listed");
        assert!(detail.contains("check-0 (want <= 0.5, got 2.1): over budget"));
    }

    #[test]
    fn failing_checks_alone_warn_even_without_step_age_policy() {
        let mut canary = old_canary(1);
        canary["stepEnteredTime"] = Value::Null;
        let isvc = isvc_with_status(json!({ "canary": canary }));
        let mut items = Vec::new();
        deployment_items("c1", &isvc, None, &mut items);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].severity, Severity::Warning);
        assert_eq!(items[0].summary, "canary: 1 check failing");
    }

    #[test]
    fn step_age_detection_stays_off_without_a_threshold() {
        let isvc = isvc_with_status(json!({ "canary": old_canary(0) }));
        let mut items = Vec::new();
        deployment_items("c1", &isvc, None, &mut items);
        assert!(items.is_empty());
    }

    #[test]
    fn healthy_fresh_canary_is_not_a_finding() {
        // Step entered in 2020, but the threshold is far larger than the age.
        let isvc = isvc_with_status(json!({ "canary": old_canary(0) }));
        let mut items = Vec::new();
        deployment_items("c1", &isvc, Some(u64::MAX), &mut items);
        assert!(items.is_empty());
    }

    #[test]
    fn stuck_step_defers_to_an_already_reported_phase() {
        // Paused already produces "awaiting promotion" — a stuck-step item on
        // top of it would say the same thing twice.
        let isvc = isvc_with_status(json!({
            "canary": old_canary(0),
            "components": { "engine": { "rolloutPhase": "Paused" } },
        }));
        let mut items = Vec::new();
        deployment_items("c1", &isvc, Some(60), &mut items);
        assert_eq!(items.len(), 1);
        assert!(items[0].summary.contains("awaiting promotion"));
    }

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
        assert_eq!(a.severity, Severity::Critical);
        assert_eq!(a.summary, "2 pods stuck: 2 CrashLoopBackOff");
        let b = items.iter().find(|i| i.name == "svc-b").unwrap();
        assert_eq!(b.severity, Severity::Warning);
        assert_eq!(b.summary, "1 pod stuck: 1 Unschedulable");
    }

    #[test]
    fn cluster_items_report_failed_and_timed_out_only() {
        assert!(cluster_item("c1", &CacheSyncState::Pending).is_none());
        assert!(cluster_item("c1", &CacheSyncState::Ready).is_none());
        let failed = cluster_item("c1", &CacheSyncState::Failed("watch dropped".to_owned()))
            .expect("failed cache is a finding");
        assert_eq!(failed.severity, Severity::Critical);
        let timed_out = cluster_item(
            "c1",
            &CacheSyncState::TimedOut("deadline exceeded".to_owned()),
        )
        .expect("timed-out cache is a finding");
        assert_eq!(timed_out.severity, Severity::Warning);
    }

    #[test]
    fn items_sort_most_severe_first_then_by_domain() {
        let item = |severity: Severity, category: Category, name: &str| AttentionItem {
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
            item(Severity::Warning, Category::Model, "m"),
            item(Severity::Critical, Category::Pods, "p"),
            item(Severity::Warning, Category::Cluster, "c"),
            item(Severity::Critical, Category::Deployment, "d"),
        ];
        sort_items(&mut items);
        let order: Vec<&str> = items.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(order, ["d", "p", "c", "m"]);
    }
}
