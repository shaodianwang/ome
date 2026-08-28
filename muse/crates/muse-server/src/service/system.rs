//! `GET /v1/system` — an admin view of OME's *own* control plane (OEP-0021.2).
//!
//! For each configured [`SystemComponent`] descriptor, on each in-scope,
//! cache-ready cluster, this resolves the matching apps/v1 Deployment/DaemonSet
//! objects from the informer caches and derives a [`ComponentHealth`]. It reports
//! desired-vs-ready — including the two states a Pod scan alone cannot see: a
//! component scaled to zero, and a component that is expected but absent
//! (`missing`). No apiserver call is made; everything is served from cache, per
//! MUSE's cache-only invariant.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Query, State},
    Json,
};
use kube::Resource;
use ome_client::{
    k8s_openapi::{
        api::{
            apps::v1::{
                DaemonSet, DaemonSetCondition, Deployment, DeploymentCondition, DeploymentStatus,
            },
            core::v1::Pod,
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    listers::Lister,
};

use super::error::ApiError;
use crate::{
    app_context::AppState,
    informers::ClusterState,
    protocol::{
        nonempty, tagged, ComponentHealth, ConditionView, ListParams, Listing, SystemComponent,
        SystemComponentKind, SystemComponentRow, SystemPodView,
    },
};

/// `GET /v1/system[?cluster=]` — one row per (configured component × in-scope
/// cluster). Each row carries its `cluster`. With no components configured the
/// listing is empty (the console renders a "no components configured" state).
pub async fn list(
    Query(params): Query<ListParams>,
    State(st): State<AppState>,
) -> Result<Json<Listing>, ApiError> {
    let components = st.system_components();
    if components.is_empty() {
        return Ok(Json(Listing::of_kind("SystemComponent", Vec::new())));
    }
    let selected = st.select(params.cluster.as_deref())?;
    let mut items = Vec::new();
    for c in selected {
        for component in components {
            if !component.applies_to(&c.name) {
                continue;
            }
            items.push(tagged(evaluate(component, c), &c.name));
        }
    }
    Ok(Json(Listing::of_kind("SystemComponent", items)))
}

/// The derived health of one matched object (or the aggregate of several).
struct Health {
    status: ComponentHealth,
    desired: i32,
    ready: i32,
    updated: Option<i32>,
    message: Option<String>,
}

/// The per-kind resolution of a descriptor on one cluster: health per matched
/// object, plus the drawer detail (object names, workload conditions, and each
/// object's pod selector for finding backing pods).
struct Matched {
    healths: Vec<Health>,
    objects: Vec<String>,
    conditions: Vec<ConditionView>,
    selectors: Vec<BTreeMap<String, String>>,
}

/// Resolve a descriptor's objects on one cluster, projecting each through the
/// kind's health/conditions/selector accessors. Zero matched objects on an
/// in-scope cluster later reads as `missing` — the descriptor names a component
/// that is not there.
fn resolve<K>(
    lister: &Lister<K>,
    component: &SystemComponent,
    health: impl Fn(&K) -> Health,
    conditions: impl Fn(&K) -> Vec<ConditionView>,
    selector: impl Fn(&K) -> BTreeMap<String, String>,
) -> Matched
where
    K: Resource<DynamicType = ()> + Clone + 'static,
{
    let objs = match_objects(lister, component);
    Matched {
        healths: objs.iter().map(|o| health(o)).collect(),
        objects: object_names(&objs),
        conditions: objs.iter().flat_map(|o| conditions(o)).collect(),
        selectors: objs.iter().map(|o| selector(o)).collect(),
    }
}

fn evaluate(component: &SystemComponent, cluster: &ClusterState) -> SystemComponentRow {
    let matched = match component.kind {
        SystemComponentKind::Deployment => resolve(
            &cluster.deployments,
            component,
            deployment_health,
            |d: &Deployment| {
                workload_conditions(d.status.as_ref().and_then(|s| s.conditions.as_deref()))
            },
            |d: &Deployment| pod_selector(d.spec.as_ref().map(|s| &s.selector)),
        ),
        SystemComponentKind::DaemonSet => resolve(
            &cluster.daemon_sets,
            component,
            daemonset_health,
            |d: &DaemonSet| {
                workload_conditions(d.status.as_ref().and_then(|s| s.conditions.as_deref()))
            },
            |d: &DaemonSet| pod_selector(d.spec.as_ref().map(|s| &s.selector)),
        ),
    };
    let Matched {
        healths,
        objects,
        conditions,
        selectors,
    } = matched;
    let pods = component_pods(&cluster.pods, &component.namespace, &selectors);
    let aggregate = combine(healths);
    // A `missing` row has no controller message; explain what was looked for.
    let message = if aggregate.status == ComponentHealth::Missing {
        Some(missing_reason(component))
    } else {
        aggregate.message
    };
    SystemComponentRow {
        name: component.name.clone(),
        group: component.group.clone(),
        kind: component.kind.as_str(),
        namespace: component.namespace.clone(),
        status: aggregate.status.as_str(),
        desired: aggregate.desired,
        ready: aggregate.ready,
        updated: aggregate.updated,
        message,
        objects,
        conditions,
        pods,
    }
}

/// The names of the matched workload objects (for the drawer's identity line).
fn object_names<K: Resource<DynamicType = ()>>(objs: &[Arc<K>]) -> Vec<String> {
    objs.iter().filter_map(|o| o.meta().name.clone()).collect()
}

/// A workload's pod selector `matchLabels` (empty when unset).
fn pod_selector(selector: Option<&LabelSelector>) -> BTreeMap<String, String> {
    selector
        .and_then(|s| s.match_labels.clone())
        .unwrap_or_default()
}

/// The pods backing a component: those in its namespace matching any matched
/// object's pod selector, reported as name, phase, and node.
fn component_pods(
    pods: &Lister<Pod>,
    namespace: &str,
    selectors: &[BTreeMap<String, String>],
) -> Vec<SystemPodView> {
    let selectors: Vec<&BTreeMap<String, String>> =
        selectors.iter().filter(|s| !s.is_empty()).collect();
    if selectors.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<SystemPodView> = pods
        .list_namespaced(namespace)
        .iter()
        .filter(|p| {
            selectors
                .iter()
                .any(|sel| matches(p.meta().labels.as_ref(), sel))
        })
        .map(|p| SystemPodView {
            name: p.meta().name.clone().unwrap_or_default(),
            phase: p
                .status
                .as_ref()
                .and_then(|s| s.phase.clone())
                .unwrap_or_default(),
            node: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The condition shape shared by the apps/v1 workload kinds, which are
/// structurally identical but unrelated types in k8s-openapi.
trait WorkloadCondition {
    fn view(&self) -> ConditionView;
}

macro_rules! workload_condition {
    ($($ty:ty),*) => {$(
        impl WorkloadCondition for $ty {
            fn view(&self) -> ConditionView {
                ConditionView::new(
                    &self.type_,
                    &self.status,
                    self.reason.clone(),
                    self.message.clone(),
                )
            }
        }
    )*};
}
workload_condition!(DeploymentCondition, DaemonSetCondition);

/// Project a workload's status conditions for the drawer.
fn workload_conditions<C: WorkloadCondition>(conds: Option<&[C]>) -> Vec<ConditionView> {
    conds.unwrap_or_default().iter().map(C::view).collect()
}

/// A human explanation for a `missing` component — what the descriptor looked for.
fn missing_reason(component: &SystemComponent) -> String {
    match component.resource_name.as_deref() {
        Some(name) => format!(
            "no {} named \"{name}\" in namespace {}",
            component.kind.as_str(),
            component.namespace
        ),
        None => format!(
            "no {} matched the selector in namespace {}",
            component.kind.as_str(),
            component.namespace
        ),
    }
}

/// The cached objects a descriptor matches on one cluster. Match by exact object
/// name when `resource_name` is set (preferred for control-plane singletons, and
/// required for workloads that carry no metadata labels of their own — the OME
/// `model-agent` DaemonSet labels only its pod template, not the DaemonSet), else
/// by the descriptor's label selector over each object's own metadata labels.
fn match_objects<K>(lister: &Lister<K>, component: &SystemComponent) -> Vec<Arc<K>>
where
    K: Resource<DynamicType = ()> + Clone + 'static,
{
    match component.resource_name.as_deref() {
        Some(name) => lister
            .get_namespaced(&component.namespace, name)
            .into_iter()
            .collect(),
        None => lister
            .list_namespaced(&component.namespace)
            .into_iter()
            .filter(|o| matches(o.meta().labels.as_ref(), &component.selector))
            .collect(),
    }
}

/// Whether an object's labels contain every entry of an exact-match selector.
fn matches(labels: Option<&BTreeMap<String, String>>, selector: &BTreeMap<String, String>) -> bool {
    selector
        .iter()
        .all(|(k, v)| labels.and_then(|l| l.get(k)).is_some_and(|x| x == v))
}

/// Aggregate the matched objects into one row. No matches ⇒ `missing`. Otherwise
/// sum the counts and take the worst status (worst-wins), so a roll-up never
/// reads `healthy` while one instance is down.
fn combine(healths: Vec<Health>) -> Health {
    if healths.is_empty() {
        return Health {
            status: ComponentHealth::Missing,
            desired: 0,
            ready: 0,
            updated: None,
            message: None,
        };
    }
    let mut desired = 0;
    let mut ready = 0;
    let mut updated: Option<i32> = None;
    let mut worst = ComponentHealth::Healthy;
    let mut message = None;
    for h in healths {
        desired += h.desired;
        ready += h.ready;
        if let Some(u) = h.updated {
            updated = Some(updated.unwrap_or(0) + u);
        }
        // Keep the message from the worst object seen so far.
        if h.status > worst {
            worst = h.status;
            message = h.message;
        } else if worst == ComponentHealth::Healthy {
            message = None;
        }
    }
    Health {
        status: worst,
        desired,
        ready,
        updated,
        message,
    }
}

/// Health of one Deployment. Desired defaults to 1 when `spec.replicas` is unset
/// (the Kubernetes default). `Available=False` while otherwise at scale is
/// reported `degraded` rather than `healthy`.
fn deployment_health(d: &Deployment) -> Health {
    let desired = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
    let status = d.status.as_ref();
    let ready = status.and_then(|s| s.ready_replicas).unwrap_or(0);
    let updated = status.and_then(|s| s.updated_replicas);
    let mut health = classify(desired, ready);
    let mut reason: Option<String> = None;

    // Only a Healthy/Degraded base can be *worsened* by rollout details; Unknown
    // (desired 0) and Unavailable (ready 0) already dominate. Applied in priority
    // order — the first problem found sets the reason.
    if matches!(health, ComponentHealth::Healthy | ComponentHealth::Degraded) {
        if let Some(stale) = rollout_staleness(
            status.and_then(|s| s.observed_generation),
            d.meta().generation,
            updated.unwrap_or(0),
            desired,
        ) {
            health = ComponentHealth::Degraded;
            reason = Some(stale);
        }
        // Stalled rollout: Progressing=False (e.g. ProgressDeadlineExceeded).
        if reason.is_none() {
            if let Some(c) = deployment_condition(status, "Progressing") {
                if c.status != "True" {
                    health = ComponentHealth::Degraded;
                    reason = nonempty(c.message.as_ref())
                        .or_else(|| Some("rollout is not progressing".to_owned()));
                }
            }
        }
        // Healthy requires Available=True — an absent or non-True Available means
        // the Deployment is not actually serving at capacity.
        if reason.is_none() && health == ComponentHealth::Healthy {
            match deployment_condition(status, "Available") {
                Some(c) if c.status == "True" => {}
                Some(c) => {
                    health = ComponentHealth::Degraded;
                    reason =
                        nonempty(c.message.as_ref()).or_else(|| Some("not available".to_owned()));
                }
                None => {
                    health = ComponentHealth::Degraded;
                    reason = Some("Available condition not yet reported".to_owned());
                }
            }
        }
    }

    let message = match health {
        ComponentHealth::Healthy => None,
        ComponentHealth::Unknown => Some("scaled to 0 (no desired replicas)".to_owned()),
        _ => reason
            .or_else(|| first_problem_message(status))
            .or_else(|| Some(format!("{ready}/{desired} ready"))),
    };
    Health {
        status: health,
        desired,
        ready,
        updated,
        message,
    }
}

/// The named Deployment status condition, if present.
fn deployment_condition<'a>(
    status: Option<&'a DeploymentStatus>,
    type_: &str,
) -> Option<&'a DeploymentCondition> {
    status
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.type_ == type_))
}

/// The message of the first non-True Available/Progressing condition — the
/// fallback detail for an Unavailable/Degraded row when no specific reason was set.
fn first_problem_message(status: Option<&DeploymentStatus>) -> Option<String> {
    ["Available", "Progressing"].into_iter().find_map(|type_| {
        deployment_condition(status, type_)
            .filter(|c| c.status != "True")
            .and_then(|c| nonempty(c.message.as_ref()))
    })
}

/// Why healthy-looking counts still mean a degraded rollout, shared by both
/// workload kinds. A stale spec — the controller hasn't observed the latest
/// generation, so the counts describe the previous spec — makes a "healthy"
/// read untrustworthy; otherwise fewer updated replicas than desired means the
/// new revision hasn't rolled out (old replicas can still report ready ==
/// desired). `None` when neither applies.
fn rollout_staleness(
    observed: Option<i64>,
    generation: Option<i64>,
    updated: i32,
    desired: i32,
) -> Option<String> {
    if let (Some(observed), Some(generation)) = (observed, generation) {
        if observed < generation {
            return Some(format!(
                "controller has not observed generation {generation} (observed {observed})"
            ));
        }
    }
    (updated < desired).then(|| format!("rollout incomplete: {updated}/{desired} updated"))
}

/// Health of one DaemonSet, using the desired/ready node counts. A DaemonSet with
/// `desiredNumberScheduled == 0` (no nodes match its selector) is `unknown`, not
/// `unavailable` — zero desired is not a failure. Like Deployments, a stale
/// generation or an incomplete update (updated < desired) reads as degraded.
fn daemonset_health(d: &DaemonSet) -> Health {
    let status = d.status.as_ref();
    let desired = status.map(|s| s.desired_number_scheduled).unwrap_or(0);
    let ready = status.map(|s| s.number_ready).unwrap_or(0);
    let updated = status.and_then(|s| s.updated_number_scheduled);
    let mut health = classify(desired, ready);
    let mut reason: Option<String> = None;
    if matches!(health, ComponentHealth::Healthy | ComponentHealth::Degraded) {
        // A missing updated count reads as complete (unlike Deployments).
        if let Some(stale) = rollout_staleness(
            status.and_then(|s| s.observed_generation),
            d.meta().generation,
            updated.unwrap_or(desired),
            desired,
        ) {
            health = ComponentHealth::Degraded;
            reason = Some(stale);
        }
    }
    let message = match health {
        ComponentHealth::Healthy => None,
        ComponentHealth::Unknown => Some("no eligible nodes (0 desired)".to_owned()),
        _ => reason.or_else(|| Some(format!("{ready}/{desired} ready"))),
    };
    Health {
        status: health,
        desired,
        ready,
        updated,
        message,
    }
}

/// The core desired-vs-ready lattice, shared by both kinds.
fn classify(desired: i32, ready: i32) -> ComponentHealth {
    if desired <= 0 {
        ComponentHealth::Unknown
    } else if ready <= 0 {
        ComponentHealth::Unavailable
    } else if ready < desired {
        ComponentHealth::Degraded
    } else {
        ComponentHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn deployment(replicas: i32, ready: Option<i32>, available: Option<&str>) -> Deployment {
        let mut status = json!({});
        if let Some(r) = ready {
            status["readyReplicas"] = json!(r);
            status["updatedReplicas"] = json!(r);
        }
        if let Some(a) = available {
            status["conditions"] =
                json!([{ "type": "Available", "status": a, "message": "deployment message" }]);
        }
        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "ome-controller-manager", "namespace": "ome" },
            "spec": {
                "replicas": replicas,
                // selector + template are required on a Deployment spec.
                "selector": { "matchLabels": { "control-plane": "ome-controller-manager" } },
                "template": { "metadata": {}, "spec": { "containers": [] } },
            },
            "status": status,
        }))
        .expect("Deployment fixture should deserialize")
    }

    fn daemonset(desired: i32, ready: i32) -> DaemonSet {
        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": { "name": "model-agent", "namespace": "ome" },
            "spec": {
                // selector + template are required on a DaemonSet spec.
                "selector": { "matchLabels": { "app.kubernetes.io/name": "model-agent" } },
                "template": { "metadata": {}, "spec": { "containers": [] } },
            },
            "status": {
                "desiredNumberScheduled": desired,
                "numberReady": ready,
                "updatedNumberScheduled": ready,
                "currentNumberScheduled": ready,
                "numberMisscheduled": 0,
            },
        }))
        .expect("DaemonSet fixture should deserialize")
    }

    /// A Deployment with independently-set rollout fields, for rollout-edge tests.
    fn deployment_rollout(
        replicas: i32,
        ready: i32,
        updated: i32,
        generation: i64,
        observed: i64,
        conditions: serde_json::Value,
    ) -> Deployment {
        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "ome-controller-manager",
                "namespace": "ome",
                "generation": generation,
            },
            "spec": {
                "replicas": replicas,
                "selector": { "matchLabels": { "control-plane": "ome-controller-manager" } },
                "template": { "metadata": {}, "spec": { "containers": [] } },
            },
            "status": {
                "readyReplicas": ready,
                "updatedReplicas": updated,
                "observedGeneration": observed,
                "conditions": conditions,
            },
        }))
        .expect("Deployment rollout fixture should deserialize")
    }

    #[test]
    fn deployment_stalled_rollout_is_degraded() {
        // At scale and Available, but Progressing=False (ProgressDeadlineExceeded).
        let d = deployment_rollout(
            2,
            2,
            2,
            1,
            1,
            json!([
                { "type": "Available", "status": "True" },
                { "type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded",
                  "message": "exceeded its progress deadline" },
            ]),
        );
        let health = deployment_health(&d);
        assert_eq!(health.status, ComponentHealth::Degraded);
        assert_eq!(
            health.message.as_deref(),
            Some("exceeded its progress deadline")
        );
    }

    #[test]
    fn deployment_incomplete_rollout_is_degraded() {
        // ready == desired, but only 1 of 3 replicas updated to the new revision.
        let d = deployment_rollout(
            3,
            3,
            1,
            1,
            1,
            json!([{ "type": "Available", "status": "True" }]),
        );
        assert_eq!(deployment_health(&d).status, ComponentHealth::Degraded);
    }

    #[test]
    fn deployment_stale_generation_is_degraded() {
        // Counts look healthy, but the controller has not observed the latest spec.
        let d = deployment_rollout(
            2,
            2,
            2,
            5,
            4,
            json!([{ "type": "Available", "status": "True" }]),
        );
        assert_eq!(deployment_health(&d).status, ComponentHealth::Degraded);
    }

    #[test]
    fn deployment_fully_rolled_out_is_healthy() {
        let d = deployment_rollout(
            2,
            2,
            2,
            3,
            3,
            json!([
                { "type": "Available", "status": "True" },
                { "type": "Progressing", "status": "True" },
            ]),
        );
        assert_eq!(deployment_health(&d).status, ComponentHealth::Healthy);
    }

    #[test]
    fn deployment_health_covers_the_lattice() {
        assert_eq!(
            deployment_health(&deployment(2, Some(2), Some("True"))).status,
            ComponentHealth::Healthy
        );
        assert_eq!(
            deployment_health(&deployment(2, Some(1), None)).status,
            ComponentHealth::Degraded
        );
        assert_eq!(
            deployment_health(&deployment(2, Some(0), None)).status,
            ComponentHealth::Unavailable
        );
        assert_eq!(
            deployment_health(&deployment(2, None, None)).status,
            ComponentHealth::Unavailable
        );
        // Scaled to zero is not a failure — it is unknown intent.
        assert_eq!(
            deployment_health(&deployment(0, Some(0), None)).status,
            ComponentHealth::Unknown
        );
        // At scale but Available=False is degraded, not healthy.
        assert_eq!(
            deployment_health(&deployment(2, Some(2), Some("False"))).status,
            ComponentHealth::Degraded
        );
    }

    #[test]
    fn daemonset_health_covers_the_lattice() {
        assert_eq!(
            daemonset_health(&daemonset(5, 5)).status,
            ComponentHealth::Healthy
        );
        assert_eq!(
            daemonset_health(&daemonset(5, 3)).status,
            ComponentHealth::Degraded
        );
        assert_eq!(
            daemonset_health(&daemonset(5, 0)).status,
            ComponentHealth::Unavailable
        );
        // No eligible nodes ⇒ zero desired ⇒ unknown, not unavailable.
        assert_eq!(
            daemonset_health(&daemonset(0, 0)).status,
            ComponentHealth::Unknown
        );
    }

    #[test]
    fn zero_matched_objects_is_missing() {
        let aggregate = combine(Vec::new());
        assert_eq!(aggregate.status, ComponentHealth::Missing);
        assert_eq!(aggregate.desired, 0);
        assert_eq!(aggregate.ready, 0);
    }

    #[test]
    fn combine_takes_the_worst_status_and_sums_counts() {
        let healths = vec![
            Health {
                status: ComponentHealth::Healthy,
                desired: 2,
                ready: 2,
                updated: Some(2),
                message: None,
            },
            Health {
                status: ComponentHealth::Unavailable,
                desired: 1,
                ready: 0,
                updated: Some(0),
                message: Some("0/1 ready".to_owned()),
            },
        ];
        let aggregate = combine(healths);
        assert_eq!(aggregate.status, ComponentHealth::Unavailable);
        assert_eq!(aggregate.desired, 3);
        assert_eq!(aggregate.ready, 2);
        assert_eq!(aggregate.updated, Some(2));
        assert_eq!(aggregate.message.as_deref(), Some("0/1 ready"));
    }

    #[test]
    fn selector_matches_requires_every_entry() {
        let mut labels = BTreeMap::new();
        labels.insert(
            "control-plane".to_owned(),
            "ome-controller-manager".to_owned(),
        );
        labels.insert("extra".to_owned(), "value".to_owned());

        let mut selector = BTreeMap::new();
        selector.insert(
            "control-plane".to_owned(),
            "ome-controller-manager".to_owned(),
        );
        assert!(matches(Some(&labels), &selector));

        selector.insert("missing".to_owned(), "x".to_owned());
        assert!(!matches(Some(&labels), &selector));
        assert!(!matches(None, &selector));
    }

    #[test]
    fn deployment_health_prefers_the_condition_message() {
        let health = deployment_health(&deployment(2, Some(0), Some("False")));
        assert_eq!(health.status, ComponentHealth::Unavailable);
        assert_eq!(health.message.as_deref(), Some("deployment message"));
    }
}
