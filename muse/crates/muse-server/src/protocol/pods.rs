//! Pod wire types: the operator's pod labels, the cached-pod projections for
//! the deployment diagnosis view, and the live-fetched pod inspection views.

use ome_client::k8s_openapi::api::core::v1::{
    ContainerStateTerminated, ContainerStatus, Event, Pod,
};
use serde::Serialize;

use super::{age, since, sum_accel, ConditionView};

/// Pod label carrying the owning InferenceService's name; the operator stamps it
/// on every serving pod it creates.
pub(crate) const INFERENCE_SERVICE_POD_LABEL: &str = "ome.io/inferenceservice";

/// Pod label carrying the serving component (engine / decoder / router).
pub(crate) const COMPONENT_POD_LABEL: &str = "component";

/// The owning InferenceService's name from a pod's ownership label, when present.
pub(crate) fn isvc_owner(pod: &Pod) -> Option<&str> {
    pod.metadata
        .labels
        .as_ref()?
        .get(INFERENCE_SERVICE_POD_LABEL)
        .map(String::as_str)
}

/// Whether a pod is owned by the named InferenceService.
pub(crate) fn owned_by(pod: &Pod, name: &str) -> bool {
    isvc_owner(pod) == Some(name)
}

/// The serving component from a pod's component label, when present.
pub(crate) fn pod_component(pod: &Pod) -> Option<&str> {
    pod.metadata
        .labels
        .as_ref()?
        .get(COMPONENT_POD_LABEL)
        .map(String::as_str)
}

/// Accelerator chips (GPU + TPU) requested by a pod's containers.
pub(crate) fn pod_accelerators(pod: &Pod) -> i64 {
    pod.spec
        .as_ref()
        .map(|s| {
            s.containers
                .iter()
                .filter_map(|c| c.resources.as_ref())
                .filter_map(|r| r.requests.as_ref())
                .map(|r| sum_accel(r, "gpu") + sum_accel(r, "tpu"))
                .sum()
        })
        .unwrap_or(0)
}

/// Why a pod is stuck, derived from its cached status: the scheduling failure
/// (`Unschedulable`), a container's waiting/terminated reason
/// (`ImagePullBackOff`, `CrashLoopBackOff`, …), or the pod-level not-Ready
/// reason (`ContainersNotReady`). `None` for a healthy pod.
pub(crate) fn pod_problem_reason(pod: &Pod) -> Option<String> {
    let status = pod.status.as_ref()?;
    let condition_reason = |cond_type: &str| {
        status
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.type_ == cond_type && c.status == "False")
            .and_then(|c| c.reason.clone())
            .filter(|r| !r.is_empty())
    };
    if let Some(reason) = condition_reason("PodScheduled") {
        return Some(reason);
    }
    for cs in status.container_statuses.iter().flatten() {
        let Some(state) = cs.state.as_ref() else {
            continue;
        };
        if let Some(reason) = state
            .waiting
            .as_ref()
            .and_then(|w| w.reason.clone())
            .filter(|r| !r.is_empty())
        {
            return Some(reason);
        }
        if let Some(terminated) = state.terminated.as_ref() {
            if terminated.exit_code != 0 {
                return Some(
                    terminated
                        .reason
                        .clone()
                        .filter(|r| !r.is_empty())
                        .unwrap_or_else(|| "Error".to_owned()),
                );
            }
        }
    }
    condition_reason("Ready")
}

/// Why a terminated container died: its reason (`OOMKilled`) or exit code.
fn termination_label(terminated: &ContainerStateTerminated) -> String {
    terminated
        .reason
        .clone()
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| format!("exit {}", terminated.exit_code))
}

/// One container of an InferenceService pod, for the expanded row: its own
/// readiness, restart count, current state, and why its last run ended.
#[derive(Serialize)]
pub struct IsvcPodContainerView {
    pub name: String,
    pub ready: bool,
    pub restarts: i32,
    /// Current state: `running` / `waiting` / `terminated`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<&'static str>,
    /// Current waiting/terminated reason (`CrashLoopBackOff`, …).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Why the previous run ended (`OOMKilled`, `exit 137`) — the answer to
    /// "it restarted, but what was wrong?".
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastRestart")]
    pub last_restart: Option<String>,
}

/// One pod of an InferenceService, for the deployment diagnosis view: identity,
/// health, and — when stuck or restarting — why.
#[derive(Serialize)]
pub struct IsvcPodView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    pub ready: bool,
    pub restarts: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Why the most-restarted container's previous run ended.
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastRestart")]
    pub last_restart: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accelerators: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<IsvcPodContainerView>,
}

impl IsvcPodView {
    pub fn of(pod: &Pod) -> Self {
        let status = pod.status.as_ref();
        let ready = status
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conds| conds.iter().find(|c| c.type_ == "Ready"))
            .is_some_and(|c| c.status == "True");
        let containers: Vec<IsvcPodContainerView> = status
            .and_then(|s| s.container_statuses.as_ref())
            .map(|statuses| {
                statuses
                    .iter()
                    .map(|cs| {
                        let state = cs.state.as_ref();
                        let (state_label, reason) = match state {
                            Some(s) if s.waiting.is_some() => (
                                Some("waiting"),
                                s.waiting.as_ref().and_then(|w| w.reason.clone()),
                            ),
                            Some(s) if s.terminated.is_some() => (
                                Some("terminated"),
                                s.terminated.as_ref().map(termination_label),
                            ),
                            Some(s) if s.running.is_some() => (Some("running"), None),
                            _ => (None, None),
                        };
                        IsvcPodContainerView {
                            name: cs.name.clone(),
                            ready: cs.ready,
                            restarts: cs.restart_count,
                            state: state_label,
                            reason: reason.filter(|r| !r.is_empty()),
                            last_restart: cs
                                .last_state
                                .as_ref()
                                .and_then(|s| s.terminated.as_ref())
                                .map(termination_label),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let restarts = containers.iter().map(|c| c.restarts).sum();
        let last_restart = containers
            .iter()
            .filter(|c| c.last_restart.is_some())
            .max_by_key(|c| c.restarts)
            .and_then(|c| c.last_restart.clone());
        let accel = pod_accelerators(pod);
        Self {
            name: pod.metadata.name.clone().unwrap_or_default(),
            component: pod_component(pod).map(str::to_owned),
            phase: status.and_then(|s| s.phase.clone()),
            ready,
            restarts,
            reason: pod_problem_reason(pod),
            last_restart,
            node: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
            accelerators: (accel > 0).then_some(accel),
            age: age(&pod.metadata),
            containers,
        }
    }

    /// Whether this pod needs operator attention — used to sort problems first.
    pub fn problematic(&self) -> bool {
        !self.ready || self.reason.is_some() || self.restarts > 0
    }
}

/// `GET /v1/inferenceservices/{name}/pods` — the service's cached pods,
/// problems first.
#[derive(Serialize)]
pub struct IsvcPodsView {
    pub namespace: String,
    pub name: String,
    pub count: usize,
    pub pods: Vec<IsvcPodView>,
}

/// One Kubernetes Event about a pod, for the inspection drill-down: the "why"
/// narrative (BackOff, probe failures, kills) the status fields can't carry.
#[derive(Serialize)]
pub struct PodEventView {
    /// `Normal` or `Warning`.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Occurrences of this event (kubelet deduplicates repeats).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,
    /// Humanized time since the last occurrence.
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastSeen")]
    pub last_seen: Option<String>,
}

impl PodEventView {
    pub fn of(event: &Event) -> Self {
        Self {
            type_: event.type_.clone(),
            reason: event.reason.clone(),
            message: event.message.clone().filter(|m| !m.is_empty()),
            count: event.count,
            last_seen: event
                .last_timestamp
                .as_ref()
                .or(event.metadata.creation_timestamp.as_ref())
                .and_then(since),
        }
    }
}

/// One container of a live-fetched pod: current state with its full message,
/// and the previous run's exit (what/when/why) — the restart post-mortem.
#[derive(Serialize)]
pub struct PodInspectContainerView {
    pub name: String,
    pub ready: bool,
    pub restarts: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The current state's message, verbatim (live fetch — not the trimmed cache).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// How long the current run has been up, when running.
    #[serde(skip_serializing_if = "Option::is_none", rename = "runningFor")]
    pub running_for: Option<String>,
    /// Previous run's exit: `OOMKilled (exit 137)` / `exit 1`.
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastExit")]
    pub last_exit: Option<String>,
    /// Humanized time since the previous run ended.
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastExitAt")]
    pub last_exit_at: Option<String>,
    /// Previous run's termination message, verbatim.
    #[serde(skip_serializing_if = "Option::is_none", rename = "lastExitMessage")]
    pub last_exit_message: Option<String>,
}

/// `GET /v1/inferenceservices/{name}/pods/{pod}` — a live, per-click apiserver
/// read of one pod: full condition/state messages plus its recent Events.
#[derive(Serialize)]
pub struct PodInspectView {
    pub name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<PodInspectContainerView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<PodEventView>,
}

impl PodInspectView {
    pub fn of(pod: &Pod, events: &[Event]) -> Self {
        let status = pod.status.as_ref();
        let conditions = status
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .map(|c| {
                        ConditionView::new(&c.type_, &c.status, c.reason.clone(), c.message.clone())
                    })
                    .collect()
            })
            .unwrap_or_default();
        let containers = status
            .and_then(|s| s.container_statuses.as_ref())
            .map(|statuses| {
                statuses
                    .iter()
                    .map(|cs| {
                        let none = (None, None, None, None);
                        let (label, reason, message, running_for) = match cs.state.as_ref() {
                            Some(s) => {
                                if let Some(w) = s.waiting.as_ref() {
                                    (Some("waiting"), w.reason.clone(), w.message.clone(), None)
                                } else if let Some(t) = s.terminated.as_ref() {
                                    (
                                        Some("terminated"),
                                        Some(termination_label(t)),
                                        t.message.clone(),
                                        None,
                                    )
                                } else if let Some(r) = s.running.as_ref() {
                                    (
                                        Some("running"),
                                        None,
                                        None,
                                        r.started_at.as_ref().and_then(since),
                                    )
                                } else {
                                    none
                                }
                            }
                            None => none,
                        };
                        let last = cs.last_state.as_ref().and_then(|s| s.terminated.as_ref());
                        Self::container_view(cs, label, reason, message, running_for, last)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            name: pod.metadata.name.clone().unwrap_or_default(),
            conditions,
            containers,
            events: events.iter().map(PodEventView::of).collect(),
        }
    }

    fn container_view(
        cs: &ContainerStatus,
        state: Option<&'static str>,
        reason: Option<String>,
        message: Option<String>,
        running_for: Option<String>,
        last: Option<&ContainerStateTerminated>,
    ) -> PodInspectContainerView {
        PodInspectContainerView {
            name: cs.name.clone(),
            ready: cs.ready,
            restarts: cs.restart_count,
            state,
            reason: reason.filter(|r| !r.is_empty()),
            message: message.filter(|m| !m.is_empty()),
            running_for,
            last_exit: last.map(|t| {
                let label = termination_label(t);
                if t.reason.as_deref().is_some_and(|r| !r.is_empty()) {
                    format!("{label} (exit {})", t.exit_code)
                } else {
                    label
                }
            }),
            last_exit_at: last.and_then(|t| t.finished_at.as_ref()).and_then(since),
            last_exit_message: last
                .and_then(|t| t.message.clone())
                .filter(|m| !m.is_empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    /// Pod fixture from bare JSON — the shapes the kubelet actually reports.
    fn pod_from(value: Value) -> Pod {
        serde_json::from_value(value).expect("pod fixture should deserialize")
    }

    #[test]
    fn pod_problem_reason_prefers_scheduling_then_container_then_readiness() {
        let unschedulable = pod_from(serde_json::json!({
            "metadata": { "name": "p" },
            "status": {
                "phase": "Pending",
                "conditions": [
                    { "type": "PodScheduled", "status": "False", "reason": "Unschedulable" }
                ]
            }
        }));
        let view = IsvcPodView::of(&unschedulable);
        assert_eq!(view.reason.as_deref(), Some("Unschedulable"));
        assert!(view.problematic());

        let image_pull = pod_from(serde_json::json!({
            "metadata": { "name": "p" },
            "status": {
                "phase": "Pending",
                "conditions": [
                    { "type": "PodScheduled", "status": "True" },
                    { "type": "Ready", "status": "False", "reason": "ContainersNotReady" }
                ],
                "containerStatuses": [{
                    "name": "worker", "ready": false, "restartCount": 3,
                    "image": "img", "imageID": "",
                    "state": { "waiting": { "reason": "ImagePullBackOff" } }
                }]
            }
        }));
        let view = IsvcPodView::of(&image_pull);
        assert_eq!(view.reason.as_deref(), Some("ImagePullBackOff"));
        assert_eq!(view.restarts, 3);

        let crashed = pod_from(serde_json::json!({
            "metadata": { "name": "p" },
            "status": {
                "phase": "Running",
                "containerStatuses": [{
                    "name": "worker", "ready": false, "restartCount": 1,
                    "image": "img", "imageID": "",
                    "state": { "terminated": { "exitCode": 137 } }
                }]
            }
        }));
        assert_eq!(IsvcPodView::of(&crashed).reason.as_deref(), Some("Error"));

        let not_ready = pod_from(serde_json::json!({
            "metadata": { "name": "p" },
            "status": {
                "phase": "Running",
                "conditions": [
                    { "type": "Ready", "status": "False", "reason": "ContainersNotReady" }
                ]
            }
        }));
        assert_eq!(
            IsvcPodView::of(&not_ready).reason.as_deref(),
            Some("ContainersNotReady")
        );
    }

    #[test]
    fn restarted_pod_carries_why_the_previous_run_ended() {
        let restarted = pod_from(serde_json::json!({
            "metadata": { "name": "p" },
            "status": {
                "phase": "Running",
                "conditions": [{ "type": "Ready", "status": "True" }],
                "containerStatuses": [{
                    "name": "worker", "ready": true, "restartCount": 1,
                    "image": "img", "imageID": "",
                    "state": { "running": {} },
                    "lastState": { "terminated": { "exitCode": 137, "reason": "OOMKilled" } }
                }]
            }
        }));
        let view = IsvcPodView::of(&restarted);
        assert!(view.ready);
        assert!(view.reason.is_none());
        assert_eq!(view.last_restart.as_deref(), Some("OOMKilled"));
        // A restart with no reason string falls back to the exit code.
        assert_eq!(view.containers.len(), 1);
        assert_eq!(view.containers[0].state, Some("running"));
        // Restarts alone mark the pod as needing a look.
        assert!(view.problematic());

        let unexplained = pod_from(serde_json::json!({
            "metadata": { "name": "p" },
            "status": {
                "containerStatuses": [{
                    "name": "worker", "ready": false, "restartCount": 2,
                    "image": "img", "imageID": "",
                    "lastState": { "terminated": { "exitCode": 1 } }
                }]
            }
        }));
        assert_eq!(
            IsvcPodView::of(&unexplained).last_restart.as_deref(),
            Some("exit 1")
        );
    }

    #[test]
    fn healthy_pod_projects_without_a_reason() {
        let healthy = pod_from(serde_json::json!({
            "metadata": { "name": "p", "labels": { "component": "engine" } },
            "spec": {
                "nodeName": "gpu-0",
                "containers": [{
                    "name": "worker",
                    "resources": { "requests": { "nvidia.com/gpu": "4" } }
                }]
            },
            "status": {
                "phase": "Running",
                "conditions": [{ "type": "Ready", "status": "True" }],
                "containerStatuses": [{
                    "name": "worker", "ready": true, "restartCount": 0,
                    "image": "img", "imageID": "",
                    "state": { "running": { } }
                }]
            }
        }));
        let view = IsvcPodView::of(&healthy);
        assert!(view.ready);
        assert!(view.reason.is_none());
        assert!(!view.problematic());
        assert_eq!(view.component.as_deref(), Some("engine"));
        assert_eq!(view.node.as_deref(), Some("gpu-0"));
        assert_eq!(view.accelerators, Some(4));
    }
}
