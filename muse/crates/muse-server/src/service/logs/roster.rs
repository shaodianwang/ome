//! Roster selection and diffing: which pods — and which of their containers —
//! one session tails, recomputed from the cached pod lister on every resync.

use std::{collections::BTreeSet, sync::Arc};

use ome_client::k8s_openapi::api::core::v1::Pod;
use serde::Serialize;

/// The serving operator stamps this label (valued with the InferenceService
/// name) on every pod it creates — single-pod deployments and multi-pod
/// leader/worker instances alike — so one selector covers every deployment
/// mode. The key mirrors the operator's Go constants.
pub(super) const ISVC_LABEL: &str = "ome.io/inferenceservice";

/// Component label the operator stamps next to [`ISVC_LABEL`] (engine /
/// decoder / router). Note the bare key: it is not `ome.io/component`.
pub(super) const COMPONENT_LABEL: &str = "component";

/// One tailed pod as reported to the client in `pods` roster snapshots.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RosterPod {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    pub phase: String,
    pub containers: Vec<String>,
}

/// The capped roster: the retained pods plus how many matched in total.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RosterSelection {
    pub pods: Vec<RosterPod>,
    pub total: usize,
    pub capped: bool,
}

/// The caller's narrowing of the roster; namespace and service are required.
pub(super) struct RosterFilter<'a> {
    pub namespace: &'a str,
    pub isvc: &'a str,
    pub component: Option<&'a str>,
    pub pod: Option<&'a str>,
}

/// Select the pods one session tails: filter the cached pods by namespace,
/// the InferenceService selector label, and the caller's narrowing; skip
/// terminal phases (no live stream to follow); sort component-then-name so a
/// cap cuts deterministically; and cap at `max_pods`.
pub(super) fn select(
    pods: &[Arc<Pod>],
    filter: &RosterFilter<'_>,
    max_pods: usize,
) -> RosterSelection {
    let mut matched: Vec<RosterPod> = pods.iter().filter_map(|pod| project(pod, filter)).collect();
    matched.sort_by(|a, b| {
        a.component
            .cmp(&b.component)
            .then_with(|| a.name.cmp(&b.name))
    });
    let total = matched.len();
    let capped = total > max_pods;
    matched.truncate(max_pods);
    RosterSelection {
        pods: matched,
        total,
        capped,
    }
}

fn project(pod: &Pod, filter: &RosterFilter<'_>) -> Option<RosterPod> {
    let meta = &pod.metadata;
    if meta.namespace.as_deref() != Some(filter.namespace) {
        return None;
    }
    let name = meta.name.clone()?;
    if let Some(exact) = filter.pod {
        if name != exact {
            return None;
        }
    }
    let labels = meta.labels.as_ref()?;
    if labels.get(ISVC_LABEL).map(String::as_str) != Some(filter.isvc) {
        return None;
    }
    let component = labels.get(COMPONENT_LABEL).cloned();
    if let Some(want) = filter.component {
        if component.as_deref() != Some(want) {
            return None;
        }
    }
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.clone())
        .unwrap_or_default();
    // Terminal pods keep their cache entry (phase survives the prune) but have
    // no stream to follow.
    if matches!(phase.as_str(), "Succeeded" | "Failed") {
        return None;
    }
    let containers = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers
                .iter()
                .map(|container| container.name.clone())
                .collect()
        })
        .unwrap_or_default();
    Some(RosterPod {
        name,
        component,
        phase,
        containers,
    })
}

/// One follow stream: a (pod, container) pair.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct TailKey {
    pub pod: String,
    pub container: String,
}

/// The streams a roster implies: every container of every retained pod — or,
/// with a container narrowing, only pods that actually have that container.
pub(super) fn tail_keys(selection: &RosterSelection, container: Option<&str>) -> BTreeSet<TailKey> {
    selection
        .pods
        .iter()
        .flat_map(|pod| {
            pod.containers
                .iter()
                .filter(|name| match container {
                    Some(want) => want == name.as_str(),
                    None => true,
                })
                .map(|name| TailKey {
                    pod: pod.name.clone(),
                    container: name.clone(),
                })
        })
        .collect()
}

/// The tail starts and stops that converge `active` onto `desired`.
pub(super) fn diff(
    active: &BTreeSet<TailKey>,
    desired: &BTreeSet<TailKey>,
) -> (Vec<TailKey>, Vec<TailKey>) {
    let start = desired.difference(active).cloned().collect();
    let stop = active.difference(desired).cloned().collect();
    (start, stop)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use ome_client::k8s_openapi::api::core::v1::Pod;
    use serde_json::json;

    use super::{diff, select, tail_keys, RosterFilter, TailKey};

    fn pod(
        name: &str,
        namespace: &str,
        isvc: Option<&str>,
        component: Option<&str>,
        phase: &str,
        containers: &[&str],
    ) -> Arc<Pod> {
        let mut labels = serde_json::Map::new();
        if let Some(isvc) = isvc {
            labels.insert("ome.io/inferenceservice".to_owned(), json!(isvc));
        }
        if let Some(component) = component {
            labels.insert("component".to_owned(), json!(component));
        }
        Arc::new(
            serde_json::from_value(json!({
                "metadata": { "name": name, "namespace": namespace, "labels": labels },
                "spec": { "containers": containers.iter().map(|c| json!({"name": c})).collect::<Vec<_>>() },
                "status": { "phase": phase }
            }))
            .expect("minimal Pod should deserialize"),
        )
    }

    fn fleet() -> Vec<Arc<Pod>> {
        vec![
            pod(
                "llama-engine-1",
                "team-a",
                Some("llama"),
                Some("engine"),
                "Running",
                &["main"],
            ),
            pod(
                "llama-engine-0",
                "team-a",
                Some("llama"),
                Some("engine"),
                "Running",
                &["main", "sidecar"],
            ),
            pod(
                "llama-decoder-0",
                "team-a",
                Some("llama"),
                Some("decoder"),
                "Pending",
                &["main"],
            ),
            pod(
                "llama-done-0",
                "team-a",
                Some("llama"),
                Some("engine"),
                "Succeeded",
                &["main"],
            ),
            pod(
                "llama-failed-0",
                "team-a",
                Some("llama"),
                Some("engine"),
                "Failed",
                &["main"],
            ),
            pod(
                "other-engine-0",
                "team-a",
                Some("other"),
                Some("engine"),
                "Running",
                &["main"],
            ),
            pod(
                "llama-engine-b",
                "team-b",
                Some("llama"),
                Some("engine"),
                "Running",
                &["main"],
            ),
            pod("unlabeled-0", "team-a", None, None, "Running", &["main"]),
        ]
    }

    fn filter<'a>(component: Option<&'a str>, pod: Option<&'a str>) -> RosterFilter<'a> {
        RosterFilter {
            namespace: "team-a",
            isvc: "llama",
            component,
            pod,
        }
    }

    #[test]
    fn roster_matches_the_service_label_within_the_namespace_and_skips_terminal_pods() {
        let selection = select(&fleet(), &filter(None, None), 10);
        let names: Vec<&str> = selection.pods.iter().map(|p| p.name.as_str()).collect();
        // Sorted component → name; terminal, foreign, unlabeled, and
        // other-namespace pods are excluded.
        assert_eq!(
            names,
            ["llama-decoder-0", "llama-engine-0", "llama-engine-1"]
        );
        assert_eq!(selection.total, 3);
        assert!(!selection.capped);
        assert_eq!(selection.pods[0].phase, "Pending");
        assert_eq!(selection.pods[1].containers, ["main", "sidecar"]);
    }

    #[test]
    fn roster_narrows_by_component_and_exact_pod() {
        let engines = select(&fleet(), &filter(Some("engine"), None), 10);
        assert_eq!(engines.pods.len(), 2);
        assert!(engines
            .pods
            .iter()
            .all(|p| p.component.as_deref() == Some("engine")));

        let one = select(&fleet(), &filter(None, Some("llama-engine-1")), 10);
        assert_eq!(one.pods.len(), 1);
        assert_eq!(one.pods[0].name, "llama-engine-1");
    }

    #[test]
    fn roster_caps_deterministically_and_reports_the_full_total() {
        let selection = select(&fleet(), &filter(None, None), 2);
        let names: Vec<&str> = selection.pods.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["llama-decoder-0", "llama-engine-0"]);
        assert_eq!(selection.total, 3);
        assert!(selection.capped);
    }

    #[test]
    fn tail_keys_cover_every_container_unless_narrowed() {
        let selection = select(&fleet(), &filter(Some("engine"), None), 10);
        let all = tail_keys(&selection, None);
        assert_eq!(all.len(), 3); // engine-0 main+sidecar, engine-1 main

        let narrowed = tail_keys(&selection, Some("sidecar"));
        let keys: Vec<&TailKey> = narrowed.iter().collect();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].pod, "llama-engine-0");
        assert_eq!(keys[0].container, "sidecar");
    }

    #[test]
    fn diff_yields_the_starts_and_stops_that_converge_the_tails() {
        let key = |pod: &str| TailKey {
            pod: pod.to_owned(),
            container: "main".to_owned(),
        };
        let active: BTreeSet<TailKey> = [key("a"), key("b")].into();
        let desired: BTreeSet<TailKey> = [key("b"), key("c")].into();

        let (start, stop) = diff(&active, &desired);
        assert_eq!(start, [key("c")]);
        assert_eq!(stop, [key("a")]);

        let (none_start, none_stop) = diff(&desired, &desired);
        assert!(none_start.is_empty());
        assert!(none_stop.is_empty());
    }
}
