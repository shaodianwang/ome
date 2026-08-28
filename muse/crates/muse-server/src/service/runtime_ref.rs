//! Runtime-identity resolution for InferenceServices: which runtime object a
//! service actually uses, resolved the way the operator resolves it — from the
//! spec-declared name, or, for auto-selected runtimes, from the runtime label
//! on the service's own pods.

use std::collections::BTreeMap;

use kube::Resource;
use ome_client::{
    k8s_openapi::api::core::v1::Pod, listers::Lister, ClusterServingRuntime, InferenceService,
    ServingRuntime,
};

use crate::protocol::{owned_by, ResolvedRuntimeRefView};

/// Pod label carrying the resolved runtime's name. For an auto-selected runtime
/// (no `spec.runtime`) this label is the only record of the operator's choice —
/// nothing on the InferenceService itself names it.
const SERVING_RUNTIME_POD_LABEL: &str = "serving-runtime";

/// The runtime name an InferenceService uses, with where it came from: the spec
/// when declared there (`"spec"`), else the runtime label on the service's
/// cached pods (`"pods"`, when a pod lister is supplied). Disagreeing pod labels
/// are settled by [`pick_pod_runtime_name`], with the runtime caches saying
/// which labels still name a real runtime.
fn runtime_name(
    isvc: &InferenceService,
    runtimes: &Lister<ServingRuntime>,
    cluster_runtimes: &Lister<ClusterServingRuntime>,
    pods: Option<&Lister<Pod>>,
) -> Option<(String, &'static str)> {
    let declared = isvc
        .spec
        .runtime
        .as_ref()
        .map(|r| r.name.clone())
        .filter(|n| !n.is_empty());
    if let Some(name) = declared {
        return Some((name, "spec"));
    }
    let namespace = isvc.meta().namespace.as_deref()?;
    let isvc_name = isvc.meta().name.as_deref()?;
    let labels: Vec<String> = pods?
        .list_namespaced(namespace)
        .iter()
        .filter(|p| owned_by(p, isvc_name))
        .filter_map(|p| {
            p.meta()
                .labels
                .as_ref()?
                .get(SERVING_RUNTIME_POD_LABEL)
                .cloned()
                .filter(|n| !n.is_empty())
        })
        .collect();
    pick_pod_runtime_name(labels, |name| {
        runtimes.get_namespaced(namespace, name).is_some() || cluster_runtimes.get(name).is_some()
    })
    .map(|name| (name, "pods"))
}

/// Pick one runtime name from the runtime labels on an InferenceService's pods.
///
/// Mid-rollout the service's old and new pods carry different runtime labels,
/// and the pod cache iterates in no particular order — so the pick must depend
/// only on the label values themselves, never on iteration order, or the
/// reported runtime would flip between refreshes. A label that resolves in a
/// runtime cache beats one that does not (a stale label often names a runtime
/// that no longer exists); after that the label on the most pods wins, and an
/// exact tie goes to the lexicographically smallest label.
fn pick_pod_runtime_name(labels: Vec<String>, resolves: impl Fn(&str) -> bool) -> Option<String> {
    let mut count_by_name: BTreeMap<String, usize> = BTreeMap::new();
    for label in labels {
        *count_by_name.entry(label).or_insert(0) += 1;
    }
    count_by_name
        .into_iter()
        .max_by(|(a, a_count), (b, b_count)| {
            (resolves(a), a_count)
                .cmp(&(resolves(b), b_count))
                // Reversed so that under `max` the smaller name wins a tie.
                .then_with(|| b.cmp(a))
        })
        .map(|(name, _)| name)
}

/// Resolve which runtime object an InferenceService's runtime name denotes, the
/// way the operator does: a namespaced ServingRuntime wins over a
/// ClusterServingRuntime with the same name, and the kind declared on the spec is
/// never consulted (the operator ignores it too). `None` when the name matches
/// neither cache — the spec-declared `runtime`/`runtimeRef` fields still carry
/// the declaration for such rows.
pub(crate) fn resolved_runtime_ref(
    isvc: &InferenceService,
    runtimes: &Lister<ServingRuntime>,
    cluster_runtimes: &Lister<ClusterServingRuntime>,
    pods: Option<&Lister<Pod>>,
) -> Option<ResolvedRuntimeRefView> {
    let (name, source) = runtime_name(isvc, runtimes, cluster_runtimes, pods)?;
    if let Some(ns) = isvc.meta().namespace.as_deref() {
        if runtimes.get_namespaced(ns, &name).is_some() {
            return Some(ResolvedRuntimeRefView {
                kind: "ServingRuntime",
                name,
                namespace: Some(ns.to_owned()),
                source,
            });
        }
    }
    cluster_runtimes
        .get(&name)
        .is_some()
        .then_some(ResolvedRuntimeRefView {
            kind: "ClusterServingRuntime",
            name,
            namespace: None,
            source,
        })
}

#[cfg(test)]
mod tests {
    use kube::runtime::{reflector, reflector::Lookup, watcher};
    use serde_json::json;

    use super::*;
    use crate::protocol::INFERENCE_SERVICE_POD_LABEL;

    fn lister_of<K>(objects: Vec<K>) -> Lister<K>
    where
        K: Lookup<DynamicType = ()> + Resource<DynamicType = ()> + Clone + 'static,
    {
        let (store, mut writer) = reflector::store::<K>();
        for object in objects {
            writer.apply_watcher_event(&watcher::Event::Apply(object));
        }
        Lister::new(store)
    }

    /// An ISVC whose spec declares `runtime` (with a deliberately wrong kind, to
    /// prove the declared kind is never consulted), or omits it entirely.
    fn isvc_with_runtime(namespace: &str, name: &str, runtime: Option<&str>) -> InferenceService {
        let spec = match runtime {
            Some(r) => json!({ "runtime": { "name": r, "kind": "ClusterServingRuntime" } }),
            None => json!({}),
        };
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "InferenceService",
            "metadata": { "name": name, "namespace": namespace },
            "spec": spec
        }))
        .expect("InferenceService fixture should deserialize")
    }

    fn serving_runtime(namespace: &str, name: &str) -> ServingRuntime {
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "ServingRuntime",
            "metadata": { "name": name, "namespace": namespace },
            "spec": {}
        }))
        .expect("ServingRuntime fixture should deserialize")
    }

    fn cluster_serving_runtime(name: &str) -> ClusterServingRuntime {
        serde_json::from_value(json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "ClusterServingRuntime",
            "metadata": { "name": name },
            "spec": {}
        }))
        .expect("ClusterServingRuntime fixture should deserialize")
    }

    /// A serving pod as the operator labels it: owner ISVC + resolved runtime.
    fn serving_pod(namespace: &str, name: &str, isvc: &str, runtime: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    INFERENCE_SERVICE_POD_LABEL: isvc,
                    SERVING_RUNTIME_POD_LABEL: runtime,
                }
            }
        }))
        .expect("Pod fixture should deserialize")
    }

    #[test]
    fn spec_runtime_name_collision_resolves_to_namespaced_runtime() {
        let runtimes = lister_of(vec![serving_runtime("team-a", "vllm")]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let isvc = isvc_with_runtime("team-a", "svc", Some("vllm"));

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, None)
            .expect("collision should resolve");
        assert_eq!(resolved.kind, "ServingRuntime");
        assert_eq!(resolved.name, "vllm");
        assert_eq!(resolved.namespace.as_deref(), Some("team-a"));
        assert_eq!(resolved.source, "spec");
    }

    #[test]
    fn spec_runtime_name_without_namespaced_match_resolves_to_cluster_runtime() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let isvc = isvc_with_runtime("team-a", "svc", Some("vllm"));

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, None)
            .expect("cluster-scoped runtime should resolve");
        assert_eq!(resolved.kind, "ClusterServingRuntime");
        assert!(resolved.namespace.is_none());
        assert_eq!(resolved.source, "spec");
    }

    #[test]
    fn auto_selected_runtime_resolves_from_the_owned_pod_labels() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm")]);
        let pods = lister_of(vec![
            // Another service's pod in the same namespace must not win.
            serving_pod("team-a", "other-0", "other", "sglang"),
            serving_pod("team-a", "svc-0", "svc", "vllm"),
        ]);
        let isvc = isvc_with_runtime("team-a", "svc", None);

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, Some(&pods))
            .expect("pod-labelled runtime should resolve");
        assert_eq!(resolved.kind, "ClusterServingRuntime");
        assert_eq!(resolved.name, "vllm");
        assert_eq!(resolved.source, "pods");
    }

    #[test]
    fn disagreeing_pod_runtime_labels_pick_deterministically() {
        let cached = |name: &str| name == "new-rt" || name == "old-rt";

        // A label that resolves in a runtime cache beats a stale one that does
        // not, regardless of how many pods still carry the stale label.
        assert_eq!(
            pick_pod_runtime_name(
                vec!["gone-rt".into(), "gone-rt".into(), "new-rt".into()],
                cached
            ),
            Some("new-rt".into())
        );

        // Among labels that both resolve, the one on the most pods wins.
        assert_eq!(
            pick_pod_runtime_name(
                vec!["old-rt".into(), "new-rt".into(), "new-rt".into()],
                cached
            ),
            Some("new-rt".into())
        );

        // An exact tie falls back to the lexicographically smallest label, no
        // matter the order the labels were collected in.
        assert_eq!(
            pick_pod_runtime_name(vec!["old-rt".into(), "new-rt".into()], cached),
            Some("new-rt".into())
        );
        assert_eq!(
            pick_pod_runtime_name(vec!["new-rt".into(), "old-rt".into()], cached),
            Some("new-rt".into())
        );

        assert_eq!(pick_pod_runtime_name(vec![], cached), None);
    }

    #[test]
    fn mid_rollout_pod_disagreement_resolves_to_the_cached_runtime() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of(vec![cluster_serving_runtime("vllm-next")]);
        // Two old pods still carry the label of a runtime that no longer
        // exists; one new pod names the cached one. The cached name must win.
        let pods = lister_of(vec![
            serving_pod("team-a", "svc-0", "svc", "vllm-old"),
            serving_pod("team-a", "svc-1", "svc", "vllm-old"),
            serving_pod("team-a", "svc-2", "svc", "vllm-next"),
        ]);
        let isvc = isvc_with_runtime("team-a", "svc", None);

        let resolved = resolved_runtime_ref(&isvc, &runtimes, &cluster_runtimes, Some(&pods))
            .expect("the cached runtime should resolve");
        assert_eq!(resolved.name, "vllm-next");
        assert_eq!(resolved.source, "pods");
    }

    #[test]
    fn unresolvable_runtime_identity_is_absent() {
        let runtimes = lister_of::<ServingRuntime>(vec![]);
        let cluster_runtimes = lister_of::<ClusterServingRuntime>(vec![]);

        // No spec.runtime and no pods to read a label from.
        let auto = isvc_with_runtime("team-a", "svc", None);
        let empty_pods = lister_of::<Pod>(vec![]);
        assert!(
            resolved_runtime_ref(&auto, &runtimes, &cluster_runtimes, Some(&empty_pods)).is_none()
        );

        // A declared name matching neither runtime cache.
        let named = isvc_with_runtime("team-a", "svc", Some("missing"));
        assert!(resolved_runtime_ref(&named, &runtimes, &cluster_runtimes, None).is_none());
    }
}
