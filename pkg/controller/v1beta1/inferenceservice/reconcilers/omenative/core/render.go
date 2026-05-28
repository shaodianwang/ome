package core

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// RenderWithRevisionForISVC is the (ReconcileParams, plan, inst,
// runner, ordinal, revisionHash) shape of workload/ops.RenderWithRevision
// used by the legacy OMENative-on-ISVC dispatch path. Wires the
// coordination.InjectPeerEnv hook so per-Component peer DNS env vars
// land on each rendered pod.
//
// Adapter callers in commit 9 skip this wrapper and pass the hook
// directly via Deps.
func RenderWithRevisionForISVC(
	p ReconcileParams,
	template *corev1.PodSpec,
	plan ComponentPlan,
	inst InstancePlan,
	runner RunnerPlan,
	ordinal int32,
	revisionHash string,
) (*corev1.Pod, error) {
	return ops.RenderWithRevision(
		p.ISVC,
		isvcGVK,
		isvcRenderKey(p.ISVC, plan.Component),
		template,
		&p.ObjectMeta,
		plan,
		inst,
		runner,
		ordinal,
		revisionHash,
		isvcRenderHook(p.ISVC),
	)
}

// isvcRenderKey is the WorkloadKey projection for the legacy ISVC
// dispatch path. After slice 3 of the workload-extraction refactor
// WorkloadKey.Component is workload.ComponentType — the v1beta1
// component the caller already holds gets cast at the boundary (the
// two share the same string underlying so the conversion is free).
func isvcRenderKey(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) workload.WorkloadKey {
	return workload.WorkloadKey{
		Namespace: isvc.Namespace,
		Name:      isvc.Name + "-" + string(component),
		Component: workload.ComponentType(component),
		Labels: map[string]string{
			"ome.io/inferenceservice": isvc.Name,
			"ome.io/component":        string(component),
			"ome.io/managed-by":       "OMENative",
		},
	}
}

// ISVCRenderHook returns the workload.RenderHook the ISVC adapter
// wires onto every Render call: coordination.InjectPeerEnv so
// OME_<PEER>_ENDPOINT vars land on every container when the ISVC
// has a rolloutCoordination block. Returns nil when the ISVC has no
// coordination block (renderer skips the no-op call).
//
// Exposed for callers (e.g. the omenative ops dispatch shim) that
// construct workload.Deps directly rather than going through the
// RenderWithRevisionForISVC wrapper.
func ISVCRenderHook(isvc *v1beta1.InferenceService) workload.RenderHook {
	return isvcRenderHook(isvc)
}

// isvcRenderHook is the unexported implementation kept as the in-package
// entry point so the existing RenderWithRevisionForISVC wiring keeps
// using the unqualified name.
func isvcRenderHook(isvc *v1beta1.InferenceService) workload.RenderHook {
	if isvc == nil || isvc.Spec.RolloutCoordination == nil {
		return nil
	}
	return func(pod *corev1.Pod, _ string, _ int32, revisionHash string) {
		groups := coordination.ResolveGroups(isvc.Spec.RolloutCoordination)
		// component is recoverable from the pod's ome.io/component label.
		c := v1beta1.ComponentType(pod.Labels["ome.io/component"])
		peers := coordination.SortedPeers(groups, c, nil)
		if len(peers) == 0 {
			return
		}
		var hashFn func(v1beta1.ComponentType) string
		if revisionHash != "" {
			hashFn = func(_ v1beta1.ComponentType) string { return revisionHash }
		}
		coordination.InjectPeerEnv(pod, isvc.Name, isvc.Namespace, peers, hashFn)
	}
}

// WouldOverlayConflictForISVC is the (PodSpec, *MigrationOverlay)
// wrapper around workload/ops.WouldOverlayConflictWithNodeAffinity.
// Provided for the legacy ops/migrate.go callers; adapter callers in
// commit 9 use the workload form directly.
func WouldOverlayConflictForISVC(spec *corev1.PodSpec, overlay *workload.MigrationOverlay) bool {
	return ops.WouldOverlayConflictWithNodeAffinity(spec, overlay)
}
