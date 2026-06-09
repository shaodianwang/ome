package status

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TopLevelComponentReadyFromLifecycle derives the standard top-level
// component-ready condition (EngineReady / DecoderReady / RouterReady)
// from OMENative counters. Returns nil for unknown Component types so
// callers leave the condition surface untouched.
//
// Ready=True iff ReadyReplicas == Replicas > 0 — i.e., every desired
// Instance has all its pods ContainersReady. Mirrors the contract
// PropagateRawStatus uses (Deployment.Available) so the ingress
// reconciler, aggregate Ready rollup, and `kubectl wait
// --for=condition=Ready` treat OMENative the same as RawDeployment.
//
// Lives in the neutral inferenceservice/status package rather than
// either reconciler — both the legacy omenative direct path
// (status_aggregate.go) and the IR-managed projector (irprojector
// status.go) call this to derive the same condition off the same
// LifecycleStatus shape. Single source of truth — any future
// change to the Ready predicate lives here only. Keeping it out of
// the omenative package also avoids a one-way coupling from the
// IR-managed path (the new path) to the legacy omenative package
// (which the design intends to delete after the IR cutover).
func TopLevelComponentReadyFromLifecycle(component v1beta1.ComponentType, cs *v1beta1.LifecycleStatus) *apis.Condition {
	var readyType apis.ConditionType
	switch component {
	case v1beta1.EngineComponent:
		readyType = v1beta1.EngineReady
	case v1beta1.DecoderComponent:
		readyType = v1beta1.DecoderReady
	case v1beta1.RouterComponent:
		readyType = v1beta1.RouterReady
	default:
		return nil
	}
	cond := &apis.Condition{Type: readyType}
	switch {
	case cs == nil || cs.Replicas == 0:
		cond.Status = corev1.ConditionFalse
		cond.Reason = "NoReplicas"
		cond.Message = "Component has no desired Instances"
	case cs.ReadyReplicas == cs.Replicas:
		cond.Status = corev1.ConditionTrue
		cond.Reason = "Ready"
		cond.Message = "All Instances are Ready"
	default:
		cond.Status = corev1.ConditionFalse
		cond.Reason = "NotAllReady"
		cond.Message = fmt.Sprintf("%d/%d Instances ready", cs.ReadyReplicas, cs.Replicas)
	}
	return cond
}
