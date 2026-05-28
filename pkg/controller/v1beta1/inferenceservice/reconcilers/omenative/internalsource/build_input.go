package internalsource

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// isvcGVK is the GroupVersionKind every OMENative-on-ISVC adapter
// stamps on every emitted resource. Decoded ISVC objects often have
// an empty TypeMeta (controller-runtime strips it during
// deserialization) so the workload package cannot derive the GVK from
// the OwnerObject — the adapter passes the correct value alongside
// via ReconcileInput.OwnerGVK.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// ISVCGVK returns the ISVC GroupVersionKind the adapter stamps on
// every emitted resource. Exposed for callers (e.g. the omenative ops
// shim) that need the GVK independent of BuildReconcileInput.
func ISVCGVK() schema.GroupVersionKind { return isvcGVK }

// BuildInputDeps captures the carrier-free subset of
// core.ReconcileParams that BuildReconcileInput needs. Kept distinct
// from core.ReconcileParams so this helper lives in internalsource/
// without taking on a backward import to core/ (which would close a
// cycle: core imports internalsource for converters).
//
// SCOPE NOTE (slice 4b of N): only the fields the Create state machine
// reads (Client, ISVC, Component, PodSpec, WorkerPodSpec, ObjectMeta)
// are populated. As peer ops migrate to workload/ops in later slices
// the bag grows to cover Lifecycle/Pacing/GangSchedulingAvailable.
type BuildInputDeps struct {
	// ISVC is the parent InferenceService being reconciled. Used as
	// both the workload OwnerObject and the EventTarget.
	ISVC *v1beta1.InferenceService

	// Component identifies which of router / engine / decoder is being
	// reconciled.
	Component v1beta1.ComponentType

	// ObjectMeta is the rendered per-Component metadata (labels,
	// annotations, owner refs) the renderer stamps on emitted pods.
	ObjectMeta metav1.ObjectMeta

	// PodSpec is the rendered leader / single-pod template for the
	// Component.
	PodSpec *corev1.PodSpec

	// WorkerPodSpec is the rendered worker template for multi-pod
	// Components. Nil for single-pod.
	WorkerPodSpec *corev1.PodSpec

	// Client is the controller-runtime client used to persist status
	// writes via the MutateInstance closure.
	Client client.Client

	// Expectations is the per-reconcile expectations cache the
	// RemoveInstance closure forgets on a successful removal. When
	// nil the workload-level DefaultExpectations singleton is used —
	// matches the fallback every legacy callsite already relies on.
	Expectations *workload.Expectations
}

// BuildReconcileInput projects (ISVC, Component, podSpec, ...) onto a
// workload.ReconcileInput so workload-side reconcilers (workload/ops
// peers) can be invoked without taking on a backward import to v1beta1.
// THE bridge between the ISVC adapter's source-of-truth shape and the
// workload package's plain-data carrier.
//
// MutateInstance wraps status.MutateInstance — the legacy ISVC status
// writer that handles apiserver round-trip under retry.RetryOnConflict
// plus the in-memory ISVC mirror. The closure round-trips a single
// InstanceStatus through the workload<->v1beta1 converters so the
// caller's mutate callback sees workload.InstanceStatus only.
//
// SelectorLabels seeds the per-Component pod selector with
// constants.InferenceServicePodLabelKey=<isvc> and
// constants.OMEComponentLabel=<component>. This is THE place those
// ISVC-specific label-key constants surface in the dispatch path —
// workload code reads input.Key.SelectorLabels opaquely. The IR
// adapter will populate its own IR-specific keys here.
func BuildReconcileInput(d BuildInputDeps) workload.ReconcileInput {
	return workload.ReconcileInput{
		OwnerObject: d.ISVC,
		OwnerGVK:    isvcGVK,
		EventTarget: d.ISVC,
		Key:         buildWorkloadKey(d.ISVC, d.Component),
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec:               d.PodSpec,
			WorkerPodSpec:         d.WorkerPodSpec,
			PodTemplateObjectMeta: &d.ObjectMeta,
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: InstanceStatusSliceToWorkload(extractInstanceStatuses(d.ISVC, d.Component)),
		},
		MutateInstance: buildMutateInstance(d.Client, d.ISVC, d.Component),
		RemoveInstance: buildRemoveInstance(d.Client, d.ISVC, d.Component, d.Expectations),
	}
}

// buildMutateInstance wraps status.MutateInstance with v1beta1 ↔ workload
// conversion. The workload callback runs against a workload.InstanceStatus
// mirror; the wrapper converts back to v1beta1 only if cb returned true
// (the legacy MutateInstance helper uses the boolean return to
// short-circuit the apiserver round-trip when nothing changed).
func buildMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		return status.MutateInstance(ctx, c, isvc, component, idx, func(v *v1beta1.OMENativeInstanceStatus) bool {
			w := InstanceStatusToWorkload(*v)
			if !mutate(&w) {
				return false
			}
			*v = InstanceStatusFromWorkload(w)
			return true
		})
	}
}

// buildRemoveInstance wraps the v1beta1-typed status removal flow as a
// workload.ReconcileInput.RemoveInstance callback. Mirrors the
// removeInstanceStatus helper that previously lived in
// omenative/ops/delete.go before the per-Instance Delete state machine
// moved to workload/ops (part 2 slice 4c of N). Kept in internalsource
// rather than the status package because the post-commit
// Expectations.Forget step needs the workload-typed component, which
// would force a forward dependency from the v1beta1-typed status
// package onto workload.
//
// exp is the expectations cache the closure forgets on successful
// removal. When nil the workload-level DefaultExpectations singleton
// is used — matches the fallback every legacy callsite relies on via
// p.ExpectationsCache().
func buildRemoveInstance(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, exp *workload.Expectations) func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		var hadEntry bool
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			hadEntry = false
			fresh := &v1beta1.InferenceService{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
				if apierrors.IsNotFound(err) {
					// ISVC deleted during reconcile — the InstanceStatus
					// entry is gone with it. Treat as a successful no-op.
					return nil
				}
				return fmt.Errorf("re-read ISVC: %w", err)
			}
			if fresh.Status.Components == nil {
				return nil
			}
			cs, ok := fresh.Status.Components[component]
			if !ok || cs.OMENative == nil {
				return nil
			}
			insts := cs.OMENative.InstanceStatuses
			pos := -1
			for i, s := range insts {
				if s.Index == idx {
					pos = i
					break
				}
			}
			if pos == -1 {
				return nil
			}
			cs.OMENative.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
			fresh.Status.Components[component] = cs

			if err := c.Status().Update(ctx, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("update ISVC status: %w", err)
			}
			hadEntry = true
			return nil
		})
		if err != nil {
			return false, err
		}
		if hadEntry {
			// Forget so a later index reuse doesn't inherit stale counters.
			cache := exp
			if cache == nil {
				cache = workload.DefaultExpectations
			}
			cache.Forget(isvc.Namespace, isvc.Name, ComponentTypeToWorkload(component), idx)
		}
		return hadEntry, nil
	}
}

// buildWorkloadKey is the ISVC-side workload.Key projection. The
// SelectorLabels carry the ISVC-specific label-key constants — this
// is the ONLY place those constants surface in the dispatch path that
// workload code observes. Workload code reads input.Key.SelectorLabels
// opaquely.
func buildWorkloadKey(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) workload.Key {
	var namespace, ownerName string
	var ownerLabels map[string]string
	if isvc != nil {
		namespace = isvc.Namespace
		ownerName = isvc.Name
		ownerLabels = isvc.Labels
	}
	return workload.Key{
		Namespace:   namespace,
		Component:   ComponentTypeToWorkload(component),
		OwnerName:   ownerName,
		OwnerLabels: ownerLabels,
		SelectorLabels: map[string]string{
			constants.InferenceServicePodLabelKey: ownerName,
			constants.OMEComponentLabel:           string(component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// extractInstanceStatuses pulls the OMENative InstanceStatuses slice
// from the ISVC for one Component. Returns nil when the parent map /
// Component entry / OMENative subtree is missing — matches the
// behavior callers relied on before the workload extraction. Mirrors
// the same function in omenative/core/params.go.
func extractInstanceStatuses(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) []v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	return cs.OMENative.InstanceStatuses
}
