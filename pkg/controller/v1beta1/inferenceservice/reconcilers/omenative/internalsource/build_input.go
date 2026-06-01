package internalsource

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
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
// without taking on a backward import to core/.
//
// SCOPE NOTE (slice 4b-4d of N): only the fields the workload Create /
// Delete / Migrate state machines read (Client, ISVC, Component,
// PodSpec, WorkerPodSpec, ObjectMeta, ClearAnnotation) are populated.
// As peer ops migrate to workload/ops in later slices the bag grows to
// cover Lifecycle/Pacing/GangSchedulingAvailable.
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

	// GangSchedulingAvailable is the cluster-discovery boolean
	// threaded through from the InferenceServiceReconciler — true
	// when the scheduler-plugins
	// `scheduling.x-k8s.io/v1alpha1` PodGroup CRD is installed at
	// controller startup. Mirrored onto
	// workload.WorkloadDesiredSpec.GangSchedulingAvailable so the
	// gang.EnsurePodGroups reconciler reads it from the
	// workload-typed projection. Tests / single-pod Components /
	// pre-wiring callers leave it false.
	GangSchedulingAvailable bool

	// ClearMigrationRequestAnnotation removes the per-UUID Alfred
	// migration request annotation from the parent ISVC. Wired into
	// the workload.ReconcileInput.ClearMigrationRequest callback so
	// the source-agnostic workload.Migrate body can clear the trigger
	// without importing v1beta1 itself. The ISVC-OMENative dispatcher
	// shim wires this to the local clearMigrationRequestAnnotation
	// helper; the IR adapter (when it lands) wires its own removal
	// flow.
	//
	// Optional input field: when unset BuildReconcileInput defaults the
	// emitted ReconcileInput.ClearMigrationRequest to a no-op closure
	// so the panic-on-nil contract documented on
	// workload.ReconcileInput is satisfied. Callers that don't drive
	// the Migrate state machine (Create-only / Delete-only) leave this
	// empty.
	ClearMigrationRequestAnnotation func(ctx context.Context, uuid string) error
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
	// ClearMigrationRequest and WarnInstanceFailed satisfy the
	// panic-on-nil contract documented on workload.ReconcileInput.
	// Create-only / Delete-only callers leave
	// d.ClearMigrationRequestAnnotation nil; the escalator path lives
	// in omenative/status_aggregate.go and constructs its own
	// ReconcileInput with a real WarnInstanceFailed wired to
	// status.RecordWarning. For every other code path through
	// BuildReconcileInput these two fields aren't exercised, but the
	// closures land here so the workload-side contract holds
	// uniformly.
	clearMigration := d.ClearMigrationRequestAnnotation
	if clearMigration == nil {
		clearMigration = func(_ context.Context, _ string) error { return nil }
	}
	return workload.ReconcileInput{
		OwnerObject: d.ISVC,
		OwnerGVK:    isvcGVK,
		EventTarget: d.ISVC,
		Key:         buildKey(d.ISVC, d.Component),
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec:                 d.PodSpec,
			WorkerPodSpec:           d.WorkerPodSpec,
			PodTemplateObjectMeta:   &d.ObjectMeta,
			GangSchedulingAvailable: d.GangSchedulingAvailable,
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: v1beta1convert.InstanceStatusSliceToWorkload(extractInstanceStatuses(d.ISVC, d.Component)),
		},
		MutateInstance:          buildMutateInstance(d.Client, d.ISVC, d.Component),
		RemoveInstance:          buildRemoveInstance(d.Client, d.ISVC, d.Component, d.Expectations),
		WriteAggregateCondition: buildWriteAggregateCondition(d.Client, d.ISVC, d.Component),
		ClearMigrationRequest:   clearMigration,
		WarnInstanceFailed:      func(_ int32, _, _ string) {},
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
			w := v1beta1convert.InstanceStatusToWorkload(*v)
			if !mutate(&w) {
				return false
			}
			*v = v1beta1convert.InstanceStatusFromWorkload(w)
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
			if !ok || cs.Lifecycle == nil {
				return nil
			}
			insts := cs.Lifecycle.InstanceStatuses
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
			cs.Lifecycle.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
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
			cache.Forget(isvc.Namespace, isvc.Name, v1beta1convert.ComponentTypeToWorkload(component), idx)
		}
		return hadEntry, nil
	}
}

// buildWriteAggregateCondition wraps the legacy ISVC-side Component
// condition writer as a workload.ReconcileInput.WriteAggregateCondition
// callback. The closure merges cond into the per-Component condition
// list under retry.RetryOnConflict so LastTransitionTime only bumps
// on a real transition and concurrent reconciles can't lose the
// write to each other. Mirrors the v1beta1-typed
// patchGangSchedulingCondition that previously lived in
// omenative/gang.go before the workload extraction (part 2 slice 4g
// of N).
//
// The legacy bumpCollisionCount + buildRemoveInstance helpers above
// use the same retry shape so a future refactor can collapse them
// onto a shared retry-wrapped status patcher.
//
// NotFound handling: if the ISVC was deleted between the caller
// pulling the work item and us re-reading, both the Get and the
// Status().Update return apierrors.IsNotFound. Treat both as
// "operation succeeded trivially" — there is no status left to
// mutate — and drop the error. Same shape as MutateInstance above.
func buildWriteAggregateCondition(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) func(ctx context.Context, cond metav1.Condition) error {
	return func(ctx context.Context, cond metav1.Condition) error {
		if c == nil || isvc == nil {
			return fmt.Errorf("buildWriteAggregateCondition: nil client or ISVC")
		}
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &v1beta1.InferenceService{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("re-read ISVC: %w", err)
			}
			if fresh.Status.Components == nil {
				fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
			}
			cs := fresh.Status.Components[component]
			if cs.Lifecycle == nil {
				cs.Lifecycle = &v1beta1.LifecycleStatus{}
			}
			// Detect a no-op before issuing the API write — saves a
			// status patch and the implicit ResourceVersion bump on
			// every reconcile. Mirrors the same no-op short-circuit
			// the legacy patchGangSchedulingCondition used.
			if existing := apimeta.FindStatusCondition(cs.Lifecycle.Conditions, cond.Type); existing != nil &&
				existing.Status == cond.Status &&
				existing.Reason == cond.Reason &&
				existing.ObservedGeneration == cond.ObservedGeneration &&
				existing.Message == cond.Message {
				return nil
			}
			apimeta.SetStatusCondition(&cs.Lifecycle.Conditions, cond)
			fresh.Status.Components[component] = cs
			if err := c.Status().Update(ctx, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("status update: %w", err)
			}
			return nil
		})
	}
}

// buildKey is the ISVC-side workload.Key projection. The
// SelectorLabels carry the ISVC-specific label-key constants — this
// is the ONLY place those constants surface in the dispatch path that
// workload code observes. Workload code reads input.Key.SelectorLabels
// opaquely.
func buildKey(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) workload.Key {
	var namespace, ownerName string
	var ownerLabels map[string]string
	if isvc != nil {
		namespace = isvc.Namespace
		ownerName = isvc.Name
		ownerLabels = isvc.Labels
	}
	return workload.Key{
		Namespace:   namespace,
		Component:   v1beta1convert.ComponentTypeToWorkload(component),
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
	if !ok || cs.Lifecycle == nil {
		return nil
	}
	return cs.Lifecycle.InstanceStatuses
}
