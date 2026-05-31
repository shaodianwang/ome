package inferencereplica

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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// irGVK is the GroupVersionKind every IR-driven workload reconcile
// stamps on emitted pods / revisions / services. Decoded objects often
// have an empty TypeMeta (controller-runtime strips it on Get), so the
// workload package cannot derive the GVK from OwnerObject — the
// adapter passes the correct value alongside via
// ReconcileInput.OwnerGVK. Mirrors the same approach
// internalsource.isvcGVK takes on the ISVC adapter side.
var irGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")

// IRGVK returns the InferenceReplica GroupVersionKind. Exposed so
// the reconciler can pass it directly to revision/podgroup helpers
// without re-deriving it from the scheme.
func IRGVK() schema.GroupVersionKind { return irGVK }

// buildReconcileInput projects (IR.Spec, IR.Status) onto a
// workload.ReconcileInput so the workload dispatcher can drive the
// per-Instance pipeline without taking on a backward import to
// v1beta1.InferenceReplica. THE bridge between the IR adapter's
// source-of-truth shape and the workload package's plain-data carrier
// — analogous to internalsource.BuildReconcileInput on the ISVC side.
//
// OwnerObject is the IR itself: every emitted pod / ControllerRevision
// / PodGroup is GC'd through the IR's controller OwnerReference.
//
// OwnerName on the workload.Key is the parent ISVC name — pod names
// (<isvc>-<component>-<idx>-<runner>-<ord>), service names, and the
// HPA scale-selector formula all key off the ISVC name. The
// SelectorLabels carry the legacy OMENative trio so existing selectors
// keep matching.
//
// EventTarget defaults to the parent ISVC when the caller passes a
// non-nil parent (so user-facing event streams stay coherent under
// `kubectl describe isvc`). When parent is nil — the IR was created
// directly without a parent in the cache, or the parent fetch failed
// — EventTarget falls back to the IR itself so events still land
// somewhere observable.
//
// DetectMigrationRequest / UpdateGate are left nil: the IR controller
// doesn't yet handle ISVC-orchestrated migrations or cross-Component
// coordination gates (both stay on the ISVC controller per the design
// doc §3.3). The workload dispatcher treats nil callbacks as "no
// migration in flight" and "always allowed" respectively.
func (r *Reconciler) buildReconcileInput(ir *v1beta1.InferenceReplica, parent client.Object) workload.ReconcileInput {
	desired := desiredFromIR(ir)
	observed := observedFromIR(ir)
	eventTarget := client.Object(ir)
	if parent != nil {
		eventTarget = parent
	}
	return workload.ReconcileInput{
		OwnerObject:             ir,
		OwnerGVK:                irGVK,
		EventTarget:             eventTarget,
		Key:                     buildKey(ir),
		DesiredSpec:             desired,
		ObservedState:           observed,
		MutateInstance:          buildMutateInstance(r.Client, ir),
		RemoveInstance:          buildRemoveInstance(r.Client, ir, r.Expectations),
		WriteAggregateCondition: buildWriteAggregateCondition(r.Client, ir),
		// IR doesn't drive ISVC migrations; no source-of-truth annotation
		// to clear. Wire a no-op stub so the workload.ReconcileInput
		// panic-on-nil contract is satisfied.
		ClearMigrationRequest: func(_ context.Context, _ string) error { return nil },
		// WarnInstanceFailed bridges the workload-side escalator to the
		// reconciler's event recorder so the operator sees the
		// per-Instance Failed escalation against the EventTarget (parent
		// ISVC when set, else IR). The stuck-pod aggregator is run
		// elsewhere in this PR but every constructed input must satisfy
		// the panic-on-nil contract.
		//
		// Also mirror the escalation onto the controller log so
		// log-aggregated views (loki/es/journal) see it without going
		// through the events stream — the event is user-facing while
		// the log is operator-facing.
		WarnInstanceFailed: func(idx int32, podName, reason string) {
			r.Log.Info("Instance escalated to Phase=Failed",
				"inferencereplica", client.ObjectKeyFromObject(ir),
				"component", ir.Spec.Component,
				"instance", idx,
				"pod", podName,
				"reason", reason)
			if r.Recorder == nil {
				return
			}
			r.Recorder.Eventf(eventTarget, corev1.EventTypeWarning, "InstanceFailed",
				"InferenceReplica %s/%s component=%s instance=%d escalated to Phase=Failed: pod %s stuck in %s (no kubelet self-recovery)",
				ir.Namespace, ir.Name, ir.Spec.Component, idx, podName, reason)
		},
	}
}

// buildHeadlessServiceSpec adapts an InferenceReplica into the typed
// workload.PerComponentServiceSpec the workload renderer consumes.
// Mirrors omenative.BuildHeadlessServiceSpec byte-for-byte except for:
// (a) the v1beta1.InferenceReplica handle, (b) the owner-ref shape —
// the IR owns its per-Component headless Service so deletion of the IR
// cascades to the Service. The ISVC controller continues to manage the
// IR lifecycle (PR 3); inside its own lifetime, the IR controller is
// the sole Service writer.
//
// OwnerName on the Service name resolves through the parent ISVC
// (ir.Spec.ParentRef.Name) so the Service name stays byte-identical to
// the legacy `<isvc>-<component>-headless` shape — every tool that
// looks for that name keeps working unchanged.
//
// Selector matches the labels the workload renderer stamps on pods
// (see render.go::podLabels): ome.io/inferenceservice + component +
// managed-by=OMENative. The trio is identical to the ISVC adapter so
// the Services from both paths select the same pod set; this is what
// keeps the shadow-mode diff in PR 3 byte-identical.
func buildHeadlessServiceSpec(ir *v1beta1.InferenceReplica) workloadtypes.PerComponentServiceSpec {
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
		constants.OMEComponentLabel:           string(ir.Spec.Component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	return workloadtypes.PerComponentServiceSpec{
		Name:      query.HeadlessServiceName(ir.Spec.ParentRef.Name, internalsource.ComponentTypeToWorkload(ir.Spec.Component)),
		Namespace: ir.Namespace,
		Selector:  selector,
		Labels:    selector,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(ir, irGVK),
		},
	}
}

// desiredFromIR projects IR.Spec.Runners + IR.Spec.Lifecycle onto
// the workload.WorkloadDesiredSpec the workload dispatcher reads.
//
// PodSpec/WorkerPodSpec come from the Runner.Template's PodSpec.
// PodTemplateObjectMeta is derived from the Runner.Template's
// ObjectMeta so user-intent pod-template labels/annotations land on
// every emitted pod (matches the legacy ObjectMeta projection from
// components/{engine,decoder,router}.go via the ISVC adapter).
func desiredFromIR(ir *v1beta1.InferenceReplica) workload.WorkloadDesiredSpec {
	replicas := int32(1)
	if ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0 {
		replicas = *ir.Spec.Replicas
	}
	desired := workload.WorkloadDesiredSpec{
		Replicas:        replicas,
		MinReadySeconds: ir.Spec.MinReadySeconds,
		Runners:         runnersFromIR(ir),
		Paused:          ir.Spec.Paused,
	}
	if ir.Spec.Pacing != nil {
		desired.Pacing = &workload.WorkloadPacing{
			Partition:      ir.Spec.Pacing.Partition,
			MaxUnavailable: ir.Spec.Pacing.MaxUnavailable,
		}
	}
	if ir.Spec.Lifecycle != nil {
		desired.Lifecycle = internalsource.LifecycleSpecToWorkload(*ir.Spec.Lifecycle)
	}
	// Pull the per-role PodSpec + ObjectMeta out of the rendered IR
	// runners. The IR contract requires at least one Runner; the
	// webhook validates the {default} | {leader, worker} shapes.
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		switch runner.Name {
		case v1beta1.RunnerNameDefault:
			spec := runner.Template.Spec
			desired.PodSpec = &spec
			meta := runner.Template.ObjectMeta
			desired.PodTemplateObjectMeta = &meta
		case v1beta1.RunnerNameLeader:
			spec := runner.Template.Spec
			desired.PodSpec = &spec
			meta := runner.Template.ObjectMeta
			desired.PodTemplateObjectMeta = &meta
			desired.MultiPod = true
		case v1beta1.RunnerNameWorker:
			spec := runner.Template.Spec
			desired.WorkerPodSpec = &spec
			desired.MultiPod = true
		}
	}
	return desired
}

// runnersFromIR converts the IR Runner list to the workload.Runner
// projection. The names round-trip 1:1 (RunnerNameDefault → "default",
// RunnerNameLeader → "leader", RunnerNameWorker → "worker"); the size
// is the per-Instance pod count for that role.
func runnersFromIR(ir *v1beta1.InferenceReplica) []workload.Runner {
	if len(ir.Spec.Runners) == 0 {
		return nil
	}
	out := make([]workload.Runner, 0, len(ir.Spec.Runners))
	for _, r := range ir.Spec.Runners {
		out = append(out, workload.Runner{
			Name: string(r.Name),
			Size: r.Size,
		})
	}
	return out
}

// observedFromIR projects IR.Status onto the workload.WorkloadObservedState
// the dispatcher reads on each pass. Conditions are NOT propagated:
// the workload package writes back into the same shape via
// WriteAggregateCondition, so reading them in here would double-stamp
// transition timestamps.
func observedFromIR(ir *v1beta1.InferenceReplica) workload.WorkloadObservedState {
	return workload.WorkloadObservedState{
		ObservedGeneration: ir.Status.ObservedGeneration,
		CollisionCount:     ir.Status.CollisionCount,
		CurrentRevision:    ir.Status.CurrentRevision,
		UpdateRevision:     ir.Status.UpdateRevision,
		InstanceStatuses:   internalsource.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
	}
}

// buildKey is the IR-side workload.Key projection. The SelectorLabels
// carry the legacy OMENative trio so existing pod selectors keep
// matching during the dual-write window (every pod the IR controller
// creates is observable by the same selector the legacy omenative
// controller used).
//
// OwnerName resolves to the parent ISVC name (from
// IR.Spec.ParentRef.Name) so pod / service names stay byte-identical
// to the legacy shape — query.PodName / query.HeadlessServiceName /
// revision.Name all key off this field.
func buildKey(ir *v1beta1.InferenceReplica) workload.Key {
	return workload.Key{
		Namespace:   ir.Namespace,
		Component:   internalsource.ComponentTypeToWorkload(ir.Spec.Component),
		OwnerName:   ir.Spec.ParentRef.Name,
		OwnerLabels: ir.Labels,
		SelectorLabels: map[string]string{
			constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
			constants.OMEComponentLabel:           string(ir.Spec.Component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// buildMutateInstance wraps the workload-typed mutate callback with an
// IR-typed apiserver round-trip. Mirrors status.MutateInstance on the
// ISVC side: re-read under retry.RetryOnConflict, locate/append the
// (idx) InstanceStatus, hand the workload-typed mirror to the caller,
// convert back to v1beta1 if the caller reported a real change, and
// persist via Status().Update().
//
// The boolean return lets the workload caller short-circuit the
// apiserver round-trip when no real change happened — matches the
// legacy ISVC-side contract so per-op writers stay idempotent.
//
// NotFound on Get or Update is treated as a successful no-op: the IR
// was deleted under us, the InstanceStatus is gone with it, dropping
// the work cleanly avoids saturating the work queue with retries on a
// gone owner.
func buildMutateInstance(c client.Client, ir *v1beta1.InferenceReplica) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	key := client.ObjectKeyFromObject(ir)
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		var committed *v1beta1.OMENativeInstanceStatus
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			committed = nil
			fresh := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, key, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			insts := fresh.Status.InstanceStatuses
			pos := -1
			for i, s := range insts {
				if s.Index == idx {
					pos = i
					break
				}
			}
			var slot *v1beta1.OMENativeInstanceStatus
			if pos == -1 {
				insts = append(insts, v1beta1.OMENativeInstanceStatus{Index: idx})
				slot = &insts[len(insts)-1]
			} else {
				slot = &insts[pos]
			}
			w := internalsource.InstanceStatusToWorkload(*slot)
			if !mutate(&w) {
				return nil
			}
			*slot = internalsource.InstanceStatusFromWorkload(w)
			fresh.Status.InstanceStatuses = insts
			if err := c.Status().Update(ctx, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			committed = slot.DeepCopy()
			return nil
		})
		if err != nil {
			return err
		}
		if committed != nil {
			mirrorInstanceStatus(ir, *committed)
		}
		return nil
	}
}

// mirrorInstanceStatus copies the just-committed InstanceStatus onto
// the caller's in-memory IR snapshot. Subsequent ops in the same
// reconcile pass observe the post-write state instead of the stale
// pre-reconcile snapshot — same shape as status/mutate.go on the
// ISVC side.
func mirrorInstanceStatus(ir *v1beta1.InferenceReplica, src v1beta1.OMENativeInstanceStatus) {
	if ir == nil {
		return
	}
	pos := -1
	for i, s := range ir.Status.InstanceStatuses {
		if s.Index == src.Index {
			pos = i
			break
		}
	}
	if pos == -1 {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, src)
	} else {
		ir.Status.InstanceStatuses[pos] = src
	}
}

// buildRemoveInstance wraps the IR-typed status removal flow as the
// workload.ReconcileInput.RemoveInstance callback. Drops the (idx)
// InstanceStatus, persists, and (on a real removal) forgets the
// expectation entry so a later index reuse doesn't inherit stale
// counters. NotFound on Get or Update is treated as success.
func buildRemoveInstance(c client.Client, ir *v1beta1.InferenceReplica, exp *workload.Expectations) func(ctx context.Context, idx int32) (bool, error) {
	key := client.ObjectKeyFromObject(ir)
	return func(ctx context.Context, idx int32) (bool, error) {
		var hadEntry bool
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			hadEntry = false
			fresh := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, key, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			insts := fresh.Status.InstanceStatuses
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
			fresh.Status.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
			if err := c.Status().Update(ctx, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			hadEntry = true
			return nil
		})
		if err != nil {
			return false, err
		}
		if hadEntry {
			cache := exp
			if cache == nil {
				cache = workload.DefaultExpectations
			}
			cache.Forget(ir.Namespace, ir.Name, internalsource.ComponentTypeToWorkload(ir.Spec.Component), idx)
		}
		return hadEntry, nil
	}
}

// buildWriteAggregateCondition wraps the IR-side per-Component
// condition writer as the workload.ReconcileInput.WriteAggregateCondition
// callback. The closure merges cond into IR.Status.Conditions under
// retry.RetryOnConflict so LastTransitionTime only bumps on a real
// transition and concurrent reconciles can't lose the write to each
// other. Today this surface is exercised only by the workload-side
// gang reconciler (GangSchedulingUnavailable); the IR controller
// inherits it for free.
func buildWriteAggregateCondition(c client.Client, ir *v1beta1.InferenceReplica) func(ctx context.Context, cond metav1.Condition) error {
	key := client.ObjectKeyFromObject(ir)
	return func(ctx context.Context, cond metav1.Condition) error {
		if c == nil || ir == nil {
			return fmt.Errorf("buildWriteAggregateCondition: nil client or IR")
		}
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, key, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("re-read IR: %w", err)
			}
			// No-op short-circuit so we don't bump ResourceVersion every
			// reconcile when nothing changed.
			if existing := apimeta.FindStatusCondition(fresh.Status.Conditions, cond.Type); existing != nil &&
				existing.Status == cond.Status &&
				existing.Reason == cond.Reason &&
				existing.ObservedGeneration == cond.ObservedGeneration &&
				existing.Message == cond.Message {
				return nil
			}
			apimeta.SetStatusCondition(&fresh.Status.Conditions, cond)
			if err := c.Status().Update(ctx, fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("update IR status: %w", err)
			}
			return nil
		})
	}
}
