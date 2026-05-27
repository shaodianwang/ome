package omenative

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podgroup"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
)

// extraPodGroupIndices returns InstanceStatus indices that the plan no
// longer covers — i.e., scale-down targets. The PodGroups for these
// Instances need GC to match the pod-level scale-down ops.delete pass
// does. Same set-difference semantic as ops.extraInstanceIndices but
// without crossing into the ops/ package (which forbids importing this
// top-level package).
func extraPodGroupIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, plan ComponentPlan) []int32 {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range cs.OMENative.InstanceStatuses {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		// In-flight migration source / surge stay in the plan via the
		// migration-protected branch in core.BuildPlan; they will not
		// land in this 'extras' list. No-op explicitly to mirror
		// ops.extraInstanceIndices' semantics in case BuildPlan ever
		// regresses.
		if s.Phase == v1beta1.OMENativeInstanceMigrating {
			continue
		}
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate {
			continue
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// ensurePodGroups drives the per-Instance PodGroup lifecycle for one
// Component reconcile. Wired into ReconcileComponent BEFORE
// ReconcileComponentOps so the gang is announced to the scheduler
// before the per-Instance Create op brings up the first pod —
// announcing it after would let the scheduler place pods individually
// and defeat the gang.
//
// Behavior:
//
//   - When the cluster has the PodGroup CRD (p.GangSchedulingAvailable
//     == true), call podgroup.EnsurePodGroup for every multi-pod
//     Instance in the plan; single-pod Instances are skipped.
//
//   - When the cluster does NOT have the PodGroup CRD AND the plan has
//     at least one multi-pod Instance, skip PodGroup creation entirely
//     and stamp the `GangSchedulingUnavailable=True` Component
//     condition. Per OEP-0007 §"Gang Scheduling" this is a soft-fail
//     (the controller still creates pods) — the condition is the
//     operator-facing signal that gang placement may race.
//
//   - When the Component is single-pod (no multi-pod Instances in the
//     plan), the condition is set to False with reason
//     `GangSchedulingAvailable` — gang scheduling is not required.
//
// Idempotent: the underlying podgroup.EnsurePodGroup uses
// CreateOrUpdate; condition writes go through apimeta.SetStatusCondition
// which only bumps LastTransitionTime on a real transition.
func ensurePodGroups(ctx context.Context, p ReconcileParams, plan ComponentPlan) error {
	if p.Client == nil {
		return fmt.Errorf("ensurePodGroups: nil client")
	}

	anyMultiPod := false
	for _, inst := range plan.Instances {
		if podgroup.IsMultiPodInstance(inst) {
			anyMultiPod = true
			break
		}
	}

	// Always (re-)stamp the condition: True only when degraded
	// (multi-pod requested AND CRD missing); False in every other
	// case. Done first so partial PodGroup-create failures still leave
	// the operator-visible degradation signal in a known state.
	if cerr := patchGangSchedulingCondition(ctx, p, p.GangSchedulingAvailable, anyMultiPod); cerr != nil {
		return fmt.Errorf("ensurePodGroups: patch GangSchedulingUnavailable condition: %w", cerr)
	}

	// No multi-pod Instances in the plan? Nothing to create. (Even when
	// p.GangSchedulingAvailable is true, single-pod Instances skip the
	// PodGroup per OEP — no gang to enforce on a single pod.)
	if !anyMultiPod {
		return nil
	}
	// Multi-pod requested but the cluster cannot enforce gangs: skip
	// PodGroup create. The condition already reflects the degradation.
	if !p.GangSchedulingAvailable {
		return nil
	}

	for _, inst := range plan.Instances {
		if !podgroup.IsMultiPodInstance(inst) {
			continue
		}
		if err := podgroup.EnsurePodGroup(ctx, p.Client, p.ISVC, plan, inst); err != nil {
			return fmt.Errorf("ensurePodGroups: instance %d: %w", inst.Index, err)
		}
	}

	// Scale-down PodGroup GC. The pod scale-down logic in ops.Delete
	// drives pods to gone; this loop ensures the matching PodGroup is
	// removed too. Cascade-delete from the ISVC handles whole-ISVC
	// teardown — this is the partial / per-Instance case.
	//
	// Skipped when the CRD is not installed (no PodGroup was created;
	// the Delete RPC would error with `no matches for kind`).
	for _, idx := range extraPodGroupIndices(p.ISVC, plan.Component, plan) {
		if err := podgroup.DeletePodGroup(ctx, p.Client, p.ISVC, plan.Component, idx); err != nil {
			return fmt.Errorf("ensurePodGroups: delete extra instance %d: %w", idx, err)
		}
	}
	return nil
}

// patchGangSchedulingCondition writes the GangSchedulingUnavailable
// Component-level condition through to the live status using the same
// retry-on-conflict pattern bumpCollisionCount uses for CollisionCount.
// The condition is merged via apimeta.SetStatusCondition so
// LastTransitionTime only updates on real Status transitions.
//
// Wrapped in retry.RetryOnConflict so concurrent component reconciles
// (BlueGreen / Sequential rollout aggregator + the OMENative reconciler
// itself) can't lose the write to each other.
func patchGangSchedulingCondition(ctx context.Context, p ReconcileParams, crdPresent, anyMultiPod bool) error {
	if p.Client == nil || p.ISVC == nil {
		return fmt.Errorf("patchGangSchedulingCondition: nil client or ISVC")
	}
	desired := status.GangSchedulingUnavailableCondition(crdPresent, anyMultiPod, p.ISVC.Generation)
	isvcGone := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				isvcGone = true
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		cs := fresh.Status.Components[p.Component]
		if cs.OMENative == nil {
			cs.OMENative = &v1beta1.OMENativeComponentStatus{}
		}
		// Detect a no-op before issuing the API write — saves a status
		// patch and the implicit ResourceVersion bump on every reconcile.
		if existing := findCondition(cs.OMENative.Conditions, status.ConditionGangSchedulingUnavailable); existing != nil &&
			existing.Status == desired.Status &&
			existing.Reason == desired.Reason &&
			existing.ObservedGeneration == desired.ObservedGeneration &&
			existing.Message == desired.Message {
			return nil
		}
		status.ApplyComponentConditions(cs.OMENative, []metav1.Condition{desired})
		fresh.Status.Components[p.Component] = cs
		if err := p.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				isvcGone = true
				return nil
			}
			return fmt.Errorf("status update: %w", err)
		}
		return nil
	})
	if isvcGone {
		return nil
	}
	return err
}

// findCondition is a tiny helper that returns a pointer to the
// condition matching condType in conditions, or nil. Local copy to
// avoid pulling apimeta into this file just for one read; the
// equivalent apimeta.FindStatusCondition does the same lookup with a
// copy semantic that we don't want here (need pointer for the no-op
// check).
func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
