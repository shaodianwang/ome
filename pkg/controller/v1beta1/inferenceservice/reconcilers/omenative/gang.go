package omenative

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podgroup"
)

// maybeNoGangSchedulerSeen tracks which (ISVC, Component) pairs have
// already received the `MaybeNoGangScheduler` Warning event so the
// controller does not re-emit it on every reconcile. Keyed by
// "<namespace>/<name>/<component>"; value is unused (presence is the
// signal). Process-scoped — on controller restart the map is empty and
// the warning fires once more, which is acceptable since operators
// re-read events on restart anyway.
//
// sync.Map (not a plain map+mutex) because reads dominate writes:
// every reconcile of an ISVC that has already warned does a lookup-only
// pass, and a sync.Map's read-mostly fast path avoids contention with
// concurrent reconciles for different ISVCs.
var maybeNoGangSchedulerSeen sync.Map

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

	// Soft heuristic: if at least one multi-pod Instance is in the plan
	// AND the rendered pod template uses the default scheduler (or no
	// scheduler at all), the PodGroup we just created will be ignored
	// by the stock kube-scheduler and the gang contract degrades to
	// per-pod scheduling. Emit a one-shot Warning so operators notice;
	// see EventReasonMaybeNoGangScheduler for why this is a heuristic
	// rather than a hard check. Wired AFTER the EnsurePodGroup loop so
	// PodGroup-create failures take precedence (event would otherwise
	// fire alongside a hard error and add noise).
	maybeWarnNoGangScheduler(p, plan.Component, p.PodSpec, p.WorkerPodSpec)

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

// maybeWarnNoGangScheduler emits a one-shot Warning event on the
// parent ISVC when a PodGroup was created for the (ISVC, Component)
// pair but the pod template's effective `spec.schedulerName` is empty
// or the upstream default ("default-scheduler"). The stock
// kube-scheduler does not read scheduler-plugins' PodGroup objects, so
// in that configuration gang scheduling is silently ineffective: each
// pod schedules individually, the LLM runtime hangs on the slowest
// peer pod to land, and the gang contract is broken.
//
// "Effective" means: if the leader (`leaderSpec`) sets a
// schedulerName, take that; otherwise fall back to the worker
// (`workerSpec`); otherwise treat it as unset. The two templates
// share the runtime's `spec.schedulerName` plumbing in nearly every
// configuration, but defending against drift is cheap.
//
// We can't reliably detect the case where the operator has installed
// scheduler-plugins AS the default scheduler (single-scheduler
// deployment with the Coscheduling plugin enabled) — from inside the
// controller, the SchedulerName field would still be empty /
// "default-scheduler". The warning is therefore a heuristic; we accept
// the false-positive cost in that configuration in exchange for
// surfacing the (much more common) misconfiguration where the
// operator forgot to set SchedulerName entirely.
//
// Dedup: at most one event per (ISVC, Component) per controller
// process. Reconciles are frequent and the warning is informational;
// re-firing every reconcile would drown out the actionable events
// (InstanceCreated, OperationDeadlineExceeded). On controller
// restart the dedup map is empty and the warning re-fires once —
// acceptable since operators typically re-scan recent events after a
// restart anyway.
//
// nil-safe: nil recorder, nil ISVC, nil pod template are all no-ops.
func maybeWarnNoGangScheduler(p ReconcileParams, component v1beta1.ComponentType, leaderSpec, workerSpec *corev1.PodSpec) {
	if p.ISVC == nil {
		return
	}
	name := effectiveSchedulerName(leaderSpec, workerSpec)
	if name != "" && name != corev1.DefaultSchedulerName {
		// Operator opted in to a non-default scheduler — trust them.
		return
	}
	key := fmt.Sprintf("%s/%s/%s", p.ISVC.Namespace, p.ISVC.Name, component)
	if _, alreadyWarned := maybeNoGangSchedulerSeen.LoadOrStore(key, struct{}{}); alreadyWarned {
		return
	}
	status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonMaybeNoGangScheduler,
		"OMENative component=%s created scheduler-plugins PodGroup objects but pod template's spec.schedulerName is %q (default kube-scheduler does not enforce PodGroup gang); set runtime.spec.schedulerName to a gang-aware scheduler (e.g. scheduler-plugins-scheduler) or install scheduler-plugins as a plugin in the default scheduler",
		component, name)
}

// effectiveSchedulerName returns the leader's spec.schedulerName when
// set, otherwise the worker's, otherwise empty. Mirrors the
// "leader-wins" precedence the runtime templating uses elsewhere; a
// nil template counts as "unset" for that side.
func effectiveSchedulerName(leaderSpec, workerSpec *corev1.PodSpec) string {
	if leaderSpec != nil && leaderSpec.SchedulerName != "" {
		return leaderSpec.SchedulerName
	}
	if workerSpec != nil && workerSpec.SchedulerName != "" {
		return workerSpec.SchedulerName
	}
	return ""
}

// resetMaybeNoGangSchedulerSeen clears the per-process dedup map.
// Test-only helper exported lowercase so the table-driven dedup test
// can reset between cases without leaking state across runs. Not used
// by production code.
func resetMaybeNoGangSchedulerSeen() {
	maybeNoGangSchedulerSeen.Range(func(k, _ any) bool {
		maybeNoGangSchedulerSeen.Delete(k)
		return true
	})
}
