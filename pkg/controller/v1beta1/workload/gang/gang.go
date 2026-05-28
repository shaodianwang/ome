// Package gang owns the per-Instance PodGroup lifecycle for OMENative
// multi-pod Components. It sits one layer above the
// `workload/podgroup` primitives (BuildPodGroup / EnsurePodGroup /
// DeletePodGroup) and orchestrates them from the workload
// reconciler's input model — every consumer of the package speaks
// workload.ReconcileInput + workload.Deps + workload.ComponentPlan,
// never the owner-CRD typed status.
//
// Why a subpackage and not the workload root: `workload/podgroup`
// already imports `workload` for the InstancePlan/ComponentPlan
// types; placing the gang body in `workload/gang.go` would close an
// import cycle through podgroup. The gang/ subpackage breaks that
// cycle (workload/gang → workload, workload/gang → workload/podgroup,
// workload/podgroup → workload — no cycle since nothing in
// workload root or workload/podgroup back-imports workload/gang).
// See `.claude/specs/2026-05-27-workload-extraction/plan.md` for the
// cost/benefit analysis.
//
// SCOPE NOTE (slice 4g of N): this package lands as part of the
// workload-extraction refactor. The legacy
// `omenative/gang.go` is gone; the ISVC adapter dispatcher calls
// `gang.EnsurePodGroups` from `omenative/reconciler.go` via
// `internalsource.BuildReconcileInput`. The future InferenceReplica
// controller will call the same entry point with its own
// ReconcileInput.
package gang

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podgroup"
)

// Condition types / reasons / event reasons stamped by the gang
// reconciler. Strings match the legacy omenative/status constants
// byte-for-byte so existing operator dashboards and `kubectl
// describe` output keep matching after the workload extraction (part
// 2 slice 4g of N). The omenative-side status package retains its own
// copies of these constants for legacy callers that still write
// through the v1beta1-typed status helpers; the values stay
// synchronized by convention. See the omenative/status package's
// conditions.go and events.go for the v1beta1-typed mirrors.
const (
	// ConditionGangSchedulingUnavailable is True when the Component
	// has at least one multi-pod Instance but the cluster does not
	// have the scheduler-plugins
	// `scheduling.x-k8s.io/v1alpha1` PodGroup CRD installed. The
	// reconciler still creates pods — gang scheduling is a soft
	// requirement per OEP-0007 §"Gang Scheduling" — but partial-gang
	// placement is possible and the runtime may hang waiting for
	// missing peer pods. False when the CRD is present or the
	// Component is single-pod.
	ConditionGangSchedulingUnavailable = "GangSchedulingUnavailable"

	// ReasonPodGroupCRDNotInstalled stamps the
	// GangSchedulingUnavailable condition with a precise diagnostic
	// — the cluster does not have the scheduler-plugins PodGroup CRD
	// installed.
	ReasonPodGroupCRDNotInstalled = "PodGroupCRDNotInstalled"

	// ReasonGangSchedulingAvailable stamps the condition Status=False
	// (the controller has gang scheduling and is using it, OR the
	// Component is single-pod and doesn't need it).
	ReasonGangSchedulingAvailable = "GangSchedulingAvailable"

	// EventReasonMaybeNoGangScheduler is a soft Warning fired the
	// first time a multi-pod Instance's PodGroup is created under a
	// pod template whose `spec.schedulerName` is unset OR equals the
	// upstream default ("default-scheduler"). The PodGroup itself
	// was created successfully — but a stock kube-scheduler does NOT
	// read `scheduling.x-k8s.io/v1alpha1` PodGroup objects, so the
	// gang contract degrades to per-pod scheduling silently.
	// Operators must either install scheduler-plugins as a secondary
	// scheduler and set
	// `runtime.spec.schedulerName=scheduler-plugins-scheduler`, OR
	// install it as a default-scheduler plugin (in which case the
	// warning is a false positive — the workload reconciler cannot
	// detect that arrangement from inside the cluster, so we err on
	// the side of surfacing the heuristic). De-duplicated per
	// (owner, Component) per controller process so reconciles do not
	// spam the event log; see maybeWarnNoGangScheduler for the dedup
	// map.
	EventReasonMaybeNoGangScheduler = "MaybeNoGangScheduler"
)

// maybeNoGangSchedulerSeen tracks which (owner, Component) pairs
// have already received the `MaybeNoGangScheduler` Warning event so
// the reconciler does not re-emit it on every reconcile. Keyed by
// "<namespace>/<name>/<component>"; value is unused (presence is
// the signal). Process-scoped — on controller restart the map is
// empty and the warning fires once more, which is acceptable since
// operators re-read events on restart anyway.
//
// sync.Map (not a plain map+mutex) because reads dominate writes:
// every reconcile of an owner that has already warned does a
// lookup-only pass, and a sync.Map's read-mostly fast path avoids
// contention with concurrent reconciles for different owners.
var maybeNoGangSchedulerSeen sync.Map

// extraPodGroupIndices returns InstanceStatus indices that the plan
// no longer covers — i.e., scale-down targets. The PodGroups for
// these Instances need GC to match the pod-level scale-down
// ops.delete pass does. Same set-difference semantic as
// ops.extraInstanceIndices but operating against workload-owned
// types so the gang reconciler stays free of the owner-CRD typed
// status.
func extraPodGroupIndices(observed []workload.InstanceStatus, plan workload.ComponentPlan) []int32 {
	if len(observed) == 0 {
		return nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range observed {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		// In-flight migration source / surge stay in the plan via the
		// migration-protected branch in BuildPlan; they will not land
		// in this 'extras' list. No-op explicitly to mirror
		// ops.extraInstanceIndices' semantics in case BuildPlan ever
		// regresses.
		if s.Phase == workload.InstancePhaseMigrating {
			continue
		}
		if s.Operation != nil && s.Operation.Type == workload.InstanceOperationMigrate {
			continue
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// EnsurePodGroups drives the per-Instance PodGroup lifecycle for one
// Component reconcile. Wired into ReconcileComponent BEFORE the
// per-Instance Create state machine so the gang is announced to the
// scheduler before the per-Instance Create op brings up the first
// pod — announcing it after would let the scheduler place pods
// individually and defeat the gang.
//
// Behavior:
//
//   - When the cluster has the PodGroup CRD
//     (input.DesiredSpec.GangSchedulingAvailable == true), call
//     podgroup.EnsurePodGroup for every multi-pod Instance in the
//     plan; single-pod Instances are skipped.
//
//   - When the cluster does NOT have the PodGroup CRD AND the plan
//     has at least one multi-pod Instance, skip PodGroup creation
//     entirely and stamp the `GangSchedulingUnavailable=True`
//     Component condition. Per OEP-0007 §"Gang Scheduling" this is
//     a soft-fail (the workload reconciler still creates pods) —
//     the condition is the operator-facing signal that gang
//     placement may race.
//
//   - When the Component is single-pod (no multi-pod Instances in the
//     plan), the condition is set to False with reason
//     `GangSchedulingAvailable` — gang scheduling is not required.
//
// Idempotent: the underlying podgroup.EnsurePodGroup uses
// CreateOrUpdate; condition writes go through
// apimeta.SetStatusCondition (inside the WriteAggregateCondition
// closure) which only bumps LastTransitionTime on a real transition.
func EnsurePodGroups(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan) error {
	if deps.Client == nil {
		return fmt.Errorf("EnsurePodGroups: nil client")
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
	// case. Done first so partial PodGroup-create failures still
	// leave the operator-visible degradation signal in a known
	// state.
	if cerr := patchGangSchedulingCondition(ctx, input, input.DesiredSpec.GangSchedulingAvailable, anyMultiPod); cerr != nil {
		return fmt.Errorf("EnsurePodGroups: patch GangSchedulingUnavailable condition: %w", cerr)
	}

	// No multi-pod Instances in the plan? Nothing to create. (Even
	// when input.DesiredSpec.GangSchedulingAvailable is true,
	// single-pod Instances skip the PodGroup per OEP — no gang to
	// enforce on a single pod.)
	if !anyMultiPod {
		return nil
	}
	// Multi-pod requested but the cluster cannot enforce gangs: skip
	// PodGroup create. The condition already reflects the
	// degradation.
	if !input.DesiredSpec.GangSchedulingAvailable {
		return nil
	}

	for _, inst := range plan.Instances {
		if !podgroup.IsMultiPodInstance(inst) {
			continue
		}
		if err := podgroup.EnsurePodGroup(ctx, deps.Client, input.OwnerObject, input.OwnerGVK, plan, inst); err != nil {
			return fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
		}
	}

	// Soft heuristic: if at least one multi-pod Instance is in the
	// plan AND the rendered pod template uses the default scheduler
	// (or no scheduler at all), the PodGroup we just created will be
	// ignored by the stock kube-scheduler and the gang contract
	// degrades to per-pod scheduling. Emit a one-shot Warning so
	// operators notice; see EventReasonMaybeNoGangScheduler for why
	// this is a heuristic rather than a hard check. Wired AFTER the
	// EnsurePodGroup loop so PodGroup-create failures take precedence
	// (event would otherwise fire alongside a hard error and add
	// noise).
	maybeWarnNoGangScheduler(deps.Recorder, input, workload.ComponentType(plan.Component), input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec)

	// Scale-down PodGroup GC. The pod scale-down logic in ops.Delete
	// drives pods to gone; this loop ensures the matching PodGroup
	// is removed too. Cascade-delete from the owner handles
	// whole-owner teardown — this is the partial / per-Instance
	// case.
	//
	// Skipped when the CRD is not installed (no PodGroup was created;
	// the Delete RPC would error with `no matches for kind`).
	for _, idx := range extraPodGroupIndices(input.ObservedState.InstanceStatuses, plan) {
		if err := podgroup.DeletePodGroup(ctx, deps.Client, input.OwnerObject, plan.Component, idx); err != nil {
			return fmt.Errorf("EnsurePodGroups: delete extra instance %d: %w", idx, err)
		}
	}
	return nil
}

// patchGangSchedulingCondition writes the GangSchedulingUnavailable
// Component-level condition through the adapter's
// WriteAggregateCondition callback. The callback wraps
// apimeta.SetStatusCondition + the apiserver round-trip under
// retry.RetryOnConflict so LastTransitionTime only updates on real
// Status transitions and concurrent reconciles can't lose the write
// to each other.
//
// When input.WriteAggregateCondition is nil — typical for tests that
// construct ReconcileInput without the bridge wiring — the call
// falls through as a no-op. Production paths populate the callback
// via internalsource.BuildReconcileInput so this branch never hits
// live.
func patchGangSchedulingCondition(ctx context.Context, input workload.ReconcileInput, crdPresent, anyMultiPod bool) error {
	if input.WriteAggregateCondition == nil {
		return nil
	}
	var generation int64
	if input.OwnerObject != nil {
		generation = input.OwnerObject.GetGeneration()
	}
	desired := gangSchedulingUnavailableCondition(crdPresent, anyMultiPod, generation)
	return input.WriteAggregateCondition(ctx, desired)
}

// gangSchedulingUnavailableCondition builds the
// `GangSchedulingUnavailable` Component-level condition with the
// canonical reason / message pair the OEP specifies. Set True when
// the Component has at least one multi-pod Instance but the cluster
// lacks the scheduler-plugins PodGroup CRD; False otherwise.
//
// Local to this package — the omenative/status package retains its
// own copy (with v1beta1-typed types) for legacy callers that still
// consult it; values are kept in lockstep by convention.
func gangSchedulingUnavailableCondition(crdPresent, anyMultiPodInstance bool, generation int64) metav1.Condition {
	now := metav1.Now()
	if !anyMultiPodInstance || crdPresent {
		// Single-pod-only Component OR the gang scheduler is
		// available — either way, no degradation to flag.
		return metav1.Condition{
			Type:               ConditionGangSchedulingUnavailable,
			Status:             metav1.ConditionFalse,
			Reason:             ReasonGangSchedulingAvailable,
			Message:            "Gang scheduling is available or not required",
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
	return metav1.Condition{
		Type:               ConditionGangSchedulingUnavailable,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonPodGroupCRDNotInstalled,
		Message:            "scheduler-plugins scheduling.x-k8s.io/v1alpha1 PodGroup CRD is not installed; multi-pod Instances may schedule partially",
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

// maybeWarnNoGangScheduler emits a one-shot Warning event on the
// owner (input.EventTarget if set, otherwise input.OwnerObject) when
// a PodGroup was created for the (owner, Component) pair but the pod
// template's effective `spec.schedulerName` is empty or the upstream
// default ("default-scheduler"). The stock kube-scheduler does not
// read scheduler-plugins' PodGroup objects, so in that configuration
// gang scheduling is silently ineffective: each pod schedules
// individually, the LLM runtime hangs on the slowest peer pod to
// land, and the gang contract is broken.
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
// "default-scheduler". The warning is therefore a heuristic; we
// accept the false-positive cost in that configuration in exchange
// for surfacing the (much more common) misconfiguration where the
// operator forgot to set SchedulerName entirely.
//
// Dedup: at most one event per (owner, Component) per controller
// process. Reconciles are frequent and the warning is informational;
// re-firing every reconcile would drown out the actionable events
// (InstanceCreated, OperationDeadlineExceeded). On controller restart
// the dedup map is empty and the warning re-fires once — acceptable
// since operators typically re-scan recent events after a restart
// anyway.
//
// nil-safe: nil recorder, nil owner, nil pod template are all
// no-ops.
func maybeWarnNoGangScheduler(rec record.EventRecorder, input workload.ReconcileInput, component workload.ComponentType, leaderSpec, workerSpec *corev1.PodSpec) {
	target := eventTarget(input)
	if target == nil {
		return
	}
	name := effectiveSchedulerName(leaderSpec, workerSpec)
	if name != "" && name != corev1.DefaultSchedulerName {
		// Operator opted in to a non-default scheduler — trust them.
		return
	}
	key := fmt.Sprintf("%s/%s/%s", target.GetNamespace(), target.GetName(), component)
	if _, alreadyWarned := maybeNoGangSchedulerSeen.LoadOrStore(key, struct{}{}); alreadyWarned {
		return
	}
	if rec == nil {
		return
	}
	rec.Eventf(target, corev1.EventTypeWarning, EventReasonMaybeNoGangScheduler,
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
// can reset between cases without leaking state across runs. Not
// used by production code.
func resetMaybeNoGangSchedulerSeen() {
	maybeNoGangSchedulerSeen.Range(func(k, _ any) bool {
		maybeNoGangSchedulerSeen.Delete(k)
		return true
	})
}

// eventTarget returns the object emitted events should be stamped
// against — input.EventTarget when set, falling back to OwnerObject.
// Local copy of the same helper in workload/ops/events.go so this
// file is callable from workload without taking a transitive
// workload/ops dep.
func eventTarget(input workload.ReconcileInput) client.Object {
	if input.EventTarget != nil {
		return input.EventTarget
	}
	return input.OwnerObject
}
