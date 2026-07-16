// Package gang owns the per-Instance PodGroup lifecycle for OMENative
// multi-pod Components. Sits one layer above the `workload/podgroup`
// primitives and orchestrates them from the workload reconciler's
// input model.
//
// Lives in its own subpackage (rather than workload root) so the
// import edges are gang → workload + gang → podgroup → workload,
// with no back-edge — would otherwise close a cycle through podgroup.
package gang

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podgroup"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// maybeNoGangSchedulerSeen tracks which (owner, Component) pairs have
// already received the MaybeNoGangScheduler Warning event so the
// reconciler does not re-emit it on every reconcile. Keyed by
// "<namespace>/<name>/<component>". Process-scoped — controller
// restart re-fires the warning, which is fine since operators re-read
// events on restart.
//
// sync.Map because reads dominate writes: an owner that has already
// warned does a lookup-only pass per reconcile.
var maybeNoGangSchedulerSeen sync.Map

// extraPodGroupIndices returns InstanceStatus indices the plan no
// longer covers — scale-down targets whose PodGroups need GC to match
// the pod-level scale-down ops.delete pass.
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

// EnsureSurgePodGroup builds the Deps.EnsureGangPodGroup callback the
// gang-surge op invokes to create a surge Instance's PodGroup inline,
// just before its pods. Keeps the workload/ops package free of a
// workload/podgroup import (which would close a cycle through podgroup's
// test deps) while still guaranteeing PodGroup-before-pods for the surge
// gang regardless of when the surge index lands in the plan the
// top-level EnsurePodGroups keys off.
//
// No-op when the cluster lacks the PodGroup CRD
// (DesiredSpec.GangSchedulingAvailable == false) or the Instance is
// single-pod. Idempotent.
func EnsureSurgePodGroup(deps workload.Deps) workload.EnsureGangPodGroupFn {
	return func(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (string, error) {
		if deps.Client == nil || !podgroup.IsMultiPodInstance(inst) {
			return plan.TopologyKeyForInstance(inst.Index), nil
		}
		reader := deps.Reader()
		pods, err := podsForPodGroup(ctx, reader, input.Key.Namespace,
			query.PodGroupName(input.Key.OwnerName, plan.Component, inst.Index))
		if err != nil {
			return "", fmt.Errorf("list surge PodGroup pods: %w", err)
		}
		if !input.DesiredSpec.GangSchedulingAvailable {
			return podgroup.EffectiveTopologyKeyForPods(
				plan.TopologyKeyForInstance(inst.Index), input.Key.OwnerName,
				plan.Component, inst.Index, pods)
		}
		return podgroup.EnsurePodGroupForPodsWithReader(ctx, deps.Client, reader, input.OwnerObject, input.OwnerGVK, input.Key.OwnerName, plan, inst, pods)
	}
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
//     Component condition. This is a soft-fail (the workload reconciler
//     still creates pods) — the condition is the operator-facing signal
//     that gang placement may race.
//
//   - When the Component is single-pod (no multi-pod Instances in the
//     plan), the condition is set to False with reason
//     `GangSchedulingAvailable` — gang scheduling is not required.
//
// Idempotent: the underlying podgroup.EnsurePodGroup reconciles owned fields;
// condition writes go through
// apimeta.SetStatusCondition (inside the WriteAggregateCondition
// closure) which only bumps LastTransitionTime on a real transition.
func EnsurePodGroups(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan) error {
	_, err := EnsurePodGroupsWithTopology(ctx, deps, input, plan)
	return err
}

// EnsurePodGroupsWithTopology reconciles PodGroups and returns the effective
// topology contract for each planned gang. Empty is valid only when topology
// was intentionally left unset. If a configured key cannot be proven from an
// active gang's immutable pods or owned PodGroup annotation, reconciliation
// fails closed before creating missing members.
func EnsurePodGroupsWithTopology(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan) (map[int32]string, error) {
	effectiveTopology := make(map[int32]string)
	if deps.Client == nil {
		return nil, fmt.Errorf("EnsurePodGroups: nil client")
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
		return nil, fmt.Errorf("EnsurePodGroups: patch GangSchedulingUnavailable condition: %w", cerr)
	}

	// No multi-pod Instances in the plan? Nothing to create. (Even
	// when input.DesiredSpec.GangSchedulingAvailable is true,
	// single-pod Instances skip the PodGroup — no gang to enforce on
	// a single pod.)
	if !anyMultiPod {
		return effectiveTopology, nil
	}
	reader := deps.Reader()
	pods, err := query.ListOMENativePodsByName(ctx, reader, input.Key.Namespace,
		input.Key.OwnerName, plan.Component, false)
	if err != nil {
		return nil, fmt.Errorf("EnsurePodGroups: list component pods: %w", err)
	}
	podsByGroup := make(map[string][]*corev1.Pod)
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		name := pod.Labels[query.LabelPodGroup]
		if name == "" {
			continue
		}
		podsByGroup[name] = append(podsByGroup[name], pod)
	}

	// Multi-pod requested but the cluster cannot enforce gangs: skip
	// PodGroup writes, while still preserving the immutable topology carried by
	// live pods. Missing members must not be recreated under a newer key merely
	// because the optional PodGroup API is unavailable.
	if !input.DesiredSpec.GangSchedulingAvailable {
		for _, inst := range plan.Instances {
			if !podgroup.IsMultiPodInstance(inst) {
				continue
			}
			name := query.PodGroupName(input.Key.OwnerName, plan.Component, inst.Index)
			key, err := podgroup.EffectiveTopologyKeyForPods(
				plan.TopologyKeyForInstance(inst.Index), input.Key.OwnerName, plan.Component,
				inst.Index, podsByGroup[name])
			if err != nil {
				return nil, fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
			}
			effectiveTopology[inst.Index] = key
		}
		return effectiveTopology, nil
	}

	for _, inst := range plan.Instances {
		if !podgroup.IsMultiPodInstance(inst) {
			continue
		}
		name := query.PodGroupName(input.Key.OwnerName, plan.Component, inst.Index)
		key, err := podgroup.EnsurePodGroupForPodsWithReader(ctx, deps.Client, reader, input.OwnerObject, input.OwnerGVK, input.Key.OwnerName, plan, inst, podsByGroup[name])
		if err != nil {
			return nil, fmt.Errorf("EnsurePodGroups: instance %d: %w", inst.Index, err)
		}
		effectiveTopology[inst.Index] = key
	}

	// Heuristic warning when the rendered pod template uses the
	// default scheduler — see EventReasonMaybeNoGangScheduler. Wired
	// AFTER EnsurePodGroup so PodGroup-create failures take precedence.
	maybeWarnNoGangScheduler(deps.Recorder, input, plan.Component, input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec)

	// Per-Instance PodGroup GC. Whole-owner teardown is handled by
	// the cascade-delete OwnerReference.
	for _, idx := range extraPodGroupIndices(input.ObservedState.InstanceStatuses, plan) {
		if err := podgroup.DeletePodGroup(ctx, deps.Client, input.OwnerObject, input.Key.OwnerName, plan.Component, idx); err != nil {
			return nil, fmt.Errorf("EnsurePodGroups: delete extra instance %d: %w", idx, err)
		}
	}
	return effectiveTopology, nil
}

func podsForPodGroup(ctx context.Context, c client.Reader, namespace, name string) ([]*corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := c.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{query.LabelPodGroup: name}); err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	return pods, nil
}

// patchGangSchedulingCondition writes the GangSchedulingUnavailable
// Component-level condition through input.WriteAggregateCondition.
// The closure wraps apimeta.SetStatusCondition + apiserver round-
// trip under retry.RetryOnConflict.
func patchGangSchedulingCondition(ctx context.Context, input workload.ReconcileInput, crdPresent, anyMultiPod bool) error {
	var generation int64
	if input.OwnerObject != nil {
		generation = input.OwnerObject.GetGeneration()
	}
	desired := gangSchedulingUnavailableCondition(crdPresent, anyMultiPod, generation)
	return input.WriteAggregateCondition(ctx, desired)
}

// gangSchedulingUnavailableCondition builds the
// GangSchedulingUnavailable condition. True when the Component has
// at least one multi-pod Instance but the cluster lacks the
// scheduler-plugins PodGroup CRD; False otherwise.
func gangSchedulingUnavailableCondition(crdPresent, anyMultiPodInstance bool, generation int64) metav1.Condition {
	now := metav1.Now()
	if !anyMultiPodInstance || crdPresent {
		// Single-pod-only Component OR the gang scheduler is
		// available — either way, no degradation to flag.
		return metav1.Condition{
			Type:               string(workload.ConditionGangSchedulingUnavailable),
			Status:             metav1.ConditionFalse,
			Reason:             string(workload.ReasonGangSchedulingAvailable),
			Message:            "Gang scheduling is available or not required",
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
	return metav1.Condition{
		Type:               string(workload.ConditionGangSchedulingUnavailable),
		Status:             metav1.ConditionTrue,
		Reason:             string(workload.ReasonPodGroupCRDNotInstalled),
		Message:            "scheduler-plugins scheduling.x-k8s.io/v1alpha1 PodGroup CRD is not installed; multi-pod Instances may schedule partially",
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

// maybeWarnNoGangScheduler emits a one-shot Warning when a PodGroup
// was created but the pod template's effective spec.schedulerName is
// empty or "default-scheduler". The stock kube-scheduler doesn't
// read scheduler-plugins PodGroup objects, so the gang contract is
// silently broken in that configuration.
//
// "Effective" schedulerName: leaderSpec wins, then workerSpec, then
// treated as unset. The warning is a heuristic — we can't detect
// scheduler-plugins installed AS the default scheduler from inside
// the controller, so accept the false-positive cost there.
//
// Dedup'd per (owner, Component) per process. Restart re-fires once,
// which is acceptable.
//
// nil-safe: nil recorder, nil owner, nil pod template are no-ops.
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
	rec.Eventf(target, corev1.EventTypeWarning, string(workload.EventReasonMaybeNoGangScheduler),
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
