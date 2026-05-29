// Package podgroup builds and reconciles scheduler-plugins
// `scheduling.x-k8s.io/v1alpha1` PodGroup objects for OMENative multi-pod
// Instances.
//
// Gang scheduling contract (per OEP-0007 §"Gang Scheduling"): every
// multi-pod Instance (leader + N workers) gets one PodGroup with
// MinMember == TotalPodsForInstance(inst). The scheduler-plugins
// coscheduler enforces that all minMember pods can be placed before any
// is bound to a node — without it, the leader can schedule alone, the
// workers stay Pending, and the LLM runtime hangs waiting for peer
// connections.
//
// Single-pod Instances do NOT get a PodGroup — there's nothing to gang.
//
// The CRD is optional: when `scheduling.x-k8s.io` is not installed on
// the cluster, the controller still creates pods but emits a
// `GangSchedulingUnavailable=True` Condition on the Component. The
// failure mode (potential partial gang placement) is documented; it is
// soft, not hard.
//
// SchedulerName contract — operator's responsibility. The PodGroup
// objects this package emits are only honored by a scheduler that
// reads `scheduling.x-k8s.io/v1alpha1` (today: scheduler-plugins'
// Coscheduling plugin, or a custom equivalent). The OEP requires the
// operator to set `runtime.spec.schedulerName` on the
// ServingRuntime (or `podSpec.schedulerName` on the InferenceService)
// to a gang-aware scheduler name — typically
// `scheduler-plugins-scheduler` when scheduler-plugins is deployed as
// a secondary scheduler. The controller does NOT enforce or default
// this field (the operator may have installed Coscheduling as a
// plugin inside the default scheduler, in which case the value
// `default-scheduler` IS gang-aware and a hard check would produce
// false positives). Instead, the workload gang reconciler emits a
// one-shot Warning event `MaybeNoGangScheduler` against the parent
// owner when a PodGroup is created for a pod whose effective
// SchedulerName is empty or `default-scheduler`; see
// `pkg/controller/v1beta1/workload/gang/gang.go`
// `maybeWarnNoGangScheduler` for the heuristic and dedup behavior.
package podgroup

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// maxScheduleTimeoutSeconds caps the per-PodGroup
// scheduleTimeoutSeconds value derived from
// LifecycleSpec.InstanceReadyTimeout. The OEP keeps the
// instanceReadyTimeout default at 30 minutes (large-model loading);
// the gang-scheduling wait should NOT block that long because the
// scheduler can only ever fail-fast on infeasibility, never wait for
// the runtime. 10 minutes is the upstream coscheduler's documented
// reasonable upper bound; anything larger just delays the
// GangSchedulingTimeout signal.
const maxScheduleTimeoutSeconds int32 = 600

// minScheduleTimeoutSeconds is the floor when the caller passes a
// zero / negative timeout (test fixtures, pre-default specs). Matches
// the scheduler-plugins default.
const minScheduleTimeoutSeconds int32 = 60

// IsMultiPodInstance reports whether the Instance has more than one pod
// across all Runners — i.e., it needs a PodGroup. Wraps
// InstancePlan.TotalPods with the OEP threshold (>=2) so callers
// don't repeat the constant.
func IsMultiPodInstance(inst workload.InstancePlan) bool {
	return inst.TotalPods() > 1
}

// BuildPodGroup produces the desired PodGroup for one multi-pod
// Instance. Caller MUST check IsMultiPodInstance(inst) first; passing
// a single-pod Instance returns an error so a stray call site can't
// silently emit a 1-member PodGroup (which the scheduler-plugins
// validation rejects with `minMember must be >= 1` only when zero,
// but a 1-member PodGroup is functionally wrong — it means "schedule
// this one pod whenever, but block other pods in the group" which has
// no other members).
//
// Properties:
//   - Name: query.PodGroupName(owner.Name, component, inst.Index) —
//     `<owner>-<comp>-<idx>`, mirrors the Instance subdomain prefix.
//   - Namespace: inherits from the parent owner.
//   - Labels: same Component-scoped trio every OMENative-emitted
//     resource carries (inferenceservice, component, managed-by).
//     The instance-index label is included so a single-Component
//     selector can fan out by Instance for debugging.
//   - OwnerReference: owner (typically the InferenceService) as
//     Controller, blockOwnerDeletion=true so the K8s garbage collector
//     handles cascade-delete when the owner is removed.
//   - Spec.MinMember: TotalPodsForInstance(inst) — every leader +
//     worker pod must be schedulable for the gang to land.
//   - Spec.ScheduleTimeoutSeconds: clamped from the caller's
//     InstanceReadyTimeout into [minScheduleTimeoutSeconds,
//     maxScheduleTimeoutSeconds]; see the constant comments above.
//
// The owner is taken as the abstract client.Object + GroupVersionKind
// (rather than a concrete v1beta1.InferenceService) so this leaf
// package stays callable from the workload reconciler — which lives
// above the owner-CRD typed boundary and never imports
// pkg/apis/ome/v1beta1. Adapters supply the concrete object plus the
// GVK at the dispatch site (omenative passes the ISVC + isvcGVK; the
// future InferenceReplica adapter will pass its own).
//
// Pure compute, no I/O.
func BuildPodGroup(owner client.Object, ownerGVK schema.GroupVersionKind, plan workload.ComponentPlan, inst workload.InstancePlan) (*schedulingv1alpha1.PodGroup, error) {
	if owner == nil {
		return nil, fmt.Errorf("BuildPodGroup: nil owner")
	}
	if !IsMultiPodInstance(inst) {
		return nil, fmt.Errorf("BuildPodGroup: instance %d is single-pod (no gang)", inst.Index)
	}

	minMember := inst.TotalPods()
	timeout := clampScheduleTimeoutSeconds(int32(plan.InstanceReadyTimeout.Seconds()))

	labels := podGroupLabels(owner.GetName(), plan.Component, inst.Index)

	return &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodGroupName(owner.GetName(), plan.Component, inst.Index),
			Namespace: owner.GetNamespace(),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(owner, ownerGVK),
			},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              minMember,
			ScheduleTimeoutSeconds: &timeout,
		},
	}, nil
}

// podGroupLabels are the Component-scoped labels stamped on every
// PodGroup. Matches the headless Service selector trio so an operator
// looking at the parent Service can pivot to the gang via the same keys.
func podGroupLabels(isvcName string, component v1beta1.ComponentType, instanceIdx int32) map[string]string {
	return map[string]string{
		"ome.io/inferenceservice": isvcName,
		"ome.io/component":        string(component),
		query.LabelManagedBy:      query.ManagedByOMENative,
		query.LabelInstanceIdx:    fmt.Sprintf("%d", instanceIdx),
	}
}

// clampScheduleTimeoutSeconds reduces the caller-supplied value into
// [minScheduleTimeoutSeconds, maxScheduleTimeoutSeconds] (defaults to
// min when zero / negative).
func clampScheduleTimeoutSeconds(in int32) int32 {
	if in <= 0 {
		return minScheduleTimeoutSeconds
	}
	if in > maxScheduleTimeoutSeconds {
		return maxScheduleTimeoutSeconds
	}
	if in < minScheduleTimeoutSeconds {
		return minScheduleTimeoutSeconds
	}
	return in
}

// EnsurePodGroup creates (or no-op-reconciles) the PodGroup for one
// multi-pod Instance. Idempotent: drift on MinMember /
// ScheduleTimeoutSeconds / Labels / OwnerReferences is corrected on
// every reconcile. No-op for single-pod Instances.
//
// The PodGroup MUST exist before any of its Instance's pods are
// created — otherwise the scheduler-plugins coscheduler sees pods
// referencing an unknown PodGroup and falls back to scheduling them
// individually (defeating the gang). Callers wire this in BEFORE the
// per-Instance Create / Restart op runs.
//
// Returns gangAvailable=false (with err=nil) when the cluster lacks
// the PodGroup CRD — the no-match error from controller-runtime is
// indistinguishable from "transient apiserver hiccup" at the typed
// level, so the discovery check is the right gate (done by
// CRDPresent above the caller).
//
// owner + ownerGVK are passed straight through to BuildPodGroup so
// callers above the owner-CRD typed boundary (the workload gang
// reconciler) can drive the PodGroup lifecycle without importing
// pkg/apis/ome/v1beta1.
func EnsurePodGroup(ctx context.Context, c client.Client, owner client.Object, ownerGVK schema.GroupVersionKind, plan workload.ComponentPlan, inst workload.InstancePlan) error {
	if c == nil {
		return fmt.Errorf("EnsurePodGroup: nil client")
	}
	if !IsMultiPodInstance(inst) {
		return nil
	}
	desired, err := BuildPodGroup(owner, ownerGVK, plan, inst)
	if err != nil {
		return fmt.Errorf("EnsurePodGroup: build desired: %w", err)
	}

	target := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, target, func() error {
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		target.Spec.MinMember = desired.Spec.MinMember
		target.Spec.ScheduleTimeoutSeconds = desired.Spec.ScheduleTimeoutSeconds
		return nil
	})
	if err != nil {
		return fmt.Errorf("EnsurePodGroup: CreateOrUpdate %s/%s: %w", target.Namespace, target.Name, err)
	}
	return nil
}

// DeletePodGroup removes the PodGroup for an Instance that has been
// drained. Safe to call when the PodGroup doesn't exist (returns nil)
// or when single-pod Instances are passed (no-op).
//
// In practice cascade-delete on the owner handles whole-owner teardown
// via the controller-owner ref; this function is for scale-down (one
// Instance shrinks out of the plan while others remain) and for
// migration (the source Instance's PodGroup is removed once the
// surge is Ready).
//
// owner is taken as the abstract client.Object (rather than a concrete
// v1beta1.InferenceService) for the same reason BuildPodGroup /
// EnsurePodGroup do — see those docstrings.
func DeletePodGroup(ctx context.Context, c client.Client, owner client.Object, component v1beta1.ComponentType, instanceIdx int32) error {
	if c == nil {
		return fmt.Errorf("DeletePodGroup: nil client")
	}
	if owner == nil {
		return fmt.Errorf("DeletePodGroup: nil owner")
	}
	pg := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodGroupName(owner.GetName(), component, instanceIdx),
			Namespace: owner.GetNamespace(),
		},
	}
	if err := c.Delete(ctx, pg); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("DeletePodGroup: delete %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	return nil
}
