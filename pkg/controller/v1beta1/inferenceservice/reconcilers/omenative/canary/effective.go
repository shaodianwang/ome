package canary

import "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"

// EffectivePartition returns the StatefulSet-style RollingUpdate.Partition the
// controller should apply to a component while a canary is in progress, reading
// the current step from status. Instances with index < Partition are held on the
// stable revision; (desired - Partition) roll to the canary revision.
//
// It is a pure read used by the controller to stamp the partition onto the merged
// component lifecycle before the component reconcilers run — the same
// spec→IR→plan→HeldByPartition path an operator-set partition takes. Returns
// (nil, false) when no canary plan is set for the InferenceService.
//
//   - The current step's NewCapacity resolved against desired.
//
// Rollback (ome.io/rollout-rollback) is intentionally NOT special-cased here:
// holding every instance on the stable revision (Partition = desired) cannot
// revert instances already rolled to the canary — the workload skips held
// instances rather than draining their canary pods — so it wedges the rollout
// at RollingBack. Canary rollback is deferred; the annotation is a guarded
// no-op (Dispatch surfaces a warning). To roll back today, revert the component
// spec to the previous revision.
func EffectivePartition(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, desiredReplicas int32) (*int32, bool) {
	plan := isvc.Spec.GetCanary()
	if plan == nil || len(plan.Steps) == 0 {
		return nil, false
	}
	idx := int32(0)
	if isvc.Status.Canary != nil {
		idx = isvc.Status.Canary.CurrentStep
	}
	// Done sentinel (CurrentStep past the last step): the canary finished — hold
	// NOTHING, the component runs entirely on the canary revision (partition 0).
	// Don't clamp to the final step and recompute: if that step's NewCapacity were
	// < 100% (admission allows it — only final TrafficWeight==100 is enforced),
	// the recompute would strand (desired-newCount) instances on the stable
	// revision permanently after completion.
	if int(idx) >= len(plan.Steps) {
		zero := int32(0)
		return &zero, true
	}
	newCount := resolveStepNewCount(plan.Steps[idx], desiredReplicas)
	p := partitionForNewCount(desiredReplicas, newCount)
	return &p, true
}

// StampStepPartition stamps the current canary step's RollingUpdate.Partition
// onto a merged Component's lifecycle (creating the lifecycle / updateStrategy /
// rollingUpdate chain as needed), so the standard spec→IR→plan→HeldByPartition
// path holds the staged split. No-op when no canary is active for the ISVC.
// desiredReplicas is read from the Component's MinReplicas (fallback MaxReplicas).
func StampStepPartition(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, ext *v1beta1.ComponentExtensionSpec) {
	if ext == nil {
		return
	}
	desired := int32(ext.MaxReplicas)
	if ext.MinReplicas != nil {
		desired = int32(*ext.MinReplicas)
	}
	p, ok := EffectivePartition(isvc, component, desired)
	if !ok {
		return
	}
	if ext.Lifecycle == nil {
		ext.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	if ext.Lifecycle.UpdateStrategy == nil {
		ext.Lifecycle.UpdateStrategy = &v1beta1.UpdateStrategy{}
	}
	if ext.Lifecycle.UpdateStrategy.RollingUpdate == nil {
		ext.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
	}
	ext.Lifecycle.UpdateStrategy.RollingUpdate.Partition = p
}
