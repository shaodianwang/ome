// Per-Component rollout budget — within-Component MaxSurge /
// MaxUnavailable ceilings sourced from
// plan.UpdateStrategy.RollingUpdate. Composes orthogonally with the
// coordination-group ceilings the ISVC adapter wires into
// input.UpdateGate: both layers are independent capacities, the
// effective gate allows an Instance start only when BOTH layers do.
//
// Background. Before this file the only rollout budget the dispatcher
// consulted was the coordination-group MaxSurge / MaxUnavailable
// (group-wide ceiling across Components in a RolloutCoordinationGroup).
// The per-Component LifecycleSpec.UpdateStrategy.RollingUpdate fields
// were widened to *intstr.IntOrString and the converter copies them onto
// workload.RollingUpdate — but the dispatcher never read them. This
// file closes that gap: budgets here cap WITHIN-Component pod surge /
// drain, regardless of whether the Component participates in any
// coordination group. Group-wide ceilings still apply through the
// UpdateGate callback in workload.Reconcile.
//
// Composition rule (documented in lifecycle_types.go on RollingUpdate.MaxSurge):
//
//	effective surge cap = min(group_max_surge, per_component_max_surge)
//	effective unavail cap = min(group_max_unavail, per_component_max_unavail)
//
// The two layers govern different things:
//   - per-Component RollingUpdate.MaxSurge — cap on extra pods the
//     Component may have alive temporarily during ONE rollout.
//   - CoordinationPacing.MaxSurge — cap on extra pods OMENative may
//     add across the whole coordination group (so two peer Components
//     rolling at once don't both surge to budget=4 each, totaling 8).
//
// Defaults: nil per-Component RollingUpdate.{MaxSurge,MaxUnavailable}
// → "no per-Component cap" (the coordination-group ceiling is the
// only constraint). The 25% / 0 defaults the API documents are
// applied by upstream defaulters (webhook) when the operator wants
// them; an unset value here is treated as "uncapped" so we don't
// silently impose a budget the operator didn't write.
package workload

import (
	"github.com/ome-projects/ome/pkg/utils"
)

// BudgetNoLimit is returned by Per* budget helpers when the source
// RollingUpdate field is nil or unparseable — signals "no within-
// Component cap from this field" so the dispatcher's min(group, per-
// Component) composition collapses to the group-level cap.
const BudgetNoLimit = int32(-1)

// PerComponentMaxSurgeBudget resolves
// plan.UpdateStrategy.RollingUpdate.MaxSurge against the Component's
// replica count. Returns BudgetNoLimit when RollingUpdate is nil or
// MaxSurge is unset — the caller treats that as "this layer does not
// cap" and defers to the coordination-group layer (which has its own
// 25% default).
//
// Integer values are returned as-is (clamped to >= 0). Percent strings
// resolve to ceil(replicas * percent / 100) to match upstream
// appsv1.Deployment.Strategy.RollingUpdate.MaxSurge semantics and the
// group-wide MaxSurgeBudget helper. A 0% expression on any replica
// count returns 0 (no surge allowed via this layer); a 25% expression
// on 4 replicas returns 1 (one extra pod allowed).
func PerComponentMaxSurgeBudget(ru *RollingUpdate, replicas int32) int32 {
	if ru == nil || ru.MaxSurge == nil {
		return BudgetNoLimit
	}
	return utils.ScaledCountFromIntOrString(ru.MaxSurge, replicas, true)
}

// PerComponentMaxUnavailableBudget is the unavailability dual of
// PerComponentMaxSurgeBudget. nil RollingUpdate or nil MaxUnavailable
// returns BudgetNoLimit; the dispatcher falls through to the
// coordination-group ceiling. Integer values return as-is, percent
// strings resolve via ceil semantics so a "25%" budget on 4 replicas
// returns 1.
func PerComponentMaxUnavailableBudget(ru *RollingUpdate, replicas int32) int32 {
	if ru == nil || ru.MaxUnavailable == nil {
		return BudgetNoLimit
	}
	return utils.ScaledCountFromIntOrString(ru.MaxUnavailable, replicas, true)
}

// HeldByPartition reports whether the Instance at `index` is held on the old
// revision by RollingUpdate.Partition (the StatefulSet-style canary): Instances
// with index < Partition are held. A nil RollingUpdate, nil Partition, or
// Partition <= 0 holds nothing. Surge Instances are allocated above the replica
// count, so their index >= replicas >= Partition and they are never held.
func HeldByPartition(ru *RollingUpdate, index int32) bool {
	if ru == nil || ru.Partition == nil || *ru.Partition <= 0 {
		return false
	}
	return index < *ru.Partition
}

// CurrentSurgeInFlight counts InstanceStatuses participating in an
// in-flight SurgeThenDrain operation — those that already have an
// extra pod alive from a prior wake-up. The dispatcher consults this
// before deciding whether to start ONE MORE surge this wake-up. The
// step-set { "Surge", "SurgeDrain", "SurgeDrainSettle" } mirrors the
// coordination-group
// CheckSurge accounting in
// pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go
// — both layers see the same in-flight signal so the budget math is
// consistent.
func CurrentSurgeInFlight(statuses []InstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Operation == nil {
			continue
		}
		// Surge contributes +1 pod alive; SurgeDrain does too (the
		// old pod hasn't been deleted yet). Other ops (Drain for
		// recreate, InPlace for in-place patch) don't add pods.
		switch s.Operation.Step {
		case "Surge", "SurgeDrain", "SurgeDrainSettle":
			n++
		}
	}
	return n
}

// CurrentUnavailableInFlight counts InstanceStatuses currently in an
// unavailable Update (non-surge step) — those whose pod is offline or being
// patched in place. A Failed Instance whose Update operation is preserved for
// retry still consumes the budget; otherwise the dispatcher can start another
// healthy Instance in the same wake-up and take the whole Component offline.
// Used as the prior-pass anchor for the per-Component MaxUnavailable budget
// check, similar to how coordination.CheckUnavailability counts
// (replicas - serving).
//
// We exclude surge lifecycle steps because those don't take pods
// offline (a new pod surges IN before the old one drains OUT). Other
// Updating steps (InPlace, Drain) DO take the pod offline, so they
// count.
func CurrentUnavailableInFlight(statuses []InstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		updating := s.Phase == InstancePhaseUpdating
		failedUpdate := s.Phase == InstancePhaseFailed && s.Operation != nil &&
			s.Operation.Type == InstanceOperationUpdate
		if !updating && !failedUpdate {
			continue
		}
		if s.Operation == nil {
			n++
			continue
		}
		switch s.Operation.Step {
		case "Surge", "SurgeDrain", "SurgeDrainSettle":
			// Surge doesn't take a pod offline.
			continue
		default:
			n++
		}
	}
	return n
}
