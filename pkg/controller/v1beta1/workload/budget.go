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
	"k8s.io/apimachinery/pkg/util/intstr"
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
	return resolveIntOrPercentCeil(ru.MaxSurge, replicas)
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
	return resolveIntOrPercentCeil(ru.MaxUnavailable, replicas)
}

// resolveIntOrPercentCeil converts an IntOrString budget against a
// total. Integers are clamped to >= 0; percent strings resolve via
// ceil(total * pct / 100); the percent is clamped to [0, 100].
// Malformed string inputs (no '%' suffix, non-numeric) return 0 to
// match the conservative behavior in coordination/status.go's
// MaxSurgeBudget — operators reading a validated webhook payload
// never hit the malformed branch.
func resolveIntOrPercentCeil(v *intstr.IntOrString, total int32) int32 {
	if v == nil {
		return 0
	}
	if v.Type == intstr.Int {
		if v.IntValue() < 0 {
			return 0
		}
		return int32(v.IntValue())
	}
	pct := parsePercentString(v.StrVal)
	return ceilPercent(pct, total)
}

// parsePercentString accepts "<n>%" and returns n. Returns 0 on parse
// failure — matches coordination/status.go's strPercent contract.
func parsePercentString(s string) int {
	if len(s) == 0 || s[len(s)-1] != '%' {
		return 0
	}
	out := 0
	for _, ch := range s[:len(s)-1] {
		if ch < '0' || ch > '9' {
			return 0
		}
		out = out*10 + int(ch-'0')
	}
	return out
}

// ceilPercent computes ceil(total * percent / 100). Percent values
// outside [0, 100] are clamped; total <= 0 returns 0. The result is
// capped at total so a 100% expression on N replicas returns N (not
// over-allocation). Mirrors coordination/status.go's surgeFromPercent
// so the per-Component and group-level layers agree on rounding.
func ceilPercent(percent int, total int32) int32 {
	if percent <= 0 || total <= 0 {
		return 0
	}
	if percent > 100 {
		percent = 100
	}
	num := int64(total) * int64(percent)
	out := num / 100
	if num%100 != 0 {
		out++
	}
	if out > int64(total) {
		out = int64(total)
	}
	return int32(out)
}

// CurrentSurgeInFlight counts InstanceStatuses participating in an
// in-flight SurgeThenDrain operation — those that already have an
// extra pod alive from a prior wake-up. The dispatcher consults this
// before deciding whether to start ONE MORE surge this wake-up. The
// step-set { "Surge", "SurgeDrain" } mirrors the coordination-group
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
		case "Surge", "SurgeDrain":
			n++
		}
	}
	return n
}

// CurrentUnavailableInFlight counts InstanceStatuses currently in
// Updating (non-surge step) — those whose pod is offline or being
// patched in place. Used as the prior-pass anchor for the per-
// Component MaxUnavailable budget check, similar to how
// coordination.CheckUnavailability counts (replicas - serving).
//
// We exclude Surge / SurgeDrain steps because those don't take pods
// offline (a new pod surges IN before the old one drains OUT). Other
// Updating steps (InPlace, Drain) DO take the pod offline, so they
// count.
func CurrentUnavailableInFlight(statuses []InstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Phase != InstancePhaseUpdating {
			continue
		}
		if s.Operation == nil {
			n++
			continue
		}
		switch s.Operation.Step {
		case "Surge", "SurgeDrain":
			// Surge doesn't take a pod offline.
			continue
		default:
			n++
		}
	}
	return n
}
