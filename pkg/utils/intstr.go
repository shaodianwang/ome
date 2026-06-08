package utils

import "k8s.io/apimachinery/pkg/util/intstr"

// PtrIntOrStringFromString returns a pointer to an IntOrString carrying
// the supplied percentage string (e.g. "25%"). Convenience for pacing
// defaulters that need an addressable percent literal.
func PtrIntOrStringFromString(s string) *intstr.IntOrString {
	out := intstr.FromString(s)
	return &out
}

// ScaledCountFromIntOrString resolves an IntOrString budget (a pod
// count or a percentage of total) into an absolute, non-negative count.
// It is the single replacement for the hand-rolled int/percent → count
// helpers that previously lived in coordination/status.go,
// workload/budget.go, and canary/capacity.go.
//
// Semantics (chosen to reproduce all three former copies byte-for-byte):
//
//   - nil v → 0.
//   - Integer form: the literal value, floored at 0. NOT clamped to
//     total — an integer MaxSurge larger than the replica count is a
//     deliberate over-allocation the former coordination/workload copies
//     returned as-is. Callers that need an upper bound (e.g. canary's
//     new-pod count) compose with ClampInt32.
//   - Percent form: round(total * pct / 100) via
//     intstr.GetScaledValueFromIntOrPercent (ceil when roundUp, floor
//     otherwise), then floored at 0 and CLAMPED to total. The clamp
//     reproduces the former copies' "a 100%+ expression never exceeds
//     total" rule: for pct in [0,100] the ceil already sits at or below
//     total so the clamp is a no-op; for pct > 100 it caps at total,
//     exactly as the old percent→count helpers did by clamping the
//     percent to 100 first.
//   - Malformed string (no '%' suffix, non-numeric): the stdlib parser
//     errors and we return 0 — matching the conservative
//     parse-failure → 0 contract of the former strPercent /
//     parsePercentString helpers. Validated webhook payloads never reach
//     this branch.
func ScaledCountFromIntOrString(v *intstr.IntOrString, total int32, roundUp bool) int32 {
	if v == nil {
		return 0
	}
	if v.Type == intstr.Int {
		n := int32(v.IntValue())
		if n < 0 {
			return 0
		}
		return n
	}
	n, err := intstr.GetScaledValueFromIntOrPercent(v, int(total), roundUp)
	if err != nil || n < 0 {
		return 0
	}
	if total >= 0 && int32(n) > total {
		return total
	}
	return int32(n)
}
