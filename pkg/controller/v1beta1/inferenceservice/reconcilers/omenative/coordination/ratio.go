package coordination

import (
	"math"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// RatioState carries the cross-Component replica counts the
// RatioBalanced pacing math reasons about. Snapshotted at rollout
// start and refreshed every reconcile while the group is in Surging /
// Waiting.
type RatioState struct {
	// Original maps Component → desired replica count at the moment
	// the rollout began. Used as the ratio anchor; the tolerance band
	// is computed against this.
	Original map[v1beta1.ComponentType]int32

	// Current maps Component → total live replica count (any
	// revision) as of this reconcile.
	Current map[v1beta1.ComponentType]int32

	// NewPods maps Component → number of new-revision pods (i.e.,
	// pods on the target ControllerRevision) as of this reconcile.
	NewPods map[v1beta1.ComponentType]int32
}

// RatioDecision is the answer for "may we surge new-revision pods for
// `component`?" produced by RatioBalanced pacing.
type RatioDecision struct {
	// AllowedSurgeDelta is the maximum number of new-revision pods we
	// may add to component this reconcile. Zero when no surge is
	// allowed; negative is normalized to zero.
	AllowedSurgeDelta int32

	// SkewRejected is true when the previous attempted surge for
	// component was refused to avoid drifting the cross-Component
	// ratio outside the tolerance band. Drives the
	// `ratio_skew_total` counter and the RatioSkewRejected event.
	SkewRejected bool

	// Reason is the operator-visible explanation.
	Reason string
}

// EvaluateSurge returns the per-Component surge budget that respects
// the cross-Component ratio. It is a pure function: feed it the
// pacing block, the snapshot state, the Component being considered,
// and the proposed delta — get back what the controller may apply.
//
// The math: with the original ratio fixed at rollout start, the
// target new-pod count for each Component is proportional. A
// candidate surge is accepted only if the projected ratio between
// the largest Component's new-pod fraction and any peer's new-pod
// fraction stays within (1 ± tolerance). The check generalizes
// pairwise — every (a, b) ratio must satisfy the band — so 3-way
// 40:20:10 topologies don't degenerate to "biggest only."
func EvaluateSurge(pacing v1beta1.CoordinationPacing, state RatioState, component v1beta1.ComponentType, proposedDelta int32) RatioDecision {
	if proposedDelta <= 0 {
		return RatioDecision{AllowedSurgeDelta: 0, Reason: "no surge requested"}
	}
	if pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "PerComponent pacing"}
	}
	if len(state.Original) <= 1 {
		// Single-Component group — ratio enforcement is trivially
		// satisfied (no peer to skew against).
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "single-component group"}
	}
	tolerance := int32(5)
	if pacing.RatioTolerance != nil {
		tolerance = *pacing.RatioTolerance
	}
	band := float64(tolerance) / 100.0

	for delta := proposedDelta; delta > 0; delta-- {
		projected := projectedNewPods(state.NewPods, component, delta)
		if respectsBand(state.Original, projected, band) {
			return RatioDecision{
				AllowedSurgeDelta: delta,
				Reason:            "within tolerance",
			}
		}
	}
	return RatioDecision{
		AllowedSurgeDelta: 0,
		SkewRejected:      true,
		Reason:            "surge would skew cross-Component ratio past tolerance",
	}
}

// projectedNewPods clones state.NewPods with component += delta.
func projectedNewPods(in map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, delta int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[component] += delta
	return out
}

// respectsBand reports whether the projected new-pod counts, taken as a
// fraction of each Component's original replica count, fall within the
// pairwise tolerance band against every peer.
//
// The band is the operator-supplied "1 ± tolerance" window. The check
// is pairwise so that asymmetric 3+-way topologies don't degenerate to
// only comparing the largest pair.
func respectsBand(original, projected map[v1beta1.ComponentType]int32, band float64) bool {
	if band < 0 {
		band = 0
	}
	frac := make(map[v1beta1.ComponentType]float64, len(original))
	for c, orig := range original {
		if orig <= 0 {
			// Components with zero original replicas can't constrain
			// ratio. Skip them.
			continue
		}
		newPods := projected[c]
		frac[c] = float64(newPods) / float64(orig)
	}
	if len(frac) <= 1 {
		return true
	}
	// Pairwise check: for every (a, b), |frac[a] - frac[b]| <= band.
	type entry struct {
		comp v1beta1.ComponentType
		f    float64
	}
	keys := make([]entry, 0, len(frac))
	for c, f := range frac {
		keys = append(keys, entry{c, f})
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if math.Abs(keys[i].f-keys[j].f) > band {
				return false
			}
		}
	}
	return true
}

// SnapshotOriginal records the desired replica counts at rollout
// start so RatioBalanced has a stable anchor. Caller passes the
// per-Component effective MinReplicas; outputs are int32 because
// ResolveGroup / status writers expect that shape.
//
// Components with MinReplicas == 0 are recorded as 1 — a zero-replica
// Component can't anchor a ratio, and treating zero as one yields the
// same "any surge allowed" behavior as the single-Component path.
func SnapshotOriginal(replicas map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(replicas))
	for c, r := range replicas {
		if r <= 0 {
			r = 1
		}
		out[c] = r
	}
	return out
}
