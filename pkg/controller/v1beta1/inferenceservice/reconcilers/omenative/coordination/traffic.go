package coordination

import (
	"sort"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// RevisionWeight is the (per-revision Service name, percent, tag) tuple
// that the coordination layer materializes into one
// ComponentTrafficTarget on Status.Components.<c>.Traffic[].
type RevisionWeight struct {
	// RevisionHash is the underlying ControllerRevision hash. The
	// writer converts this into the per-revision Service name via
	// PerRevisionServiceName.
	RevisionHash string

	// Percent is the traffic percentage routed to this revision. Sum
	// across all weights for one Component must be 100.
	Percent int32

	// Tag is a short human-readable tag for the revision ("latest",
	// "prev"). Cosmetic.
	Tag string

	// LatestRevision marks this entry as the LatestRolledoutRevision
	// for the Component.
	LatestRevision bool
}

// BuildTrafficTargets converts a sorted weight list into the
// Status.Components.<c>.Traffic[] entries the HTTPRoute weighted-
// backendRef consumer reads. The writer fills RevisionName with the
// per-revision Service name (`<isvc>-<component>-rev-<hash>`) so the
// consumer can use it directly as a backend reference.
func BuildTrafficTargets(isvcName string, component v1beta1.ComponentType, weights []RevisionWeight) []v1beta1.ComponentTrafficTarget {
	if len(weights) == 0 {
		return nil
	}
	out := make([]v1beta1.ComponentTrafficTarget, 0, len(weights))
	for _, w := range weights {
		if w.Percent <= 0 || w.RevisionHash == "" {
			continue
		}
		out = append(out, v1beta1.ComponentTrafficTarget{
			RevisionName:   PerRevisionServiceName(isvcName, component, w.RevisionHash),
			Percent:        w.Percent,
			Tag:            w.Tag,
			LatestRevision: w.LatestRevision,
		})
	}
	return out
}

// SortedWeights returns weights sorted by descending Percent, breaking
// ties by RevisionHash, so the producer writes a stable slice across
// reconciles (a no-op write doesn't bounce the apiserver).
func SortedWeights(weights []RevisionWeight) []RevisionWeight {
	out := append([]RevisionWeight(nil), weights...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Percent != out[j].Percent {
			return out[i].Percent > out[j].Percent
		}
		return out[i].RevisionHash < out[j].RevisionHash
	})
	return out
}

// NormalizeWeights snaps a slice of revision weights so the values sum
// to 100 even when float division round-off would produce 99 or 101.
// The largest entry absorbs the residual to minimize visible drift.
//
// Inputs with sum == 0 are returned unchanged; the caller is expected
// to filter out empty traffic before invoking this.
func NormalizeWeights(in []RevisionWeight) []RevisionWeight {
	if len(in) == 0 {
		return in
	}
	total := int32(0)
	maxIdx := 0
	for i, w := range in {
		total += w.Percent
		if w.Percent > in[maxIdx].Percent {
			maxIdx = i
		}
	}
	if total == 0 {
		return in
	}
	delta := int32(100) - total
	if delta == 0 {
		return in
	}
	out := append([]RevisionWeight(nil), in...)
	out[maxIdx].Percent += delta
	if out[maxIdx].Percent < 0 {
		// Pathological input — clamp at zero so the consumer
		// doesn't see a negative weight.
		out[maxIdx].Percent = 0
	}
	return out
}

// ComputeWeightsFromPods produces a RevisionWeight slice from observed
// per-revision pod counts. New-vs-old apportioning is the natural
// "what fraction of capacity is the new revision" — equivalent to the
// OEP's producer contract.
//
// totals must be the total live pod count for the Component (any
// revision); a zero total returns nil.
//
// latestHash, when non-empty, tags the matching weight as
// LatestRevision=true.
func ComputeWeightsFromPods(perRevisionPods map[string]int32, total int32, latestHash string) []RevisionWeight {
	if total <= 0 || len(perRevisionPods) == 0 {
		return nil
	}
	out := make([]RevisionWeight, 0, len(perRevisionPods))
	for hash, count := range perRevisionPods {
		if hash == "" || count <= 0 {
			continue
		}
		pct := int32((int64(count) * 100) / int64(total))
		out = append(out, RevisionWeight{
			RevisionHash:   hash,
			Percent:        pct,
			LatestRevision: hash == latestHash,
		})
	}
	if len(out) == 0 {
		return nil
	}
	out = SortedWeights(out)
	out = NormalizeWeights(out)
	return out
}

// TrafficDiffersMeaningfully reports whether the desired traffic
// targets differ from the observed ones in a way the controller should
// write. Used to avoid no-op status writes that would bounce the
// apiserver and add reconcile pressure.
//
// The comparison is set-aware on RevisionName + Percent and ignores
// Tag and LatestRevision drift (those are cosmetic).
func TrafficDiffersMeaningfully(desired, observed []v1beta1.ComponentTrafficTarget) bool {
	if len(desired) != len(observed) {
		return true
	}
	want := make(map[string]int32, len(desired))
	for _, t := range desired {
		want[t.RevisionName] = t.Percent
	}
	for _, t := range observed {
		if w, ok := want[t.RevisionName]; !ok || w != t.Percent {
			return true
		}
	}
	return false
}
