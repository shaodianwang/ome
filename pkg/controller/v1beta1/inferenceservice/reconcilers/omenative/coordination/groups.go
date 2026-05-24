package coordination

import (
	"fmt"
	"strconv"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ResolvedGroup carries the spec-derived view of one coordination
// group, after defaults are applied and the group is indexed.
type ResolvedGroup struct {
	// Name is the stable identifier for the group, derived from the
	// group's position in spec.rolloutCoordination.groups[].
	Name string

	// Index is the group's position in
	// spec.rolloutCoordination.groups[].
	Index int

	// Components is the deduplicated, order-preserving Component set
	// for the group.
	Components []v1beta1.ComponentType

	// Policy is the coordination policy after spec resolution.
	Policy v1beta1.CoordinationPolicy

	// Order pins the in-group sequence. Required for Sequential;
	// optional for Atomic; nil for Independent / PairedMixable.
	Order []v1beta1.ComponentType

	// Pacing is the effective pacing block after defaults are applied.
	// Never nil.
	Pacing v1beta1.CoordinationPacing
}

// ResolveGroups turns spec.rolloutCoordination into a slice of
// ResolvedGroups in declaration order. Returns an empty slice when no
// coordination block is declared.
func ResolveGroups(spec *v1beta1.RolloutCoordinationSpec) []ResolvedGroup {
	if spec == nil || len(spec.Groups) == 0 {
		return nil
	}
	out := make([]ResolvedGroup, 0, len(spec.Groups))
	for i := range spec.Groups {
		out = append(out, resolveGroup(i, &spec.Groups[i]))
	}
	return out
}

// resolveGroup produces one ResolvedGroup from a spec group, applying
// the pacing defaults if the webhook hasn't already filled them in.
func resolveGroup(idx int, g *v1beta1.RolloutCoordinationGroup) ResolvedGroup {
	out := ResolvedGroup{
		Name:       strconv.Itoa(idx),
		Index:      idx,
		Components: dedupComponents(g.Components),
		Policy:     g.Policy,
		Order:      append([]v1beta1.ComponentType(nil), g.Order...),
		Pacing:     pacingWithDefaults(g.Pacing),
	}
	return out
}

// pacingWithDefaults applies the pacing default fills (25% surge,
// 0 unavailable, PerComponent type, 5% ratio tolerance for
// RatioBalanced). Callers can rely on the returned struct having
// every relevant field populated to its documented default.
func pacingWithDefaults(in *v1beta1.CoordinationPacing) v1beta1.CoordinationPacing {
	out := v1beta1.CoordinationPacing{}
	if in != nil {
		out = *in.DeepCopy()
	}
	if out.Type == "" {
		out.Type = v1beta1.CoordinationPacingPerComponent
	}
	if out.MaxSurge == nil {
		out.MaxSurge = ptrIntOrStringFromString("25%")
	}
	if out.MaxUnavailable == nil {
		out.MaxUnavailable = ptrIntOrStringFromInt(0)
	}
	if out.Type == v1beta1.CoordinationPacingRatioBalanced && out.RatioTolerance == nil {
		t := int32(5)
		out.RatioTolerance = &t
	}
	return out
}

// dedupComponents preserves declaration order and removes duplicate
// Components within a single group. Cross-group uniqueness is the
// admission webhook's job; this is the within-group safety belt for
// resolved code paths.
func dedupComponents(in []v1beta1.ComponentType) []v1beta1.ComponentType {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[v1beta1.ComponentType]struct{}, len(in))
	out := make([]v1beta1.ComponentType, 0, len(in))
	for _, c := range in {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

// MembershipFor returns the ResolvedGroup that owns the given Component,
// or (zero, false) when the Component rolls independently.
func MembershipFor(groups []ResolvedGroup, c v1beta1.ComponentType) (ResolvedGroup, bool) {
	for _, g := range groups {
		for _, gc := range g.Components {
			if gc == c {
				return g, true
			}
		}
	}
	return ResolvedGroup{}, false
}

// IsCoordinated reports whether the Component participates in any
// non-Independent group. Independent membership is treated as
// uncoordinated for state-machine routing.
func IsCoordinated(groups []ResolvedGroup, c v1beta1.ComponentType) bool {
	g, ok := MembershipFor(groups, c)
	if !ok {
		return false
	}
	return g.Policy != v1beta1.CoordinationPolicyIndependent
}

// PeerComponents returns the Components in the same group as c, excluding
// c itself. Empty slice when c is uncoordinated or alone in its group.
func PeerComponents(groups []ResolvedGroup, c v1beta1.ComponentType) []v1beta1.ComponentType {
	g, ok := MembershipFor(groups, c)
	if !ok {
		return nil
	}
	peers := make([]v1beta1.ComponentType, 0, len(g.Components))
	for _, peer := range g.Components {
		if peer == c {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

// SequentialPosition reports the 0-based position of c in a Sequential
// group's Order. Returns (-1, false) when the group is not Sequential
// or when c is not in the order. Order is treated as the ground truth
// for Sequential — the webhook ensures order covers every Component in
// components[].
func SequentialPosition(g ResolvedGroup, c v1beta1.ComponentType) (int, bool) {
	if g.Policy != v1beta1.CoordinationPolicySequential {
		return -1, false
	}
	for i, oc := range g.Order {
		if oc == c {
			return i, true
		}
	}
	return -1, false
}

// SequentialPredecessor returns the Component immediately preceding c in
// a Sequential group's Order. Returns "" when c is at position 0 (no
// predecessor) or when the lookup fails.
func SequentialPredecessor(g ResolvedGroup, c v1beta1.ComponentType) v1beta1.ComponentType {
	pos, ok := SequentialPosition(g, c)
	if !ok || pos <= 0 {
		return ""
	}
	return g.Order[pos-1]
}

// SequentialSuccessor returns the Component immediately following c in a
// Sequential group's Order, or "" when c is the last entry / the lookup
// fails.
func SequentialSuccessor(g ResolvedGroup, c v1beta1.ComponentType) v1beta1.ComponentType {
	pos, ok := SequentialPosition(g, c)
	if !ok || pos < 0 || pos >= len(g.Order)-1 {
		return ""
	}
	return g.Order[pos+1]
}

// ValidateGroupShape returns a non-nil error when g is internally
// inconsistent (Sequential without Order, Sequential single-Component,
// etc.). This is the runtime safety net for the admission webhook —
// the controller refuses to act on a group that the webhook would
// have rejected, in case the object was created via a path that
// bypassed validation.
func ValidateGroupShape(g ResolvedGroup) error {
	switch g.Policy {
	case v1beta1.CoordinationPolicyAtomic,
		v1beta1.CoordinationPolicyIndependent,
		v1beta1.CoordinationPolicyPairedMixable,
		v1beta1.CoordinationPolicySequential:
	default:
		return fmt.Errorf("group %s: unknown policy %q", g.Name, g.Policy)
	}

	if len(g.Components) == 0 {
		return fmt.Errorf("group %s: empty components", g.Name)
	}

	if g.Policy == v1beta1.CoordinationPolicyPairedMixable && len(g.Components) < 2 {
		return fmt.Errorf("group %s: PairedMixable requires at least 2 components", g.Name)
	}

	if g.Policy == v1beta1.CoordinationPolicySequential {
		if len(g.Components) < 2 {
			return fmt.Errorf("group %s: Sequential requires at least 2 components", g.Name)
		}
		if len(g.Order) == 0 {
			return fmt.Errorf("group %s: Sequential requires order", g.Name)
		}
		if len(g.Order) != len(g.Components) {
			return fmt.Errorf("group %s: Sequential order must cover every component", g.Name)
		}
		want := make(map[v1beta1.ComponentType]struct{}, len(g.Components))
		for _, c := range g.Components {
			want[c] = struct{}{}
		}
		for _, c := range g.Order {
			if _, ok := want[c]; !ok {
				return fmt.Errorf("group %s: order entry %q is not in components", g.Name, c)
			}
			delete(want, c)
		}
	}
	return nil
}
