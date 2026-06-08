package coordination

import (
	"fmt"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/utils"
)

// ResolvedGroup carries the spec-derived view of one coordination
// group, after defaults are applied and the group is indexed.
type ResolvedGroup struct {
	// Name is the stable identifier for the group, derived from the
	// group's position in spec.rollout.coordination.groups[].
	Name string

	// Index is the group's position in
	// spec.rollout.coordination.groups[].
	Index int

	// Components is the deduplicated, order-preserving Component set
	// for the group.
	Components []v1beta1.ComponentType

	// Policy is the coordination policy after spec resolution.
	Policy v1beta1.CoordinationPolicy

	// Order pins the in-group sequence. Required for Sequential;
	// optional for BlueGreen; nil for Independent / RollingUpdate.
	Order []v1beta1.ComponentType

	// Pacing is the effective pacing block after defaults are applied.
	// Never nil.
	Pacing v1beta1.CoordinationPacing

	// Soak is the duration the Sequential state machine waits after a
	// Component completes its rollout before starting the next
	// Component in Order. Zero means "no wait." Only honored for
	// Policy=Sequential; ignored for every other policy.
	Soak time.Duration
}

// ResolveGroups turns spec.rollout.coordination into a slice of
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
	if g.Soak != nil {
		out.Soak = g.Soak.Duration
	}
	return out
}

// pacingWithDefaults applies the pacing default fills (25% surge,
// 25% unavailable, PerComponent type, 5% ratio tolerance for
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
		out.MaxSurge = utils.PtrIntOrStringFromString("25%")
	}
	if out.MaxUnavailable == nil {
		// 25% matches K8s Deployment convention. A default of 0
		// deadlocks single-replica rollouts because the gate would
		// refuse to take the only pod offline; 25% gives at-least-1
		// budget at any positive replica count via the floor-rounded
		// MaxUnavailableBudget math.
		out.MaxUnavailable = utils.PtrIntOrStringFromString("25%")
	}
	if out.Type == v1beta1.CoordinationPacingRatioBalanced && out.RatioTolerancePercent == nil {
		t := int32(5)
		out.RatioTolerancePercent = &t
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
	case v1beta1.CoordinationPolicyBlueGreen,
		v1beta1.CoordinationPolicyIndependent,
		v1beta1.CoordinationPolicyRollingUpdate,
		v1beta1.CoordinationPolicySequential:
	default:
		return fmt.Errorf("group %s: unknown policy %q", g.Name, g.Policy)
	}

	if len(g.Components) == 0 {
		return fmt.Errorf("group %s: empty components", g.Name)
	}

	if g.Policy == v1beta1.CoordinationPolicyRollingUpdate && len(g.Components) < 2 {
		return fmt.Errorf("group %s: RollingUpdate requires at least 2 components", g.Name)
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
		// RatioBalanced is meaningless for Sequential — only one
		// Component is in flight at a time, so there's no cross-Component
		// ratio to balance. Reject so operators see the conflict at
		// admission instead of getting silently downgraded pacing.
		if g.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced {
			return fmt.Errorf("group %s: Sequential cannot use RatioBalanced pacing — only one Component rolls at a time", g.Name)
		}
	}

	// maxSurge=0 AND maxUnavailable=0 deadlocks any rollout (no surge
	// headroom, no drain headroom). Applies to every policy that uses
	// pacing for per-Component bumps (BlueGreen, Independent,
	// RollingUpdate); Sequential's per-Component pacing inherits the
	// same check.
	if g.Pacing.Type == v1beta1.CoordinationPacingPerComponent {
		if pacingZeroSurge(g.Pacing) && pacingZeroUnavailable(g.Pacing) {
			return fmt.Errorf("group %s: maxSurge=0 AND maxUnavailable=0 deadlocks rollouts — set at least one non-zero", g.Name)
		}
	}
	return nil
}

// pacingZeroSurge reports whether the resolved pacing's MaxSurge
// computes to 0 for any non-zero replica count (i.e., MaxSurge is
// literally 0 or 0%, not just a percent that rounds to 0 at low
// replica counts).
func pacingZeroSurge(p v1beta1.CoordinationPacing) bool {
	if p.MaxSurge == nil {
		return false
	}
	if p.MaxSurge.Type == intstr.Int {
		return p.MaxSurge.IntValue() == 0
	}
	return p.MaxSurge.StrVal == "0%" || p.MaxSurge.StrVal == "0"
}

// pacingZeroUnavailable mirrors pacingZeroSurge for MaxUnavailable.
// Nil is treated as zero defensively; in practice the resolved group
// has defaults applied via pacingWithDefaults before this is called.
func pacingZeroUnavailable(p v1beta1.CoordinationPacing) bool {
	if p.MaxUnavailable == nil {
		return true
	}
	if p.MaxUnavailable.Type == intstr.Int {
		return p.MaxUnavailable.IntValue() == 0
	}
	return p.MaxUnavailable.StrVal == "0%" || p.MaxUnavailable.StrVal == "0"
}
