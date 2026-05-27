package coordination

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestResolveGroups_NilSpecReturnsNil(t *testing.T) {
	if got := ResolveGroups(nil); got != nil {
		t.Errorf("nil spec: got %+v want nil", got)
	}
}

func TestResolveGroups_EmptyGroupsReturnsNil(t *testing.T) {
	if got := ResolveGroups(&v1beta1.RolloutCoordinationSpec{}); got != nil {
		t.Errorf("empty Groups: got %+v want nil", got)
	}
}

func TestResolveGroups_AppliesPacingDefaults(t *testing.T) {
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
			},
		},
	}
	groups := ResolveGroups(spec)
	if len(groups) != 1 {
		t.Fatalf("groups: got %d want 1", len(groups))
	}
	g := groups[0]
	if g.Name != "0" {
		t.Errorf("Name: got %q want 0", g.Name)
	}
	if g.Pacing.Type != v1beta1.CoordinationPacingPerComponent {
		t.Errorf("Pacing.Type default: got %q want PerComponent", g.Pacing.Type)
	}
	if g.Pacing.MaxSurge == nil || g.Pacing.MaxSurge.StrVal != "25%" {
		t.Errorf("Pacing.MaxSurge default: got %+v want 25%%", g.Pacing.MaxSurge)
	}
	if g.Pacing.MaxUnavailable == nil || g.Pacing.MaxUnavailable.IntValue() != 0 {
		t.Errorf("Pacing.MaxUnavailable default: got %+v want 0", g.Pacing.MaxUnavailable)
	}
}

func TestResolveGroups_RatioBalancedDefaultsTolerance(t *testing.T) {
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type: v1beta1.CoordinationPacingRatioBalanced,
				},
			},
		},
	}
	g := ResolveGroups(spec)[0]
	if g.Pacing.RatioTolerance == nil {
		t.Fatalf("RatioTolerance default: got nil want 5")
	}
	if *g.Pacing.RatioTolerance != 5 {
		t.Errorf("RatioTolerance default: got %d want 5", *g.Pacing.RatioTolerance)
	}
}

func TestResolveGroups_DedupComponentsWithinGroup(t *testing.T) {
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent,
					v1beta1.EngineComponent,
					v1beta1.DecoderComponent,
				},
				Policy: v1beta1.CoordinationPolicyBlueGreen,
			},
		},
	}
	g := ResolveGroups(spec)[0]
	if len(g.Components) != 2 {
		t.Errorf("dedup: got %d components want 2", len(g.Components))
	}
	if g.Components[0] != v1beta1.EngineComponent || g.Components[1] != v1beta1.DecoderComponent {
		t.Errorf("order preserved: got %v", g.Components)
	}
}

func TestResolveGroups_PreservesUserSuppliedPacing(t *testing.T) {
	maxSurge := intstr.FromInt(5)
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyIndependent,
				Pacing: &v1beta1.CoordinationPacing{
					Type:     v1beta1.CoordinationPacingPerComponent,
					MaxSurge: &maxSurge,
				},
			},
		},
	}
	g := ResolveGroups(spec)[0]
	if g.Pacing.MaxSurge.IntValue() != 5 {
		t.Errorf("MaxSurge preserved: got %+v want 5", g.Pacing.MaxSurge)
	}
}

func TestMembershipFor_FindsComponentsAcrossGroups(t *testing.T) {
	groups := ResolveGroups(&v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Policy: v1beta1.CoordinationPolicyIndependent},
		},
	})
	if g, ok := MembershipFor(groups, v1beta1.EngineComponent); !ok || g.Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("engine: got (%+v, %v) want BlueGreen", g, ok)
	}
	if g, ok := MembershipFor(groups, v1beta1.RouterComponent); !ok || g.Policy != v1beta1.CoordinationPolicyIndependent {
		t.Errorf("router: got (%+v, %v) want Independent", g, ok)
	}
}

func TestMembershipFor_UnknownComponentReturnsFalse(t *testing.T) {
	groups := ResolveGroups(&v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	})
	if _, ok := MembershipFor(groups, v1beta1.DecoderComponent); ok {
		t.Errorf("decoder should not be a member")
	}
}

func TestIsCoordinated(t *testing.T) {
	groups := ResolveGroups(&v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Policy: v1beta1.CoordinationPolicyIndependent},
		},
	})
	if !IsCoordinated(groups, v1beta1.EngineComponent) {
		t.Errorf("engine should be coordinated (BlueGreen)")
	}
	if IsCoordinated(groups, v1beta1.RouterComponent) {
		t.Errorf("router under Independent is treated as uncoordinated")
	}
	if IsCoordinated(groups, v1beta1.DecoderComponent) {
		t.Errorf("decoder is not in any group")
	}
}

func TestPeerComponents(t *testing.T) {
	groups := ResolveGroups(&v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen},
		},
	})
	peers := PeerComponents(groups, v1beta1.EngineComponent)
	if len(peers) != 2 {
		t.Fatalf("engine peers: got %d want 2", len(peers))
	}
	wantPeers := map[v1beta1.ComponentType]bool{v1beta1.DecoderComponent: true, v1beta1.RouterComponent: true}
	for _, p := range peers {
		if !wantPeers[p] {
			t.Errorf("unexpected peer: %s", p)
		}
	}
}

func TestPeerComponents_Uncoordinated(t *testing.T) {
	peers := PeerComponents(nil, v1beta1.EngineComponent)
	if peers != nil {
		t.Errorf("uncoordinated: got %v want nil", peers)
	}
}

func TestSequentialPosition(t *testing.T) {
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	if pos, ok := SequentialPosition(g, v1beta1.DecoderComponent); !ok || pos != 0 {
		t.Errorf("decoder: got (%d, %v) want (0, true)", pos, ok)
	}
	if pos, ok := SequentialPosition(g, v1beta1.EngineComponent); !ok || pos != 1 {
		t.Errorf("engine: got (%d, %v) want (1, true)", pos, ok)
	}
	if _, ok := SequentialPosition(g, v1beta1.RouterComponent); ok {
		t.Errorf("router should not be in the order")
	}
}

func TestSequentialPosition_NotSequentialPolicy(t *testing.T) {
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicyBlueGreen,
		Order:  []v1beta1.ComponentType{v1beta1.EngineComponent},
	}
	if _, ok := SequentialPosition(g, v1beta1.EngineComponent); ok {
		t.Errorf("non-Sequential policy should return false")
	}
}

func TestSequentialPredecessor(t *testing.T) {
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	if got := SequentialPredecessor(g, v1beta1.EngineComponent); got != v1beta1.DecoderComponent {
		t.Errorf("engine predecessor: got %q want decoder", got)
	}
	if got := SequentialPredecessor(g, v1beta1.DecoderComponent); got != "" {
		t.Errorf("decoder (first) predecessor: got %q want empty", got)
	}
}

func TestSequentialSuccessor(t *testing.T) {
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	if got := SequentialSuccessor(g, v1beta1.DecoderComponent); got != v1beta1.EngineComponent {
		t.Errorf("decoder successor: got %q want engine", got)
	}
	if got := SequentialSuccessor(g, v1beta1.EngineComponent); got != "" {
		t.Errorf("engine (last) successor: got %q want empty", got)
	}
}

func TestValidateGroupShape_BlueGreen(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	if err := ValidateGroupShape(g); err != nil {
		t.Errorf("BlueGreen two-Component: unexpected error: %v", err)
	}
}

func TestValidateGroupShape_RejectsEmptyComponents(t *testing.T) {
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicyBlueGreen,
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("empty Components: want error")
	}
}

func TestValidateGroupShape_RollingUpdateNeedsAtLeastTwo(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicyRollingUpdate,
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("RollingUpdate single-Component: want error")
	}
}

func TestValidateGroupShape_SequentialRequiresOrder(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential without Order: want error")
	}
}

func TestValidateGroupShape_SequentialOrderMustCoverComponents(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential partial Order: want error")
	}
}

func TestValidateGroupShape_SequentialOrderMustNotIntroduceForeignComponents(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent},
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential Order with foreign Component: want error")
	}
}

func TestValidateGroupShape_RejectsUnknownPolicy(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicy("Bogus"),
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("unknown policy: want error")
	}
}

func TestValidateGroupShape_SequentialSingleComponent(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
	}
	if err := ValidateGroupShape(g); err == nil {
		t.Errorf("Sequential single-Component: want error")
	}
}
