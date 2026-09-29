package routing

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestSingleRoutingUsesCommittedWinner(t *testing.T) {
	for _, tt := range []struct {
		name   string
		winner string
		move   *v1beta1.PlacementSingleMoveStatus
		want   []string
	}{
		{name: "race candidates are not published"},
		{name: "selection without serving authority is held", move: &v1beta1.PlacementSingleMoveStatus{Selected: "member-b"}},
		{name: "bounded probes stay unpublished", winner: "member-a", move: &v1beta1.PlacementSingleMoveStatus{}, want: []string{"member-a"}},
		{name: "selected replacement can serve alongside winner", winner: "member-a", move: &v1beta1.PlacementSingleMoveStatus{Selected: "member-b"}, want: []string{"member-a", "member-b"}},
		{name: "committed handoff excludes original", winner: "member-b", move: &v1beta1.PlacementSingleMoveStatus{Selected: "member-b"}, want: []string{"member-b"}},
		{name: "cancelled selection excludes replacement", winner: "member-a", move: &v1beta1.PlacementSingleMoveStatus{Selected: "member-a"}, want: []string{"member-a"}},
		{name: "only retained home is published", winner: "member-b", want: []string{"member-b"}},
		{name: "absent winner does not publish loser", winner: "member-c"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := splitISVC([]string{"member-a", "member-b"}, []int32{3, 1}, []int32{3, 1})
			source.Spec.Placement.Mode = v1beta1.PlacementModeSingle
			source.Status.Placement.Cluster = "member-a"
			source.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSingle, Winner: tt.winner, SingleMove: tt.move}
			var got []string
			for _, c := range servingCandidates(source) {
				got = append(got, c.Cluster)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestCancelledSingleSelectionKeepsServingUntilDrain(t *testing.T) {
	for _, tt := range []struct {
		name       string
		pending    bool
		allocation v1beta1.CandidateAllocationStatus
		want       []string
	}{
		{name: "selected home can still back the floor", allocation: v1beta1.CandidateAllocationStatus{CurrentReplicas: 1}, want: []string{"member-a", "member-b"}},
		{name: "allocator authorizes retiring selected home", allocation: v1beta1.CandidateAllocationStatus{CurrentReplicas: 1, DrainRequested: true}, want: []string{"member-a"}},
		{name: "unselected race probe never receives traffic", allocation: v1beta1.CandidateAllocationStatus{CurrentReplicas: 1, RaceCandidate: true}, want: []string{"member-a"}},
		{name: "missing selection cannot publish a retained copy", pending: true, allocation: v1beta1.CandidateAllocationStatus{CurrentReplicas: 1}, want: []string{"member-a"}},
		{name: "retired selection has no positive authority", want: []string{"member-a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := splitISVC([]string{"member-a", "member-b"}, []int32{1, 1}, []int32{1, 1})
			source.Spec.Placement.Mode = v1beta1.PlacementModeSingle
			source.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSingle, Winner: "member-a", SingleMove: &v1beta1.PlacementSingleMoveStatus{Selected: "member-a"}}
			if tt.pending {
				source.Status.Placement.Plan.SingleMove.Selected = ""
			}
			source.Status.Placement.Candidates[1].Allocation = tt.allocation.DeepCopy()
			var got []string
			for _, c := range servingCandidates(source) {
				got = append(got, c.Cluster)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
