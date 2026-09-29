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
		want   []string
	}{
		{name: "race candidates are not published"},
		{name: "only retained home is published", winner: "member-b", want: []string{"member-b"}},
		{name: "absent winner does not publish loser", winner: "member-c"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := splitISVC([]string{"member-a", "member-b"}, []int32{3, 1}, []int32{3, 1})
			source.Spec.Placement.Mode = v1beta1.PlacementModeSingle
			source.Status.Placement.Cluster = "member-a"
			source.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSingle, Winner: tt.winner}
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
