package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func singleProposalFixture() Proposal {
	home := &v1beta1.PlacementHomePolicy{InputDigest: "intent-a", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}}}
	return Proposal{Mode: v1beta1.PlacementModeSingle, InputDigest: "intent-a", Assignments: map[string]v1beta1.CandidateAllocationStatus{
		"member-a": {ClusterUID: "registration-a", CurrentReplicas: 3, DesiredReplicas: 3, CurrentHome: home.DeepCopy(), DesiredHome: home.DeepCopy(), RaceCandidate: true},
		"member-b": {ClusterUID: "registration-b", CurrentReplicas: 3, DesiredReplicas: 3, CurrentHome: home.DeepCopy(), DesiredHome: home.DeepCopy(), RaceCandidate: true},
	}}
}

func chooseSingleWinner(p *Proposal, name string) {
	p.Winner = name
	for cluster, a := range p.Assignments {
		if cluster == name {
			a.RaceCandidate = false
			a.OriginalReplicas = a.DesiredReplicas
		} else {
			a.CurrentReplicas, a.DesiredReplicas = 0, 0
			a.DesiredHome = nil
			a.DrainRequested = true
		}
		p.Assignments[cluster] = a
	}
}

func TestSinglePlanAuthority(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*Proposal)
		wantErr bool
	}{
		{name: "distributed race"},
		{name: "committed winner", edit: func(p *Proposal) { chooseSingleWinner(p, "member-b") }},
		{name: "missing winner", edit: func(p *Proposal) { p.Winner = "member-c" }, wantErr: true},
		{name: "race loser cannot be winner", edit: func(p *Proposal) { p.Winner = "member-a" }, wantErr: true},
		{name: "only winner retains desired authority", edit: func(p *Proposal) {
			p.Winner = "member-a"
			a := p.Assignments["member-a"]
			a.RaceCandidate = false
			p.Assignments["member-a"] = a
		}, wantErr: true},
		{name: "unnominated positive authority", edit: func(p *Proposal) {
			a := p.Assignments["member-a"]
			a.RaceCandidate = false
			p.Assignments["member-a"] = a
		}, wantErr: true},
		{name: "winner cannot belong to All", edit: func(p *Proposal) { chooseSingleWinner(p, "member-a"); p.Mode = v1beta1.PlacementModeAll }, wantErr: true},
		{name: "full floor requires resolved policy", edit: func(p *Proposal) { a := p.Assignments["member-a"]; a.CurrentHome = nil; p.Assignments["member-a"] = a }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := singleProposalFixture()
			if tt.edit != nil {
				tt.edit(&p)
			}
			source := sourceFixture()
			source.Spec.Placement.Mode = p.Mode
			store := storeFixture(t, source, interceptor.Funcs{})
			got, err := store.Persist(t.Context(), source, p)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error %v: %s", err, diff)
			}
			if tt.wantErr {
				return
			}
			if diff := cmp.Diff(p.Winner, got.Status.Placement.Cluster); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(p.Winner, got.Status.Placement.Plan.Winner); diff != "" {
				t.Fatal(diff)
			}
			before := got.DeepCopy()
			got = mustPersist(t, store, got, p)
			if diff := cmp.Diff(before, got); diff != "" {
				t.Fatalf("restart changed authority: %s", diff)
			}
		})
	}
}

func TestSingleWinnerPersistenceReadback(t *testing.T) {
	for _, tt := range []struct {
		name  string
		prune func(*v1beta1.InferenceService)
	}{
		{name: "winner retained"},
		{name: "winner pruned", prune: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Winner = "" }},
		{name: "race permission pruned", prune: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[1].Allocation.RaceCandidate = false }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			source.Spec.Placement.Mode = v1beta1.PlacementModeSingle
			store := storeFixture(t, source, interceptor.Funcs{})
			p := singleProposalFixture()
			race := mustPersist(t, store, source, p)
			chooseSingleWinner(&p, "member-a")
			base := store.Client.(client.WithWatch)
			store.Client = interceptor.NewClient(base, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				copy := obj.(*v1beta1.InferenceService).DeepCopy()
				if tt.prune != nil {
					tt.prune(copy)
				}
				return cl.SubResource(sub).Update(ctx, copy, opts...)
			}})
			got, err := store.Persist(t.Context(), race, p)
			if tt.prune != nil {
				if !errors.Is(err, ErrSchemaUnsupported) || got != nil {
					t.Fatalf("pruned authority accepted: %v, %v", got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(race.Status.Placement.Plan.Revision+1, got.Status.Placement.Plan.Revision); diff != "" {
				t.Fatal(diff)
			}
			if race.Status.Placement.Plan.ID == got.Status.Placement.Plan.ID {
				t.Fatal("winner reused race authority")
			}
			if diff := cmp.Diff(int64(3), got.Status.Placement.Plan.AssignedReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
