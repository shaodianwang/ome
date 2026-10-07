/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugins

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"tpu.google.com/slice-scheduler/internal/cost"
	"tpu.google.com/slice-scheduler/internal/interfaces"
)

type mockState struct {
	trees []*interfaces.PartitionInfo
}

func (m *mockState) GetPartitionTrees() []*interfaces.PartitionInfo {
	return m.trees
}

func (m *mockState) GetPartitionInfo(ids []string) []*interfaces.PartitionInfo {
	return nil
}

func TestPreemptionScorer(t *testing.T) {
	scorer := &PreemptionScorer{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
	}
	state := &mockState{}

	tests := []struct {
		name      string
		candidate *interfaces.PlacementCandidate
		wantZero  bool // true if we expect zero preemption cost
	}{
		{
			name: "No victims",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "p1", Topology: "4x4"}},
				},
				Victims: nil,
			},
			wantZero: true,
		},
		{
			name: "One victim",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "p1", Topology: "4x4"}},
				},
				Victims: []*interfaces.SliceInfo{{Name: "v1", Topology: "4x4", Priority: 10}},
			},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scorer.Score(ctx, req, tt.candidate, state)
			pc, ok := got.(cost.PreemptionCost)
			if tt.wantZero {
				if ok && len(pc) > 0 {
					t.Errorf("PreemptionScorer.Score() expected zero cost, got %v", got)
				}
			} else {
				if !ok || len(pc) == 0 {
					t.Errorf("PreemptionScorer.Score() expected non-zero cost, got %v", got)
				}
			}
		})
	}
}

func TestTightestFitScorer(t *testing.T) {
	scorer := &TightestFitScorer{}
	ctx := context.Background()
	req := interfaces.GroupAllocationRequest{
		Slices: []interfaces.SliceRequirement{{SliceName: "test"}},
	}

	// Setup function to get a fresh tree for basic fit tests.
	getTree := func() *interfaces.PartitionInfo {
		return &interfaces.PartitionInfo{
			ID:       "root",
			Topology: "16x16",
			PodName:  "pod1",
			Children: []*interfaces.PartitionInfo{
				{
					ID:       "child1",
					Topology: "4x4",
					PodName:  "pod1",
					Children: []*interfaces.PartitionInfo{
						{
							ID:       "leaf1",
							Topology: "2x2",
							PodName:  "pod1",
						},
					},
				},
				{
					ID:       "child2",
					Topology: "4x4",
					PodName:  "pod1",
				},
			},
		}
	}

	tests := []struct {
		name      string
		candidate *interfaces.PlacementCandidate
		setupFunc func(*interfaces.PartitionInfo) *interfaces.PartitionInfo
		expected  cost.TightestFitCost
	}{
		{
			name: "Tighter fit (dirty 4x4)",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "leaf1", Topology: "2x2", PodName: "pod1"}},
				},
			},
			setupFunc: func(root *interfaces.PartitionInfo) *interfaces.PartitionInfo {
				// Mark child2 as broken to make root not "completely free".
				root.Children[1].IsBroken = true
				return root
			},
			// Primary Fit: child1 = 16 chips
			// Density: 0 free roots, 16 free chips -> 0*10000 + 16 = 16
			expected: cost.TightestFitCost{FitScore: 16, DensityScore: 16},
		},
		{
			name: "Looser fit (clean root)",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "leaf1", Topology: "2x2", PodName: "pod1"}},
				},
			},
			setupFunc: func(root *interfaces.PartitionInfo) *interfaces.PartitionInfo {
				return root
			},
			// Primary Fit: root = 256 chips
			// Density: 1 free root, 256 free chips -> 1*10000 + 256 = 10256
			expected: cost.TightestFitCost{FitScore: 256, DensityScore: 10256},
		},
		{
			name: "Multi-cube Ironwood tie-break (Pod B)",
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"test": {{ID: "c1-p1-b", Topology: "4x4x4", PodName: "podB"}, {ID: "c1-p2-b", Topology: "4x4x4", PodName: "podB"}},
				},
			},
			setupFunc: func(root *interfaces.PartitionInfo) *interfaces.PartitionInfo {
				return root
			},
			// Pod B root is 4x4x8 (128 chips) with 2 children, all free
			// FitScore: min of each assigned partition's largest free ancestor = podB root (128 chips)
			// DensityScore: 1 free root (podB) * 10000 + 128 free chips = 10128
			expected: cost.TightestFitCost{FitScore: 128, DensityScore: 10128},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state interfaces.ClusterState
			if tt.name == "Multi-cube Ironwood tie-break (Pod B)" {
				// Custom state for the multi-pod density tie-break scenario.
				podA := &interfaces.PartitionInfo{ID: "podA", Topology: "4x4x16", PodName: "podA", Children: []*interfaces.PartitionInfo{
					{ID: "c1-p1", Topology: "4x4x4", PodName: "podA"}, {ID: "c1-p2", Topology: "4x4x4", PodName: "podA"},
					{ID: "c1-p3", Topology: "4x4x4", PodName: "podA"}, {ID: "c1-p4", Topology: "4x4x4", PodName: "podA"},
				}}
				podB := &interfaces.PartitionInfo{ID: "podB", Topology: "4x4x8", PodName: "podB", Children: []*interfaces.PartitionInfo{
					{ID: "c1-p1-b", Topology: "4x4x4", PodName: "podB"}, {ID: "c1-p2-b", Topology: "4x4x4", PodName: "podB"},
				}}
				state = &mockState{trees: []*interfaces.PartitionInfo{podA, podB}}
			} else {
				root := tt.setupFunc(getTree())
				state = &mockState{trees: []*interfaces.PartitionInfo{root}}
			}

			got := scorer.Score(ctx, req, tt.candidate, state)
			tfc, ok := got.(cost.TightestFitCost)
			if !ok {
				t.Fatalf("Expected TightestFitCost, got %T", got)
			}
			if tfc.FitScore != tt.expected.FitScore {
				t.Errorf("FitScore = %d, want %d", tfc.FitScore, tt.expected.FitScore)
			}
			if tfc.DensityScore != tt.expected.DensityScore {
				t.Errorf("DensityScore = %d, want %d", tfc.DensityScore, tt.expected.DensityScore)
			}
		})
	}
}

func TestDegradationSinkScorer(t *testing.T) {
	scorer := &DegradationSinkScorer{}
	ctx := context.Background()

	tests := []struct {
		name     string
		req      interfaces.GroupAllocationRequest
		cand     *interfaces.PlacementCandidate
		state    interfaces.ClusterState
		wantCost cost.DegradationCost
	}{
		{
			name:     "No slices → zero cost",
			req:      interfaces.GroupAllocationRequest{},
			cand:     &interfaces.PlacementCandidate{},
			state:    &mockState{},
			wantCost: 0,
		},
		{
			name: "Non-tpu7x → zero cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "v6e", Topology: "4x4x4", DegradationTolerance: "all"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
			}},
			wantCost: 0,
		},
		{
			name: "tolerance=all, 1 normal partition → cost 1",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "all"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeNormal}},
			}},
			wantCost: 1,
		},
		{
			name: "tolerance=all, 1 degraded partition → cost 0",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "all"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
			}},
			wantCost: 0,
		},
		{
			name: "tolerance=prefer-none treated as none, full-cube → zero cost (not eligible)",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x8", DegradationTolerance: "prefer-none"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}, {ID: "p2", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
				{ID: "p2", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeNormal}},
			}},
			wantCost: 0,
		},
		{
			name: "tolerance=none, full-cube → zero cost (not eligible)",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "4x4x4", DegradationTolerance: "none"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "4x4x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "4x4x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeDegraded}},
			}},
			wantCost: 0,
		},
		{
			name: "sub-cube + any tolerance → eligible, cost per normal partitions",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{{SliceName: "s", Type: "tpu7x", Topology: "2x2x4"}},
			},
			cand: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"s": {{ID: "p1", Topology: "2x2x4"}},
				},
			},
			state: &mockState{trees: []*interfaces.PartitionInfo{
				{ID: "p1", Topology: "2x2x4", DegradationState: &interfaces.DegradationState{Mode: interfaces.DegradationModeNormal}},
			}},
			wantCost: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scorer.Score(ctx, tt.req, tt.cand, tt.state)
			dc, ok := got.(cost.DegradationCost)
			if !ok {
				t.Fatalf("Expected DegradationCost, got %T", got)
			}
			if dc != tt.wantCost {
				t.Errorf("DegradationSinkScorer.Score() = %d, want %d", dc, tt.wantCost)
			}
		})
	}
}

func TestMinimizeSwappedCubesScorer_Score(t *testing.T) {
	testCases := []struct {
		name      string
		req       interfaces.GroupAllocationRequest
		candidate *interfaces.PlacementCandidate
		want      cost.SwapCost
	}{
		{
			name: "New slice (no current partitions) has 0 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: nil},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}, {ID: "p2"}},
				},
			},
			want: cost.SwapCost(0),
		},
		{
			name: "Rescheduling slice with identical partitions has 0 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1", "p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}, {ID: "p2"}},
				},
			},
			want: cost.SwapCost(0),
		},
		{
			name: "Rescheduling slice swapping 1 cube has 1 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1", "p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p1"}, {ID: "p3"}},
				},
			},
			want: cost.SwapCost(1),
		},
		{
			name: "Rescheduling slice relocating completely has 2 swap cost",
			req: interfaces.GroupAllocationRequest{
				Slices: []interfaces.SliceRequirement{
					{SliceName: "slice-1", CurrentPartitionIDs: []string{"p1", "p2"}},
				},
			},
			candidate: &interfaces.PlacementCandidate{
				Assignments: map[string][]*interfaces.PartitionInfo{
					"slice-1": {{ID: "p3"}, {ID: "p4"}},
				},
			},
			want: cost.SwapCost(2),
		},
	}

	scorer := &MinimizeSwappedCubesScorer{}
	if scorer.Name() != "MinimizeSwappedCubesScorer" {
		t.Errorf("unexpected scorer name: %s", scorer.Name())
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := scorer.Score(context.Background(), tc.req, tc.candidate, nil)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Score() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
