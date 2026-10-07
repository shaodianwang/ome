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

package preemption

import (
	"context"
	"sort"
	"testing"

	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/watcher"

	"github.com/google/go-cmp/cmp"
)

func TestMultiCubeTransformer(t *testing.T) {
	slice5 := &interfaces.SliceInfo{Name: "s5", Priority: 5, Topology: "2x2x2", ManagedBy: "slice-scheduler"}
	slice6 := &interfaces.SliceInfo{Name: "s6", Priority: 6, Topology: "2x2x4", ManagedBy: "slice-scheduler"}
	slice10 := &interfaces.SliceInfo{Name: "s10", Priority: 10, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice11 := &interfaces.SliceInfo{Name: "s11", Priority: 11, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice20 := &interfaces.SliceInfo{Name: "s20", Priority: 20, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice30 := &interfaces.SliceInfo{Name: "s30", Priority: 30, Topology: "4x4x8", ManagedBy: "slice-scheduler"}
	slice40 := &interfaces.SliceInfo{Name: "s40", Priority: 40, Topology: "4x4x4", ManagedBy: "slice-scheduler"}

	tests := []struct {
		name              string
		numCubesRequested map[string]int
		candidates        []*watcher.PreemptionCandidateSet
		want              []*watcher.PreemptionCandidateSet
	}{
		{
			name:              "no candidates",
			numCubesRequested: map[string]int{"pod1": 1},
			candidates:        []*watcher.PreemptionCandidateSet{},
			want:              []*watcher.PreemptionCandidateSet{},
		},
		{
			name:              "single pod enough cubes",
			numCubesRequested: map[string]int{"pod1": 3},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10}, ToBeAssignedPartitions: []string{"p1"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p2"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice30}, ToBeAssignedPartitions: []string{"p3", "p4"}}, // 2 cubes
			},
			want: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10, slice20, slice30}, ToBeAssignedPartitions: []string{"p1", "p2", "p3", "p4"}},
			},
		},
		{
			name:              "greedy selection lowest priority first",
			numCubesRequested: map[string]int{"pod1": 3},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice40}, ToBeAssignedPartitions: []string{"p1"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10}, ToBeAssignedPartitions: []string{"p2"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p3"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice30}, ToBeAssignedPartitions: []string{"p4", "p5"}}, // 2 cubes
			},
			want: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10, slice20, slice30}, ToBeAssignedPartitions: []string{"p2", "p3", "p4", "p5"}},
			},
		},
		{
			name:              "tie breaking cost and cubes",
			numCubesRequested: map[string]int{"pod1": 2},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p1"}},        // 1 cube, cost 20
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10}, ToBeAssignedPartitions: []string{"p2"}},        // 1 cube, cost 10
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice11}, ToBeAssignedPartitions: []string{"p3"}},        // 1 cube, cost 11
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice5, slice6}, ToBeAssignedPartitions: []string{"p4"}}, // 1 cube, cost 6
			},
			want: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice5, slice6, slice10}, ToBeAssignedPartitions: []string{"p4", "p2"}},
			},
		},
		{
			name:              "subslice cube count",
			numCubesRequested: map[string]int{"pod1": 1},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice5, slice6}, ToBeAssignedPartitions: []string{"p1"}}, // 1 cube
			},
			want: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice5, slice6}, ToBeAssignedPartitions: []string{"p1"}},
			},
		},
		{
			name:              "single pod not enough cubes",
			numCubesRequested: map[string]int{"pod1": 5},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10}, ToBeAssignedPartitions: []string{"p1"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p2"}},       // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice30}, ToBeAssignedPartitions: []string{"p3", "p4"}}, // 2 cubes
			},
			want: []*watcher.PreemptionCandidateSet{},
		},
		{
			name:              "multiple pods",
			numCubesRequested: map[string]int{"pod1": 2, "pod2": 1},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice40}, ToBeAssignedPartitions: []string{"p1-1"}},         // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10}, ToBeAssignedPartitions: []string{"p1-2"}},         // 1 cube
				{PodName: "pod2", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p2-1"}},         // 1 cube
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice30}, ToBeAssignedPartitions: []string{"p1-3", "p1-4"}}, // 2 cubes
			},
			want: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10, slice30}, ToBeAssignedPartitions: []string{"p1-2", "p1-3", "p1-4"}},
				{PodName: "pod2", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p2-1"}},
			},
		},
		{
			name:              "multiple pods not enough for one",
			numCubesRequested: map[string]int{"pod1": 5, "pod2": 1},
			candidates: []*watcher.PreemptionCandidateSet{
				{PodName: "pod1", Slices: []*interfaces.SliceInfo{slice10}, ToBeAssignedPartitions: []string{"p1"}}, // 1 cube
				{PodName: "pod2", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p2"}}, // 1 cube
			},
			want: []*watcher.PreemptionCandidateSet{
				{PodName: "pod2", Slices: []*interfaces.SliceInfo{slice20}, ToBeAssignedPartitions: []string{"p2"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := MultiCubeTransformer(tt.numCubesRequested)
			got := transformer(context.Background(), tt.candidates)

			// Sort slices by name for stable comparison
			sortSlices := func(slices []*interfaces.SliceInfo) {
				if slices == nil {
					return
				}
				sort.Slice(slices, func(i, j int) bool {
					return slices[i].Name < slices[j].Name
				})
			}

			for _, g := range got {
				sortSlices(g.Slices)
			}
			for _, w := range tt.want {
				sortSlices(w.Slices)
			}

			// Sort the outer slice by PodName for stable comparison
			sort.Slice(got, func(i, j int) bool {
				return got[i].PodName < got[j].PodName
			})
			sort.Slice(tt.want, func(i, j int) bool {
				return tt.want[i].PodName < tt.want[j].PodName
			})

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("MultiCubeTransformer() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
