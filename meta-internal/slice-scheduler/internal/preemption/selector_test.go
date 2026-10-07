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
	"reflect"
	"sort"
	"testing"

	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

func TestNewSelector(t *testing.T) {
	tests := []struct {
		name           string
		opts           []Option
		wantFilter     int
		wantTransform  bool
		wantComparator bool
	}{
		{
			name:           "no_options",
			opts:           []Option{},
			wantFilter:     0,
			wantComparator: true, // Default policy is not nil
		},
		{
			name: "with_filter",
			opts: []Option{
				WithFilter(BasicFilter(10)),
			},
			wantFilter:     1,
			wantComparator: true,
		},
		{
			name: "with transform",
			opts: []Option{
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 3})),
			},
			wantTransform:  true,
			wantComparator: true,
		},
		{
			name: "with_comparator",
			opts: []Option{
				WithComparator(LowestMaxPriority()),
			},
			wantFilter:     0,
			wantComparator: true,
		},
		{
			name: "with_chained_comparator",
			opts: []Option{
				WithChainedComparator(LowestMaxPriority()),
			},
			wantFilter:     0,
			wantComparator: true,
		},
		{
			name: "with_all",
			opts: []Option{
				WithFilter(BasicFilter(10)),
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 3})),
				WithComparator(LowestMaxPriority()),
			},
			wantFilter:     1,
			wantTransform:  true,
			wantComparator: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSelector(tt.opts...)
			if len(s.filters) != tt.wantFilter {
				t.Errorf("NewSelector() filter count = %v, want %v", len(s.filters), tt.wantFilter)
			}
			if (s.transform != nil) != tt.wantTransform {
				t.Errorf("NewSelector() transform presence = %v, want %v", s.transform != nil, tt.wantTransform)
			}
			if (s.compare != nil) != tt.wantComparator {
				t.Errorf("NewSelector() policy presence = %v, want %v", s.compare != nil, tt.wantComparator)
			}
		})
	}
}

func TestSelector_Select_Subslice(t *testing.T) {
	slice10 := &interfaces.SliceInfo{Name: "s10", Priority: 10, Topology: "4x4", ManagedBy: "slice-scheduler"}
	slice10_1 := &interfaces.SliceInfo{Name: "s10_1", Priority: 10, Topology: "4x4", ManagedBy: "slice-scheduler"}
	slice20 := &interfaces.SliceInfo{Name: "s20", Priority: 20, Topology: "4x8", ManagedBy: "slice-scheduler"}
	slice30 := &interfaces.SliceInfo{Name: "s30", Priority: 30, Topology: "4x4", ManagedBy: "slice-scheduler"}

	tests := []struct {
		name       string
		selector   *Selector
		candidates []*watcher.PreemptionCandidateSet
		want       *watcher.PreemptionCandidateSet
	}{
		{
			name:       "no candidates",
			selector:   NewSelector(),
			candidates: []*watcher.PreemptionCandidateSet{},
			want:       nil,
		},
		{
			name:     "single candidate no filters",
			selector: NewSelector(WithComparator(LowestMaxPriority())),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice10},
			},
		},
		{
			name: "basic filter all pass",
			selector: NewSelector(
				WithFilter(BasicFilter(30)),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10, slice30},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice20},
			},
		},
		{
			name: "basic filter some fail",
			selector: NewSelector(
				WithFilter(BasicFilter(15)),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10, slice20}, // Filtered
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10_1},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod2",
				Slices:  []*interfaces.SliceInfo{slice10_1},
			},
		},
		{
			name: "basic filter all fail",
			selector: NewSelector(
				WithFilter(BasicFilter(5)),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
			},
			want: nil,
		},
		{
			name: "chained policy tie break",
			selector: NewSelector(
				WithChainedComparator(LowestMaxPriority(), func(a, b *watcher.PreemptionCandidateSet) bool {
					// Prefer set with fewer slices as tie breaker
					return len(a.Slices) < len(b.Slices)
				}),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10_1, slice20},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.selector.Select(context.Background(), tt.candidates)

			// Sort slices by name for stable comparison
			sortSlices := func(slices []*interfaces.SliceInfo) {
				if slices == nil {
					return
				}
				sort.Slice(slices, func(i, j int) bool {
					return slices[i].Name < slices[j].Name
				})
			}

			if got != nil {
				sortSlices(got.Slices)
			}
			if tt.want != nil {
				sortSlices(tt.want.Slices)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Selector.Select() = %v, want %v", got, tt.want)
			}
		})
	}

}

func TestSelector_Select_MultiCube(t *testing.T) {
	slice10 := &interfaces.SliceInfo{Name: "s10", Priority: 10, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice10_1 := &interfaces.SliceInfo{Name: "s10_1", Priority: 10, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice11 := &interfaces.SliceInfo{Name: "s11", Priority: 11, Topology: "8x8x8", ManagedBy: "slice-scheduler"}
	slice20 := &interfaces.SliceInfo{Name: "s20", Priority: 20, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice30 := &interfaces.SliceInfo{Name: "s30", Priority: 30, Topology: "4x4x8", ManagedBy: "slice-scheduler"}
	slice35 := &interfaces.SliceInfo{Name: "s35", Priority: 35, Topology: "8x8x8", ManagedBy: "slice-scheduler"}
	slice40 := &interfaces.SliceInfo{Name: "s40", Priority: 40, Topology: "4x4x4", ManagedBy: "slice-scheduler"}
	slice60 := &interfaces.SliceInfo{Name: "s60", Priority: 60, Topology: "2x4x4", ManagedBy: "slice-scheduler"}
	slice5 := &interfaces.SliceInfo{Name: "s5", Priority: 5, Topology: "2x2x2", ManagedBy: "slice-scheduler"}
	slice6 := &interfaces.SliceInfo{Name: "s6", Priority: 6, Topology: "2x2x4", ManagedBy: "slice-scheduler"}
	slice7 := &interfaces.SliceInfo{Name: "s7", Priority: 7, Topology: "2x4x4", ManagedBy: "slice-scheduler"}

	tests := []struct {
		name       string
		selector   *Selector
		candidates []*watcher.PreemptionCandidateSet
		want       *watcher.PreemptionCandidateSet
	}{
		{
			name:       "no candidates",
			selector:   NewSelector(),
			candidates: []*watcher.PreemptionCandidateSet{},
			want:       nil,
		},
		{
			name:     "single pod candidates no filters",
			selector: NewSelector(WithComparator(LowestMaxPriority())),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20},
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice30},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice10},
			},
		},
		{
			name: "basic filter all pass",
			selector: NewSelector(
				WithFilter(BasicFilter(25)),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod2",
				Slices:  []*interfaces.SliceInfo{slice10},
			},
		},
		{
			name: "basic filter some fail",
			selector: NewSelector(
				WithFilter(BasicFilter(15)),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod2",
				Slices:  []*interfaces.SliceInfo{slice10},
			},
		},
		{
			name: "basic filter all fail",
			selector: NewSelector(
				WithFilter(BasicFilter(5)),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
			},
			want: nil,
		},
		{
			name: "multi-cube preemptor single pod candidate",
			selector: NewSelector(
				WithFilter(BasicFilter(50)),
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 3})),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice30}, // 2 cubes
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice40}, // 1 cube

				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice5, slice6}, // 1 cube has two small slices
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice7}, // 1 cube is occupied by another slice
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice5, slice6, slice7, slice10},
			},
		},
		{
			name: "multi-cube preemptor single pod candidate with high priority",
			selector: NewSelector(
				WithFilter(BasicFilter(50)),
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 3})),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice11}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice20}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice30}, // 2 cubes
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice40}, // 1 cube

				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice5, slice60}, // 1 cube has two small slices
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice7}, // 1 cube is occupied by another slice
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice7, slice10, slice11},
			},
		},
		{
			name: "multi-cube preemptor two pods candidates",
			selector: NewSelector(
				WithFilter(BasicFilter(50)),
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 2, "pod2": 3})),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice40}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice30}, // 2 cubes
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10}, // 1 cube
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice35}, // 8 cubes
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice5, slice60}, // 1 cube has two small slices
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice7}, // 1 cube is occupied by another slice
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice30},
			},
		},
		{
			name: "multi-cube preemptor three pods candidates",
			selector: NewSelector(
				WithFilter(BasicFilter(50)),
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 2, "pod2": 3, "pod3": 1})),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice40}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice30}, // 2 cubes
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10}, // 1 cube
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice35}, // 8 cubes
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice5, slice60}, // 1 cube has two small slices
				},
				{
					PodName: "pod3",
					Slices:  []*interfaces.SliceInfo{slice7}, // 1 cube is occupied by another slice
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod3",
				Slices:  []*interfaces.SliceInfo{slice7},
			},
		},
		{
			name: "multi-cube preemptor not enough cubes",
			selector: NewSelector(
				WithFilter(BasicFilter(50)),
				WithTransform(MultiCubeTransformer(map[string]int{"pod1": 4})),
				WithComparator(LowestMaxPriority()),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice40}, // 1 cube
				},
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice30}, // 2 cubes
				},
			},
			want: nil,
		},
		{
			name: "chained policy tie break",
			selector: NewSelector(
				WithChainedComparator(LowestMaxPriority(), func(a, b *watcher.PreemptionCandidateSet) bool {
					// Prefer set with fewer slices as tie breaker
					return len(a.Slices) < len(b.Slices)
				}),
			),
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices:  []*interfaces.SliceInfo{slice10},
				},
				{
					PodName: "pod2",
					Slices:  []*interfaces.SliceInfo{slice10_1, slice20},
				},
			},
			want: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices:  []*interfaces.SliceInfo{slice10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.selector.Select(context.Background(), tt.candidates)

			// Sort slices by name for stable comparison
			sortSlices := func(slices []*interfaces.SliceInfo) {
				if slices == nil {
					return
				}
				sort.Slice(slices, func(i, j int) bool {
					return slices[i].Name < slices[j].Name
				})
			}

			if got != nil {
				sortSlices(got.Slices)
			}
			if tt.want != nil {
				sortSlices(tt.want.Slices)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Selector.Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChain(t *testing.T) {
	s1 := &watcher.PreemptionCandidateSet{Slices: []*interfaces.SliceInfo{{Priority: 10}}}
	s2 := &watcher.PreemptionCandidateSet{Slices: []*interfaces.SliceInfo{{Priority: 20}}}
	s3 := &watcher.PreemptionCandidateSet{Slices: []*interfaces.SliceInfo{{Priority: 10}}}

	comparator1 := LowestMaxPriority()
	comparator2 := func(a, b *watcher.PreemptionCandidateSet) bool {
		// Dummy tie breaker: prefer a if it's s1
		return a == s1
	}

	tests := []struct {
		name        string
		comparators []LessFunc
		a           *watcher.PreemptionCandidateSet
		b           *watcher.PreemptionCandidateSet
		want        bool
	}{
		{
			name:        "first comparator decides true",
			comparators: []LessFunc{comparator1, comparator2},
			a:           s1,
			b:           s2,
			want:        true,
		},
		{
			name:        "first comparator decides false",
			comparators: []LessFunc{comparator1, comparator2},
			a:           s2,
			b:           s1,
			want:        false,
		},
		{
			name:        "second comparator decides true",
			comparators: []LessFunc{comparator1, comparator2},
			a:           s1,
			b:           s3,
			want:        true, // s1 == s1
		},
		{
			name:        "second comparator decides false",
			comparators: []LessFunc{comparator1, comparator2},
			a:           s3,
			b:           s1,
			want:        false, // s3 != s1
		},
		{
			name:        "all tie",
			comparators: []LessFunc{comparator1, comparator2},
			a:           s3,
			b:           s3,
			want:        false,
		},
		{
			name:        "no comparators",
			comparators: []LessFunc{},
			a:           s1,
			b:           s2,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chained := Chain(tt.comparators...)
			if got := chained(tt.a, tt.b); got != tt.want {
				t.Errorf("Chain() = %v, want %v", got, tt.want)
			}
		})
	}
}
