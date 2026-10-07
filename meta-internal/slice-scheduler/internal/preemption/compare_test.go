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
	"testing"

	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

func TestLowestMaxPriority(t *testing.T) {
	tests := []struct {
		name string
		a    *watcher.PreemptionCandidateSet
		b    *watcher.PreemptionCandidateSet
		want bool
	}{
		{
			name: "a has lower max priority",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 10, ManagedBy: "slice-scheduler"},
					{Priority: 20, ManagedBy: "slice-scheduler"}, // max 20
				},
			},
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 30, ManagedBy: "slice-scheduler"},
					{Priority: 40, ManagedBy: "slice-scheduler"}, // max 40
				},
			},
			want: true, // 20 < 40
		},
		{
			name: "b has lower max priority",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 30, ManagedBy: "slice-scheduler"},
					{Priority: 40, ManagedBy: "slice-scheduler"}, // max 40
				},
			},
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 10, ManagedBy: "slice-scheduler"},
					{Priority: 20, ManagedBy: "slice-scheduler"}, // max 20
				},
			},
			want: false, // 40 < 20 is false
		},
		{
			name: "equal max priority",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 10, ManagedBy: "slice-scheduler"},
					{Priority: 40, ManagedBy: "slice-scheduler"}, // max 40
				},
			},
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 20, ManagedBy: "slice-scheduler"},
					{Priority: 40, ManagedBy: "slice-scheduler"}, // max 40
				},
			},
			want: false, // 40 < 40 is false
		},
		{
			name: "a is empty, has lower max priority",
			a:    &watcher.PreemptionCandidateSet{}, // max 0
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 10, ManagedBy: "slice-scheduler"}, // max 10
				},
			},
			want: true, // 0 < 10
		},
		{
			name: "b is empty, has lower max priority",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Priority: 10, ManagedBy: "slice-scheduler"}, // max 10
				},
			},
			b:    &watcher.PreemptionCandidateSet{}, // max 0
			want: false,                             // 10 < 0 is false
		},
		{
			name: "both empty",
			a:    &watcher.PreemptionCandidateSet{}, // max 0
			b:    &watcher.PreemptionCandidateSet{}, // max 0
			want: false,                             // 0 < 0 is false
		},
	}

	lessFunc := LowestMaxPriority()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lessFunc(tt.a, tt.b); got != tt.want {
				t.Errorf("LowestMaxPriority() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNumSlices(t *testing.T) {
	tests := []struct {
		name string
		a    *watcher.PreemptionCandidateSet
		b    *watcher.PreemptionCandidateSet
		want bool
	}{
		{
			name: "a has fewer slices",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{{Name: "s1"}},
			},
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{{Name: "s2"}, {Name: "s3"}},
			},
			want: true,
		},
		{
			name: "b has fewer slices",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{{Name: "s1"}, {Name: "s2"}},
			},
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{{Name: "s3"}},
			},
			want: false,
		},
		{
			name: "equal number of slices",
			a: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{{Name: "s1"}},
			},
			b: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{{Name: "s2"}},
			},
			want: false,
		},
	}

	lessFunc := NumSlices()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lessFunc(tt.a, tt.b); got != tt.want {
				t.Errorf("NumSlices() = %v, want %v", got, tt.want)
			}
		})
	}
}
