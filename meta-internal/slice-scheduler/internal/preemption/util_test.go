/*
Copyright 2026.

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
	"testing"

	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

func TestSelectPreemptionCandidate(t *testing.T) {
	tests := []struct {
		name        string
		candidates  []*watcher.PreemptionCandidateSet
		minPriority int
		wantNil     bool
		wantPodName string
	}{
		{
			name:        "nil candidates",
			candidates:  nil,
			minPriority: 50,
			wantNil:     true,
		},
		{
			name:        "empty candidates",
			candidates:  []*watcher.PreemptionCandidateSet{},
			minPriority: 50,
			wantNil:     true,
		},
		{
			name: "single candidate with lower priority",
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices: []*interfaces.SliceInfo{
						{Name: "victim", Priority: 10, ManagedBy: interfaces.SliceSchedulerName},
					},
				},
			},
			minPriority: 50,
			wantNil:     false,
			wantPodName: "pod1",
		},
		{
			name: "single candidate with higher priority (not preemptable)",
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices: []*interfaces.SliceInfo{
						{Name: "victim", Priority: 100, ManagedBy: interfaces.SliceSchedulerName},
					},
				},
			},
			minPriority: 50,
			wantNil:     true,
		},
		{
			name: "single candidate with equal priority (not preemptable)",
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices: []*interfaces.SliceInfo{
						{Name: "victim", Priority: 50, ManagedBy: interfaces.SliceSchedulerName},
					},
				},
			},
			minPriority: 50,
			wantNil:     true,
		},
		{
			name: "multiple candidates, select lowest max priority",
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod-high",
					Slices: []*interfaces.SliceInfo{
						{Name: "v1", Priority: 40, ManagedBy: interfaces.SliceSchedulerName},
					},
				},
				{
					PodName: "pod-low",
					Slices: []*interfaces.SliceInfo{
						{Name: "v2", Priority: 10, ManagedBy: interfaces.SliceSchedulerName},
					},
				},
			},
			minPriority: 50,
			wantNil:     false,
			wantPodName: "pod-low",
		},
		{
			name: "unmanaged slice filtered out",
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices: []*interfaces.SliceInfo{
						{Name: "external", Priority: 10, ManagedBy: "manual"},
					},
				},
			},
			minPriority: 50,
			wantNil:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectPreemptionCandidate(context.Background(), tt.candidates, tt.minPriority)
			if tt.wantNil {
				if got != nil {
					t.Errorf("SelectPreemptionCandidate() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("SelectPreemptionCandidate() = nil, want non-nil")
			}
			if got.PodName != tt.wantPodName {
				t.Errorf("PodName = %q, want %q", got.PodName, tt.wantPodName)
			}
		})
	}
}

func TestSelectMultiCubePreemptionCandidate(t *testing.T) {
	tests := []struct {
		name           string
		candidates     []*watcher.PreemptionCandidateSet
		cubeRequestMap map[string]int
		minPriority    int
		wantNil        bool
		wantPodName    string
	}{
		{
			name:           "nil candidates",
			candidates:     nil,
			cubeRequestMap: map[string]int{"pod1": 2},
			minPriority:    50,
			wantNil:        true,
		},
		{
			name: "valid multi-cube candidate",
			candidates: []*watcher.PreemptionCandidateSet{
				{
					PodName: "pod1",
					Slices: []*interfaces.SliceInfo{
						{Name: "v1", Priority: 10, ManagedBy: interfaces.SliceSchedulerName, Topology: "4x4x4"},
					},
					ToBeAssignedPartitions: []string{"p1"},
				},
			},
			cubeRequestMap: map[string]int{"pod1": 1},
			minPriority:    50,
			wantNil:        false,
			wantPodName:    "pod1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectMultiCubePreemptionCandidate(context.Background(), tt.candidates, tt.cubeRequestMap, tt.minPriority)
			if tt.wantNil {
				if got != nil {
					t.Errorf("SelectMultiCubePreemptionCandidate() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("SelectMultiCubePreemptionCandidate() = nil, want non-nil")
			}
			if got.PodName != tt.wantPodName {
				t.Errorf("PodName = %q, want %q", got.PodName, tt.wantPodName)
			}
		})
	}
}
