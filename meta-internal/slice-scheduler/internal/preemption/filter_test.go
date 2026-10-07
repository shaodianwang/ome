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
	"testing"

	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

func TestBasicFilter(t *testing.T) {
	tests := []struct {
		name              string
		preemptorPriority int
		set               *watcher.PreemptionCandidateSet
		want              bool
	}{
		{
			name:              "nil set",
			preemptorPriority: 50,
			set:               nil,
			want:              false,
		},
		{
			name:              "empty slice candidates",
			preemptorPriority: 50,
			set:               &watcher.PreemptionCandidateSet{PodName: "pod1"},
			want:              false,
		},
		{
			name:              "all candidates lower priority",
			preemptorPriority: 50,
			set: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices: []*interfaces.SliceInfo{
					{Priority: 20, ManagedBy: "slice-scheduler"}, {Priority: 30, ManagedBy: "slice-scheduler"},
				},
			},
			want: true,
		},
		{
			name:              "some candidates higher priority",
			preemptorPriority: 25,
			set: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices: []*interfaces.SliceInfo{
					{Priority: 20, ManagedBy: "slice-scheduler"}, {Priority: 30, ManagedBy: "slice-scheduler"},
				},
			},
			want: false,
		},
		{
			name:              "all candidates higher priority",
			preemptorPriority: 5,
			set: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices: []*interfaces.SliceInfo{
					{Priority: 20, ManagedBy: "slice-scheduler"}, {Priority: 30, ManagedBy: "slice-scheduler"},
				},
			},
			want: false,
		},
		{
			name:              "equal priority",
			preemptorPriority: 5,
			set: &watcher.PreemptionCandidateSet{
				PodName: "pod1",
				Slices: []*interfaces.SliceInfo{
					{Priority: 5, ManagedBy: "slice-scheduler"},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := BasicFilter(tt.preemptorPriority)
			got := filter(context.Background(), tt.set)
			if got != tt.want {
				t.Errorf("BasicFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestManagedBySchedulerFilter(t *testing.T) {
	tests := []struct {
		name string
		set  *watcher.PreemptionCandidateSet
		want bool
	}{
		{
			name: "all managed by scheduler",
			set: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Name: "s1", ManagedBy: "slice-scheduler"},
					{Name: "s2", ManagedBy: "slice-scheduler"},
				},
			},
			want: true,
		},
		{
			name: "some not managed by scheduler",
			set: &watcher.PreemptionCandidateSet{
				Slices: []*interfaces.SliceInfo{
					{Name: "s1", ManagedBy: "slice-scheduler"},
					{Name: "s2", ManagedBy: "other-scheduler"},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := ManagedBySchedulerFilter()
			got := filter(context.Background(), tt.set)
			if got != tt.want {
				t.Errorf("ManagedBySchedulerFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
