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

package watcher

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
)

func TestPartitionWatcher(t *testing.T) {
	pc := NewPartitionCache(false)
	watcher := NewPartitionWatcher(pc)

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root-1",
				Topology:    "TRILLIUM_4_4",
				State:       partitiontreev1.PartitionStateHealthy,
			},
		},
	}
	pc.UpdateTree(context.Background(), tree, nil)

	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Errorf("Expected 1 tree, got %d", len(trees))
	}
	if trees[0].ID != "root-1" {
		t.Errorf("Expected tree ID 'root-1', got %s", trees[0].ID)
	}

	slicesA := []*interfaces.SliceInfo{
		{Name: "slice-a", PartitionIds: []string{"root-1"}, Topology: "4x4"},
	}
	err := watcher.Reserve(context.Background(), slicesA)
	if err != nil {
		t.Errorf("Unexpected error reserving: %v", err)
	}

	// Try reserving again, should fail
	slicesB := []*interfaces.SliceInfo{
		{Name: "slice-b", PartitionIds: []string{"root-1"}, Topology: "4x4"},
	}
	err = watcher.Reserve(context.Background(), slicesB)
	if err == nil {
		t.Error("Expected error reserving already used partition")
	}

	watcher.Unreserve(context.Background(), slicesA)
}

func TestPartitionState_toInterfacePartitionInfo_DegradationState(t *testing.T) {
	tests := []struct {
		name         string
		state        partitiontreev1.PartitionStateType
		wantDegState bool
		wantMode     string
		wantOcsName  string
	}{
		{
			name:         "Degraded state produces DegradationState",
			state:        partitiontreev1.PartitionStateDegraded,
			wantDegState: true,
			wantMode:     interfaces.DegradationModeDegraded,
			wantOcsName:  "DEFAULT_DEGRADATION_DOMAIN",
		},
		{
			name:         "Healthy state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateHealthy,
			wantDegState: false,
		},
		{
			name:         "Unhealthy state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateUnhealthy,
			wantDegState: false,
		},
		{
			name:         "Unknown state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateUnknown,
			wantDegState: false,
		},
		{
			name:         "Incomplete state produces nil DegradationState",
			state:        partitiontreev1.PartitionStateIncomplete,
			wantDegState: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := NewPartitionCache(false)
			watcher := NewPartitionWatcher(pc)

			tree := &partitiontreev1.PartitionTree{
				ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
				Status: partitiontreev1.PartitionTreeStatus{
					Root: &partitiontreev1.PartitionInfo{
						PartitionID: "root-1",
						Topology:    "4x4x4",
						State:       tt.state,
					},
				},
			}
			pc.UpdateTree(context.Background(), tree, nil)

			state := watcher.GetState()
			trees := state.GetPartitionTrees()
			if len(trees) != 1 {
				t.Fatalf("Expected 1 tree, got %d", len(trees))
			}

			info := trees[0]
			if tt.wantDegState {
				if info.DegradationState == nil {
					t.Fatal("Expected non-nil DegradationState")
				}
				if info.DegradationState.Mode != tt.wantMode {
					t.Errorf("DegradationState.Mode = %q, want %q", info.DegradationState.Mode, tt.wantMode)
				}
				if info.DegradationState.ToleratedOcsName != tt.wantOcsName {
					t.Errorf("DegradationState.ToleratedOcsName = %q, want %q", info.DegradationState.ToleratedOcsName, tt.wantOcsName)
				}
			} else {
				if info.DegradationState != nil {
					t.Errorf("Expected nil DegradationState, got %+v", info.DegradationState)
				}
			}
		})
	}
}

func TestPartitionState_toInterfacePartitionInfo_DegradedState(t *testing.T) {
	pc := NewPartitionCache(false)

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root-1",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateDegraded,
			},
		},
	}
	pc.UpdateTree(context.Background(), tree, nil)

	watcher := NewPartitionWatcher(pc)
	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Fatalf("Expected 1 tree, got %d", len(trees))
	}

	info := trees[0]
	if info.DegradationState == nil {
		t.Fatal("Expected DegradationState to be non-nil for Degraded partition")
	}
	if info.DegradationState.Mode != interfaces.DegradationModeDegraded {
		t.Errorf("DegradationState.Mode = %q, want %q", info.DegradationState.Mode, interfaces.DegradationModeDegraded)
	}
	if info.DegradationState.ToleratedOcsName != "DEFAULT_DEGRADATION_DOMAIN" {
		t.Errorf("DegradationState.ToleratedOcsName = %q, want %q", info.DegradationState.ToleratedOcsName, "DEFAULT_DEGRADATION_DOMAIN")
	}
}

func TestPartitionState_toInterfacePartitionInfo_HealthyState_NilDegradation(t *testing.T) {
	pc := NewPartitionCache(false)

	tree := &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "test-tree"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root-1",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
			},
		},
	}
	pc.UpdateTree(context.Background(), tree, nil)

	watcher := NewPartitionWatcher(pc)
	state := watcher.GetState()
	trees := state.GetPartitionTrees()
	if len(trees) != 1 {
		t.Fatalf("Expected 1 tree, got %d", len(trees))
	}

	info := trees[0]
	if info.DegradationState != nil {
		t.Errorf("Expected DegradationState to be nil for Healthy partition, got %+v", info.DegradationState)
	}
}
