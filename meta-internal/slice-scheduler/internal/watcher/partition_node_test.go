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

	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
)

// ---------------------------------------------------------------------------
// Helpers: build a PartitionNode with common defaults for tests.
// ---------------------------------------------------------------------------
func newPN(id, topology string, state partitiontreev1.PartitionStateType) *PartitionNode {
	return &PartitionNode{
		Info: &partitiontreev1.PartitionInfo{
			PartitionID: id,
			Topology:    topology,
			State:       state,
		},
	}
}

func newHealthyPN(id, topology string) *PartitionNode {
	return newPN(id, topology, partitiontreev1.PartitionStateHealthy)
}

func newLeafPN(id, topology, nodeName string) *PartitionNode {
	return &PartitionNode{
		Info: &partitiontreev1.PartitionInfo{
			PartitionID: id,
			Topology:    topology,
			State:       partitiontreev1.PartitionStateHealthy,
			NodeNames:   []string{nodeName},
		},
		KubeNode: &KubeNode{Name: nodeName, Ready: true},
	}
}

// ---------------------------------------------------------------------------
// Tests: KubeNode.IsReady
// ---------------------------------------------------------------------------
func TestKubeNode_IsReady(t *testing.T) {
	tests := []struct {
		name  string
		ready bool
		want  bool
	}{
		{"Ready node", true, true},
		{"Not ready node", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kn := &KubeNode{Name: "n1", Ready: tt.ready}
			if got := kn.IsReady(); got != tt.want {
				t.Errorf("KubeNode.IsReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: PartitionNode.IsUsed
// ---------------------------------------------------------------------------
func TestPartitionNode_IsUsed(t *testing.T) {
	tests := []struct {
		name    string
		inUseBy *interfaces.SliceInfo
		want    bool
	}{
		{"Not in use", nil, false},
		{"In use", &interfaces.SliceInfo{Name: "slice-1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := newHealthyPN("p1", "4x4")
			pn.InUseBy = tt.inUseBy
			if got := pn.IsUsed(); got != tt.want {
				t.Errorf("IsUsed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isLeafPartitionNode
// ---------------------------------------------------------------------------
func TestIsLeafPartitionNode(t *testing.T) {
	tests := []struct {
		name      string
		nodeNames []string
		want      bool
	}{
		{"Leaf node - single node name", []string{"h1"}, true},
		{"Non-leaf - no node names", nil, false},
		{"Non-leaf - multiple node names", []string{"h1", "h2"}, false},
		{"Non-leaf - empty", []string{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := newHealthyPN("p1", "4x4")
			pn.Info.NodeNames = tt.nodeNames
			if got := pn.isLeafPartitionNode(); got != tt.want {
				t.Errorf("isLeafPartitionNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isKubeNodeSchedulable
// ---------------------------------------------------------------------------
func TestIsKubeNodeSchedulable(t *testing.T) {
	tests := []struct {
		name                string
		pn                  *PartitionNode
		enableNodeReadiness bool
		want                bool
	}{
		{
			name:                "NodeReadiness disabled - always schedulable",
			pn:                  newHealthyPN("p1", "4x4"),
			enableNodeReadiness: false,
			want:                true,
		},
		{
			name:                "NodeReadiness enabled, not leaf - always schedulable",
			pn:                  newHealthyPN("p1", "4x4"), // No NodeNames -> not leaf
			enableNodeReadiness: true,
			want:                true,
		},
		{
			name:                "NodeReadiness enabled, leaf with ready KubeNode",
			pn:                  newLeafPN("p1", "4x4", "h1"),
			enableNodeReadiness: true,
			want:                true,
		},
		{
			name: "NodeReadiness enabled, leaf with unready KubeNode",
			pn: func() *PartitionNode {
				pn := newLeafPN("p1", "4x4", "h1")
				pn.KubeNode.Ready = false
				return pn
			}(),
			enableNodeReadiness: true,
			want:                false,
		},
		{
			name: "NodeReadiness enabled, leaf with nil KubeNode",
			pn: func() *PartitionNode {
				pn := &PartitionNode{
					Info: &partitiontreev1.PartitionInfo{
						PartitionID: "p1",
						Topology:    "4x4",
						State:       partitiontreev1.PartitionStateHealthy,
						NodeNames:   []string{"h1"},
					},
					KubeNode: nil,
				}
				return pn
			}(),
			enableNodeReadiness: true,
			want:                false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pn.isKubeNodeSchedulable(tt.enableNodeReadiness); got != tt.want {
				t.Errorf("isKubeNodeSchedulable(%v) = %v, want %v", tt.enableNodeReadiness, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isBroken
// ---------------------------------------------------------------------------
func TestIsBroken(t *testing.T) {
	tests := []struct {
		name                string
		state               partitiontreev1.PartitionStateType
		enableNodeReadiness bool
		leafNotReady        bool
		want                bool
	}{
		{"Healthy - not broken", partitiontreev1.PartitionStateHealthy, false, false, false},
		{"Unhealthy - broken", partitiontreev1.PartitionStateUnhealthy, false, false, true},
		{"Unknown - broken", partitiontreev1.PartitionStateUnknown, false, false, true},
		{"Degraded - not broken", partitiontreev1.PartitionStateDegraded, false, false, false},
		{"Incomplete - broken", partitiontreev1.PartitionStateIncomplete, false, false, true},
		{"Healthy but node not ready - broken", partitiontreev1.PartitionStateHealthy, true, true, true},
		{"Healthy, node readiness disabled, node not ready - not broken", partitiontreev1.PartitionStateHealthy, false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := newPN("p1", "4x4", tt.state)
			if tt.leafNotReady {
				pn.Info.NodeNames = []string{"h1"}
				pn.KubeNode = &KubeNode{Name: "h1", Ready: false}
			}
			if got := pn.isBroken(tt.enableNodeReadiness); got != tt.want {
				t.Errorf("isBroken(%v) = %v, want %v", tt.enableNodeReadiness, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: isPartitionNodeSchedulable
// ---------------------------------------------------------------------------
func TestIsPartitionNodeSchedulable(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                string
		buildNode           func() *PartitionNode
		enableNodeReadiness bool
		want                bool
	}{
		{
			name: "Healthy, free, no children",
			buildNode: func() *PartitionNode {
				return newHealthyPN("p1", "4x4")
			},
			want: true,
		},
		{
			name: "Unhealthy state",
			buildNode: func() *PartitionNode {
				return newPN("p1", "4x4", partitiontreev1.PartitionStateUnhealthy)
			},
			want: false,
		},
		{
			name: "Unknown state",
			buildNode: func() *PartitionNode {
				return newPN("p1", "4x4", partitiontreev1.PartitionStateUnknown)
			},
			want: false,
		},
		{
			name: "Degraded state - schedulable (degraded is not broken)",
			buildNode: func() *PartitionNode {
				return newPN("p1", "4x4", partitiontreev1.PartitionStateDegraded)
			},
			want: false, // Still not schedulable: isPartitionNodeSchedulable requires Healthy state
		},
		{
			name: "Incomplete state",
			buildNode: func() *PartitionNode {
				return newPN("p1", "4x4", partitiontreev1.PartitionStateIncomplete)
			},
			want: false,
		},
		{
			name: "In use",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.InUseBy = &interfaces.SliceInfo{Name: "slice-1"}
				return pn
			},
			want: false,
		},
		{
			name: "Occupied (CRD)",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.Info.Occupancy = partitiontreev1.OccupancyOccupied
				return pn
			},
			want: false,
		},
		{
			name: "SliceId set",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.Info.SliceId = "some-slice"
				return pn
			},
			want: false,
		},
		{
			name: "Child not schedulable",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "8x8")
				child := newHealthyPN("c1", "4x4")
				child.InUseBy = &interfaces.SliceInfo{Name: "slice-1"}
				pn.Children = []*PartitionNode{child}
				return pn
			},
			want: false,
		},
		{
			name: "All children schedulable",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "8x8")
				pn.Children = []*PartitionNode{
					newHealthyPN("c1", "4x4"),
					newHealthyPN("c2", "4x4"),
				}
				return pn
			},
			want: true,
		},
		{
			name: "Leaf node, readiness enabled, ready",
			buildNode: func() *PartitionNode {
				return newLeafPN("p1", "2x4", "h1")
			},
			enableNodeReadiness: true,
			want:                true,
		},
		{
			name: "Leaf node, readiness enabled, not ready",
			buildNode: func() *PartitionNode {
				pn := newLeafPN("p1", "2x4", "h1")
				pn.KubeNode.Ready = false
				return pn
			},
			enableNodeReadiness: true,
			want:                false,
		},
		{
			name: "Deep tree - all healthy",
			buildNode: func() *PartitionNode {
				gc := newHealthyPN("gc1", "2x2")
				child := newHealthyPN("c1", "4x4")
				child.Children = []*PartitionNode{gc}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{child}
				return root
			},
			want: true,
		},
		{
			name: "Deep tree - grandchild broken",
			buildNode: func() *PartitionNode {
				gc := newPN("gc1", "2x2", partitiontreev1.PartitionStateUnhealthy)
				child := newHealthyPN("c1", "4x4")
				child.Children = []*PartitionNode{gc}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{child}
				return root
			},
			want: false,
		},
		{
			name: "Deep tree - grandchild in use",
			buildNode: func() *PartitionNode {
				gc := newHealthyPN("gc1", "2x2")
				gc.InUseBy = &interfaces.SliceInfo{Name: "deep-slice"}
				child := newHealthyPN("c1", "4x4")
				child.Children = []*PartitionNode{gc}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{child}
				return root
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := tt.buildNode()
			if got := pn.isPartitionNodeSchedulable(ctx, tt.enableNodeReadiness); got != tt.want {
				t.Errorf("isPartitionNodeSchedulable() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: validatePreemptionCandidate
// ---------------------------------------------------------------------------
func TestValidatePreemptionCandidate(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name           string
		buildNode      func() *PartitionNode
		wantValid      bool
		wantSliceCount int
	}{
		{
			name: "Broken node - invalid",
			buildNode: func() *PartitionNode {
				return newPN("p1", "4x4", partitiontreev1.PartitionStateUnhealthy)
			},
			wantValid:      false,
			wantSliceCount: 0,
		},
		{
			name: "In use - valid with slice",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.InUseBy = &interfaces.SliceInfo{Name: "victim-slice", Priority: 10}
				return pn
			},
			wantValid:      true,
			wantSliceCount: 1,
		},
		{
			name: "Not in use, no children - valid, no slices",
			buildNode: func() *PartitionNode {
				return newHealthyPN("p1", "4x4")
			},
			wantValid:      true,
			wantSliceCount: 0,
		},
		{
			name: "Children in use - valid, collects slices",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "8x8")
				c1 := newHealthyPN("c1", "4x4")
				c1.InUseBy = &interfaces.SliceInfo{Name: "v1"}
				c2 := newHealthyPN("c2", "4x4")
				c2.InUseBy = &interfaces.SliceInfo{Name: "v2"}
				pn.Children = []*PartitionNode{c1, c2}
				return pn
			},
			wantValid:      true,
			wantSliceCount: 2,
		},
		{
			name: "One child broken - invalid",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "8x8")
				c1 := newHealthyPN("c1", "4x4")
				c1.InUseBy = &interfaces.SliceInfo{Name: "v1"}
				c2 := newPN("c2", "4x4", partitiontreev1.PartitionStateUnhealthy)
				pn.Children = []*PartitionNode{c1, c2}
				return pn
			},
			wantValid:      false,
			wantSliceCount: 0,
		},
		{
			name: "Nested children - collects all slices",
			buildNode: func() *PartitionNode {
				grandchild := newHealthyPN("gc1", "2x2")
				grandchild.InUseBy = &interfaces.SliceInfo{Name: "deep-victim"}
				child := newHealthyPN("c1", "4x4")
				child.Children = []*PartitionNode{grandchild}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{child}
				return root
			},
			wantValid:      true,
			wantSliceCount: 1,
		},
		{
			name: "Multiple children at different levels - collects from all",
			buildNode: func() *PartitionNode {
				gc := newHealthyPN("gc1", "2x2")
				gc.InUseBy = &interfaces.SliceInfo{Name: "deep-v"}
				c1 := newHealthyPN("c1", "4x4")
				c1.InUseBy = &interfaces.SliceInfo{Name: "v1"}
				c2 := newHealthyPN("c2", "4x4")
				c2.Children = []*PartitionNode{gc}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			wantValid:      true,
			wantSliceCount: 2, // v1 + deep-v
		},
		{
			name: "Deep tree - degraded grandchild (no longer broken, valid for preemption)",
			buildNode: func() *PartitionNode {
				gc := newPN("gc1", "2x2", partitiontreev1.PartitionStateDegraded)
				child := newHealthyPN("c1", "4x4")
				child.Children = []*PartitionNode{gc}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{child}
				return root
			},
			wantValid:      true,
			wantSliceCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := tt.buildNode()
			gotValid, gotSlices := pn.validatePreemptionCandidate(ctx, false)
			if gotValid != tt.wantValid {
				t.Errorf("validatePreemptionCandidate() valid = %v, want %v", gotValid, tt.wantValid)
			}
			if len(gotSlices) != tt.wantSliceCount {
				t.Errorf("validatePreemptionCandidate() slices count = %d, want %d", len(gotSlices), tt.wantSliceCount)
			}
		})
	}
}

// TestValidatePreemptionCandidate_WithNodeReadiness tests node readiness integration.
func TestValidatePreemptionCandidate_WithNodeReadiness(t *testing.T) {
	ctx := context.Background()

	t.Run("Leaf with unready node - invalid", func(t *testing.T) {
		pn := &PartitionNode{
			Info: &partitiontreev1.PartitionInfo{
				PartitionID: "p1",
				Topology:    "2x4",
				State:       partitiontreev1.PartitionStateHealthy,
				NodeNames:   []string{"h1"},
			},
			KubeNode: &KubeNode{Name: "h1", Ready: false},
		}
		valid, _ := pn.validatePreemptionCandidate(ctx, true)
		if valid {
			t.Error("Expected invalid for unready leaf with node readiness enabled")
		}
	})

	t.Run("Leaf with ready node - valid (no slices)", func(t *testing.T) {
		pn := newLeafPN("p1", "2x4", "h1")
		valid, slices := pn.validatePreemptionCandidate(ctx, true)
		if !valid {
			t.Error("Expected valid for ready leaf")
		}
		if len(slices) != 0 {
			t.Errorf("Expected 0 slices for ready unused leaf, got %d", len(slices))
		}
	})

	t.Run("Leaf with ready node and in use - valid with slice", func(t *testing.T) {
		pn := newLeafPN("p1", "2x4", "h1")
		pn.InUseBy = &interfaces.SliceInfo{Name: "victim"}
		valid, slices := pn.validatePreemptionCandidate(ctx, true)
		if !valid {
			t.Error("Expected valid for ready in-use leaf")
		}
		if len(slices) != 1 {
			t.Errorf("Expected 1 slice, got %d", len(slices))
		}
	})
}

// TestValidatePreemptionCandidate_ReturnsCopy verifies that returned slices are copies.
func TestValidatePreemptionCandidate_ReturnsCopy(t *testing.T) {
	ctx := context.Background()
	pn := newHealthyPN("p1", "4x4")
	originalSlice := &interfaces.SliceInfo{Name: "victim", Priority: 42}
	pn.InUseBy = originalSlice

	_, gotSlices := pn.validatePreemptionCandidate(ctx, false)
	if len(gotSlices) != 1 {
		t.Fatalf("Expected 1 slice, got %d", len(gotSlices))
	}
	// Verify it's a copy, not the same pointer
	if gotSlices[0] == originalSlice {
		t.Error("validatePreemptionCandidate returned same pointer, want a copy")
	}
	if gotSlices[0].Name != "victim" {
		t.Errorf("Copy name = %q, want %q", gotSlices[0].Name, "victim")
	}
}

// ---------------------------------------------------------------------------
// Tests: SliceInfo.DeepCopy
// ---------------------------------------------------------------------------
func TestSliceInfoDeepCopy(t *testing.T) {
	tests := []struct {
		name  string
		input *interfaces.SliceInfo
	}{
		{"Nil input", nil},
		{
			"Non-nil input",
			&interfaces.SliceInfo{
				Name:         "s1",
				Priority:     42,
				Topology:     "4x4",
				Type:         "tpu",
				PartitionIds: []string{"p1", "p2"},
				ManagedBy:    "scheduler",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.DeepCopy()
			if tt.input == nil {
				if got != nil {
					t.Errorf("DeepCopy(nil) = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("DeepCopy() = nil, want non-nil")
			}
			if got == tt.input {
				t.Error("DeepCopy() returned same pointer, want a copy")
			}
			if got.Name != tt.input.Name {
				t.Errorf("Name = %q, want %q", got.Name, tt.input.Name)
			}
			if got.Priority != tt.input.Priority {
				t.Errorf("Priority = %d, want %d", got.Priority, tt.input.Priority)
			}
			if got.Topology != tt.input.Topology {
				t.Errorf("Topology = %q, want %q", got.Topology, tt.input.Topology)
			}
			if got.Type != tt.input.Type {
				t.Errorf("Type = %q, want %q", got.Type, tt.input.Type)
			}
			if got.ManagedBy != tt.input.ManagedBy {
				t.Errorf("ManagedBy = %q, want %q", got.ManagedBy, tt.input.ManagedBy)
			}
			// Verify PartitionIds are the same content
			if len(got.PartitionIds) != len(tt.input.PartitionIds) {
				t.Fatalf("PartitionIds length = %d, want %d", len(got.PartitionIds), len(tt.input.PartitionIds))
			}
			for i := range tt.input.PartitionIds {
				if got.PartitionIds[i] != tt.input.PartitionIds[i] {
					t.Errorf("PartitionIds[%d] = %q, want %q", i, got.PartitionIds[i], tt.input.PartitionIds[i])
				}
			}
		})
	}
}

// TestSliceInfoDeepCopy_Independence verifies that modifying the original does not
// affect the copy for scalar fields.
func TestSliceInfoDeepCopy_Independence(t *testing.T) {
	original := &interfaces.SliceInfo{
		Name:         "s1",
		Priority:     10,
		PartitionIds: []string{"p1", "p2"},
	}
	copied := original.DeepCopy()

	// Modify original scalar fields
	original.Name = "modified"
	original.Priority = 999

	// Verify copy is independent for scalars
	if copied.Name != "s1" {
		t.Errorf("Copy Name changed to %q after original modification", copied.Name)
	}
	if copied.Priority != 10 {
		t.Errorf("Copy Priority changed to %d after original modification", copied.Priority)
	}
}

// TestSliceInfoDeepCopy_PartitionIdsIndependence verifies that DeepCopy performs
// a deep copy of PartitionIds (independent slice).
func TestSliceInfoDeepCopy_PartitionIdsIndependence(t *testing.T) {
	original := &interfaces.SliceInfo{
		Name:         "s1",
		PartitionIds: []string{"p1", "p2"},
	}
	copied := original.DeepCopy()
	if copied == nil {
		t.Fatal("DeepCopy() = nil, want non-nil")
	}
	if len(copied.PartitionIds) != 2 {
		t.Fatalf("PartitionIds length = %d, want 2", len(copied.PartitionIds))
	}
	if copied.PartitionIds[0] != "p1" || copied.PartitionIds[1] != "p2" {
		t.Errorf("PartitionIds = %v, want [p1 p2]", copied.PartitionIds)
	}
	// Verify independence: modifying original does not affect copy
	original.PartitionIds[0] = "modified"
	if copied.PartitionIds[0] != "p1" {
		t.Errorf("Copy PartitionIds[0] changed to %q after original modification", copied.PartitionIds[0])
	}
}

// ---------------------------------------------------------------------------
// Tests: isAnySubPartitionNotBroken
// ---------------------------------------------------------------------------
func TestIsAnySubPartitionNotBroken(t *testing.T) {
	tests := []struct {
		name      string
		buildNode func() *PartitionNode
		topology  string
		wantID    string
		wantOK    bool
	}{
		{
			name: "Direct match - healthy",
			buildNode: func() *PartitionNode {
				return newHealthyPN("p1", "4x4")
			},
			topology: "4x4",
			wantID:   "p1",
			wantOK:   true,
		},
		{
			name: "Direct match - broken",
			buildNode: func() *PartitionNode {
				return newPN("p1", "4x4", partitiontreev1.PartitionStateUnhealthy)
			},
			topology: "4x4",
			wantID:   "",
			wantOK:   false,
		},
		{
			name: "Child match",
			buildNode: func() *PartitionNode {
				root := newHealthyPN("root", "8x8")
				child := newHealthyPN("c1", "4x4")
				root.Children = []*PartitionNode{child}
				return root
			},
			topology: "4x4",
			wantID:   "c1",
			wantOK:   true,
		},
		{
			name: "All children broken at target topology",
			buildNode: func() *PartitionNode {
				root := newHealthyPN("root", "8x8")
				c1 := newPN("c1", "4x4", partitiontreev1.PartitionStateUnhealthy)
				c2 := newPN("c2", "4x4", partitiontreev1.PartitionStateUnknown)
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			topology: "4x4",
			wantID:   "",
			wantOK:   false,
		},
		{
			name: "No match at requested topology",
			buildNode: func() *PartitionNode {
				return newHealthyPN("root", "8x8")
			},
			topology: "16x16",
			wantID:   "",
			wantOK:   false,
		},
		{
			name: "Deep nested - grandchild match",
			buildNode: func() *PartitionNode {
				gc := newHealthyPN("gc1", "2x2")
				child := newHealthyPN("c1", "4x4")
				child.Children = []*PartitionNode{gc}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{child}
				return root
			},
			topology: "2x2",
			wantID:   "gc1",
			wantOK:   true,
		},
		{
			name: "First child broken, second healthy - returns second",
			buildNode: func() *PartitionNode {
				root := newHealthyPN("root", "8x8")
				c1 := newPN("c1", "4x4", partitiontreev1.PartitionStateUnhealthy)
				c2 := newHealthyPN("c2", "4x4")
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			topology: "4x4",
			wantID:   "c2",
			wantOK:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := tt.buildNode()
			gotID, gotOK := pn.isAnySubPartitionNotBroken(tt.topology, false)
			if gotOK != tt.wantOK {
				t.Errorf("isAnySubPartitionNotBroken() ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotID != tt.wantID {
				t.Errorf("isAnySubPartitionNotBroken() id = %q, want %q", gotID, tt.wantID)
			}
		})
	}
}

// TestIsAnySubPartitionNotBroken_WithNodeReadiness tests with node readiness enabled.
func TestIsAnySubPartitionNotBroken_WithNodeReadiness(t *testing.T) {
	t.Run("Leaf with unready node - considered broken", func(t *testing.T) {
		pn := &PartitionNode{
			Info: &partitiontreev1.PartitionInfo{
				PartitionID: "leaf1",
				Topology:    "4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				NodeNames:   []string{"h1"},
			},
			KubeNode: &KubeNode{Name: "h1", Ready: false},
		}
		gotID, gotOK := pn.isAnySubPartitionNotBroken("4x4", true)
		if gotOK {
			t.Error("Expected false for unready leaf with readiness enabled")
		}
		if gotID != "" {
			t.Errorf("Expected empty ID, got %q", gotID)
		}
	})

	t.Run("Leaf with ready node - not broken", func(t *testing.T) {
		pn := newLeafPN("leaf1", "4x4", "h1")
		gotID, gotOK := pn.isAnySubPartitionNotBroken("4x4", true)
		if !gotOK {
			t.Error("Expected true for ready leaf with readiness enabled")
		}
		if gotID != "leaf1" {
			t.Errorf("Expected ID 'leaf1', got %q", gotID)
		}
	})
}

// ---------------------------------------------------------------------------
// Tests: findValidPreemptionCandidateSet
// ---------------------------------------------------------------------------
func TestFindValidPreemptionCandidateSet(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name           string
		buildNode      func() *PartitionNode
		topology       string
		podName        string
		wantCount      int
		wantPartitions []string // expected ToBeAssignedPartitions of first result
		wantVictims    []string // expected victim names of first result
	}{
		{
			name: "In-use root at matching topology",
			buildNode: func() *PartitionNode {
				pn := newHealthyPN("p1", "4x4")
				pn.InUseBy = &interfaces.SliceInfo{Name: "victim", PartitionIds: []string{"p1"}}
				return pn
			},
			topology:       "4x4",
			podName:        "pod1",
			wantCount:      1,
			wantPartitions: []string{"p1"},
			wantVictims:    []string{"victim"},
		},
		{
			name: "In-use root at larger topology with healthy sub-partition",
			buildNode: func() *PartitionNode {
				child := newHealthyPN("c1", "4x4")
				root := newHealthyPN("root", "8x8")
				root.InUseBy = &interfaces.SliceInfo{Name: "big-victim", PartitionIds: []string{"root"}}
				root.Children = []*PartitionNode{child}
				return root
			},
			topology:       "4x4",
			podName:        "pod1",
			wantCount:      1,
			wantPartitions: []string{"c1"}, // sub-partition
			wantVictims:    []string{"big-victim"},
		},
		{
			name: "In-use root with broken sub-partition - no candidates",
			buildNode: func() *PartitionNode {
				child := newPN("c1", "4x4", partitiontreev1.PartitionStateUnhealthy)
				root := newHealthyPN("root", "8x8")
				root.InUseBy = &interfaces.SliceInfo{Name: "victim", PartitionIds: []string{"root"}}
				root.Children = []*PartitionNode{child}
				return root
			},
			topology:  "4x4",
			podName:   "pod1",
			wantCount: 0,
		},
		{
			name: "Not in use, matching topology, children in use",
			buildNode: func() *PartitionNode {
				c1 := newHealthyPN("c1", "2x2")
				c1.InUseBy = &interfaces.SliceInfo{Name: "v1"}
				c2 := newHealthyPN("c2", "2x2")
				c2.InUseBy = &interfaces.SliceInfo{Name: "v2"}
				root := newHealthyPN("root", "4x4")
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			topology:       "4x4",
			podName:        "pod1",
			wantCount:      1,
			wantPartitions: []string{"root"},
			wantVictims:    []string{"v1", "v2"},
		},
		{
			name: "Not in use, smaller topology - recurse",
			buildNode: func() *PartitionNode {
				c1 := newHealthyPN("c1", "4x4")
				c1.InUseBy = &interfaces.SliceInfo{Name: "v1"}
				c2 := newHealthyPN("c2", "4x4")
				c2.InUseBy = &interfaces.SliceInfo{Name: "v2"}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			topology:  "4x4",
			podName:   "pod1",
			wantCount: 2, // One candidate per child
		},
		{
			name: "Empty tree - no children, no in-use, non-matching topology",
			buildNode: func() *PartitionNode {
				return newHealthyPN("root", "8x8")
			},
			topology:  "4x4",
			podName:   "pod1",
			wantCount: 0,
		},
		{
			name: "Matching topology, free, no children - valid but no victims",
			buildNode: func() *PartitionNode {
				return newHealthyPN("p1", "4x4")
			},
			topology:  "4x4",
			podName:   "pod1",
			wantCount: 0, // valid but no slices => nil slices => not added
		},
		{
			name: "In-use root with multiple children - one healthy sub-partition",
			buildNode: func() *PartitionNode {
				c1 := newPN("c1", "4x4", partitiontreev1.PartitionStateUnhealthy)
				c2 := newHealthyPN("c2", "4x4")
				root := newHealthyPN("root", "8x8")
				root.InUseBy = &interfaces.SliceInfo{Name: "victim", PartitionIds: []string{"root"}}
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			topology:       "4x4",
			podName:        "pod1",
			wantCount:      1,
			wantPartitions: []string{"c2"}, // c1 is broken, c2 is healthy
			wantVictims:    []string{"victim"},
		},
		{
			// At non-matching topology, recurse children. One child is in-use
			// at the target topology, another is broken — only the in-use one
			// produces a candidate.
			name: "Mixed children at non-matching topology - broken sibling skipped",
			buildNode: func() *PartitionNode {
				c1 := newHealthyPN("c1", "4x4")
				c1.InUseBy = &interfaces.SliceInfo{Name: "v1", PartitionIds: []string{"c1"}}
				c2 := newPN("c2", "4x4", partitiontreev1.PartitionStateUnhealthy)
				c2.InUseBy = &interfaces.SliceInfo{Name: "v2", PartitionIds: []string{"c2"}}
				root := newHealthyPN("root", "8x8")
				root.Children = []*PartitionNode{c1, c2}
				return root
			},
			topology:  "4x4",
			podName:   "pod1",
			wantCount: 1, // Only c1 produces a candidate; c2 is broken
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pn := tt.buildNode()
			got := pn.findValidPreemptionCandidateSet(ctx, tt.topology, tt.podName, false)

			if len(got) != tt.wantCount {
				t.Fatalf("findValidPreemptionCandidateSet() count = %d, want %d", len(got), tt.wantCount)
			}

			if tt.wantCount > 0 && tt.wantPartitions != nil {
				gotParts := got[0].ToBeAssignedPartitions
				if len(gotParts) != len(tt.wantPartitions) {
					t.Fatalf("ToBeAssignedPartitions = %v, want %v", gotParts, tt.wantPartitions)
				}
				for i, wp := range tt.wantPartitions {
					if gotParts[i] != wp {
						t.Errorf("ToBeAssignedPartitions[%d] = %q, want %q", i, gotParts[i], wp)
					}
				}
			}

			if tt.wantCount > 0 && tt.wantVictims != nil {
				gotVictims := got[0].Slices
				if len(gotVictims) != len(tt.wantVictims) {
					t.Fatalf("Slices count = %d, want %d", len(gotVictims), len(tt.wantVictims))
				}
				for i, wv := range tt.wantVictims {
					if gotVictims[i].Name != wv {
						t.Errorf("Slices[%d].Name = %q, want %q", i, gotVictims[i].Name, wv)
					}
				}
			}

			// Verify PodName is always correctly set
			for _, cs := range got {
				if cs.PodName != tt.podName {
					t.Errorf("PodName = %q, want %q", cs.PodName, tt.podName)
				}
			}
		})
	}
}

// TestFindValidPreemptionCandidateSet_WithNodeReadiness tests with node readiness enabled.
func TestFindValidPreemptionCandidateSet_WithNodeReadiness(t *testing.T) {
	ctx := context.Background()

	t.Run("Leaf node not ready - invalid candidate", func(t *testing.T) {
		leaf := &PartitionNode{
			Info: &partitiontreev1.PartitionInfo{
				PartitionID: "leaf1",
				Topology:    "4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				NodeNames:   []string{"h1"},
			},
			KubeNode: &KubeNode{Name: "h1", Ready: false},
			InUseBy:  &interfaces.SliceInfo{Name: "victim"},
		}
		got := leaf.findValidPreemptionCandidateSet(ctx, "4x4", "pod1", true)
		if len(got) != 0 {
			t.Errorf("Expected 0 candidates for unready leaf with readiness enabled, got %d", len(got))
		}
	})

	t.Run("Leaf node ready and in use - valid candidate", func(t *testing.T) {
		leaf := newLeafPN("leaf1", "4x4", "h1")
		leaf.InUseBy = &interfaces.SliceInfo{Name: "victim", PartitionIds: []string{"leaf1"}}
		got := leaf.findValidPreemptionCandidateSet(ctx, "4x4", "pod1", true)
		if len(got) != 1 {
			t.Errorf("Expected 1 candidate for ready in-use leaf, got %d", len(got))
		}
	})
}

// TestFindValidPreemptionCandidateSet_SliceInfoIsCopied verifies that victims are copies.
func TestFindValidPreemptionCandidateSet_SliceInfoIsCopied(t *testing.T) {
	ctx := context.Background()
	original := &interfaces.SliceInfo{Name: "victim", Priority: 10, PartitionIds: []string{"p1"}}
	pn := newHealthyPN("p1", "4x4")
	pn.InUseBy = original

	got := pn.findValidPreemptionCandidateSet(ctx, "4x4", "pod1", false)
	if len(got) != 1 {
		t.Fatalf("Expected 1 candidate, got %d", len(got))
	}
	if len(got[0].Slices) != 1 {
		t.Fatalf("Expected 1 slice, got %d", len(got[0].Slices))
	}
	// Verify it's a copy
	if got[0].Slices[0] == original {
		t.Error("Slice info should be a copy, not the same pointer")
	}
	if got[0].Slices[0].Name != "victim" {
		t.Errorf("Slice name = %q, want %q", got[0].Slices[0].Name, "victim")
	}
}

// TestFindValidPreemptionCandidateSet_ReadinessOnValidatePath tests that node
// readiness is enforced through the validatePreemptionCandidate code path.
func TestFindValidPreemptionCandidateSet_ReadinessOnValidatePath(t *testing.T) {
	ctx := context.Background()

	t.Run("Matching topology, child with unready leaf - invalid", func(t *testing.T) {
		child := newLeafPN("c1", "2x4", "h1")
		child.KubeNode.Ready = false
		child.InUseBy = &interfaces.SliceInfo{Name: "v1"}
		root := newHealthyPN("root", "4x4")
		root.Children = []*PartitionNode{child}

		got := root.findValidPreemptionCandidateSet(ctx, "4x4", "pod1", true)
		if len(got) != 0 {
			t.Errorf("Expected 0 candidates (unready child), got %d", len(got))
		}
	})

	t.Run("Matching topology, child with ready leaf - valid", func(t *testing.T) {
		child := newLeafPN("c1", "2x4", "h1")
		child.InUseBy = &interfaces.SliceInfo{Name: "v1"}
		root := newHealthyPN("root", "4x4")
		root.Children = []*PartitionNode{child}

		got := root.findValidPreemptionCandidateSet(ctx, "4x4", "pod1", true)
		if len(got) != 1 {
			t.Errorf("Expected 1 candidate, got %d", len(got))
		}
	})
}
