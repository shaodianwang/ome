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

package testutil

import (
	"tpu.google.com/slice-scheduler/internal/interfaces"
)

// MockClusterState implements interfaces.ClusterState.
type MockClusterState struct {
	PartitionTrees []*interfaces.PartitionInfo
}

func (m *MockClusterState) GetPartitionTrees() []*interfaces.PartitionInfo {
	return m.PartitionTrees
}

func (m *MockClusterState) GetPartitionInfo(ids []string) []*interfaces.PartitionInfo {
	return nil
}

// MockStateBuilder provides a fluent DSL for building complex TPU cluster states.
type MockStateBuilder struct {
	pods map[string][]*nodeBuilder
}

type nodeBuilder struct {
	id               string
	topology         string
	inUseBy          *interfaces.SliceInfo
	isBroken         bool
	state            string
	degradationState *interfaces.DegradationState
	children         []*nodeBuilder
}

func NewStateBuilder() *MockStateBuilder {
	return &MockStateBuilder{
		pods: make(map[string][]*nodeBuilder),
	}
}

// Pod enters a pod context.
func (b *MockStateBuilder) Pod(podName string) *PodContext {
	return &PodContext{builder: b, name: podName}
}

type PodContext struct {
	builder *MockStateBuilder
	name    string
}

// Tree adds a root partition tree to the current pod.
func (pc *PodContext) Tree(topology, id string) *TreeContext {
	nb := &nodeBuilder{id: id, topology: topology}
	pc.builder.pods[pc.name] = append(pc.builder.pods[pc.name], nb)
	return &TreeContext{pod: pc, node: nb, parent: nil}
}

// Pod allows adding/switching to another pod from the pod context.
func (pc *PodContext) Pod(name string) *PodContext {
	return pc.builder.Pod(name)
}

// Build constructs the MockClusterState.
func (pc *PodContext) Build() *MockClusterState {
	return pc.builder.Build()
}

type TreeContext struct {
	pod    *PodContext
	node   *nodeBuilder
	parent *TreeContext
}

// Child adds a child to the current node and enters its context.
func (tc *TreeContext) Child(topology, id string) *TreeContext {
	child := &nodeBuilder{id: id, topology: topology}
	tc.node.children = append(tc.node.children, child)
	return &TreeContext{pod: tc.pod, node: child, parent: tc}
}

// UsedBy marks the current node as used by a slice.
func (tc *TreeContext) UsedBy(name string, priority int, managedBy string) *TreeContext {
	tc.node.inUseBy = &interfaces.SliceInfo{
		Name:         name,
		Priority:     priority,
		ManagedBy:    managedBy,
		Topology:     tc.node.topology,
		PartitionIds: []string{tc.node.id},
	}
	return tc
}

// Broken marks the current node as broken.
func (tc *TreeContext) Broken() *TreeContext {
	tc.node.isBroken = true
	return tc
}

// Degraded marks the current node as degraded with the given OCS domain.
func (tc *TreeContext) Degraded(ocsName string) *TreeContext {
	tc.node.degradationState = &interfaces.DegradationState{
		Mode:             interfaces.DegradationModeDegraded,
		ToleratedOcsName: ocsName,
	}
	tc.node.state = interfaces.PartitionStateDegraded
	return tc
}

// ParentNode returns to the parent node context.
func (tc *TreeContext) ParentNode() *TreeContext {
	return tc.parent
}

// ParentPod returns to the pod context.
func (tc *TreeContext) ParentPod() *PodContext {
	return tc.pod
}

// End returns the builder for finalization.
func (tc *TreeContext) End() *MockStateBuilder {
	return tc.pod.builder
}

// Build constructs the MockClusterState.
func (tc *TreeContext) Build() *MockClusterState {
	return tc.pod.builder.Build()
}

// Build constructs the MockClusterState and applies propagation rules.
func (b *MockStateBuilder) Build() *MockClusterState {
	var roots []*interfaces.PartitionInfo
	for podName, builders := range b.pods {
		for _, nb := range builders {
			root := b.buildRecursive(nb, podName, nil)
			roots = append(roots, &root)
		}
	}
	return &MockClusterState{PartitionTrees: roots}
}

func (b *MockStateBuilder) buildRecursive(nb *nodeBuilder, podName string, parentInUse *interfaces.SliceInfo) interfaces.PartitionInfo {
	// Top-Down Occupancy: If parent is in use, child inherits it.
	currentInUse := nb.inUseBy
	if currentInUse == nil {
		currentInUse = parentInUse
	}

	info := interfaces.PartitionInfo{
		ID:               nb.id,
		Topology:         nb.topology,
		PodName:          podName,
		InUseBy:          currentInUse, // Propagate owner down
		IsBroken:         nb.isBroken,
		State:            nb.state,
		DegradationState: nb.degradationState,
	}

	if nb.degradationState != nil {
		info.State = interfaces.PartitionStateDegraded
	}

	if currentInUse != nil {
		info.IsOccupied = true
		info.IsAvailable = false
	} else {
		info.IsAvailable = !nb.isBroken
	}

	// Recursively build children.
	hasBrokenChild := false
	if len(nb.children) > 0 {
		info.Children = make([]*interfaces.PartitionInfo, len(nb.children))
		for i, childBuilder := range nb.children {
			childInfo := b.buildRecursive(childBuilder, podName, currentInUse)
			if childInfo.IsBroken {
				hasBrokenChild = true
			}
			info.Children[i] = &childInfo
		}
	}

	// Bottom-Up Health: If any child is broken, parent is broken.
	if hasBrokenChild {
		info.IsBroken = true
		info.IsAvailable = false
	}

	return info
}
