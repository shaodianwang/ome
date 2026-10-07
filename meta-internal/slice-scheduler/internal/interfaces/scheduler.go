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

package interfaces

import (
	"context"
	"fmt"
	"sync"
)

// --- Constants ---

const (
	SliceSchedulerName  = "slice-scheduler"
	EvictPodsAnnotation = "cloud.google.com/evict-pods"

	// Boolean string values
	TrueStr  = "true"
	FalseStr = "false"

	// Degradation tolerance modes (from workload annotation)
	DegradationToleranceNone = "none"
	DegradationToleranceAll  = "all"

	// Degradation modes (internal representation, matches Bloom's DegradationModeType)
	DegradationModeNormal   = "NORMAL"
	DegradationModeDegraded = "DEGRADED"

	// Partition states (matches Cloud CRD PartitionStateType)
	PartitionStateDegraded = "Degraded" // NOTE: title-case for Cloud CRD, NOT "DEGRADED"
)

// --- Domain Models ---

// SliceInfo holds information about the slice using a partition.
type SliceInfo struct {
	Name         string
	Priority     int
	Topology     string
	Type         string
	PartitionIds []string
	ManagedBy    string
}

// DeepCopy returns a deep copy of the SliceInfo.
func (s *SliceInfo) DeepCopy() *SliceInfo {
	if s == nil {
		return nil
	}
	out := new(SliceInfo)
	*out = *s
	if s.PartitionIds != nil {
		out.PartitionIds = make([]string, len(s.PartitionIds))
		copy(out.PartitionIds, s.PartitionIds)
	}
	return out
}

// DegradationState represents the degradation status of a partition.
type DegradationState struct {
	Mode             string // "NORMAL", "DEGRADED"
	ToleratedOcsName string
}

// DeepCopy returns a deep copy of the DegradationState.
func (d *DegradationState) DeepCopy() *DegradationState {
	if d == nil {
		return nil
	}
	out := new(DegradationState)
	*out = *d
	return out
}

// PartitionInfo represents a partition and its status in a generic way.
type PartitionInfo struct {
	ID               string
	Topology         string
	IsAvailable      bool // True if the partition itself is available (e.g. HEALTHY/EMPTY)
	IsOccupied       bool // True if the partition is physically occupied
	IsBroken         bool // True if the partition or its nodes are unhealthy/broken
	IsCordoned       bool // True if the partition is a leaf whose node is cordoned (spec.unschedulable)
	InUseBy          *SliceInfo
	PodName          string
	State            string // Raw CRD state for domain fallback
	Children         []*PartitionInfo
	DegradationState *DegradationState
}

// DeepCopy returns a deep copy of the PartitionInfo.
func (p *PartitionInfo) DeepCopy() *PartitionInfo {
	if p == nil {
		return nil
	}
	out := new(PartitionInfo)
	*out = *p
	out.InUseBy = p.InUseBy.DeepCopy()
	if p.Children != nil {
		out.Children = make([]*PartitionInfo, len(p.Children))
		for i, v := range p.Children {
			out.Children[i] = v.DeepCopy()
		}
	}
	out.DegradationState = p.DegradationState.DeepCopy()
	return out
}

// PreemptionCandidateSet represents a set of slices that could be preempted together.
type PreemptionCandidateSet struct {
	Slices                 []*SliceInfo
	PodName                string
	ToBeAssignedPartitions []string
}

// SliceRequirement encapsulates the needs of an individual slice within a group.
type SliceRequirement struct {
	SliceName            string
	Type                 string   // e.g. "tpu7x"
	Topology             string   // e.g. "4x4x4"
	DegradationTolerance string   // "none", "all"
	CurrentPartitionIDs  []string // currently assigned partitions
}

// DeepCopy returns a deep copy of the SliceRequirement.
func (s *SliceRequirement) DeepCopy() *SliceRequirement {
	if s == nil {
		return nil
	}
	out := new(SliceRequirement)
	*out = *s
	if s.CurrentPartitionIDs != nil {
		out.CurrentPartitionIDs = make([]string, len(s.CurrentPartitionIDs))
		copy(out.CurrentPartitionIDs, s.CurrentPartitionIDs)
	}
	return out
}

// SliceGroup represents a group of slices that should be scheduled together.
type SliceGroup struct {
	Name         string                // From slice-group-name annotation
	ExpectedSize int                   // From num-slices annotation
	MinSize      int                   // Minimum size for scheduling (defaults to ExpectedSize)
	Slices       map[string]*SliceInfo // Track slices by their name
	Priority     int32                 // Priority for the group
}

// IsReady returns true if the group has reached its expected size.
func (sg *SliceGroup) IsReady() bool {
	return len(sg.Slices) >= sg.ExpectedSize
}

// GroupAllocationRequest represents the requirements for a slice allocation.
type GroupAllocationRequest struct {
	GroupName string
	Priority  int32
	MinSize   int
	Slices    []SliceRequirement
}

// DeepCopy returns a deep copy of the GroupAllocationRequest.
func (g *GroupAllocationRequest) DeepCopy() *GroupAllocationRequest {
	if g == nil {
		return nil
	}
	out := new(GroupAllocationRequest)
	*out = *g
	if g.Slices != nil {
		out.Slices = make([]SliceRequirement, len(g.Slices))
		for i, v := range g.Slices {
			out.Slices[i] = *v.DeepCopy()
		}
	}
	return out
}

// GroupAllocationResult represents the result of a scheduling attempt.
type GroupAllocationResult struct {
	IsSchedulable bool

	// Candidate is the chosen placement. The orchestrator uses this for reservation.
	Candidate *PlacementCandidate

	FailureReason string
}

// ClusterState represents a read-only snapshot of available resources.
type ClusterState interface {
	// GetPartitionTrees returns the root partitions for all TPU pods in the cluster.
	GetPartitionTrees() []*PartitionInfo
	// GetPartitionInfo returns the PartitionInfo associated with the specified partition IDs.
	GetPartitionInfo(ids []string) []*PartitionInfo
}

// NewConflictError creates a new error representing a reservation conflict.
func NewConflictError(sliceName string, partitions []string, existingSlice string) error {
	return fmt.Errorf("partition was taken during scheduling, sliceName: %s, partitionIds: %v, existingSlice: %s", sliceName, partitions, existingSlice)
}

// --- Component 1: The Watcher ---

// ClusterWatcher maintains a real-time, read-only cache of the cluster state and handles optimistic locking.
type ClusterWatcher interface {
	// GetState returns a thread-safe, read-only snapshot of the cluster
	GetState() ClusterState

	// Reserve optimistically marks resources in the local cache.
	// It should be transactional: if reserving any slice fails, it rolls back (releases)
	// any slices successfully reserved during this call.
	Reserve(ctx context.Context, slices []*SliceInfo) error
	// Unreserve releases reservations for a list of slices.
	Unreserve(ctx context.Context, slices []*SliceInfo)
}

// --- Component 2: The Core Library & Plugins ---

// PlacementOption represents a selected group of physical partitions (cubes)
// and their associated victims inside a single pod, before being mapped to
// a PlacementCandidate.
type PlacementOption struct {
	Partitions []*PartitionInfo
	Victims    []*SliceInfo
}

// PlacementCandidate represents a potential assignment (free or via preemption).
type PlacementCandidate struct {
	// Map of SliceName -> List of assigned partition IDs
	Assignments map[string][]*PartitionInfo

	// The complete list of victim slices to evict to satisfy the entire group
	Victims []*SliceInfo
}

// DeepCopy returns a deep copy of the PlacementCandidate.
func (p *PlacementCandidate) DeepCopy() *PlacementCandidate {
	if p == nil {
		return nil
	}
	out := new(PlacementCandidate)
	if p.Assignments != nil {
		out.Assignments = make(map[string][]*PartitionInfo)
		for k, v := range p.Assignments {
			if v != nil {
				nodes := make([]*PartitionInfo, len(v))
				for i, n := range v {
					nodes[i] = n.DeepCopy()
				}
				out.Assignments[k] = nodes
			}
		}
	}
	if p.Victims != nil {
		out.Victims = make([]*SliceInfo, len(p.Victims))
		for i, v := range p.Victims {
			out.Victims[i] = v.DeepCopy()
		}
	}
	return out
}

// FilterPlugin evaluates a node during tree traversal for early pruning.
type FilterPlugin interface {
	Name() string
	Filter(ctx context.Context, req GroupAllocationRequest, node *PartitionInfo) bool
}

// Score represents a generic score returned by a ScorePlugin.
type Score interface {
	Less(other Score) bool
}

// ScorePlugin ranks final candidates.
type ScorePlugin interface {
	Name() string
	Score(ctx context.Context, req GroupAllocationRequest, candidate *PlacementCandidate, state ClusterState) Score
}

// PlacementGenerator encapsulates TPU topology traversal logic.
type PlacementGenerator interface {
	Name() string
	Generate(ctx context.Context, req GroupAllocationRequest, state ClusterState, eligiblePartitions map[string]struct{}) []*PlacementCandidate
}

// SchedulerLibrary is the entry point for the core logic.
type SchedulerLibrary interface {
	// Name returns the unique identifier for this implementation (e.g., "Default").
	Name() string
	// Schedule calculates the allocation based on the request and current state.
	Schedule(ctx context.Context, req GroupAllocationRequest, state ClusterState) GroupAllocationResult
}

// Registry stores multiple SchedulerLibrary implementations so the controller can pick one.
type Registry struct {
	mu         sync.RWMutex
	schedulers map[string]SchedulerLibrary
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		schedulers: make(map[string]SchedulerLibrary),
	}
}

// Register adds a SchedulerLibrary to the registry.
func (r *Registry) Register(s SchedulerLibrary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schedulers[s.Name()] = s
}

// Get retrieves a SchedulerLibrary from the registry by name.
func (r *Registry) Get(name string) (SchedulerLibrary, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schedulers[name]
	if !ok {
		return nil, fmt.Errorf("scheduler implementation %q not found", name)
	}
	return s, nil
}
