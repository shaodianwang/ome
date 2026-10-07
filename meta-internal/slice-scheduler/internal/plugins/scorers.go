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
	"math"
	"sync"

	"tpu.google.com/slice-scheduler/internal/cost"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/util"
)

const (
	// densityWeightFreeRoots is the weight multiplier for free root partitions
	// in the composite density score. This ensures free-root count dominates
	// over total free chip count when comparing placement candidates.
	densityWeightFreeRoots = 10000
)

// PreemptionScorer prefers candidates that require fewer/cheaper preemptions.
type PreemptionScorer struct{}

func (s *PreemptionScorer) Name() string { return "PreemptionScorer" }

func (s *PreemptionScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	return cost.NewPreemptionCost(candidate.Victims)
}

type podDensity struct {
	totalFree int
	freeRoots int
}

// TightestFitScorer prioritizes placements in already fragmented trees to maximize bin-packing.
type TightestFitScorer struct {
	mu    sync.RWMutex
	state interfaces.ClusterState
	cache map[string]podDensity
}

func (s *TightestFitScorer) Name() string { return "TightestFitScorer" }

func (s *TightestFitScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	if len(req.Slices) == 0 {
		return cost.TightestFitCost{}
	}
	sliceReq := req.Slices[0]
	assignedPartitions := candidate.Assignments[sliceReq.SliceName]

	if len(assignedPartitions) == 0 {
		return cost.TightestFitCost{}
	}

	podName := assignedPartitions[0].PodName

	// 1. Calculate Primary Fit Score (Local constraint)
	minFitScore := math.MaxInt
	pathBuf := make([]*interfaces.PartitionInfo, 0, 16)
	for _, targetPart := range assignedPartitions {
		targetID := targetPart.ID
		pathBuf = pathBuf[:0]
		found := false
		for _, pt := range state.GetPartitionTrees() {
			if pt.PodName != podName {
				continue
			}
			if findPath(pt, targetID, &pathBuf) {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		largestFreeAncestor := pathBuf[len(pathBuf)-1]
		for i := len(pathBuf) - 2; i >= 0; i-- {
			if isCompletelyFree(pathBuf[i]) {
				largestFreeAncestor = pathBuf[i]
			} else {
				break
			}
		}
		fit := util.ChipCount(largestFreeAncestor.Topology)
		if fit < minFitScore {
			minFitScore = fit
		}
	}
	// 2. Calculate Pod-level Density Score (Tie-breaker)
	s.mu.RLock()
	stateChanged := s.state != state
	var density podDensity
	var cached bool
	if !stateChanged {
		density, cached = s.cache[podName]
	}
	s.mu.RUnlock()

	if stateChanged || !cached {
		s.mu.Lock()
		if s.state != state {
			s.state = state
			s.cache = make(map[string]podDensity)
		}
		density, cached = s.cache[podName]
		if !cached {
			totalFreeInPod := 0
			freeRootsInPod := 0
			for _, pt := range state.GetPartitionTrees() {
				if pt.PodName == podName {
					chips := countFreeChips(pt)
					totalFreeInPod += chips
					if chips == util.ChipCount(pt.Topology) {
						freeRootsInPod++
					}
				}
			}
			density = podDensity{
				totalFree: totalFreeInPod,
				freeRoots: freeRootsInPod,
			}
			s.cache[podName] = density
		}
		s.mu.Unlock()
	}

	totalFreeInPod := density.totalFree
	freeRootsInPod := density.freeRoots

	return cost.TightestFitCost{
		FitScore:     minFitScore,
		DensityScore: freeRootsInPod*densityWeightFreeRoots + totalFreeInPod,
	}
}

func countFreeChips(node *interfaces.PartitionInfo) int {
	if node == nil {
		return 0
	}
	if isCompletelyFree(node) {
		return util.ChipCount(node.Topology)
	}
	count := 0
	for _, child := range node.Children {
		count += countFreeChips(child)
	}
	return count
}

func isCompletelyFree(node *interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	if node.IsBroken || node.InUseBy != nil {
		return false
	}
	if len(node.Children) == 0 && node.IsOccupied {
		return false
	}

	for _, child := range node.Children {
		if !isCompletelyFree(child) {
			return false
		}
	}
	return true
}

func findPath(node *interfaces.PartitionInfo, targetID string, path *[]*interfaces.PartitionInfo) bool {
	if node == nil {
		return false
	}
	*path = append(*path, node)
	if node.ID == targetID {
		return true
	}
	for _, child := range node.Children {
		if findPath(child, targetID, path) {
			return true
		}
	}
	*path = (*path)[:len(*path)-1]
	return false
}

// getRootDegradationState finds the root partition's degradation state for a given partition ID.
func getRootDegradationState(targetID string, state interfaces.ClusterState) *interfaces.DegradationState {
	pathBuf := make([]*interfaces.PartitionInfo, 0, 16)
	for _, pt := range state.GetPartitionTrees() {
		pathBuf = pathBuf[:0]
		if findPath(pt, targetID, &pathBuf) {
			return pathBuf[0].DegradationState
		}
	}
	return nil
}

// DegradationSinkScorer prefers candidates that use degraded cubes for workloads that tolerate it.
type DegradationSinkScorer struct{}

func (s *DegradationSinkScorer) Name() string { return "DegradationSinkScorer" }

func (s *DegradationSinkScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	if len(req.Slices) == 0 {
		return cost.DegradationCost(0)
	}

	// Determine the most restrictive tolerance and collect assigned partitions
	// only from slices that qualify for degradation scoring.
	// The scorer activates for sub-cube Ironwood OR tolerant workloads.
	var allAssignedPartitions []*interfaces.PartitionInfo
	for _, sliceReq := range req.Slices {
		// OCS degradation only applies to Ironwood (tpu7x).
		if sliceReq.Type != "tpu7x" {
			continue
		}

		isSubCube := util.ChipCount(sliceReq.Topology) < util.ChipsPerCube
		toleratesDegradation := sliceReq.DegradationTolerance == interfaces.DegradationToleranceAll

		if isSubCube || toleratesDegradation {
			allAssignedPartitions = append(allAssignedPartitions, candidate.Assignments[sliceReq.SliceName]...)
		}
	}

	if len(allAssignedPartitions) == 0 {
		return cost.DegradationCost(0)
	}

	// Degradation cost is the number of NORMAL partitions that are used.
	normalCount := 0
	for _, part := range allAssignedPartitions {
		degState := getRootDegradationState(part.ID, state)
		if degState == nil || degState.Mode != interfaces.DegradationModeDegraded {
			normalCount++
		}
	}

	return cost.DegradationCost(normalCount * 1)
}

// MinimizeSwappedCubesScorer prefers candidates that swap/relocate fewer existing partitions.
type MinimizeSwappedCubesScorer struct{}

func (s *MinimizeSwappedCubesScorer) Name() string { return "MinimizeSwappedCubesScorer" }

func (s *MinimizeSwappedCubesScorer) Score(ctx context.Context, req interfaces.GroupAllocationRequest, candidate *interfaces.PlacementCandidate, state interfaces.ClusterState) interfaces.Score {
	currentIDSet := make(map[string]bool)
	for _, sliceReq := range req.Slices {
		for _, id := range sliceReq.CurrentPartitionIDs {
			currentIDSet[id] = true
		}
	}

	totalSwaps := 0
	for _, sliceReq := range req.Slices {
		if len(sliceReq.CurrentPartitionIDs) == 0 {
			continue
		}
		assigned := candidate.Assignments[sliceReq.SliceName]
		for _, part := range assigned {
			if !currentIDSet[part.ID] {
				totalSwaps++
			}
		}
	}
	return cost.SwapCost(totalSwaps)
}
