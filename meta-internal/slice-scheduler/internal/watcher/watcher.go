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
	"sync"

	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/util"
)

// defaultDegradationDomain is the placeholder OCS domain used by the Cloud
// adapter. Cloud's CRD does not carry OCS link names, so we use a sentinel
// value to satisfy the scheduler's internal DegradationState contract.
const defaultDegradationDomain = "DEFAULT_DEGRADATION_DOMAIN"

// PartitionWatcher implements interfaces.ClusterWatcher and wraps PartitionCache.
type PartitionWatcher struct {
	cache *PartitionCache
}

// NewPartitionWatcher creates a new PartitionWatcher.
func NewPartitionWatcher(cache *PartitionCache) *PartitionWatcher {
	return &PartitionWatcher{
		cache: cache,
	}
}

// GetState returns a read-only snapshot of the cluster state.
func (pw *PartitionWatcher) GetState() interfaces.ClusterState {
	return &partitionState{
		cache: pw.cache,
	}
}

// Reserve marks partitions as in-use for a list of slices.
func (pw *PartitionWatcher) Reserve(ctx context.Context, slices []*interfaces.SliceInfo) error {
	if len(slices) == 0 {
		return nil
	}

	// Reserve all slices in the group
	reservedSlices := make([]*interfaces.SliceInfo, 0, len(slices))
	for _, sInfo := range slices {
		if ok, existing := pw.cache.ReservePartition(ctx, sInfo); !ok {
			// Rollback: Unreserve what we just reserved
			pw.cache.ReleasePartitions(ctx, reservedSlices)
			return interfaces.NewConflictError(sInfo.Name, sInfo.PartitionIds, existing)
		}
		reservedSlices = append(reservedSlices, sInfo)
	}

	return nil
}

// Unreserve releases reservations for a list of slices.
func (pw *PartitionWatcher) Unreserve(ctx context.Context, slices []*interfaces.SliceInfo) {
	if len(slices) > 0 {
		pw.cache.ReleasePartitions(ctx, slices)
	}
}

// partitionState implements interfaces.ClusterState.
type partitionState struct {
	cache *PartitionCache

	once        sync.Once
	cachedTrees []*interfaces.PartitionInfo
}

// GetPartitionTrees returns the current partition trees from the cache.
func (ps *partitionState) GetPartitionTrees() []*interfaces.PartitionInfo {
	ps.once.Do(func() {
		ps.cache.mu.RLock()
		defer ps.cache.mu.RUnlock()

		result := make([]*interfaces.PartitionInfo, 0, len(ps.cache.partitionTreeCache))
		for _, root := range ps.cache.partitionTreeCache {
			result = append(result, ps.toInterfacePartitionInfo(root, nil))
		}
		ps.cachedTrees = result
	})
	return ps.cachedTrees
}

func (ps *partitionState) GetPartitionInfo(ids []string) []*interfaces.PartitionInfo {
	ps.cache.mu.RLock()
	defer ps.cache.mu.RUnlock()
	result := make([]*interfaces.PartitionInfo, 0, len(ids))
	for _, id := range ids {
		node, ok := ps.cache.partitionNodeCache[id]
		if !ok {
			continue
		}
		result = append(result, ps.toInterfacePartitionInfo(node, nil))
	}
	return result
}

func (ps *partitionState) toInterfacePartitionInfo(node *PartitionNode, parentInUse *interfaces.SliceInfo) *interfaces.PartitionInfo {
	currentInUse := node.InUseBy
	if currentInUse == nil {
		currentInUse = parentInUse
	}

	info := &interfaces.PartitionInfo{
		ID:          node.Info.PartitionID,
		Topology:    util.ToTopology(node.Info.Topology),
		IsAvailable: util.IsPartitionAvailable(node.Info),
		IsOccupied:  util.IsPartitionOccupied(node.Info),
		IsBroken:    node.isBroken(ps.cache.enableNodeReadiness),
		IsCordoned:  node.isCordoned(),
		InUseBy:     currentInUse,
		PodName:     node.PodName,
		State:       string(node.Info.State),
	}

	// Adapter: Cloud's CRD uses State enum ("Degraded"), not a separate
	// DegradationState struct. Translate to the scheduler's internal representation.
	if node.Info.State == partitiontreev1.PartitionStateDegraded {
		info.DegradationState = &interfaces.DegradationState{
			Mode:             interfaces.DegradationModeDegraded,
			ToleratedOcsName: defaultDegradationDomain,
		}
	}

	if len(node.Children) > 0 {
		info.Children = make([]*interfaces.PartitionInfo, len(node.Children))
		for i, child := range node.Children {
			info.Children[i] = ps.toInterfacePartitionInfo(child, currentInUse)
		}
	}
	return info
}
