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

	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/util"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	ManagedByAnnotation = "cloud.google.com/managed-by"
)

func (kn *KubeNode) IsReady() bool {
	return kn.Ready
}

// Returns true if the k8s node is schedulable.
func (pn *PartitionNode) isKubeNodeSchedulable(enableNodeReadiness bool) bool {
	if enableNodeReadiness && pn.isLeafPartitionNode() {
		return pn.KubeNode != nil && pn.KubeNode.IsReady()
	}
	return true
}

// Returns true if the partition node is schedulable.
// A partition node is schedulable iff:
//  1. If the partition node's corresponding partition is available,
//     meaning a same shape slice can be created on this partition, and
//  2. None of the descendant partition node is in use, and
//  3. All the leaf partition nodes' kube node are in ready status.
func (pn *PartitionNode) isPartitionNodeSchedulable(ctx context.Context, enableNodeReadiness bool) bool {
	logf := log.FromContext(ctx)
	if pn.Info.State != partitiontreev1.PartitionStateHealthy {
		logf.V(2).Info("Partition node not schedulable: state is not Healthy", "partitionID", pn.Info.PartitionID, "state", pn.Info.State)
		return false
	}
	if pn.IsUsed() {
		logf.V(2).Info("Partition node not schedulable: already in use in cache", "partitionID", pn.Info.PartitionID, "inUseBy", pn.InUseBy.Name)
		return false
	}
	if pn.Info.Occupancy == partitiontreev1.OccupancyOccupied {
		logf.V(2).Info("Partition node not schedulable: occupied according to CRD", "partitionID", pn.Info.PartitionID, "sliceId", pn.Info.SliceId)
		return false
	}
	if pn.Info.SliceId != "" {
		logf.V(2).Info("Partition node not schedulable: SliceId set in CRD", "partitionID", pn.Info.PartitionID, "sliceId", pn.Info.SliceId)
		return false
	}
	if !pn.isKubeNodeSchedulable(enableNodeReadiness) {
		logf.V(2).Info("Partition node not schedulable: KubeNode not schedulable", "partitionID", pn.Info.PartitionID)
		return false
	}
	for _, child := range pn.Children {
		if !child.isPartitionNodeSchedulable(ctx, enableNodeReadiness) {
			logf.V(2).Info("Partition node not schedulable: child not schedulable", "partitionID", pn.Info.PartitionID, "childID", child.Info.PartitionID)
			return false
		}
	}
	return true
}

// Returns true if the partition is a leaf whose k8s node is cordoned. Unlike
// a node that is not ready, a cordoned node keeps the slices it already runs,
// so this never makes a partition broken.
func (pn *PartitionNode) isCordoned() bool {
	return pn.isLeafPartitionNode() && pn.KubeNode != nil && pn.KubeNode.Unschedulable
}

func (p *PartitionNode) isLeafPartitionNode() bool {
	return len(p.Info.NodeNames) == 1
}

func (pn *PartitionNode) isBroken(enableNodeReadiness bool) bool {
	broken := pn.Info.State == partitiontreev1.PartitionStateUnhealthy ||
		pn.Info.State == partitiontreev1.PartitionStateUnknown ||
		pn.Info.State == partitiontreev1.PartitionStateIncomplete
	return broken || !pn.isKubeNodeSchedulable(enableNodeReadiness)
}

// Traverses down a sub-tree and collects all the slices that are using the partition.
// They are all added to the PreeptionCandidateSet.
// If any of the partitions are broken, then it will return a false, nil value.
func (pn *PartitionNode) validatePreemptionCandidate(ctx context.Context, enableNodeReadiness bool) (bool, []*interfaces.SliceInfo) {
	logf := log.FromContext(ctx)
	var candidate []*interfaces.SliceInfo
	// If it is broken then this is not a valid candidate
	if pn.isBroken(enableNodeReadiness) {
		logf.V(2).Info("Preemption candidate invalid: partition is broken", "partitionID", pn.Info.PartitionID, "state", pn.Info.State)
		return false, candidate
	}
	// If it is used by a slice then this is a valid candidate for preemption
	if pn.IsUsed() {
		logf.V(2).Info("Preemption candidate valid: partition is in use", "partitionID", pn.Info.PartitionID, "inUseBy", pn.InUseBy.Name)
		return true, []*interfaces.SliceInfo{pn.InUseBy.DeepCopy()}
	}
	// If it is not in use, then we should check its children
	logf.V(2).Info("Preemption candidate not in direct use, recursing children", "partitionID", pn.Info.PartitionID)
	for _, child := range pn.Children {
		if isValid, slices := child.validatePreemptionCandidate(ctx, enableNodeReadiness); isValid {
			candidate = append(candidate, slices...)
		} else {
			logf.V(2).Info("Preemption candidate invalid: child is not valid", "partitionID", pn.Info.PartitionID, "childID", child.Info.PartitionID)
			return false, nil
		}
	}
	return true, candidate
}

// Traverses a top level paritionTree and returns a list of valid PreeptionCandidateSet
func (pn *PartitionNode) findValidPreemptionCandidateSet(ctx context.Context, topology, podName string, enableNodeReadiness bool) []*PreemptionCandidateSet {
	var candidates []*PreemptionCandidateSet
	// If we see that an ancestor is already in use then this is our only possible
	// candidate.
	if pn.IsUsed() {
		// Before we return this as a legit candidate we should check if any of the
		// partitions at desired topology is not broken.
		if resPn, ok := pn.isAnySubPartitionNotBroken(topology, enableNodeReadiness); ok {
			// If its a direct match, then ToBeAssignedPartitions should be equal to
			// the partitions assigned to the slice.
			if util.ToTopology(pn.Info.Topology) == topology {
				return []*PreemptionCandidateSet{{Slices: []*interfaces.SliceInfo{pn.InUseBy.DeepCopy()}, PodName: podName, ToBeAssignedPartitions: pn.InUseBy.PartitionIds}}
			}
			return []*PreemptionCandidateSet{{Slices: []*interfaces.SliceInfo{pn.InUseBy.DeepCopy()}, PodName: podName, ToBeAssignedPartitions: []string{resPn}}}
		}
		return candidates
	}
	// We reached our desired topology and the node is not in use. So we must
	// now consider this whole sub-tree as a single viable candidate
	if util.ToTopology(pn.Info.Topology) == topology {
		isValid, slices := pn.validatePreemptionCandidate(ctx, enableNodeReadiness)
		if isValid && slices != nil {
			return []*PreemptionCandidateSet{{Slices: slices, PodName: podName, ToBeAssignedPartitions: []string{pn.Info.PartitionID}}}
		}
	} else { // Traverse down to desired topology
		for _, child := range pn.Children {
			if cs := child.findValidPreemptionCandidateSet(ctx, topology, podName, enableNodeReadiness); cs != nil {
				candidates = append(candidates, cs...)
			}
		}
	}
	return candidates
}

func (pn *PartitionNode) IsUsed() bool {
	return pn.InUseBy != nil
}

// Returns true and partitionName if any partition at topology is not broken. It
// does not consider InUse as a bad state.
func (pn *PartitionNode) isAnySubPartitionNotBroken(topology string, enableNodeReadiness bool) (string, bool) {
	if util.ToTopology(pn.Info.Topology) == topology && !pn.isBroken(enableNodeReadiness) {
		return pn.Info.PartitionID, true
	}
	for _, child := range pn.Children {
		if childPn, ok := child.isAnySubPartitionNotBroken(topology, enableNodeReadiness); ok {
			return childPn, ok
		}
	}
	return "", false
}
