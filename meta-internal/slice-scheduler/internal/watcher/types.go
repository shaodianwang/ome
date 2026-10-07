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
	"sync"

	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
)

type KubeNode struct {
	Name  string
	Ready bool
}

// PartitionNode is an in-memory representation of a partition, tracking its state and usage.
type PartitionNode struct {
	Info     *partitiontreev1.PartitionInfo
	InUseBy  *interfaces.SliceInfo // The slice using this partition, if any. This field does not consider the descendant in use case. The partitionInfo.STATE_IN_USE will be set if any of the descendant is in use.
	PodName  string                // The name of the TPU pod the partition belongs to.
	Parent   *PartitionNode
	Children []*PartitionNode
	KubeNode *KubeNode // KubeNode encapsulates the information from k8s node. This field is only set on the leaf node.
}

// PartitionCache holds the state of all partitions of a specific type (e.g., Trillium or Ironwood).
type PartitionCache struct {
	// partitionTreeCache stores the in-memory representation of all partition trees. The key is the name of the PartitionTree CR.
	partitionTreeCache map[string]*PartitionNode
	// partitionNodeCache provides fast lookups from a partition ID to its node in the tree.
	partitionNodeCache map[string]*PartitionNode
	// leafPartitionNodeCache provides fast lookups from a kube node name to a partition node.
	leafPartitionNodeCache map[string]*PartitionNode
	mu                     sync.RWMutex
	enableNodeReadiness    bool // If true, the scheduler will consider the node presence and readiness for scheduling.
}

// PreemptionCandidateSet is a slice list candidates that could be preempted.
// The SliceCandidate is used as a list of SliceInfo list to consolidate
// both trillium and ironwood preemption scenarios.
type PreemptionCandidateSet struct {
	Slices                 []*interfaces.SliceInfo
	PodName                string   // The name of the TPU pod
	ToBeAssignedPartitions []string // Partitions that would be be free and can be used by the preemptor slice
}
