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
	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
)

const (
	ManagedByAnnotation = "cloud.google.com/managed-by"
)

func (kn *KubeNode) IsReady() bool {
	return kn.Ready
}

func (pn *PartitionNode) isTestingNotReady() bool {
	if pn.isLeafPartitionNode() {
		return pn.KubeNode != nil && pn.KubeNode.TestingNotReady
	}
	for _, child := range pn.Children {
		if child.isTestingNotReady() {
			return true
		}
	}
	return false
}

// Returns true if the k8s node is schedulable.
func (pn *PartitionNode) isKubeNodeSchedulable(enableNodeReadiness bool) bool {
	if enableNodeReadiness && pn.isLeafPartitionNode() {
		return pn.KubeNode != nil && pn.KubeNode.IsReady()
	}
	return true
}

// Returns true if the partition is a leaf whose k8s node is cordoned. Unlike
// a node that is not ready, a cordoned node keeps the slices it already runs,
// so this never makes a partition broken.
func (pn *PartitionNode) isCordoned() bool {
	return pn.isLeafPartitionNode() && pn.KubeNode != nil && pn.KubeNode.Unschedulable
}

// isLeafPartitionNode reports whether this node is a leaf of the partition
// tree. Leafness is a property of the tree, not of how many kube nodes the CRD
// happens to list: defining it as len(NodeNames) == 1 answers false for a
// childless partition carrying zero or several node names, which then silently
// escapes the readiness checks in isKubeNodeSchedulable and isBroken.
func (p *PartitionNode) isLeafPartitionNode() bool {
	return len(p.Children) == 0
}

func (pn *PartitionNode) isBroken(enableNodeReadiness bool) bool {
	broken := pn.Info.State == partitiontreev1.PartitionStateUnhealthy ||
		pn.Info.State == partitiontreev1.PartitionStateUnknown ||
		pn.Info.State == partitiontreev1.PartitionStateIncomplete
	return broken || !pn.isKubeNodeSchedulable(enableNodeReadiness)
}

func (pn *PartitionNode) IsUsed() bool {
	return pn.InUseBy != nil
}
