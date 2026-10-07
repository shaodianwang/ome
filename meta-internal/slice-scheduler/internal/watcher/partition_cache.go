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

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/util"
)

const (
	TpuPodNameLabel = "cloud.google.com/tpu-pod-id"
)

func NewPartitionCache(enableNodeReadiness bool) *PartitionCache {
	return &PartitionCache{
		partitionTreeCache:     make(map[string]*PartitionNode),
		partitionNodeCache:     make(map[string]*PartitionNode),
		leafPartitionNodeCache: make(map[string]*PartitionNode),
		enableNodeReadiness:    enableNodeReadiness,
	}
}

func (pc *PartitionCache) GetRootNode(name string) (*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	node, ok := pc.partitionTreeCache[name]
	return node, ok
}

func (pc *PartitionCache) SetRootNode(name string, node *PartitionNode) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.partitionTreeCache[name] = node
}

// DeletePartitionTree atomically removes a full partition tree from the cache.
func (pc *PartitionCache) DeletePartitionTree(ctx context.Context, name string) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if root, ok := pc.partitionTreeCache[name]; ok {
		logf.Info("Deleting partition tree from cache", "treeName", name)
		pc.deleteNodesFromMapRecursive(root)
		delete(pc.partitionTreeCache, name)
	}
}

func (pc *PartitionCache) GetNode(partitionID string) (*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	node, ok := pc.partitionNodeCache[partitionID]
	return node, ok
}

// GetNodes returns the partition nodes for all given partition IDs.
// Returns false if any partition ID is not found in the cache.
func (pc *PartitionCache) GetNodes(partitionIDs []string) (map[string]*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	nodes := make(map[string]*PartitionNode)
	for _, id := range partitionIDs {
		node, ok := pc.partitionNodeCache[id]
		if !ok {
			return nil, false
		}
		nodes[id] = node
	}
	return nodes, true
}

func (pc *PartitionCache) GetLeafNode(nodeName string) (*PartitionNode, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	node, ok := pc.leafPartitionNodeCache[nodeName]
	return node, ok
}

// ReservePartition marks a partition as in-use. It returns false if the partition is already in use.
func (pc *PartitionCache) ReservePartition(ctx context.Context, sInfo *interfaces.SliceInfo) (success bool, existingSlice string) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	isFailed := false
	reservedPartitions := []string{}
	for _, partitionID := range sInfo.PartitionIds {
		node, ok := pc.partitionNodeCache[partitionID]
		if !ok {
			// This can happen if a slice references a partition from a tree that doesn't match its type.
			logf.V(1).Info("Attempted to reserve non-existent partition", "sliceName", sInfo.Name, "partitionId", partitionID)
			isFailed = true
			break
		}
		if node.IsUsed() && node.InUseBy.Name != sInfo.Name {
			existingSlice = node.InUseBy.Name
			// This can happen for two reasons:
			// 1) Race condition where scheduler picks a partition that becomes unavailable later
			// 2) Slices created outside of the scheduler with duplicate partitions
			// A webhook should prevent 2 from ever happening for any scheduler to reliably work
			logf.Info("Failed to reserve partition for slice; already in use", "sliceName", sInfo.Name, "partitionId", partitionID, "existingSlice", node.InUseBy.Name)
			isFailed = true
			break
		}
		node.InUseBy = sInfo
		reservedPartitions = append(reservedPartitions, partitionID)
	}
	if isFailed {
		// on failures, roll back the reservations we've made so far.
		for _, reservedID := range reservedPartitions {
			if rNode, rok := pc.partitionNodeCache[reservedID]; rok {
				rNode.InUseBy = nil
			}
		}
		return false, existingSlice
	}

	logf.Info("Successfully reserved partitions for slice", "sliceName", sInfo.Name, "partitionIds", sInfo.PartitionIds)
	return true, ""
}

// ReleasePartitions frees a partition if it's held by the given slices.
func (pc *PartitionCache) ReleasePartitions(ctx context.Context, slices []*interfaces.SliceInfo) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, s := range slices {
		for _, partitionID := range s.PartitionIds {
			if node, ok := pc.partitionNodeCache[partitionID]; ok && node.InUseBy != nil && node.InUseBy.Name == s.Name {
				node.InUseBy = nil
				logf.Info("Released partition for slice", "sliceName", s.Name, "partitionId", partitionID)
			}
		}
	}
}

func (pc *PartitionCache) BuildPreemptionCandidates(ctx context.Context, topology string) []*PreemptionCandidateSet {
	logf := log.FromContext(ctx)
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	seenSlices := make(map[string]struct{})

	all_candidates := []*PreemptionCandidateSet{}
	for _, p := range pc.partitionTreeCache {
		if candidateSet := p.findValidPreemptionCandidateSet(ctx, topology, p.PodName, pc.enableNodeReadiness); candidateSet != nil {
			// If there is only one slice as the candidate then de-duplicate it by only
			// adding it once to the PreemptionCandidateSet. This will be true for slices
			// that take up muptiple (4x4x4) partitions.
			if topology == "4x4x4" && len(candidateSet) == 1 && len(candidateSet[0].Slices) == 1 {
				if _, ok := seenSlices[candidateSet[0].Slices[0].Name]; ok {
					continue
				} else {
					seenSlices[candidateSet[0].Slices[0].Name] = struct{}{}
				}
			}
			all_candidates = append(all_candidates, candidateSet...)
		}
	}
	logf.V(1).Info("Built preemption candidates", "topology", topology, "numCandidates", len(all_candidates))
	return all_candidates
}

// BuildAvailabilityMap creates a map of topology to a list of maximal available partitions from the in-memory cache.
func (pc *PartitionCache) BuildAvailabilityMap(ctx context.Context) map[string][]*partitiontreev1.PartitionInfo {
	logf := log.FromContext(ctx)
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	availabilityMap := make(map[string][]*partitiontreev1.PartitionInfo)

	var findMaximal func(p *PartitionNode)
	findMaximal = func(p *PartitionNode) {
		// Critical termination condition, should not traverse down the children if the
		// partition node is entirely occupied by a slice.
		if p.IsUsed() {
			return
		}
		// Short-circuit: if root has SliceId set, this is a MultiMig tree —
		// the entire MIG is claimed by a super-slice, no sub-partitions are
		// available. Skip instead of traversing all Occupied children.
		if p.Info.SliceId != "" {
			return
		}

		if p.isPartitionNodeSchedulable(ctx, pc.enableNodeReadiness) {
			logf.V(2).Info("Found schedulable partition node", "partitionID", p.Info.PartitionID, "topology", util.ToTopology(p.Info.Topology))
			topology := util.ToTopology(p.Info.Topology)
			availabilityMap[topology] = append(availabilityMap[topology], p.Info)
			return
		}

		// Recurse down to find smaller available partitions.
		for _, child := range p.Children {
			findMaximal(child)
		}
	}

	for _, rootNode := range pc.partitionTreeCache {
		findMaximal(rootNode)
	}

	for topo, partitions := range availabilityMap {
		logf.V(1).Info("Availability map entry", "topology", topo, "numPartitions", len(partitions))
	}
	return availabilityMap
}

// BuildPodAvailabilityMap creates a map of pod name to a list of available top
// level partition names.
func (pc *PartitionCache) BuildPodAvailabilityMap(ctx context.Context) map[string][]string {
	logf := log.FromContext(ctx)
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	podAvailabilityMap := make(map[string][]string)

	for _, p := range pc.partitionTreeCache {
		if podAvailabilityMap[p.PodName] == nil {
			podAvailabilityMap[p.PodName] = []string{}
		}
		// Short-circuit MultiMig trees: SliceId on root means the entire
		// MIG is claimed by a super-slice, no capacity available.
		if p.Info.SliceId != "" {
			logf.V(2).Info("Skipping MultiMig tree", "podName", p.PodName, "partitionID", p.Info.PartitionID, "sliceId", p.Info.SliceId)
			continue
		}
		if p.isPartitionNodeSchedulable(ctx, pc.enableNodeReadiness) {
			podAvailabilityMap[p.PodName] = append(podAvailabilityMap[p.PodName], p.Info.PartitionID)
		}
	}
	for pod, partitions := range podAvailabilityMap {
		logf.V(1).Info("Pod availability map entry", "podName", pod, "numPartitions", len(partitions))
	}
	return podAvailabilityMap
}

func (pc *PartitionCache) preserveKubeNodeState(node *PartitionNode, kNodeMap map[string]*KubeNode) {
	if node == nil || kNodeMap == nil {
		return
	}
	if node.isLeafPartitionNode() && node.KubeNode != nil {
		if _, ok := kNodeMap[node.KubeNode.Name]; !ok {
			kNodeMap[node.KubeNode.Name] = &KubeNode{Name: node.KubeNode.Name, Ready: node.KubeNode.Ready}
		}
	}
	for _, child := range node.Children {
		pc.preserveKubeNodeState(child, kNodeMap)
	}
}

// deleteNodesFromMapRecursive is a recursive helper. Must be called with lock held.
func (pc *PartitionCache) deleteNodesFromMapRecursive(node *PartitionNode) {
	delete(pc.partitionNodeCache, node.Info.PartitionID)
	if node.isLeafPartitionNode() {
		delete(pc.leafPartitionNodeCache, node.Info.NodeNames[0])
	}
	for _, child := range node.Children {
		pc.deleteNodesFromMapRecursive(child)
	}
}

// getUsageState extracts the inUseBy mapping from the cache. Must be called with lock held.
func (pc *PartitionCache) getUsageState() map[string]*interfaces.SliceInfo {
	usageMap := make(map[string]*interfaces.SliceInfo)
	for _, root := range pc.partitionTreeCache {
		var extract func(*PartitionNode)
		extract = func(node *PartitionNode) {
			if node.IsUsed() {
				usageMap[node.Info.PartitionID] = node.InUseBy
			}
			for _, child := range node.Children {
				extract(child)
			}
		}
		extract(root)
	}
	return usageMap
}

// applyUsageState applies a saved usage state to the cache. Must be called with lock held.
func (pc *PartitionCache) applyUsageState(usageMap map[string]*interfaces.SliceInfo) {
	for pID, sInfo := range usageMap {
		if node, ok := pc.partitionNodeCache[pID]; ok {
			node.InUseBy = sInfo
		}
	}
}

// buildNodeTree is a recursive helper to construct the in-memory partitionNode tree. Must be called with lock held.
func (pc *PartitionCache) buildNodeTree(info *partitiontreev1.PartitionInfo, parent *PartitionNode, podName string, kNodeMap map[string]*KubeNode, loggr logr.Logger) *PartitionNode {
	node := &PartitionNode{
		Info:   info,
		Parent: parent,
	}
	pc.partitionNodeCache[info.PartitionID] = node
	if podName != "" {
		node.PodName = podName
	}

	node.Children = make([]*PartitionNode, len(info.ChildPartitions))
	for i := range info.ChildPartitions {
		node.Children[i] = pc.buildNodeTree(&info.ChildPartitions[i], node, "", kNodeMap, loggr)
	}
	// Populate the k8s node information to the leaf partition node
	if len(info.ChildPartitions) == 0 && len(info.NodeNames) == 1 {
		// populate the leafPartitionNodeCache
		pc.leafPartitionNodeCache[info.NodeNames[0]] = node

		// Only set if the k8s node is found.
		// We are using the condition node.KubeNode != nil to make the scheduling decision.
		// Check details in isPartitionNodeSchedulable().
		// So DO NOT create kubeNode if the k8s node is not retrieved from the cache.
		if kNode, ok := kNodeMap[info.NodeNames[0]]; ok {
			node.KubeNode = kNode
			loggr.V(2).Info("Added kube node to a leaf partition node", "name", node.KubeNode.Name, "ready", node.KubeNode.Ready)
		}
	}
	return node
}

// UpdateTree atomically updates a partition tree in the cache.
func (pc *PartitionCache) UpdateTree(ctx context.Context, tree *partitiontreev1.PartitionTree, nodes []corev1.Node) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	logf.V(2).Info("Updating partition tree in cache", "treeName", tree.Name, "numKubeNodes", len(nodes), "enableNodeReadiness", pc.enableNodeReadiness)

	// convert the node list to a map, key is the node name, value is the node object
	nodeMap := make(map[string]*KubeNode)
	for _, node := range nodes {
		nodeMap[node.Name] = &KubeNode{
			Name:  node.Name,
			Ready: isNodeReady(&node),
		}
	}

	// Preserve current usage state and kube node state
	tempUsageMap := pc.getUsageState()
	pc.preserveKubeNodeState(pc.partitionTreeCache[tree.Name], nodeMap)

	// Remove old tree if it exists
	if oldRoot, ok := pc.partitionTreeCache[tree.Name]; ok {
		pc.deleteNodesFromMapRecursive(oldRoot)
	}

	// Add new tree
	if tree.Status.Root != nil {
		newRoot := pc.buildNodeTree(tree.Status.Root, nil, tree.Labels[TpuPodNameLabel], nodeMap, logf)
		pc.partitionTreeCache[tree.Name] = newRoot
	}

	// Restore usage state
	pc.applyUsageState(tempUsageMap)
}

func (pc *PartitionCache) UpdateLeafNodeCache(ctx context.Context, node *corev1.Node) {
	logf := log.FromContext(ctx)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if kNode, ok := pc.leafPartitionNodeCache[node.Name]; ok {
		if kNode.KubeNode == nil {
			kNode.KubeNode = &KubeNode{Name: node.Name}
		}
		kNode.KubeNode.Ready = isNodeReady(node)
		logf.V(2).Info("Updating leaf node status in cache", "node name", node.Name, "status", kNode.KubeNode.Ready)
	}
}

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
