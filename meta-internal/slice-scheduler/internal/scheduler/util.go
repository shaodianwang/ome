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

package scheduler

import (
	"context"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	partitiontree "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "tpu.google.com/slice-scheduler/apis/slice/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

func getSlicePriority(ctx context.Context, slice *slicev1beta1.Slice) int {
	logf := log.FromContext(ctx)
	priorityStr, ok := slice.Annotations[SlicePriorityAnnotation]
	if !ok {
		logf.Info("Slice has no priority annotation, using default 0", "sliceName", slice.Name)
		return 0 // Default priority
	}
	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		logf.Info("Failed to parse slice priority annotation, using default 0", "sliceName", slice.Name, "annotationValue", priorityStr, "error", err)
		return 0 // Default priority on parsing error
	}
	logf.V(1).Info("Read slice priority", "sliceName", slice.Name, "priority", priority)
	return priority
}

func getSliceGroupName(slice *slicev1beta1.Slice) string {
	if name, ok := slice.Annotations[SliceGroupNameAnnotation]; ok {
		return name
	}
	return slice.Name
}

func getNumSlices(ctx context.Context, slice *slicev1beta1.Slice) int {
	logf := log.FromContext(ctx)
	numSlicesStr, ok := slice.Annotations[NumSlicesAnnotation]
	if !ok {
		return 1 // Default to 1 if annotation is missing
	}
	numSlices, err := strconv.Atoi(numSlicesStr)
	if err != nil {
		logf.Error(err, "Failed to parse num-slices annotation, using default 1", "sliceName", slice.Name, "annotationValue", numSlicesStr)
		return 1 // Default to 1 on parsing error
	}
	return numSlices
}

// Returns a map of pod-name to how many missing cubes it has to satisfy a multi-cube
// request.

// Returns tpuType for the partitionTree
func partitionTreeType(tree *partitiontree.PartitionTree) string {
	if tree.Status.AcceleratorType != "" {
		if tree.Status.AcceleratorType == "v7x" {
			return TPU7x
		}
		return tree.Status.AcceleratorType
	}
	if tree.Status.Root == nil {
		return ""
	}
	s := tree.Status.Root.Topology
	if strings.HasPrefix(s, "IRONWOOD_") {
		return TPU7x
	}
	if strings.HasPrefix(s, "TRILLIUM_") {
		return TPU6e
	}
	return ""
}

// Returns tpuType for the k8s node
func nodeType(node *corev1.Node) string {
	for k := range node.Labels {
		if strings.HasPrefix(k, NodeLabelPartitionID) {
			if strings.Contains(k, TrilliumRootPartitionIDLabel) {
				return TPU6e
			}
			if strings.Contains(k, IronwoodRootPartitionIDLabel) {
				return TPU7x
			}
		}
	}
	return "" // for unknown types
}

// Converts slicev1beta1.Slice to interfaces.SliceInfo
func sliceInfo(ctx context.Context, slice *slicev1beta1.Slice) *interfaces.SliceInfo {
	managedBy := slice.Annotations[watcher.ManagedByAnnotation]
	return &interfaces.SliceInfo{
		Name:         slice.Name,
		Priority:     getSlicePriority(ctx, slice),
		Topology:     slice.Spec.Topology,
		Type:         string(slice.Spec.Type),
		PartitionIds: slice.Spec.PartitionIds,
		ManagedBy:    managedBy,
	}
}
