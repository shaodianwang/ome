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

package pod_reconciler

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

const (
	JobCompletionIndexAnnotation = "batch.kubernetes.io/job-completion-index"
	WorkerIdAnnotation           = "tpu.google.com/worker-id"
	PodIndexLabel                = "apps.kubernetes.io/pod-index"
	LWSWorkerIndexLabel          = "leaderworkerset.sigs.k8s.io/worker-index"
)

// appendHostNamesRecursive recursively collects hostnames from a partition node and its children.
func appendHostNamesRecursive(node *watcher.PartitionNode, hostNames *[]string) {
	if node == nil {
		return
	}
	// If it's a leaf node (no children), collect its hostnames.
	if len(node.Children) == 0 {
		*hostNames = append(*hostNames, node.Info.NodeNames...)
	}
	for _, child := range node.Children {
		appendHostNamesRecursive(child, hostNames)
	}
}

// parsePodIndex extracts the 0-indexed ordinal rank of a pod within its TPU Slice.
// Hierarchy resolution order:
// 1. Explicit Job / JobSet index annotations (batch.kubernetes.io/job-completion-index)
// 2. Explicit TPU worker annotations (tpu.google.com/worker-id)
// 3. StatefulSet ordinal labels (apps.kubernetes.io/pod-index)
// 4. LeaderWorkerSet worker index label (leaderworkerset.sigs.k8s.io/worker-index)
// 5. Fallback string derivation parsing trailing hyphenated numeric substring from pod.Name
func parsePodIndex(pod *corev1.Pod) (int, error) {
	if pod.Annotations != nil {
		if indexStr, ok := pod.Annotations[JobCompletionIndexAnnotation]; ok {
			return strconv.Atoi(indexStr)
		}
		if indexStr, ok := pod.Annotations[WorkerIdAnnotation]; ok {
			return strconv.Atoi(indexStr)
		}
	}

	if pod.Labels != nil {
		if indexStr, ok := pod.Labels[PodIndexLabel]; ok {
			return strconv.Atoi(indexStr)
		}
		if indexStr, ok := pod.Labels[LWSWorkerIndexLabel]; ok {
			return strconv.Atoi(indexStr)
		}
	}

	podName := pod.Name
	parts := strings.Split(podName, "-")
	if len(parts) >= 2 {
		ordinalStr := parts[len(parts)-1]
		if index, err := strconv.Atoi(ordinalStr); err == nil {
			return index, nil
		}
	}

	return 0, fmt.Errorf("could not determine pod index for %s, expected one of: "+
		"'batch.kubernetes.io/job-completion-index' annotation, "+
		"'tpu.google.com/worker-id' annotation, "+
		"'apps.kubernetes.io/pod-index' label, "+
		"'leaderworkerset.sigs.k8s.io/worker-index' label, "+
		"or <name>-<ordinal> naming format", podName)
}
