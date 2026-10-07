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

package preemption

import (
	"context"

	"tpu.google.com/slice-scheduler/internal/watcher"
)

// SelectMultiCubePreemptionCandidate selects the best preemption candidate for
// multi-cube workloads. It filters by minimum priority and managed-by-scheduler
// annotation, transforms via the multi-cube cube request map, and selects the
// candidate with the lowest maximum priority (fewest high-priority evictions).
func SelectMultiCubePreemptionCandidate(ctx context.Context, candidates []*watcher.PreemptionCandidateSet, cubeRequestMap map[string]int, minPriority int) *watcher.PreemptionCandidateSet {
	selector := NewSelector(
		WithFilter(BasicFilter(minPriority)),
		WithFilter(ManagedBySchedulerFilter()),
		WithTransform(MultiCubeTransformer(cubeRequestMap)),
		WithChainedComparator(LowestMaxPriority(), NumSlices()),
	)
	return selector.Select(ctx, candidates)
}

// SelectPreemptionCandidate selects the best preemption candidate.
func SelectPreemptionCandidate(ctx context.Context, candidates []*watcher.PreemptionCandidateSet, minPriority int) *watcher.PreemptionCandidateSet {
	selector := NewSelector(
		WithFilter(BasicFilter(minPriority)),
		WithFilter(ManagedBySchedulerFilter()),
		WithChainedComparator(LowestMaxPriority(), NumSlices()),
	)
	return selector.Select(ctx, candidates)
}
