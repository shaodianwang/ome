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
	"tpu.google.com/slice-scheduler/internal/watcher"
)

// LowestMaxPriority returns a LessFunc that prefers the set
// with the lowest maximum priority slice.
func LowestMaxPriority() LessFunc {
	getMaxPrio := func(set *watcher.PreemptionCandidateSet) int {
		maxPrio := 0 // Priorities are non-negative
		for _, s := range set.Slices {
			if s.Priority > maxPrio {
				maxPrio = s.Priority
			}
		}
		return maxPrio
	}

	return func(a, b *watcher.PreemptionCandidateSet) bool {
		return getMaxPrio(a) < getMaxPrio(b)
	}
}

// NumSlices returns a LessFunc that prefers the set
// with the fewer number of slices.
func NumSlices() LessFunc {
	return func(a, b *watcher.PreemptionCandidateSet) bool {
		return len(a.Slices) < len(b.Slices)
	}
}
