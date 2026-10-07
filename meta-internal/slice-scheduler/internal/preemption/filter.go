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

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// BasicFilter performs basic checks and *prunes* the Slice list
// based on priority. List containing a slice that cannot be
// preempted is removed.
func BasicFilter(preemptorPriority int) FilterFunc {
	return func(ctx context.Context, set *watcher.PreemptionCandidateSet) bool {
		logf := log.FromContext(ctx)
		// 1. Non-empty check
		if set == nil || len(set.Slices) == 0 {
			logf.V(2).Info("Filtering preemption candidate: set is nil or empty")
			return false
		}

		// 2. Priority check
		for _, s := range set.Slices {
			if s.Priority >= preemptorPriority {
				logf.V(2).Info("Filtering preemption candidate: slice priority is too high", "sliceName", s.Name, "slicePriority", s.Priority, "preemptorPriority", preemptorPriority)
				return false
			}
		}
		return true
	}
}

// ManagedBySchedulerFilter checks if the slice is managed by the slice scheduler.
func ManagedBySchedulerFilter() FilterFunc {
	return func(ctx context.Context, set *watcher.PreemptionCandidateSet) bool {
		logf := log.FromContext(ctx)
		for _, s := range set.Slices {
			if s.ManagedBy != "slice-scheduler" {
				logf.V(2).Info("Filtering preemption candidate: slice is not managed by the scheduler", "sliceName", s.Name)
				return false
			}
		}
		return true
	}
}
