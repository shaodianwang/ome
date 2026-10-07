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
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/util"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

// listCost aggregates "cost" (max priority) and "value" (cubes)
// for a single slice candidate list.
type listCost struct {
	set   *watcher.PreemptionCandidateSet
	cost  int // The "max priority" of this list
	cubes int // The number of cubes this list provides
	index int // Original index for stable sort
}

// getCandidateMaxPriority finds the highest priority of any slice in a set.
func getCandidateMaxPriority(set *watcher.PreemptionCandidateSet) int {
	maxPrio := 0 // Use 0 as the floor
	for _, slice := range set.Slices {
		if slice.Priority > maxPrio {
			maxPrio = slice.Priority
		}
	}
	return maxPrio
}

// getCandidateCubeCount calculates the total number of v7x cubes for a list of slices.
func getCandidateCubeCount(set *watcher.PreemptionCandidateSet) int {
	if set == nil || len(set.Slices) == 0 {
		return 0
	}
	// Special case for a cube with subslices.
	if len(set.Slices) >= 1 && util.NumOfCubes(set.Slices[0].Topology) == 0 {
		return 1
	}

	return util.NumOfCubes(set.Slices[0].Topology)
}

// MultiCubeTransformer returns a function that performs below actions:
// 1. Group the input list of PreemptionCandidateSet by pod name
// 2. Find the "cheapest" combination of slices that meets numCubesRequested, populate as new PreemptionCandidateSet
// 3. Return the one PreemptionCandidateSet from each pod.
func MultiCubeTransformer(numCubesRequested map[string]int) TransformFunc {
	return func(ctx context.Context, list []*watcher.PreemptionCandidateSet) []*watcher.PreemptionCandidateSet {
		logf := log.FromContext(ctx)
		if len(list) == 0 {
			return []*watcher.PreemptionCandidateSet{}
		}

		candidatesByPod := make(map[string][]*watcher.PreemptionCandidateSet)
		for _, set := range list {
			candidatesByPod[set.PodName] = append(candidatesByPod[set.PodName], set)
		}
		logf.V(2).Info("Grouped preemption candidates by pod", "numPods", len(candidatesByPod))

		retList := []*watcher.PreemptionCandidateSet{}
		for podName, candidates := range candidatesByPod {
			// 1. Build a list of all available PreemptionCandidateSet with their cost and value
			totalAvailableCubes := 0
			costs := make([]listCost, len(candidates))
			for i, candidate := range candidates {
				costs[i] = listCost{
					set:   candidate,
					cost:  getCandidateMaxPriority(candidate),
					cubes: getCandidateCubeCount(candidate),
					index: i,
				}
				totalAvailableCubes += costs[i].cubes
			}

			// 2. Check if meeting the target is even possible
			cubeNeeded := numCubesRequested[podName]
			if totalAvailableCubes < cubeNeeded {
				logf.V(2).Info("Not enough total cubes in pod to satisfy request", "podName", podName, "availableCubes", totalAvailableCubes, "neededCubes", cubeNeeded)
				continue // Skip this pod, not enough cubes
			}

			// 3. Sort lists by cost (lowest max priority), then by cubes (descending) as a tie-break
			sort.Slice(costs, func(i, j int) bool {
				if costs[i].cost != costs[j].cost {
					return costs[i].cost < costs[j].cost // Lower cost is better
				}
				if costs[i].cubes != costs[j].cubes {
					return costs[i].cubes < costs[j].cubes // Fewer cubes is better
				}
				return costs[i].index < costs[j].index // Stable sort
			})

			// 4. Greedy selection, put all the chosen slices into one list.
			var finalListOfPartitions []string
			finalLists := []*interfaces.SliceInfo{}
			cubesAccumulated := 0
			for _, item := range costs {
				if cubesAccumulated >= cubeNeeded {
					break // Target met
				}

				// Add the PreemptionCandidateSet to the list
				finalLists = append(finalLists, item.set.Slices...)
				finalListOfPartitions = append(finalListOfPartitions, item.set.ToBeAssignedPartitions...)
				cubesAccumulated += item.cubes
			}

			// 5. Final check
			if cubesAccumulated < cubeNeeded {
				logf.V(2).Info("Could not find a combination of candidates to satisfy cube request", "podName", podName, "accumulatedCubes", cubesAccumulated, "neededCubes", cubeNeeded)
				// This should be impossible due to check #2, but as a safeguard:
				continue // Couldn't find a combination (logic error?)
			}
			logf.V(2).Info("Found a valid preemption combination for pod", "podName", podName, "numSlices", len(finalLists), "numCubes", cubesAccumulated)
			retList = append(retList, &watcher.PreemptionCandidateSet{
				PodName:                podName,
				Slices:                 finalLists,
				ToBeAssignedPartitions: finalListOfPartitions,
			})
		}

		return retList
	}
}
