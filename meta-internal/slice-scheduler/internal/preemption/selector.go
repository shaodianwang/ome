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

// FilterFunc is a function that can reject a PreemptionCandidateSet, return
// false if the PreemptionCandidateSet is rejected.
type FilterFunc func(ctx context.Context, set *watcher.PreemptionCandidateSet) bool

// TransformFunc is a function that can transform a list of PreemptionCandidateSet
// to a new format. In multi-cube selector, this could perform a in-pod-slice transoformation.
type TransformFunc func(ctx context.Context, l []*watcher.PreemptionCandidateSet) []*watcher.PreemptionCandidateSet

// LessFunc is a function type that defines a preemption policy.
// It compares two candidate sets 'a' and 'b'.
// It should return 'true' if 'a' is "better" (i.e., should be chosen over) 'b'.
type LessFunc func(a, b *watcher.PreemptionCandidateSet) bool

// Selector is a configurable struct that selects the best candidate set
// based on a given policy and filters.
type Selector struct {
	filters   []FilterFunc
	transform TransformFunc
	compare   LessFunc
}

// Option is a function type that configures a Selector.
type Option func(*Selector)

// NewSelector creates a new Selector with the given options.
func NewSelector(opts ...Option) *Selector {
	s := &Selector{
		// Default preprocessor list is empty
		filters: []FilterFunc{},
		// Default transform is nil
		transform: nil,
		// Default policy is nil.
		compare: func(a, b *watcher.PreemptionCandidateSet) bool { return false },
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithFilter adds a new filter function to the Selector's chain.
// Filters are executed in the order they are added.
func WithFilter(fn FilterFunc) Option {
	return func(s *Selector) {
		s.filters = append(s.filters, fn)
	}
}

func WithTransform(fn TransformFunc) Option {
	return func(s *Selector) {
		s.transform = fn
	}
}

// WithComparator sets the primary sorting algorithm for the Selector.
func WithComparator(c LessFunc) Option {
	return func(s *Selector) {
		s.compare = c
	}
}

// WithChainedComparator sets the primary sorting algorithm using a chain of comparators.
// This is a convenience wrapper around WithPolicy(Chain(...)).
func WithChainedComparator(fns ...LessFunc) Option {
	return func(s *Selector) {
		s.compare = Chain(fns...)
	}
}

// Chain allows you to combine multiple LessFunc functions into a single one.
// Functions are evaluated in order. The first function that finds a difference
// (i.e., doesn't result in a tie) determines the result.
func Chain(fns ...LessFunc) LessFunc {
	return func(a, b *watcher.PreemptionCandidateSet) bool {
		for _, fn := range fns {
			if fn(a, b) {
				// 'a' is better than 'b' according to this policy
				return true
			}
			if fn(b, a) {
				// 'b' is better than 'a' according to this policy
				return false
			}
			// If neither is true, it's a tie.
			// Continue to the next policy in the chain.
		}
		// If all policies result in a tie, return false (maintaining stability).
		return false
	}
}

// Select finds the best PreemptionCandidateSet from a list.
// This is the main function that will execute the selection algorithm in 3 phases:
//  1. Filter: run all the candidates through the filters, remove the ineligible candidates
//     based on filter rules.
//  2. Tansform: modify the candidates from the phase 1 based on transform functions, note that
//     this phase might not preserve the exact same PreemptionCandidateSet from the input. It
//     could end up generate a new list of candidates for next phase.
//  3. Sort: sort the candidate based on the comparators, chose the "best" PreemptionCandidateSet
//     and return.
func (s *Selector) Select(ctx context.Context, candidates []*watcher.PreemptionCandidateSet) *watcher.PreemptionCandidateSet {
	logf := log.FromContext(ctx)
	logf.V(2).Info("Starting preemption candidate selection", "initialCandidates", len(candidates))

	if len(candidates) == 0 {
		logf.V(2).Info("No preemption candidates to select from.")
		return nil
	}

	var best *watcher.PreemptionCandidateSet
	// Filter the candidates with filter rules
	filteredCandidates := []*watcher.PreemptionCandidateSet{}
	for _, candidate := range candidates {
		reject := false
		for _, filter := range s.filters {
			if !filter(ctx, candidate) {
				reject = true
				break
			}
		}
		if !reject {
			filteredCandidates = append(filteredCandidates, candidate)
		}
	}
	logf.V(2).Info("Preemption candidates after filtering", "filteredCandidates", len(filteredCandidates))

	// Multi-cube selector only for now.
	if s.transform != nil {
		filteredCandidates = s.transform(ctx, filteredCandidates)
		logf.V(2).Info("Preemption candidates after transformation", "transformedCandidates", len(filteredCandidates))
	}

	// Sort
	for _, c := range filteredCandidates {
		if best == nil {
			best = c // This is the first valid candidate
			continue
		}

		if s.compare(c, best) {
			best = c // This one is better than the current best
		}
	}

	if best != nil {
		logf.V(2).Info("Selected best preemption candidate", "podName", best.PodName, "numSlices", len(best.Slices), "toBeAssignedPartitions", best.ToBeAssignedPartitions)
	} else {
		logf.V(2).Info("No best preemption candidate found after selection.")
	}

	return best
}
