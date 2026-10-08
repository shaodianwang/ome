/*
Copyright 2026.

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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	slicev1beta1 "tpu.google.com/slice-scheduler/apis/slice/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
)

// gainedPartitions returns the IDs in now that are not in was, or nil if there are none.
func gainedPartitions(was, now map[string]bool) map[string]bool {
	var gained map[string]bool
	for id := range now {
		if !was[id] {
			if gained == nil {
				gained = make(map[string]bool)
			}
			gained[id] = true
		}
	}
	return gained
}

// requeueSlicesOnNoExecutePartitions sends every slice we own that holds a NoExecute-tainted
// partition of the named tree through Reconcile. A bound slice is not in the scheduling
// queue and a taint changes the PartitionTree rather than the Slice, so nothing else would
// pick it up. Reconcile queues the group after rescheduleUnhealthyDelay, and the scheduling
// loop then marks the slice for eviction. onlyPartitions restricts the sweep to slices
// holding one of those partitions; nil sweeps the whole tree.
func (r *SliceReconciler) requeueSlicesOnNoExecutePartitions(treeName string, q workqueue.TypedRateLimitingInterface[reconcile.Request], onlyPartitions map[string]bool) {
	if r.cache == nil || q == nil {
		return
	}
	occupants := r.cache.SlicesOnNoExecutePartitions(treeName)
	if len(occupants) == 0 {
		return
	}
	logf := r.log.WithValues("partitionTree", treeName)
	queued := make(map[string]bool, len(occupants))
	for _, sInfo := range occupants {
		if !r.ownsReservation(sInfo) || queued[sInfo.Name] {
			continue
		}
		if onlyPartitions != nil && !holdsAnyPartition(sInfo, onlyPartitions) {
			continue
		}
		queued[sInfo.Name] = true
		// Slices are cluster-scoped, so the name alone identifies the object.
		q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Name: sInfo.Name}})
		logf.Info("Requeued slice holding a NoExecute partition", "sliceName", sInfo.Name)
	}
}

// holdsAnyPartition reports whether the reservation covers one of the given partitions.
func holdsAnyPartition(sInfo *interfaces.SliceInfo, ids map[string]bool) bool {
	for _, pID := range sInfo.PartitionIds {
		if ids[pID] {
			return true
		}
	}
	return false
}

// ownsSlice and ownsReservation report whether this scheduler manages a Slice or a cached
// reservation. ownsSlice is the For() predicate's ownership test; the NoExecute sweeps
// enqueue by name, which bypasses that predicate, so they apply it themselves. The cache also
// holds reservations belonging to other schedulers and TPU types.
func (r *SliceReconciler) ownsSlice(slice *slicev1beta1.Slice) bool {
	return slice.Annotations[ManagedByAnnotation] == interfaces.SliceSchedulerName &&
		string(slice.Spec.Type) == r.tpuType
}

func (r *SliceReconciler) ownsReservation(sInfo *interfaces.SliceInfo) bool {
	return sInfo != nil && sInfo.ManagedBy == interfaces.SliceSchedulerName && sInfo.Type == r.tpuType
}

// isSliceOnNoExecutePartition reports whether the slice holds a partition that has, or sits
// above, a NoExecute taint the scheduler evicts for. A taint on a partition above it doesn't
// count: an interior partition-state taint says the larger partition can't form a working
// group, not that every partition inside it is bad. A slice already marked for eviction
// doesn't count: the pod reconciler is moving it, and counting it would send every Slice
// update during the drain back through the scheduling queue. Always false unless
// --enable-taint-filtering is set.
func (r *SliceReconciler) isSliceOnNoExecutePartition(slice *slicev1beta1.Slice) bool {
	if r.cache == nil || slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
		return false
	}
	for _, pID := range slice.Spec.PartitionIds {
		if r.cache.HasNoExecuteTaint(pID) {
			return true
		}
	}
	return false
}

// anyMemberAwaitingNoExecuteEviction reports whether a member is still bound to a NoExecute
// partition and not yet marked for eviction.
func (r *SliceReconciler) anyMemberAwaitingNoExecuteEviction(sliceNames []string,
	groupSlices map[string]*slicev1beta1.Slice) bool {
	for _, name := range sliceNames {
		slice, ok := groupSlices[name]
		if ok && r.isEvictionCandidate(slice) && r.isSliceOnNoExecutePartition(slice) {
			return true
		}
	}
	return false
}

// skippedByPolicy reports whether the policy wants the slice left alone, e.g. while the
// slice-controller deactivates it.
func (r *SliceReconciler) skippedByPolicy(slice *slicev1beta1.Slice) bool {
	skip, _, _ := r.policy.ShouldSkipReconcile(slice)
	return skip
}

// isEvictionCandidate reports whether the scheduler may mark the slice for eviction at all:
// it holds partitions, is ours, isn't being deleted or left alone by the policy, and isn't
// marked already.
func (r *SliceReconciler) isEvictionCandidate(slice *slicev1beta1.Slice) bool {
	return len(slice.Spec.PartitionIds) > 0 && slice.DeletionTimestamp.IsZero() && r.ownsSlice(slice) &&
		!r.skippedByPolicy(slice) && slice.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr
}

// evictMembersOnNoExecutePartitions marks every bound member of the group that holds a
// NoExecute partition for eviction, and reports whether it marked any. A member the policy
// skips is left for prepareGroupAllocationRequest, which requeues the group.
//
// TODO(rishibathina): the only wait before eviction is rescheduleUnhealthyDelay (5s by
// default), for every NoExecute key. node.kubernetes.io/not-ready and unreachable need a
// much longer grace, like the 300s default tolerationSeconds Kubernetes applies to them, so
// a short kubelet restart or network blip on one host doesn't evict a whole multi-host
// slice. The controller-owned partition-state and nodes-not-found keys can keep the
// short delay.
func (r *SliceReconciler) evictMembersOnNoExecutePartitions(ctx context.Context, logf logr.Logger,
	sliceNames []string, groupSlices map[string]*slicev1beta1.Slice) (bool, error) {
	marked := false
	for _, name := range sliceNames {
		slice, ok := groupSlices[name]
		if !ok || !r.isEvictionCandidate(slice) || !r.isSliceOnNoExecutePartition(slice) {
			continue
		}
		didMark, err := r.markSliceForEviction(ctx, logf, name, r.isSliceOnNoExecutePartition,
			r.recordNoExecuteEvictionEvent)
		if err != nil {
			return marked, err
		}
		marked = marked || didMark
	}
	return marked, nil
}

// markSliceForEviction sets the evict-pods annotation, so the pod reconciler evicts the
// slice's pods and then clears its partitions, records the event with record, and reports
// whether it did. It re-checks stillApplies against the fetched slice, since the cached
// copy the caller looked at may be stale.
func (r *SliceReconciler) markSliceForEviction(ctx context.Context, logf logr.Logger, sliceName string,
	stillApplies func(*slicev1beta1.Slice) bool, record func(*slicev1beta1.Slice)) (bool, error) {
	for attempt := 0; attempt < maxBindRetries; attempt++ {
		slice := &slicev1beta1.Slice{}
		if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if !r.isEvictionCandidate(slice) || !stillApplies(slice) {
			return false, nil
		}
		if slice.Annotations == nil {
			slice.Annotations = make(map[string]string)
		}
		slice.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
		if err := r.Update(ctx, slice); err != nil {
			if errors.IsConflict(err) {
				time.Sleep(10 * time.Millisecond * time.Duration(attempt+1))
				continue
			}
			return false, fmt.Errorf("failed to annotate slice %s for eviction: %w", sliceName, err)
		}
		logf.Info("Marked slice for eviction", "sliceName", sliceName, "partitionIds", slice.Spec.PartitionIds)
		record(slice)
		return true, nil
	}
	return false, fmt.Errorf("failed to annotate slice %s for eviction after %d retries due to conflicts", sliceName, maxBindRetries)
}
