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
	"container/heap"
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	partitiontree "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "tpu.google.com/slice-scheduler/apis/slice/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/metrics"
	"tpu.google.com/slice-scheduler/internal/policy"
	"tpu.google.com/slice-scheduler/internal/pq"
	"tpu.google.com/slice-scheduler/internal/util"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

const (
	TpuPodNameLabel                = "cloud.google.com/tpu-pod-id"
	GceTopologyBlockLabel          = "cloud.google.com/gce-topology-block"
	SlicePriorityAnnotation        = "cloud.google.com/slice-priority"
	NodeLabelPartitionID           = "cloud.google.com/gke-tpu-partition-"
	TrilliumRootPartitionIDLabel   = "cloud.google.com/gke-tpu-partition-16x16-id"
	IronwoodRootPartitionIDLabel   = "cloud.google.com/gke-tpu-partition-4x4x4-id"
	TPU7x                          = "tpu7x"
	TPU6e                          = "v6e"
	ManagedByAnnotation            = "cloud.google.com/managed-by"
	SliceGroupNameAnnotation       = "cloud.google.com/slice-group-name"
	NumSlicesAnnotation            = "cloud.google.com/num-slices"
	DegradationToleranceAnnotation = "cloud.google.com/ici-degradation-tolerance"

	// Degradation tolerance values
	ToleranceNone = "none"
	ToleranceAll  = "all"
)

// Retry delays for scheduling operations.
const (
	retryDelayFetchSlice     = 1 * time.Second
	retryDelayNotSchedulable = 5 * time.Second
	retryDelayReservation    = 1 * time.Second
	retryDelayBind           = 1 * time.Second

	// maxBindRetries is the number of times to retry a slice update on conflict.
	maxBindRetries = 3
)

// APIUpdateError represents an error that occurred during API update.
// It carries the details of which victims failed to be cleared and/or
// which candidates failed to be bound to allow precise rollback in cache.
type APIUpdateError struct {
	FailedVictims    []*interfaces.SliceInfo
	FailedCandidates []*interfaces.SliceInfo
	Err              error
}

func (e *APIUpdateError) Error() string {
	return fmt.Sprintf("API update failed: %v", e.Err)
}

func (e *APIUpdateError) Unwrap() error {
	return e.Err
}

// SliceTaskState tracks the async operation state for a slice.
type SliceTaskState string

const (
	SliceStateBinding  SliceTaskState = "binding"
	SliceStateEvicting SliceTaskState = "evicting"
)

// SliceReconciler reconciles a Slice object
type SliceReconciler struct {
	client.Client
	Scheme                   *runtime.Scheme
	Recorder                 record.EventRecorder
	log                      logr.Logger
	tpuType                  string
	cache                    *watcher.PartitionCache
	watcher                  interfaces.ClusterWatcher
	schedulerLib             interfaces.SchedulerLibrary
	policy                   policy.SlicePolicy
	rescheduleHealthyDelay   time.Duration
	rescheduleUnhealthyDelay time.Duration
	rescheduleTimeout        time.Duration

	// failureTimestamps tracks when a slice first entered a failure backoff
	// state. Used to enforce a timeout on healthy-partition backoff.
	failureTimestamps map[string]time.Time
	failureMutex      sync.RWMutex

	// schedulingMutex serializes the schedule→reserve→bind critical section.
	// This prevents watch-handler cache updates from corrupting in-flight
	// scheduling state. Since there is only one scheduling worker goroutine,
	// this mutex serializes watch handlers against the scheduler, not
	// concurrent schedulers.
	schedulingMutex sync.Mutex

	// schedulingQueue holds slices waiting to be scheduled.
	schedulingQueue *pq.PriorityQueue
	queueMutex      sync.Mutex
	queueCond       *sync.Cond

	// tryScheduleSliceGroupFn is a function that attempts to schedule a slice.
	// This is a field to allow for mocking in tests.
	tryScheduleSliceGroupFn func(context.Context, string) (bool, time.Duration)

	// requeueWithDelayFn is a function that requeues a slice item with a delay.
	// This is a field to allow for mocking in tests.
	requeueWithDelayFn  func(context.Context, *pq.Item, time.Duration)
	enableNodeReadiness bool
	// group-aware cache
	groups          map[string]*interfaces.SliceGroup
	sliceToGroup    map[string]string
	groupMutex      sync.RWMutex
	sliceTasks      map[string]SliceTaskState
	sliceTasksMutex sync.RWMutex

	// bindingQueue holds tasks for background API binding.
	bindingQueue   chan *BindingTask
	bindingWorkers int
}

// BindingTask holds all data needed by a background worker to complete
// the Kubernetes API updates for a scheduled group.
type BindingTask struct {
	groupId         string
	groupSlices     map[string]*slicev1beta1.Slice
	slicesToReserve []*interfaces.SliceInfo
	victims         []*interfaces.SliceInfo
	req             interfaces.GroupAllocationRequest
	candidate       *interfaces.PlacementCandidate // for metrics
	firstTopology   string
	startTime       time.Time
}

func NewSliceReconciler(k8sClient client.Client, scheme *runtime.Scheme, logger logr.Logger, tpuType string, recorder record.EventRecorder, enableNodeReadiness bool, schedulerLib interfaces.SchedulerLibrary, cache *watcher.PartitionCache, rescheduleHealthyDelay time.Duration, rescheduleUnhealthyDelay time.Duration, slicePolicy policy.SlicePolicy, rescheduleTimeout time.Duration) (*SliceReconciler, error) {
	if tpuType != TPU6e && tpuType != TPU7x {
		return nil, fmt.Errorf("invalid tpuType %s", tpuType)
	}
	r := &SliceReconciler{
		Client:                   k8sClient,
		Scheme:                   scheme,
		Recorder:                 recorder,
		log:                      logger,
		tpuType:                  tpuType,
		enableNodeReadiness:      enableNodeReadiness,
		schedulerLib:             schedulerLib,
		cache:                    cache,
		policy:                   slicePolicy,
		rescheduleHealthyDelay:   rescheduleHealthyDelay,
		rescheduleUnhealthyDelay: rescheduleUnhealthyDelay,
		rescheduleTimeout:        rescheduleTimeout,
		failureTimestamps:        make(map[string]time.Time),
		groups:                   make(map[string]*interfaces.SliceGroup),
		sliceToGroup:             make(map[string]string),
		sliceTasks:               make(map[string]SliceTaskState),
	}

	r.watcher = watcher.NewPartitionWatcher(r.cache)
	r.schedulingQueue = pq.NewPriorityQueue()
	r.queueCond = sync.NewCond(&r.queueMutex)
	r.tryScheduleSliceGroupFn = r.tryScheduleSliceGroup
	r.requeueWithDelayFn = r.requeueWithDelay

	return r, nil
}

// EnableAsyncBinding explicitly enables background asynchronous API updates.
func (r *SliceReconciler) EnableAsyncBinding(queueSize int, numWorkers int) {
	r.bindingQueue = make(chan *BindingTask, queueSize)
	r.bindingWorkers = numWorkers
}

// Start is called by the Manager. It builds the initial in-memory cache of the cluster's partition state.
// This is guaranteed to complete before any watch events or reconcile loops are started.
func (r *SliceReconciler) Start(ctx context.Context) error {
	logf := r.log.WithName("cache-builder")
	logf.Info("Building initial partition state cache...")

	// 1. List all PartitionTree CRs to build the basic tree structure.
	partitionTreeList := &partitiontree.PartitionTreeList{}
	if err := r.List(ctx, partitionTreeList); err != nil {
		return fmt.Errorf("listing partition trees for cache: %w", err)
	}

	for i := range partitionTreeList.Items {
		tree := &partitionTreeList.Items[i]

		if tree.Status.Root == nil {
			logf.Info("Skipping PartitionTree with no root (GCE poll may not have completed yet)",
				"treeName", tree.Name)
			continue
		}

		var rootPartitionIDLabelKey string
		switch r.tpuType {
		case TPU7x:
			rootPartitionIDLabelKey = IronwoodRootPartitionIDLabel
		case TPU6e:
			rootPartitionIDLabelKey = TrilliumRootPartitionIDLabel
		}
		rootPartitionIDValue := tree.Status.Root.PartitionID
		// Get nodes with matching pod name label to build the tree
		nodeList := &corev1.NodeList{}
		if err := r.List(ctx, nodeList, client.MatchingLabels{rootPartitionIDLabelKey: rootPartitionIDValue}); err != nil {
			// Currently we are not making the node readiness as partition-picking prerequisite if the k8s node object is not retrievable.
			logf.Error(err, "unable to list nodes for cache creation", "tree", tree.Name)
		}
		r.cache.UpdateTree(ctx, tree, nodeList.Items)
	}

	// 2. List all Slice CRs to mark partitions that are already in use.
	// This is to make sure we get the partitions assign to slices that are not
	// yet created in the underlying accelerator infrastructure
	allSlices := &slicev1beta1.SliceList{}
	if err := r.List(ctx, allSlices); err != nil {
		return fmt.Errorf("listing slices for cache: %w", err)
	}
	for _, s := range allSlices.Items {
		r.cache.ReservePartition(ctx, sliceInfo(ctx, &s))
		r.updateGroupCache(ctx, &s)
	}

	// Launch the scheduler worker goroutine now that the cache is initialized.
	if r.schedulerLib != nil {
		go r.processSchedulingQueue(ctx)
		if r.bindingQueue != nil {
			// Spawn a pool of workers for asynchronous binding.
			for i := 0; i < r.bindingWorkers; i++ {
				go r.processBindingQueue(ctx, i)
			}
		}
	}

	logf.Info("Initial partition state cache built successfully")
	return nil
}

// updateGroupCache updates the reconciler's internal group cache with the latest state of the given slice.
// It returns the slice group name associated with the slice.
func (r *SliceReconciler) updateGroupCache(ctx context.Context, slice *slicev1beta1.Slice) string {
	r.groupMutex.Lock()
	defer r.groupMutex.Unlock()

	sliceGroupName := getSliceGroupName(slice)

	// Remove from old group if slice group name changed
	if oldGroupId, ok := r.sliceToGroup[slice.Name]; ok && oldGroupId != sliceGroupName {
		if oldGroup, ok := r.groups[oldGroupId]; ok {
			delete(oldGroup.Slices, slice.Name)
			if len(oldGroup.Slices) == 0 {
				delete(r.groups, oldGroupId)
			}
		}
	}

	numSlices := getNumSlices(ctx, slice)
	priority := int32(getSlicePriority(ctx, slice))

	group, ok := r.groups[sliceGroupName]
	if !ok {
		group = &interfaces.SliceGroup{
			Name:         sliceGroupName,
			ExpectedSize: numSlices,
			MinSize:      numSlices, // Default to ExpectedSize
			Slices:       make(map[string]*interfaces.SliceInfo),
			Priority:     priority,
		}
		r.groups[sliceGroupName] = group
	}

	// Update the slice in the group
	group.Slices[slice.Name] = sliceInfo(ctx, slice)
	r.sliceToGroup[slice.Name] = sliceGroupName

	// Update priority if it's different (we assume consistent priority across group, but we'll take the latest)
	group.Priority = priority

	return sliceGroupName
}

// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=partition.tpu.google.com,resources=partitiontrees,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *SliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logf := log.FromContext(ctx)
	logf.Info("Reconciliation triggered, adding slice to scheduling queue", "sliceName", req.Name)

	// Fetch the Slice instance to get its priority
	slice := &slicev1beta1.Slice{}
	if err := r.Get(ctx, req.NamespacedName, slice); err != nil {
		logf.Error(err, "unable to fetch Slice for queueing")
		metrics.ReconciliationErrorsTotal.WithLabelValues("GetSliceError").Inc()
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Policy check: skip reconciliation for environment-specific reasons
	// (e.g., deactivation in progress in cloud). This policy-based check
	// runs BEFORE updateGroupCache.
	if skip, reason, requeueAfter := r.policy.ShouldSkipReconcile(slice); skip {
		logf.Info("Skipping slice reconciliation", "sliceName", req.Name, "reason", reason)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	sliceGroupName := r.updateGroupCache(ctx, slice)

	if !r.needsScheduling(slice) {
		logf.Info("Slice does not need scheduling, skipping queueing", "sliceName", req.Name)
		return ctrl.Result{}, nil
	}

	r.groupMutex.RLock()
	group, ok := r.groups[sliceGroupName]
	r.groupMutex.RUnlock()

	priority := 0
	if ok {
		priority = int(group.Priority)
	} else {
		priority = getSlicePriority(ctx, slice)
	}

	item := &pq.Item{
		GroupId:  sliceGroupName,
		Topology: slice.Spec.Topology,
		Priority: priority,
	}

	// If the rescheduling happens due to failed/unhealthy slice then wait some
	// time before re-scheduling. This is to give enough time for the underlying
	// infrastructure and scheduler cache to sync.
	if r.policy.NeedsRescheduling(slice) {
		// TODO(prod-readiness): This goroutine has no deduplication — rapid
		// healthy/unhealthy flapping can spawn many delay goroutines for the
		// same slice. Bounded by context cancellation + PQ dedup, but consider
		// adding a per-slice timer map to prevent goroutine accumulation.
		go r.requeueWithDelayFn(ctx, item, r.rescheduleUnhealthyDelay)
	} else {
		r.requeueWithDelayFn(ctx, item, 0*time.Second)
	}

	return ctrl.Result{}, nil
}

// Requeues a slice item in the priority queue with a delay
func (r *SliceReconciler) requeueWithDelay(ctx context.Context, item *pq.Item, delay time.Duration) {
	// Use a timer channel to make the sleep interruptible
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// Context cancelled, don't requeue
		return
	case <-timer.C:
		// Timer finished, do the requeue
		r.queueMutex.Lock()
		item.EnqueueTimeStamp = time.Now()
		heap.Push(r.schedulingQueue, item)
		r.updateQueueDepthMetric()
		r.queueCond.Signal() // Signal the worker
		r.queueMutex.Unlock()
	}
}

// updateQueueDepthMetric updates the SliceQueueDepth gauge metric by counting
// the number of items in the queue for each topology.
// The caller must hold the queueMutex.
func (r *SliceReconciler) updateQueueDepthMetric() {
	metrics.SliceQueueDepth.Reset()
	topologyCounts := make(map[string]int)
	for _, item := range *r.schedulingQueue {
		if item != nil {
			topologyCounts[item.Topology]++
		}
	}
	for topology, count := range topologyCounts {
		metrics.SliceQueueDepth.WithLabelValues(r.tpuType, topology).Set(float64(count))
	}
}

func (r *SliceReconciler) processSchedulingQueue(ctx context.Context) {
	logf := r.log.WithName("scheduler-worker")
	logf.Info("Scheduler worker started.")

	// Start a goroutine to shut down the worker when context is done.
	// We do this by broadcasting to all waiting workers.
	go func() {
		<-ctx.Done()
		logf.Info("Context cancelled, signaling worker to shut down.")
		r.queueCond.Broadcast() // Wake up all waiting goroutines
	}()

	for {
		r.queueMutex.Lock()

		// We use a 'for' loop instead of 'if' to handle spurious wakeups.
		for r.schedulingQueue.Len() == 0 {

			// Check for shutdown *before* waiting
			if ctx.Err() != nil {
				r.queueMutex.Unlock()
				logf.Info("Scheduler worker shutting down.")
				return
			}

			// Wait() atomically unlocks the mutex, sleeps, and re-locks on wake-up
			logf.V(1).Info("Queue is empty, worker is going to sleep.")
			r.queueCond.Wait()
		}

		// If we are here, we have the lock AND the queue is not empty.

		// Check for shutdown *after* wake-up
		if ctx.Err() != nil {
			r.queueMutex.Unlock()
			logf.Info("Scheduler worker shutting down.")
			return
		}

		item := heap.Pop(r.schedulingQueue).(*pq.Item)
		metrics.SliceSchedulerQueueLatencySeconds.WithLabelValues(r.tpuType, item.Topology).Observe(time.Since(item.EnqueueTimeStamp).Seconds())
		r.updateQueueDepthMetric()
		r.queueMutex.Unlock() // Unlock *before* processing the item

		r.groupMutex.RLock()
		group, ok := r.groups[item.GroupId]
		r.groupMutex.RUnlock()

		if !ok {
			logf.Info("Group not found in cache, skipping", "groupId", item.GroupId)
			continue
		}

		if !group.IsReady() {
			logf.Info("Group is not yet complete, skipping", "groupId", item.GroupId, "currentSize", len(group.Slices), "expectedSize", group.ExpectedSize)
			continue
		}

		logf.Info("Processing group from queue", "groupId", item.GroupId)
		if requeue, requeueAfter := r.tryScheduleSliceGroupFn(ctx, item.GroupId); requeue {
			logf.Info("Requeuing failed group", "groupId", item.GroupId, "requeueAfter", requeueAfter)
			go r.requeueWithDelayFn(ctx, item, requeueAfter)
		}
	}
}

// tryScheduleSliceGroup contains the core logic for scheduling a group of slices.
// It returns a boolean indicating if the group should be requeued, and a duration for the requeue delay.
//
//nolint:gocyclo
func (r *SliceReconciler) tryScheduleSliceGroup(ctx context.Context, groupId string) (bool, time.Duration) {
	logf := log.FromContext(ctx, "groupId", groupId)
	startTime := time.Now()
	success := false

	// Step 1: Fetch the list of slice names and group priority from memory.
	sliceNames, groupPriority, ok := r.getGroupInfo(groupId)
	if !ok {
		logf.Info("Group not found for scheduling, likely removed by concurrent update", "groupId", groupId)
		return false, 0
	}

	// Step 2: Fetch the latest Kubernetes Slice resources for this group from the API.
	groupSlices, err := r.fetchGroupSlices(ctx, groupId, sliceNames)
	if err != nil {
		logf.Error(err, "unable to fetch Slice in group")
		return true, retryDelayFetchSlice
	}

	// Check if any slice is currently blocked (binding/evicting) using
	// the already-fetched objects to avoid redundant API calls.
	anyBlocked := false
	for _, name := range sliceNames {
		if slice, ok := groupSlices[name]; ok {
			if r.isSliceBlockedWithObj(slice) {
				anyBlocked = true
				break
			}
		}
	}
	if anyBlocked {
		logf.Info("Skipping scheduling because some slices in gang are blocked (evicting or binding)")
		return false, 0
	}

	// Step 3: Construct the internal allocation request and check for rescheduling needs.
	req, isRescheduling, reschedulingPartitions, requeueAfter, err := r.prepareGroupAllocationRequest(groupId, groupPriority, groupSlices, sliceNames)
	if err != nil {
		logf.Info("Group scheduling aborted", "reason", err)
		return true, requeueAfter
	}
	if len(req.Slices) == 0 {
		logf.Info("Group needs no scheduling")
		return false, 0
	}

	var firstTopology string
	if len(req.Slices) > 0 {
		firstTopology = req.Slices[0].Topology
	}

	// Cloud-specific: rescheduleTimeout + policy backoff
	if r.rescheduleTimeout > 0 && r.shouldBackoffHealthy(logf, isRescheduling, reschedulingPartitions) {
		shouldBackoff := true
		var backoffSliceName string

		// sliceNames from getGroupInfo is already sorted.
		sortedSliceNames := sliceNames

		for _, sliceName := range sortedSliceNames {
			slice := groupSlices[sliceName]
			if r.needsScheduling(slice) && len(slice.Spec.PartitionIds) > 0 {
				if !r.policy.ShouldBackoffOnFailure(slice) {
					shouldBackoff = false
				}
				if backoffSliceName == "" {
					backoffSliceName = sliceName
				}
			}
		}
		if backoffSliceName == "" {
			shouldBackoff = false
		}
		if shouldBackoff {
			if continueBackoff, delay := r.checkFailureBackoff(backoffSliceName); continueBackoff {
				return true, delay
			}
			logf.Info("Backoff timeout reached, forcing reschedule", "sliceName", backoffSliceName,
				"timeout", r.rescheduleTimeout)
		}
		for sliceName := range groupSlices {
			r.clearFailureTimestamp(sliceName)
		}
	}

	logf.Info("Attempting to schedule group", "isRescheduling", isRescheduling)

	if r.schedulerLib == nil {
		logf.Info("Ignoring group with unknown TPU type", "tpuType", r.tpuType)
		return false, 0
	}

	metrics.SliceScheduleAttemptsTotal.WithLabelValues(r.tpuType, firstTopology, fmt.Sprintf("%t", isRescheduling)).Inc()
	var result interfaces.GroupAllocationResult
	defer func() {
		if !success && (r.bindingQueue == nil || !result.IsSchedulable) {
			duration := time.Since(startTime).Seconds()
			metrics.SliceScheduleDurationSeconds.WithLabelValues(r.tpuType, firstTopology, interfaces.FalseStr).Observe(duration)
		}
	}()

	// Hold the scheduling mutex for the schedule→reserve critical section.
	r.schedulingMutex.Lock()

	// If any slice is being rescheduled, release its partitions from the cache for this attempt.
	if isRescheduling {
		logf.Info("Releasing partitions for rescheduling attempt", "groupId", groupId)
		for _, slice := range groupSlices {
			if len(slice.Spec.PartitionIds) == 0 {
				continue
			}
			r.watcher.Unreserve(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
		}
	}

	state := r.watcher.GetState()
	result = r.schedulerLib.Schedule(ctx, req, state)

	if !result.IsSchedulable {
		r.schedulingMutex.Unlock()
		return r.handleUnschedulableResult(ctx, groupId, groupSlices, req, result)
	}

	// Step 8: Extract assigned slices and potential preempted victim slices from the result.
	slicesToReserve, victims := r.buildSlicesToReserveAndVictims(req, result)

	// Step 9: Reserve the newly allocated partitions in the local PartitionCache.
	requeue, delay, reserveErr := r.reserveInCache(ctx, slicesToReserve, victims)
	r.schedulingMutex.Unlock()
	if reserveErr != nil || requeue {
		return requeue, delay
	}

	// Step 10: Queue the binding task for asynchronous API update.
	groupSlicesCopy := make(map[string]*slicev1beta1.Slice, len(groupSlices))
	for k, v := range groupSlices {
		groupSlicesCopy[k] = v.DeepCopy()
	}

	// Register task states to protect against concurrent scheduling interference.
	r.sliceTasksMutex.Lock()
	if r.sliceTasks == nil {
		r.sliceTasks = make(map[string]SliceTaskState)
	}
	for _, s := range slicesToReserve {
		r.sliceTasks[s.Name] = SliceStateBinding
	}
	for _, v := range victims {
		r.sliceTasks[v.Name] = SliceStateEvicting
	}
	r.sliceTasksMutex.Unlock()

	task := &BindingTask{
		groupId:         groupId,
		groupSlices:     groupSlicesCopy,
		slicesToReserve: slicesToReserve,
		victims:         deepCopySliceInfo(victims),
		req:             req,
		candidate:       result.Candidate,
		firstTopology:   firstTopology,
		startTime:       startTime,
	}

	// Attempt asynchronous binding if the queue is available and not full.
	if r.bindingQueue != nil {
		select {
		case r.bindingQueue <- task:
			logf.Info("Group scheduled in cache and queued for async API binding", "groupId", groupId)
			success = true
			return false, 0
		default:
			logf.Info("Binding queue is full, falling back to synchronous API binding")
		}
	} else {
		logf.Info("Binding queue is nil, performing synchronous API binding")
	}

	// Synchronous binding fallback.
	// Keep task state in r.sliceTasks while updateAPIAndHandleErrors runs to protect the sync operation.
	requeue, delay = r.updateAPIAndHandleErrors(ctx, groupId, groupSlices, slicesToReserve, victims)

	r.cleanUpSliceTasks(slicesToReserve, victims)

	if requeue {
		return requeue, delay
	}

	success = true
	r.finalizeSchedulingSuccess(logf, task)
	return false, 0
}

// getGroupInfo thread-safely fetches the list of slice names and the priority for a given slice group.
func (r *SliceReconciler) getGroupInfo(groupId string) ([]string, int32, bool) {
	r.groupMutex.RLock()
	defer r.groupMutex.RUnlock()
	group, ok := r.groups[groupId]
	if !ok {
		return nil, 0, false
	}
	sliceNames := make([]string, 0, len(group.Slices))
	for sliceName := range group.Slices {
		sliceNames = append(sliceNames, sliceName)
	}
	sort.Strings(sliceNames)
	return sliceNames, group.Priority, true
}

// isSliceBlocked checks if a slice is currently undergoing an active binding or eviction task,
// or if it has existing partitions and is annotated for pod eviction.
func (r *SliceReconciler) isSliceBlocked(ctx context.Context, sliceName string) bool {
	r.sliceTasksMutex.RLock()
	_, hasTask := r.sliceTasks[sliceName]
	r.sliceTasksMutex.RUnlock()
	if hasTask {
		return true
	}

	slice := &slicev1beta1.Slice{}
	if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err == nil {
		if len(slice.Spec.PartitionIds) > 0 && slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
			return true
		}
	}
	return false
}

// isSliceBlockedWithObj checks if a slice is blocked using an already-fetched
// Slice object, avoiding redundant API calls. Use this when the caller has
// already fetched the slice (e.g., via fetchGroupSlices).
func (r *SliceReconciler) isSliceBlockedWithObj(slice *slicev1beta1.Slice) bool {
	r.sliceTasksMutex.RLock()
	_, hasTask := r.sliceTasks[slice.Name]
	r.sliceTasksMutex.RUnlock()
	if hasTask {
		return true
	}
	if len(slice.Spec.PartitionIds) > 0 && slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
		return true
	}
	return false
}

func (r *SliceReconciler) isSliceEvicting(ctx context.Context, sliceName string) bool {
	r.sliceTasksMutex.RLock()
	state, hasTask := r.sliceTasks[sliceName]
	r.sliceTasksMutex.RUnlock()
	if hasTask && state == SliceStateEvicting {
		return true
	}

	slice := &slicev1beta1.Slice{}
	if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err == nil {
		if len(slice.Spec.PartitionIds) > 0 && slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr {
			return true
		}
	}
	return false
}

// fetchGroupSlices fetches the latest Kubernetes Slice resources for all slices in a group from the API server.
func (r *SliceReconciler) fetchGroupSlices(ctx context.Context, groupId string, sliceNames []string) (map[string]*slicev1beta1.Slice, error) {
	logf := log.FromContext(ctx, "groupId", groupId)
	groupSlices := make(map[string]*slicev1beta1.Slice)
	for _, sliceName := range sliceNames {
		slice := &slicev1beta1.Slice{}
		if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err != nil {
			logf.Error(err, "unable to fetch Slice in group", "sliceName", sliceName)
			return nil, err
		}
		groupSlices[sliceName] = slice
	}
	return groupSlices, nil
}

// prepareGroupAllocationRequest constructs the internal scheduling request structure, determines if this is a rescheduling attempt,
// and identifies currently reserved partitions to backoff if healthy.
func (r *SliceReconciler) prepareGroupAllocationRequest(groupId string, groupPriority int32, groupSlices map[string]*slicev1beta1.Slice, sliceNames []string) (interfaces.GroupAllocationRequest, bool, []string, time.Duration, error) {
	logf := r.log.WithValues("groupId", groupId)
	req := interfaces.GroupAllocationRequest{
		GroupName: groupId,
		Priority:  groupPriority,
		Slices:    make([]interfaces.SliceRequirement, 0, len(groupSlices)),
	}
	isRescheduling := false
	var reschedulingPartitions []string

	for _, sliceName := range sliceNames {
		slice := groupSlices[sliceName]
		if !r.needsScheduling(slice) {
			continue
		}
		// Cloud-specific: policy skip check in worker path.
		if skip, reason, requeueAfter := r.policy.ShouldSkipReconcile(slice); skip {
			logf.Info("Aborting group scheduling, slice being managed by policy",
				"sliceName", sliceName, "reason", reason)
			return interfaces.GroupAllocationRequest{}, false, nil, requeueAfter, fmt.Errorf("policy skip: %s", reason)
		}
		tolerance := ToleranceNone
		if string(slice.Spec.Type) == TPU7x {
			tolerance = slice.Annotations[DegradationToleranceAnnotation]
			switch tolerance {
			case ToleranceNone, ToleranceAll:
				// Valid values
			default:
				if util.IsSingleCubeOrLess(slice.Spec.Topology) {
					tolerance = ToleranceAll
				} else {
					tolerance = ToleranceNone
				}
			}
		}
		req.Slices = append(req.Slices, interfaces.SliceRequirement{
			SliceName:            sliceName,
			Type:                 string(slice.Spec.Type),
			Topology:             slice.Spec.Topology,
			DegradationTolerance: tolerance,
			CurrentPartitionIDs:  slice.Spec.PartitionIds, // read currently assigned partitions; attach them to the request
		})
		if len(slice.Spec.PartitionIds) > 0 {
			isRescheduling = true
			reschedulingPartitions = append(reschedulingPartitions, slice.Spec.PartitionIds...)
		}
	}
	return req, isRescheduling, reschedulingPartitions, 0, nil
}

// buildSlicesToReserveAndVictims extracts SliceInfo structures for both the newly assigned slices and preempted victim slices.
func (r *SliceReconciler) buildSlicesToReserveAndVictims(req interfaces.GroupAllocationRequest, result interfaces.GroupAllocationResult) ([]*interfaces.SliceInfo, []*interfaces.SliceInfo) {
	slicesToReserve := make([]*interfaces.SliceInfo, 0, len(req.Slices))
	for _, sliceReq := range req.Slices {
		var ids []string
		for _, p := range result.Candidate.Assignments[sliceReq.SliceName] {
			ids = append(ids, p.ID)
		}
		slicesToReserve = append(slicesToReserve, &interfaces.SliceInfo{
			Name:         sliceReq.SliceName,
			Priority:     int(req.Priority),
			Topology:     sliceReq.Topology,
			Type:         sliceReq.Type,
			PartitionIds: ids,
			ManagedBy:    interfaces.SliceSchedulerName,
		})
	}
	return slicesToReserve, result.Candidate.Victims
}

// reserveInCache reserves the assigned partitions for candidates in the PartitionCache
// and handles the necessary un-reservations for preempted victims.
// Caller MUST hold schedulingMutex.
func (r *SliceReconciler) reserveInCache(ctx context.Context, slicesToReserve []*interfaces.SliceInfo, victims []*interfaces.SliceInfo) (bool, time.Duration, error) {
	logf := log.FromContext(ctx, "slicesToReserve", len(slicesToReserve))
	if len(victims) > 0 {
		r.watcher.Unreserve(ctx, victims)
	}
	firstTopology := ""
	if len(slicesToReserve) > 0 {
		firstTopology = slicesToReserve[0].Topology
	}
	if err := r.watcher.Reserve(ctx, slicesToReserve); err != nil {
		logf.Error(err, "Failed to reserve partitions for group")
		groupTpuType := ""
		if len(slicesToReserve) > 0 {
			groupTpuType = slicesToReserve[0].Type
		}
		metrics.SliceScheduleErrorsTotal.WithLabelValues(groupTpuType, firstTopology, "reserve_failed").Inc()
		if len(victims) > 0 {
			for _, vInfo := range victims {
				if err := r.safeReserve(ctx, vInfo); err != nil {
					logf.Error(err, "Failed to restore victim on reserve failure", "sliceName", vInfo.Name)
				}
			}
		}
		return true, retryDelayReservation, err
	}
	return false, 0, nil
}

// updateAPIAndHandleErrors binds the allocated partitions to the slices via the Kubernetes API
// and handles any API errors by rolling back the reservations in the PartitionCache as needed.
func (r *SliceReconciler) updateAPIAndHandleErrors(ctx context.Context, groupId string, groupSlices map[string]*slicev1beta1.Slice, slicesToReserve []*interfaces.SliceInfo, victims []*interfaces.SliceInfo) (bool, time.Duration) {
	logf := log.FromContext(ctx, "groupId", groupId)

	// Abort if any candidate is already being evicted.
	anyCandidateEvicting := false
	for _, s := range slicesToReserve {
		if r.isSliceEvicting(ctx, s.Name) {
			anyCandidateEvicting = true
			break
		}
	}

	firstTopology := ""
	if len(slicesToReserve) > 0 {
		firstTopology = slicesToReserve[0].Topology
	} else if len(victims) > 0 {
		firstTopology = victims[0].Topology
	}

	if len(slicesToReserve) > 0 && anyCandidateEvicting {
		logf.Info("Aborting async binding because some candidates are being evicted in the meantime", "groupId", groupId)
		r.rollbackFull(ctx, slicesToReserve, victims, "abort")
		groupTpuType := ""
		if len(slicesToReserve) > 0 {
			groupTpuType = slicesToReserve[0].Type
		} else if len(victims) > 0 {
			groupTpuType = victims[0].Type
		}
		metrics.SliceScheduleErrorsTotal.WithLabelValues(groupTpuType, firstTopology, "aborted_candidate_evicting").Inc()
		return true, retryDelayNotSchedulable
	}

	if err := r.bindPartitionsToGroup(ctx, groupId, groupSlices, slicesToReserve, victims); err != nil {
		logf.Error(err, "Failed to update partitions in API")

		errReason := "api_update_failed"
		var apiErr *APIUpdateError
		if stderrors.As(err, &apiErr) {
			if len(apiErr.FailedVictims) > 0 {
				errReason = "clear_victims_failed"
			} else {
				errReason = "bind_failed"
			}
			r.rollbackPartial(ctx, apiErr)
		} else {
			r.rollbackFull(ctx, slicesToReserve, victims, "update failure")
		}

		groupTpuType := ""
		if len(slicesToReserve) > 0 {
			groupTpuType = slicesToReserve[0].Type
		} else if len(victims) > 0 {
			groupTpuType = victims[0].Type
		}
		metrics.SliceScheduleErrorsTotal.WithLabelValues(groupTpuType, firstTopology, errReason).Inc()
		return true, retryDelayBind
	}
	return false, 0
}

// handleUnschedulableResult manages rollback and event emission when a group cannot be scheduled.
func (r *SliceReconciler) handleUnschedulableResult(ctx context.Context, groupId string, groupSlices map[string]*slicev1beta1.Slice, req interfaces.GroupAllocationRequest, result interfaces.GroupAllocationResult) (bool, time.Duration) {
	logf := log.FromContext(ctx, "groupId", groupId)
	logf.Info("Group is not schedulable", "reason", result.FailureReason)
	firstTopology := ""
	if len(req.Slices) > 0 {
		firstTopology = req.Slices[0].Topology
	}
	metrics.SliceScheduleErrorsTotal.WithLabelValues(r.tpuType, firstTopology, result.FailureReason).Inc()

	if r.hasExistingAllocations(groupSlices) {
		logf.Info("Evicting unschedulable group with existing allocations", "groupId", groupId)

		var victims []*interfaces.SliceInfo
		for _, slice := range groupSlices {
			if len(slice.Spec.PartitionIds) > 0 {
				victims = append(victims, sliceInfo(ctx, slice))
			}
		}

		if len(victims) > 0 {
			// Register task states to protect against race conditions during eviction.
			r.sliceTasksMutex.Lock()
			if r.sliceTasks == nil {
				r.sliceTasks = make(map[string]SliceTaskState)
			}
			for _, v := range victims {
				r.sliceTasks[v.Name] = SliceStateEvicting
			}
			r.sliceTasksMutex.Unlock()

			// Try async eviction first.
			if r.bindingQueue != nil {
				evictTopology := ""
				if len(req.Slices) > 0 {
					evictTopology = req.Slices[0].Topology
				} else if len(victims) > 0 {
					evictTopology = victims[0].Topology
				}
				task := &BindingTask{
					groupId:         groupId,
					groupSlices:     nil,
					victims:         victims,
					slicesToReserve: nil,
					req:             req,
					firstTopology:   evictTopology,
					startTime:       time.Now(),
				}

				select {
				case r.bindingQueue <- task:
					logf.Info("Queued async eviction for unschedulable group", "groupId", groupId)
					return false, 0
				default:
					logf.Info("Binding queue is full, falling back to sync eviction")
				}
			}

			// Sync fallback: Cloud uses evictSliceWithRetry.
			for sliceName, slice := range groupSlices {
				if len(slice.Spec.PartitionIds) > 0 {
					if err := r.evictSliceWithRetry(ctx, logf, sliceName); err != nil {
						logf.Error(err, "Failed to evict partitions from slice", "sliceName", sliceName)
					}
				}
				r.recordSchedulingFailure(slice)
			}

			r.cleanUpSliceTasks(nil, victims)
		}
	} else {
		for _, sliceReq := range req.Slices {
			if slice, ok := groupSlices[sliceReq.SliceName]; ok {
				r.recordSchedulingFailure(slice)
			}
		}
	}
	return true, retryDelayNotSchedulable
}

// safeReserve reserves a partition in the cache only if the slice still exists in the API.
func (r *SliceReconciler) safeReserve(ctx context.Context, sInfo *interfaces.SliceInfo) error {
	if sInfo == nil {
		return nil
	}

	if err := r.watcher.Reserve(ctx, []*interfaces.SliceInfo{sInfo}); err != nil {
		return err
	}

	// Post-reserve check to close the race window.
	slice := &slicev1beta1.Slice{}
	err := r.Get(ctx, types.NamespacedName{Name: sInfo.Name}, slice)
	if err != nil {
		if errors.IsNotFound(err) {
			log.FromContext(ctx).Info("Slice deleted concurrently during reservation, rolling back cache", "sliceName", sInfo.Name)
			r.watcher.Unreserve(ctx, []*interfaces.SliceInfo{sInfo})
		} else {
			log.FromContext(ctx).Error(err, "Transient API error during safeReserve verification, keeping reservation", "sliceName", sInfo.Name)
		}
	}
	return nil
}

// rollbackFull rolls back all reservations for both candidates and victims.
func (r *SliceReconciler) rollbackFull(ctx context.Context, slicesToReserve []*interfaces.SliceInfo, victims []*interfaces.SliceInfo, reason string) {
	logf := log.FromContext(ctx)
	if len(slicesToReserve) > 0 {
		r.watcher.Unreserve(ctx, slicesToReserve)
	}
	if len(victims) > 0 {
		for _, vInfo := range victims {
			if err := r.safeReserve(ctx, vInfo); err != nil {
				logf.Error(err, "Failed to restore victim on "+reason, "sliceName", vInfo.Name)
			}
		}
	}
}

// rollbackPartial rolls back only the failed parts of an API update.
func (r *SliceReconciler) rollbackPartial(ctx context.Context, apiErr *APIUpdateError) {
	logf := log.FromContext(ctx)
	r.watcher.Unreserve(ctx, apiErr.FailedCandidates)
	if len(apiErr.FailedVictims) > 0 {
		for _, vInfo := range apiErr.FailedVictims {
			if err := r.safeReserve(ctx, vInfo); err != nil {
				logf.Error(err, "Failed to restore failed victim on update failure", "sliceName", vInfo.Name)
			}
		}
	}
}

func (r *SliceReconciler) cleanUpSliceTasks(slicesToReserve []*interfaces.SliceInfo, victims []*interfaces.SliceInfo) {
	r.sliceTasksMutex.Lock()
	defer r.sliceTasksMutex.Unlock()
	if r.sliceTasks == nil {
		return
	}
	for _, s := range slicesToReserve {
		if state, exists := r.sliceTasks[s.Name]; exists && state == SliceStateBinding {
			delete(r.sliceTasks, s.Name)
		}
	}
	for _, v := range victims {
		if state, exists := r.sliceTasks[v.Name]; exists && state == SliceStateEvicting {
			delete(r.sliceTasks, v.Name)
		}
	}
}

func deepCopySliceInfo(slices []*interfaces.SliceInfo) []*interfaces.SliceInfo {
	if slices == nil {
		return nil
	}
	out := make([]*interfaces.SliceInfo, len(slices))
	for i, s := range slices {
		out[i] = s.DeepCopy()
	}
	return out
}

func (r *SliceReconciler) hasExistingAllocations(groupSlices map[string]*slicev1beta1.Slice) bool {
	for _, slice := range groupSlices {
		if len(slice.Spec.PartitionIds) > 0 {
			return true
		}
	}
	return false
}

func (r *SliceReconciler) shouldBackoffHealthy(logf logr.Logger, isRescheduling bool, reschedulingPartitions []string) bool {
	if !isRescheduling {
		return false
	}

	state := r.watcher.GetState()
	allHealthy := true

	infos := state.GetPartitionInfo(reschedulingPartitions)
	foundCount := len(infos)
	if foundCount != len(reschedulingPartitions) {
		allHealthy = false
	} else {
		for _, info := range infos {
			if info.IsBroken {
				allHealthy = false
				break
			}
		}
	}

	if allHealthy && foundCount == len(reschedulingPartitions) {
		logf.Info("Rescheduling requested but all current partitions are healthy, backing off", "delay", r.rescheduleHealthyDelay)
		return true
	}
	return false
}

// checkFailureBackoff checks if the group should continue backing off.
// Returns true if backoff should continue, with the appropriate delay.
func (r *SliceReconciler) checkFailureBackoff(sliceName string) (bool, time.Duration) {
	r.failureMutex.Lock()
	defer r.failureMutex.Unlock()

	firstFailure, exists := r.failureTimestamps[sliceName]
	if !exists {
		r.failureTimestamps[sliceName] = time.Now()
		return true, r.rescheduleHealthyDelay
	}
	if r.rescheduleTimeout > 0 && time.Since(firstFailure) < r.rescheduleTimeout {
		return true, r.rescheduleHealthyDelay
	}
	// Timeout reached — clear and allow reschedule
	delete(r.failureTimestamps, sliceName)
	return false, 0
}

// clearFailureTimestamp removes the failure timestamp for a slice,
// preventing stale entries from accumulating after successful scheduling.
func (r *SliceReconciler) clearFailureTimestamp(sliceName string) {
	r.failureMutex.Lock()
	defer r.failureMutex.Unlock()
	delete(r.failureTimestamps, sliceName)
}

// bindPartitionsToGroup handles the API updates for both victims and the scheduled group.
// It clears victims and binds new slices/assignments in a unified API call sequence.
// If clearing victims fails, it returns an APIUpdateError indicating the failed victims and all candidates.
// If binding new slices fails, it returns an APIUpdateError indicating the remaining unbound candidates.
func (r *SliceReconciler) bindPartitionsToGroup(ctx context.Context, groupId string, groupSlices map[string]*slicev1beta1.Slice, slicesToReserve []*interfaces.SliceInfo, victims []*interfaces.SliceInfo) error {
	logf := log.FromContext(ctx, "groupId", groupId)

	// 1. Clear victims in API (with retry on conflict)
	for i, vInfo := range victims {
		if err := r.clearVictimWithRetry(ctx, logf, vInfo.Name, groupId); err != nil {
			return &APIUpdateError{
				FailedVictims:    victims[i:],
				FailedCandidates: slicesToReserve,
				Err:              err,
			}
		}
	}

	// 2. Update group slices with assigned partitions
	for i, sInfo := range slicesToReserve {
		if err := r.assignPartitionsWithRetry(ctx, logf, sInfo.Name, sInfo.PartitionIds); err != nil {
			return &APIUpdateError{
				FailedVictims:    nil,
				FailedCandidates: slicesToReserve[i:],
				Err:              err,
			}
		}
	}

	return nil
}

// clearVictimWithRetry annotates a victim slice for eviction by the pod
// reconciler, retrying with a fresh fetch on optimistic concurrency conflicts.
// Instead of clearing PartitionIds directly, the scheduler annotates the
// victim so the pod reconciler can gracefully evict pods before clearing.
func (r *SliceReconciler) clearVictimWithRetry(ctx context.Context, logf logr.Logger, victimName string, preemptorGroup string) error {
	for attempt := 0; attempt < maxBindRetries; attempt++ {
		victim := &slicev1beta1.Slice{}
		if err := r.Get(ctx, types.NamespacedName{Name: victimName}, victim); err != nil {
			if errors.IsNotFound(err) {
				return nil // Victim already deleted, nothing to clear
			}
			return fmt.Errorf("failed to fetch victim slice %s: %w", victimName, err)
		}
		if len(victim.Spec.PartitionIds) == 0 {
			return nil // Already cleared (e.g., by a prior attempt or concurrent operation)
		}
		if victim.Annotations == nil {
			victim.Annotations = make(map[string]string)
		}
		victim.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
		if err := r.Update(ctx, victim); err != nil {
			if errors.IsConflict(err) {
				logf.Info("Conflict annotating victim for eviction, retrying with fresh fetch",
					"victim", victimName, "attempt", attempt+1)
				time.Sleep(10 * time.Millisecond * time.Duration(attempt+1))
				continue
			}
			return fmt.Errorf("failed to annotate victim %s for eviction: %w", victimName, err)
		}
		r.recordPreemptionEvent(victim, preemptorGroup)
		return nil
	}
	return fmt.Errorf("failed to annotate victim %s for eviction after %d retries due to conflicts", victimName, maxBindRetries)
}

// assignPartitionsWithRetry assigns partitions to a slice,
// retrying with a fresh fetch on optimistic concurrency conflicts.
func (r *SliceReconciler) assignPartitionsWithRetry(ctx context.Context, logf logr.Logger, sliceName string, partitionIds []string) error {
	for attempt := 0; attempt < maxBindRetries; attempt++ {
		// Always re-fetch the slice to get the latest resourceVersion.
		// This is critical to avoid stale-write conflicts when multiple
		// preemption operations modify slices in the same scheduling cycle.
		slice := &slicev1beta1.Slice{}
		if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, slice); err != nil {
			return fmt.Errorf("failed to fetch slice %s for partition assignment: %w", sliceName, err)
		}
		// Idempotency: if the slice already has the desired partitions
		// (e.g., from a previous successful bind for the same group),
		// skip the update. The webhook rejects N→M transitions, so
		// attempting to overwrite existing partitions would fail.
		if partitionsMatch(slice.Spec.PartitionIds, partitionIds) {
			logf.Info("Slice already has desired partitions, skipping update",
				"slice", sliceName, "partitions", partitionIds)
			return nil
		}
		if len(slice.Spec.PartitionIds) > 0 {
			// Slice has different partitions — a previous scheduling cycle
			// already assigned it. The webhook only allows 0→N or N→0,
			// so we cannot overwrite. Return error to trigger Unreserve.
			logf.Info("Slice already has different partitions, cannot overwrite",
				"slice", sliceName, "existing", slice.Spec.PartitionIds, "desired", partitionIds)
			return fmt.Errorf("slice %s already has partitions %v, cannot assign %v (N→M not allowed)", sliceName, slice.Spec.PartitionIds, partitionIds)
		}
		slice.Spec.PartitionIds = partitionIds
		if err := r.Update(ctx, slice); err != nil {
			if errors.IsConflict(err) {
				logf.Info("Conflict assigning partitions, retrying with fresh fetch",
					"slice", sliceName, "attempt", attempt+1)
				time.Sleep(10 * time.Millisecond * time.Duration(attempt+1))
				continue
			}
			return fmt.Errorf("failed to update slice %s with partitions: %w", sliceName, err)
		}
		r.recordSuccessfulScheduleEvent(slice, partitionIds)
		return nil
	}
	return fmt.Errorf("failed to assign partitions to slice %s after %d retries due to conflicts", sliceName, maxBindRetries)
}

// evictSliceWithRetry clears partition assignments from a slice during eviction
// (when the group is unschedulable and needs rescheduling), retrying with a
// fresh fetch on optimistic concurrency conflicts.
func (r *SliceReconciler) evictSliceWithRetry(ctx context.Context, logf logr.Logger, sliceName string) error {
	for attempt := 0; attempt < maxBindRetries; attempt++ {
		fresh := &slicev1beta1.Slice{}
		if err := r.Get(ctx, types.NamespacedName{Name: sliceName}, fresh); err != nil {
			if errors.IsNotFound(err) {
				return nil // Already deleted
			}
			return fmt.Errorf("failed to fetch slice %s for eviction: %w", sliceName, err)
		}
		if len(fresh.Spec.PartitionIds) == 0 {
			return nil // Already cleared
		}
		fresh.Spec.PartitionIds = []string{}
		if err := r.Update(ctx, fresh); err != nil {
			if errors.IsConflict(err) {
				logf.Info("Conflict evicting slice, retrying with fresh fetch",
					"slice", sliceName, "attempt", attempt+1)
				time.Sleep(10 * time.Millisecond * time.Duration(attempt+1))
				continue
			}
			return fmt.Errorf("failed to evict partitions from slice %s: %w", sliceName, err)
		}
		r.recordEvictionEvent(fresh)
		return nil
	}
	return fmt.Errorf("failed to evict slice %s after %d retries due to conflicts", sliceName, maxBindRetries)
}

// partitionsMatch returns true if two partition ID slices contain the same elements,
// regardless of order.
func partitionsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, id := range a {
		counts[id]++
	}
	for _, id := range b {
		counts[id]--
		if counts[id] < 0 {
			return false
		}
	}
	return true
}

// SetupWithManager sets up the controller with the Manager.
func (r *SliceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&slicev1beta1.Slice{}, builder.WithPredicates(predicate.Funcs{
			// We only reconcile slices that need scheduling.
			// Other slice changes are handled by the watch handlers below.
			CreateFunc: func(e event.CreateEvent) bool {
				slice, ok := e.Object.(*slicev1beta1.Slice)
				if !ok {
					return false
				}
				return slice.Annotations[ManagedByAnnotation] == interfaces.SliceSchedulerName && r.needsScheduling(slice) && string(slice.Spec.Type) == r.tpuType
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				newSlice, ok := e.ObjectNew.(*slicev1beta1.Slice)
				if !ok {
					return false
				}

				return string(newSlice.Spec.Type) == r.tpuType && r.needsScheduling(newSlice) && newSlice.Annotations[ManagedByAnnotation] == interfaces.SliceSchedulerName
			},
			DeleteFunc: func(e event.DeleteEvent) bool { return false },
		})).
		Named("slice-scheduler").
		// Watch for changes to Slices and PartitionTrees to keep the cache authoritative.
		Watches(&slicev1beta1.Slice{}, handler.Funcs{
			CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handleSliceCreate(ctx, e)
			},
			UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handleSliceUpdate(ctx, e)
			},
			DeleteFunc: func(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handleSliceDelete(ctx, e)
			},
		}).
		Watches(&partitiontree.PartitionTree{}, handler.Funcs{
			CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handlePartitionTreeCreate(ctx, e)
			},
			UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handlePartitionTreeUpdate(ctx, e)
			},
			DeleteFunc: func(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handlePartitionTreeDelete(ctx, e)
			},
		}).
		Watches(&corev1.Node{}, handler.Funcs{
			CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				r.handleNodeCreate(ctx, e)
			},
			UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
				// This now only runs if the predicate below returns true
				r.handleNodeUpdate(ctx, e)
			},
		}, builder.WithPredicates(predicate.Funcs{ // Add predicates for this watch
			CreateFunc: func(e event.CreateEvent) bool {
				if nodeType := nodeType(e.Object.(*corev1.Node)); nodeType != r.tpuType {
					return false
				}
				// Always process node creations
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				// Only process updates if node readiness or cordon changed
				oldNode, okOld := e.ObjectOld.(*corev1.Node)
				newNode, okNew := e.ObjectNew.(*corev1.Node)
				if !okOld || !okNew || nodeType(oldNode) != r.tpuType || nodeType(newNode) != r.tpuType {
					return false // Not a node or not the right TPU node, don't process
				}
				return nodeSchedulingChanged(oldNode, newNode)
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				// The original handler did nothing, so don't process deletes
				return false
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false // Ignore generic events
			},
		})).
		Complete(r)
}

// nodeSchedulingChanged reports whether an update changes what the cache keeps
// about a node: its readiness or whether it is cordoned.
func nodeSchedulingChanged(oldNode, newNode *corev1.Node) bool {
	return getNodeReadyStatus(oldNode) != getNodeReadyStatus(newNode) ||
		oldNode.Spec.Unschedulable != newNode.Spec.Unschedulable
}

// getNodeReadyStatus is a helper function to extract the NodeReady condition.
func getNodeReadyStatus(node *corev1.Node) corev1.ConditionStatus {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status
		}
	}
	// Default to Unknown if the condition isn't found
	return corev1.ConditionUnknown
}

func (r *SliceReconciler) handleNodeCreate(ctx context.Context, e event.TypedCreateEvent[client.Object]) {
	node, ok := e.Object.(*corev1.Node)
	if !ok || r.cache == nil {
		return
	}

	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.log.Info("New node created", "node name", node.Name, "status", node.Status.Conditions, "labels", node.Labels)
	r.cache.UpdateLeafNodeCache(ctx, node)
}

func (r *SliceReconciler) handleNodeUpdate(ctx context.Context, e event.TypedUpdateEvent[client.Object]) {
	newNode, ok := e.ObjectNew.(*corev1.Node)
	if !ok || r.cache == nil {
		return
	}

	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.log.Info("Node updated", "node name", newNode.Name, "unschedulable", newNode.Spec.Unschedulable, "status", newNode.Status.Conditions, "labels", newNode.Labels)
	r.cache.UpdateLeafNodeCache(ctx, newNode)
}

// handleSliceCreate is called when a Slice is created. It marks any pre-assigned partitions as in-use.
// Acquires schedulingMutex to prevent cache modifications during an in-flight schedule→reserve→bind cycle.
func (r *SliceReconciler) handleSliceCreate(ctx context.Context, e event.TypedCreateEvent[client.Object]) {
	slice, ok := e.Object.(*slicev1beta1.Slice)
	// We only care about slices that are created with partitions already assigned.
	// Slices that need scheduling are handled by the main reconcile loop.
	if !ok || len(slice.Spec.PartitionIds) == 0 || string(slice.Spec.Type) != r.tpuType || r.cache == nil {
		return
	}

	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.log.Info("Observed slice creation with pre-assigned partitions", "slice", slice.Name)
	sInfo := sliceInfo(ctx, slice)
	if ok, existingSlice := r.cache.ReservePartition(ctx, sInfo); !ok {
		r.log.Error(nil, "Conflict detected: Partition assigned to new slice is already in use", "partitionIds", slice.Spec.PartitionIds, "existingSlice", existingSlice, "newSlice", slice.Name)
	}
}

// handleSliceUpdate is called when a Slice is updated. It updates the cache based on partition changes.
// Acquires schedulingMutex to prevent cache modifications during an in-flight schedule→reserve→bind cycle.
func (r *SliceReconciler) handleSliceUpdate(ctx context.Context, e event.TypedUpdateEvent[client.Object]) {
	oldSlice, okOld := e.ObjectOld.(*slicev1beta1.Slice)
	newSlice, okNew := e.ObjectNew.(*slicev1beta1.Slice)
	if !okOld || !okNew || string(oldSlice.Spec.Type) != r.tpuType || string(newSlice.Spec.Type) != r.tpuType || r.cache == nil {
		return
	}

	oldPartitions := make(map[string]bool)
	for _, pID := range oldSlice.Spec.PartitionIds {
		oldPartitions[pID] = true
	}

	newPartitions := make(map[string]bool)
	for _, pID := range newSlice.Spec.PartitionIds {
		newPartitions[pID] = true
	}

	// Partitions to free: in old but not in new
	partitionsToFree := []string{}
	for pID := range oldPartitions {
		if !newPartitions[pID] {
			partitionsToFree = append(partitionsToFree, pID)
		}
	}

	// Partitions to reserve: in new but not in old
	partitionsToReserve := []string{}
	for pID := range newPartitions {
		if !oldPartitions[pID] {
			partitionsToReserve = append(partitionsToReserve, pID)
		}
	}

	if len(partitionsToFree) == 0 && len(partitionsToReserve) == 0 {
		return // No change in partitions
	}

	// Acquire the scheduling mutex to prevent cache modifications during an
	// in-flight schedule→reserve→bind cycle. Without this, a watch event from
	// victim eviction can corrupt cache state between Reserve() and bind().
	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.log.Info("Observed slice update with partition changes", "slice", newSlice.Name)
	// TODO(forrestli): Use computed partitionsToFree/partitionsToReserve diffs instead
	// of full release/reserve. Currently, unchanged partitions are momentarily unreserved
	// between ReleasePartitions and ReservePartition. The schedulingMutex prevents the
	// scheduler from seeing this, but the PodReconciler reads via cache.mu (a different
	// mutex) and could observe a brief inconsistency. Self-healing on next reconcile.
	// Free up old partitions
	r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, oldSlice)})

	// Reserve new partitions
	if ok, existingSlice := r.cache.ReservePartition(ctx, sliceInfo(ctx, newSlice)); !ok {
		r.log.Error(nil, "Conflict detected: Partition assigned to updated slice is already in use", "partitionIds", partitionsToReserve, "existingSlice", existingSlice, "updatedSlice", newSlice.Name)
	}
}

// handleSliceDelete is called when a Slice is deleted. It frees up the partition in the cache.
// Acquires schedulingMutex to prevent cache modifications during an in-flight schedule→reserve→bind cycle.
func (r *SliceReconciler) handleSliceDelete(ctx context.Context, e event.TypedDeleteEvent[client.Object]) {
	slice, ok := e.Object.(*slicev1beta1.Slice)
	if !ok || string(slice.Spec.Type) != r.tpuType {
		return
	}

	if len(slice.Spec.PartitionIds) > 0 && r.cache != nil {
		func() {
			r.schedulingMutex.Lock()
			defer r.schedulingMutex.Unlock()
			r.log.Info("Freeing partition due to slice deletion", "slice", slice.Name, "partitionId", slice.Spec.PartitionIds)
			r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
		}()
	}

	// Clean up failure timestamp to prevent memory leak.
	r.clearFailureTimestamp(slice.Name)

	r.groupMutex.Lock()
	defer r.groupMutex.Unlock()
	if groupId, ok := r.sliceToGroup[slice.Name]; ok {
		if group, ok := r.groups[groupId]; ok {
			delete(group.Slices, slice.Name)
			if len(group.Slices) == 0 {
				delete(r.groups, groupId)
			}
		}
		delete(r.sliceToGroup, slice.Name)
	}
}

// handlePartitionTreeCreate is called when a PartitionTree is created.
// Acquires schedulingMutex to prevent cache modifications during an in-flight schedule→reserve→bind cycle.
func (r *SliceReconciler) handlePartitionTreeCreate(ctx context.Context, e event.TypedCreateEvent[client.Object]) {
	tree, ok := e.Object.(*partitiontree.PartitionTree)
	if !ok || r.cache == nil || partitionTreeType(tree) != r.tpuType {
		return
	}

	podName := tree.Labels[TpuPodNameLabel]
	// Get nodes with matching GCE topology block label to build the tree
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList, client.MatchingLabels{GceTopologyBlockLabel: podName}); err != nil {
		// Currently we are not making the node readiness as partition-picking prerequisite if the k8s node object is not retrievable.
		r.log.Error(err, "unable to list nodes for cache creation")
	}

	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.log.Info("Adding new partition tree to cache", "partitionTree", tree.Name)
	r.cache.UpdateTree(ctx, tree, nodeList.Items)
}

// handlePartitionTreeUpdate is called when a PartitionTree is updated.
// Acquires schedulingMutex to prevent cache modifications during an in-flight schedule→reserve→bind cycle.
func (r *SliceReconciler) handlePartitionTreeUpdate(ctx context.Context, e event.TypedUpdateEvent[client.Object]) {
	newTree, ok := e.ObjectNew.(*partitiontree.PartitionTree)
	if !ok || r.cache == nil || partitionTreeType(newTree) != r.tpuType {
		return
	}

	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.log.Info("Updating partition tree in cache", "partitionTree", newTree.Name)
	r.cache.UpdateTree(ctx, newTree, nil)
}

// handlePartitionTreeDelete is called when a PartitionTree is deleted.
// Acquires schedulingMutex to prevent cache modifications during an in-flight schedule→reserve→bind cycle.
func (r *SliceReconciler) handlePartitionTreeDelete(ctx context.Context, e event.TypedDeleteEvent[client.Object]) {
	tree, ok := e.Object.(*partitiontree.PartitionTree)
	if !ok || r.cache == nil || partitionTreeType(tree) != r.tpuType {
		return
	}

	r.schedulingMutex.Lock()
	defer r.schedulingMutex.Unlock()

	r.cache.DeletePartitionTree(ctx, tree.Name)
}

// A slice needs scheduling if it's new (no partitions) or if the policy says
// it needs rescheduling (unhealthy/failed status).
func (r *SliceReconciler) needsScheduling(slice *slicev1beta1.Slice) bool {
	isNew := len(slice.Spec.PartitionIds) == 0
	return slice.DeletionTimestamp.IsZero() && (isNew || r.policy.NeedsRescheduling(slice))
}

func (r *SliceReconciler) finalizeSchedulingSuccess(logf logr.Logger, task *BindingTask) {
	if task.candidate == nil {
		duration := time.Since(task.startTime).Seconds()
		logf.Info("Group preemption task finished successfully", "groupId", task.groupId, "totalDuration", duration)
		return
	}
	// Extract podName for Prometheus metrics
	podName := "unknown"
	for _, parts := range task.candidate.Assignments {
		if len(parts) > 0 {
			podName = parts[0].PodName
			break
		}
	}

	if len(task.victims) > 0 {
		metrics.SlicePreemptionsTotal.WithLabelValues(r.tpuType, task.firstTopology, podName).Inc()
		for _, victim := range task.victims {
			metrics.PreemptedSlicesTotal.WithLabelValues(r.tpuType, victim.Topology, podName).Inc()
		}
	}

	metrics.SliceScheduleSuccessTotal.WithLabelValues(r.tpuType, task.firstTopology, podName).Inc()

	// Clear failure timestamps for all slices in the group on successful scheduling.
	for sliceName := range task.groupSlices {
		r.clearFailureTimestamp(sliceName)
	}

	duration := time.Since(task.startTime).Seconds()
	metrics.SliceScheduleDurationSeconds.WithLabelValues(r.tpuType, task.firstTopology, interfaces.TrueStr).Observe(duration)
	logf.Info("Group reconciliation finished successfully", "groupId", task.groupId, "totalDuration", duration)
}

func (r *SliceReconciler) processBindingQueue(ctx context.Context, workerID int) {
	logf := r.log.WithName("binding-worker").WithValues("workerID", workerID)
	logf.Info("Binding worker started.")

	for {
		select {
		case <-ctx.Done():
			logf.Info("Context cancelled, shutting down binding worker")
			return
		case task := <-r.bindingQueue:
			r.processBindingTask(ctx, logf, task)
		}
	}
}

func (r *SliceReconciler) processBindingTask(ctx context.Context, logf logr.Logger, task *BindingTask) {
	logf.Info("Processing binding task", "groupId", task.groupId)

	// Bind the allocated partitions in the Kubernetes API and handle any partial failures/rollbacks.
	requeue, delay := r.updateAPIAndHandleErrors(ctx, task.groupId, task.groupSlices, task.slicesToReserve, task.victims)

	r.cleanUpSliceTasks(task.slicesToReserve, task.victims)

	if requeue {
		logf.Info("Async API binding failed, requeuing group", "groupId", task.groupId, "delay", delay)
		// Requeue the failed group with delay
		priority := 0
		r.groupMutex.RLock()
		if group, ok := r.groups[task.groupId]; ok {
			priority = int(group.Priority)
		}
		r.groupMutex.RUnlock()
		item := &pq.Item{
			GroupId:  task.groupId,
			Priority: priority,
			Topology: task.firstTopology,
		}
		if r.requeueWithDelayFn != nil {
			go r.requeueWithDelayFn(ctx, item, delay)
		}

		// Record failure duration metric
		duration := time.Since(task.startTime).Seconds()
		metrics.SliceScheduleDurationSeconds.WithLabelValues(r.tpuType, task.firstTopology, interfaces.FalseStr).Observe(duration)
		return
	}

	r.finalizeSchedulingSuccess(logf, task)
}
