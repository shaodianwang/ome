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
	"maps"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	partitiontreev1 "tpu.google.com/slice-scheduler/apis/partition/v1beta1"
	slicev1beta1 "tpu.google.com/slice-scheduler/apis/slice/v1beta1"
	"tpu.google.com/slice-scheduler/internal/generators"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/plugins"
	"tpu.google.com/slice-scheduler/internal/plugins/taint"
	"tpu.google.com/slice-scheduler/internal/pod_reconciler"
	"tpu.google.com/slice-scheduler/internal/policy"
	"tpu.google.com/slice-scheduler/internal/pq"
	"tpu.google.com/slice-scheduler/internal/schedulerlib"
	"tpu.google.com/slice-scheduler/internal/testutil"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

// TestShouldBackoffHealthy_BlockedTaint checks that a blocked NoExecute taint, which leaves
// IsBroken false, counts as unhealthy for backoff, and that a NoSchedule taint does not.
func TestShouldBackoffHealthy_BlockedTaint(t *testing.T) {
	taint := func(key string, effect partitiontreev1.TaintEffect) []*partitiontreev1.PartitionTaint {
		return []*partitiontreev1.PartitionTaint{{Key: key, Effect: effect}}
	}

	tests := []struct {
		name           string
		isRescheduling bool
		partition      *interfaces.PartitionInfo
		wantBackoff    bool
	}{
		{
			name:           "not rescheduling never backs off",
			isRescheduling: false,
			partition:      &interfaces.PartitionInfo{ID: "p1"},
			wantBackoff:    false,
		},
		{
			name:           "healthy and untainted backs off",
			isRescheduling: true,
			partition:      &interfaces.PartitionInfo{ID: "p1"},
			wantBackoff:    true,
		},
		{
			name:           "broken does not back off",
			isRescheduling: true,
			partition:      &interfaces.PartitionInfo{ID: "p1", IsBroken: true},
			wantBackoff:    false,
		},
		{
			name:           "blocked NoExecute taint does not back off even though IsBroken is false",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint(interfaces.TaintKeyNodesNotFound, partitiontreev1.TaintEffectNoExecute),
			},
			wantBackoff: false,
		},
		{
			name:           "blocked NoSchedule taint still backs off",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint(corev1.TaintNodeUnschedulable, partitiontreev1.TaintEffectNoSchedule),
			},
			wantBackoff: true,
		},
		{
			name:           "PreferNoSchedule is not a blocked effect, so it still backs off",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint(interfaces.TaintKeyPartitionState, partitiontreev1.TaintEffectPreferNoSchedule),
			},
			wantBackoff: true,
		},
		{
			name:           "unrecognised taint key is not blocked, so it still backs off",
			isRescheduling: true,
			partition: &interfaces.PartitionInfo{
				ID:     "p1",
				Taints: taint("custom.taint.io/x", partitiontreev1.TaintEffectNoSchedule),
			},
			wantBackoff: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &SliceReconciler{watcher: &stateWatcher{state: &testutil.MockClusterState{
				PartitionTrees: []*interfaces.PartitionInfo{tt.partition},
			}}}

			got := r.shouldBackoffHealthy(logr.Discard(), tt.isRescheduling, []string{"p1"})
			if got != tt.wantBackoff {
				t.Errorf("shouldBackoffHealthy() = %v, want %v", got, tt.wantBackoff)
			}
		})
	}
}

// TestShouldBackoffHealthy_TaintedLeafUnderHeldCube uses the real cache. A slice holds the
// cube, but the controller records the node taint on a leaf beneath it; backoff only sees
// it because the cache unions descendants' taints onto the cube.
func TestShouldBackoffHealthy_TaintedLeafUnderHeldCube(t *testing.T) {
	cache := watcher.NewPartitionCache(false, watcher.WithTaintFiltering(true))
	cache.UpdateTree(context.Background(), &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a"},
		Status: partitiontreev1.PartitionTreeStatus{
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "cube",
				Topology:    "4x4x4",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{
						PartitionID: "half-0",
						Topology:    "2x4x4",
						State:       partitiontreev1.PartitionStateHealthy,
						Taints:      []partitiontreev1.PartitionTaint{{Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute}},
					},
					{PartitionID: "half-1", Topology: "2x4x4", State: partitiontreev1.PartitionStateHealthy},
				},
			},
		},
	}, nil)

	r := &SliceReconciler{watcher: watcher.NewPartitionWatcher(cache)}
	if r.shouldBackoffHealthy(logr.Discard(), true, []string{"cube"}) {
		t.Error("shouldBackoffHealthy() = true for a cube with an unreachable leaf, want false")
	}
}

var nodeUnreachable = partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnreachable, Effect: partitiontreev1.TaintEffectNoExecute}

// noExecuteTree is a v6e tree with two cubes, each split into two halves. leafTaints go
// on cube-a's first half, which is where the controller records a node's taints.
func noExecuteTree(leafTaints ...partitiontreev1.PartitionTaint) *partitiontreev1.PartitionTree {
	half := func(id string, taints []partitiontreev1.PartitionTaint) partitiontreev1.PartitionInfo {
		return partitiontreev1.PartitionInfo{PartitionID: id, Topology: "2x4", State: partitiontreev1.PartitionStateHealthy, Taints: taints}
	}
	return &partitiontreev1.PartitionTree{
		ObjectMeta: metav1.ObjectMeta{Name: "pt-taint", Labels: map[string]string{TpuPodIDLabel: "pt-taint"}},
		Status: partitiontreev1.PartitionTreeStatus{
			AcceleratorType: TPU6e,
			Root: &partitiontreev1.PartitionInfo{
				PartitionID: "root",
				Topology:    "4x8",
				State:       partitiontreev1.PartitionStateHealthy,
				ChildPartitions: []partitiontreev1.PartitionInfo{
					{PartitionID: "cube-a", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy, ChildPartitions: []partitiontreev1.PartitionInfo{
						half("cube-a-0", leafTaints), half("cube-a-1", nil),
					}},
					{PartitionID: "cube-b", Topology: "4x4", State: partitiontreev1.PartitionStateHealthy, ChildPartitions: []partitiontreev1.PartitionInfo{
						half("cube-b-0", nil), half("cube-b-1", nil),
					}},
				},
			},
		},
	}
}

func taintTestSlice(name, sliceType, managedBy string, partitionIDs ...string) *slicev1beta1.Slice {
	return &slicev1beta1.Slice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				ManagedByAnnotation:      managedBy,
				SliceGroupNameAnnotation: name + "-group",
			},
		},
		Spec: slicev1beta1.SliceSpec{Type: slicev1beta1.Type(sliceType), Topology: "4x4", PartitionIds: partitionIDs},
	}
}

func newTaintReconciler(t *testing.T, filtering bool, tree *partitiontreev1.PartitionTree, slices ...*slicev1beta1.Slice) *SliceReconciler {
	t.Helper()
	ctx := context.Background()
	objs := []client.Object{tree.DeepCopy()}
	for _, s := range slices {
		objs = append(objs, s.DeepCopy())
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
	cache := watcher.NewPartitionCache(false, watcher.WithTaintFiltering(filtering))
	r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, record.NewFakeRecorder(10),
		false, nil, cache, 0, 0, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	cache.UpdateTree(ctx, tree, nil)
	for _, s := range slices {
		cache.ReservePartition(ctx, sliceInfo(ctx, s))
	}
	return r
}

func newTestQueue() workqueue.TypedRateLimitingInterface[reconcile.Request] {
	return workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
}

func drainNames(q workqueue.TypedRateLimitingInterface[reconcile.Request]) []string {
	var names []string
	for q.Len() > 0 {
		req, _ := q.Get()
		names = append(names, req.Name)
		q.Done(req)
	}
	sort.Strings(names)
	return names
}

func TestHandlePartitionTreeUpdate_NoExecuteSweep(t *testing.T) {
	owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	elsewhere := taintTestSlice("elsewhere", TPU6e, interfaces.SliceSchedulerName, "cube-b")
	foreign := taintTestSlice("foreign", TPU6e, "someone-else", "cube-a-1")
	cordon := partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnschedulable, Effect: partitiontreev1.TaintEffectNoSchedule}

	tests := []struct {
		name      string
		filtering bool
		old, new  *partitiontreev1.PartitionTree
		want      []string
	}{
		{
			name:      "leaf gaining NoExecute requeues our slice on the cube above it",
			filtering: true,
			old:       noExecuteTree(),
			new:       noExecuteTree(nodeUnreachable),
			want:      []string{"owned"},
		},
		{
			name:      "taint already present is not swept again",
			filtering: true,
			old:       noExecuteTree(nodeUnreachable),
			new:       noExecuteTree(nodeUnreachable),
		},
		{
			name:      "NoSchedule does not evict",
			filtering: true,
			old:       noExecuteTree(),
			new:       noExecuteTree(cordon),
		},
		{
			name:      "filtering disabled",
			filtering: false,
			old:       noExecuteTree(),
			new:       noExecuteTree(nodeUnreachable),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTaintReconciler(t, tt.filtering, tt.old, owned, elsewhere, foreign)
			q := newTestQueue()
			defer q.ShutDown()

			r.handlePartitionTreeUpdate(context.Background(), event.TypedUpdateEvent[client.Object]{ObjectOld: tt.old, ObjectNew: tt.new}, q)

			if got := drainNames(q); !slices.Equal(got, tt.want) {
				t.Errorf("requeued %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandlePartitionTreeCreate_NoExecuteSweep(t *testing.T) {
	owned := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a-0")
	otherType := taintTestSlice("other-type", TPU7x, interfaces.SliceSchedulerName, "cube-a-1")
	// Seed the cache with an untainted copy so the slices are reserved, then deliver the
	// tainted tree as a create, as happens when a tree is recreated.
	r := newTaintReconciler(t, true, noExecuteTree(), owned, otherType)
	q := newTestQueue()
	defer q.ShutDown()

	r.handlePartitionTreeCreate(context.Background(), event.TypedCreateEvent[client.Object]{Object: noExecuteTree(nodeUnreachable)}, q)

	if got, want := drainNames(q), []string{"owned"}; !slices.Equal(got, want) {
		t.Errorf("requeued %v, want %v", got, want)
	}
}

func TestIsSliceOnNoExecutePartition(t *testing.T) {
	tests := []struct {
		name      string
		filtering bool
		taints    []partitiontreev1.PartitionTaint
		partition string
		marked    bool
		want      bool
	}{
		{name: "slice on the tainted leaf", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a-0", want: true},
		{name: "slice on the cube above it", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a", want: true},
		{name: "slice on an untainted sibling", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a-1", want: false},
		{name: "slice already marked for eviction", filtering: true, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a", marked: true, want: false},
		{name: "filtering disabled", filtering: false, taints: []partitiontreev1.PartitionTaint{nodeUnreachable}, partition: "cube-a", want: false},
		{
			name:      "nodes-not-found from the controller",
			filtering: true,
			taints:    []partitiontreev1.PartitionTaint{{Key: interfaces.TaintKeyNodesNotFound, Effect: partitiontreev1.TaintEffectNoExecute}},
			partition: "cube-a",
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice := taintTestSlice("s", TPU6e, interfaces.SliceSchedulerName, tt.partition)
			if tt.marked {
				slice.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
			}
			r := newTaintReconciler(t, tt.filtering, noExecuteTree(tt.taints...))
			if got := r.isSliceOnNoExecutePartition(slice); got != tt.want {
				t.Errorf("isSliceOnNoExecutePartition() = %v, want %v", got, tt.want)
			}
			if got := r.needsScheduling(slice); got != tt.want {
				t.Errorf("needsScheduling() = %v, want %v", got, tt.want)
			}
		})
	}
}

func isMarkedForEviction(t *testing.T, r *SliceReconciler, name string) bool {
	t.Helper()
	slice := &slicev1beta1.Slice{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name}, slice); err != nil {
		t.Fatalf("Get(%s) error: %v", name, err)
	}
	return slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr
}

func TestMarkSliceForEviction(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		slice      *slicev1beta1.Slice
		clearTaint bool
		sliceName  string
		wantMarked bool
	}{
		{
			name:       "still tainted is marked",
			slice:      taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
			sliceName:  "owned",
			wantMarked: true,
		},
		{
			name:       "taint cleared during the delay is left alone",
			slice:      taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
			clearTaint: true,
			sliceName:  "owned",
		},
		{
			name:      "slice managed by someone else is left alone",
			slice:     taintTestSlice("foreign", TPU6e, "someone-else", "cube-a"),
			sliceName: "foreign",
		},
		{
			name:      "slice being deactivated is left alone",
			slice:     deactivatingSlice("owned", "cube-a"),
			sliceName: "owned",
		},
		{
			name:      "deleted slice is not an error",
			sliceName: "gone",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []*slicev1beta1.Slice
			if tt.slice != nil {
				objs = append(objs, tt.slice)
			}
			r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), objs...)
			if tt.clearTaint {
				r.cache.UpdateTree(ctx, noExecuteTree(), nil)
			}
			marked, err := r.markSliceForEviction(ctx, logr.Discard(), tt.sliceName,
				r.isSliceOnNoExecutePartition, r.recordNoExecuteEvictionEvent)
			if err != nil {
				t.Fatalf("markSliceForEviction() error: %v", err)
			}
			if marked != tt.wantMarked {
				t.Errorf("markSliceForEviction() = %v, want %v", marked, tt.wantMarked)
			}
			if tt.slice != nil && isMarkedForEviction(t, r, tt.sliceName) != tt.wantMarked {
				t.Errorf("evict-pods annotation set = %v, want %v", !tt.wantMarked, tt.wantMarked)
			}
		})
	}
}

// taintAwareScheduler is the test scheduler with TaintFilter added, so placement avoids
// tainted partitions the way cmd/main.go configures it.
func taintAwareScheduler() interfaces.SchedulerLibrary {
	gens := []interfaces.PlacementGenerator{
		generators.NewGroupGenerator(generators.GroupGeneratorConfig{BeamWidth: 10}),
	}
	filters := []interfaces.FilterPlugin{
		&plugins.HealthyFilter{},
		&taint.TaintFilter{},
		&plugins.ManagedFilter{},
		&plugins.PriorityFilter{},
	}
	scorers := []interfaces.ScorePlugin{
		&plugins.PreemptionScorer{},
		&plugins.TightestFitScorer{},
	}
	return schedulerlib.NewDefaultScheduler(gens, filters, scorers)
}

func getSlice(t *testing.T, r *SliceReconciler, name string) *slicev1beta1.Slice {
	t.Helper()
	slice := &slicev1beta1.Slice{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name}, slice); err != nil {
		t.Fatalf("Get(%s) error: %v", name, err)
	}
	return slice
}

// delayRecorder captures the delay requeueWithDelayFn is given for each group; Reconcile
// and Start call it from a goroutine.
type delayRecorder struct {
	mu    sync.Mutex
	calls map[string]time.Duration
	done  chan struct{}
}

func newDelayRecorder() *delayRecorder {
	return &delayRecorder{calls: make(map[string]time.Duration), done: make(chan struct{}, 16)}
}

func (d *delayRecorder) record(_ context.Context, item *pq.Item, delay time.Duration) {
	d.mu.Lock()
	d.calls[item.GroupId] = delay
	d.mu.Unlock()
	d.done <- struct{}{}
}

func (d *delayRecorder) wait(t *testing.T, n int) map[string]time.Duration {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-d.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for requeue %d of %d", i+1, n)
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.calls)
}

// TestReconcile_NoExecuteUsesUnhealthyDelay checks that a slice on a NoExecute partition
// is queued after rescheduleUnhealthyDelay, like any other failed slice.
func TestReconcile_NoExecuteUsesUnhealthyDelay(t *testing.T) {
	slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
	r.rescheduleUnhealthyDelay = 42 * time.Second
	rec := newDelayRecorder()
	r.requeueWithDelayFn = rec.record

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}}); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if got := rec.wait(t, 1)["owned-group"]; got != r.rescheduleUnhealthyDelay {
		t.Errorf("requeue delay = %v, want %v", got, r.rescheduleUnhealthyDelay)
	}
}

// TestTryScheduleSliceGroup_NoExecuteEvictsThenReschedules walks a slice through the whole
// move: the scheduling loop marks it, the pod reconciler unbinds it (N→0), and the next
// pass places it on a clean partition (0→M). Placing it straight there would be rejected.
func TestTryScheduleSliceGroup_NoExecuteEvictsThenReschedules(t *testing.T) {
	ctx := context.Background()
	slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
	r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
	r.schedulerLib = taintAwareScheduler()
	r.updateGroupCache(ctx, slice)

	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Error("tryScheduleSliceGroup() requeued after marking the slice; the unbind requeues it")
	}
	got := getSlice(t, r, "owned")
	if got.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
		t.Fatal("slice not marked for eviction")
	}
	if !slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
		t.Fatalf("PartitionIds = %v, want unchanged [cube-a] until the pod reconciler unbinds", got.Spec.PartitionIds)
	}

	podReconciler := pod_reconciler.NewPodReconciler(r.Client, scheme.Scheme, logr.Discard(), r.cache,
		record.NewFakeRecorder(10), 1, false, policy.NewCloudPolicy())
	if _, err := podReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "owned"}}); err != nil {
		t.Fatalf("PodReconciler.Reconcile() error: %v", err)
	}
	got = getSlice(t, r, "owned")
	if len(got.Spec.PartitionIds) != 0 {
		t.Fatalf("PartitionIds = %v after eviction, want empty", got.Spec.PartitionIds)
	}
	// What handleSliceUpdate does when it sees the unbind.
	r.cache.ReleasePartitions(ctx, []*interfaces.SliceInfo{sliceInfo(ctx, slice)})
	r.updateGroupCache(ctx, got)

	if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
		t.Fatal("tryScheduleSliceGroup() requeued instead of placing the unbound slice")
	}
	if got := getSlice(t, r, "owned").Spec.PartitionIds; !slices.Equal(got, []string{"cube-b"}) {
		t.Errorf("PartitionIds = %v, want [cube-b]", got)
	}
}

func TestTryScheduleSliceGroup_NoExecuteEviction(t *testing.T) {
	ctx := context.Background()

	t.Run("failed mark is retried", func(t *testing.T) {
		slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)
		r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
			Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return apierrors.NewInternalError(fmt.Errorf("injected"))
			},
		})

		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != retryDelayBind {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, retryDelayBind)
		}
	})

	t.Run("taint cleared before the pass is left alone", func(t *testing.T) {
		slice := taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)
		r.cache.UpdateTree(ctx, noExecuteTree(), nil)

		if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
			t.Error("tryScheduleSliceGroup() requeued a healthy bound slice")
		}
		if got := getSlice(t, r, "owned"); got.Annotations[interfaces.EvictPodsAnnotation] != "" ||
			!slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
			t.Errorf("slice changed after its taint cleared: annotations %v, partitions %v", got.Annotations, got.Spec.PartitionIds)
		}
	})

	t.Run("only the tainted member of a gang is marked", func(t *testing.T) {
		tainted := taintTestSlice("tainted", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		healthy := taintTestSlice("healthy", TPU6e, interfaces.SliceSchedulerName, "cube-b")
		for _, s := range []*slicev1beta1.Slice{tainted, healthy} {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = "2"
		}
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), tainted, healthy)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, tainted)
		r.updateGroupCache(ctx, healthy)

		r.tryScheduleSliceGroup(ctx, "gang")
		if !isMarkedForEviction(t, r, "tainted") {
			t.Error("tainted member not marked for eviction")
		}
		if isMarkedForEviction(t, r, "healthy") {
			t.Error("healthy member marked for eviction")
		}
	})

	gang := func(t *testing.T, markTainted, deactivateTainted bool) *SliceReconciler {
		tainted := taintTestSlice("tainted", TPU6e, interfaces.SliceSchedulerName, "cube-a")
		if deactivateTainted {
			tainted = deactivatingSlice("tainted", "cube-a")
		}
		binding := taintTestSlice("binding", TPU6e, interfaces.SliceSchedulerName, "cube-b")
		for _, s := range []*slicev1beta1.Slice{tainted, binding} {
			s.Annotations[SliceGroupNameAnnotation] = "gang"
			s.Annotations[NumSlicesAnnotation] = "2"
		}
		if markTainted {
			tainted.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
		}
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), tainted, binding)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, tainted)
		r.updateGroupCache(ctx, binding)
		r.sliceTasks["binding"] = SliceStateBinding
		return r
	}

	t.Run("gang blocked by a bind keeps retrying a tainted member", func(t *testing.T) {
		r := gang(t, false, false)
		requeue, delay := r.tryScheduleSliceGroup(ctx, "gang")
		if !requeue || delay != retryDelayBind {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, retryDelayBind)
		}
		if isMarkedForEviction(t, r, "tainted") {
			t.Error("tainted member marked while the gang is blocked")
		}

		r.sliceTasksMutex.Lock()
		delete(r.sliceTasks, "binding")
		r.sliceTasksMutex.Unlock()
		r.tryScheduleSliceGroup(ctx, "gang")
		if !isMarkedForEviction(t, r, "tainted") {
			t.Error("tainted member not marked once the bind finished")
		}
	})

	t.Run("gang blocked by a bind with the tainted member already marked is not retried", func(t *testing.T) {
		r := gang(t, true, false)
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Error("tryScheduleSliceGroup() requeued; the unbind requeues the group")
		}
	})

	t.Run("gang blocked by a bind with the tainted member being deactivated is not retried", func(t *testing.T) {
		r := gang(t, false, true)
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "gang"); requeue {
			t.Error("tryScheduleSliceGroup() requeued a member the evict pass would skip")
		}
	})

	t.Run("slice being deactivated is not marked", func(t *testing.T) {
		slice := deactivatingSlice("owned", "cube-a")
		r := newTaintReconciler(t, true, noExecuteTree(nodeUnreachable), slice)
		r.schedulerLib = taintAwareScheduler()
		r.updateGroupCache(ctx, slice)

		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != policy.DeactivationRequeueDelay {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, policy.DeactivationRequeueDelay)
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("slice being deactivated marked for eviction")
		}
	})
}

func deactivatingSlice(name string, partitionIDs ...string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, partitionIDs...)
	slice.Annotations[policy.ProvisionedPartitionAnnotation] = partitionIDs[0]
	return slice
}

func markedSlice(name string, partitionIDs ...string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, partitionIDs...)
	slice.Annotations[interfaces.EvictPodsAnnotation] = interfaces.TrueStr
	return slice
}

func TestStart_RequeuesSlicesOnNoExecutePartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objs := []client.Object{
		noExecuteTree(nodeUnreachable),
		taintTestSlice("tainted", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
		taintTestSlice("healthy", TPU6e, interfaces.SliceSchedulerName, "cube-b"),
		taintTestSlice("foreign", TPU6e, "someone-else", "cube-a-1"),
		deactivatingSlice("deactivating", "cube-a-0"),
		markedSlice("marked", "cube-a-0"),
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
	r, err := NewSliceReconciler(fakeClient, scheme.Scheme, logr.Discard(), TPU6e, record.NewFakeRecorder(10),
		false, nil, watcher.NewPartitionCache(false, watcher.WithTaintFiltering(true)), 0, 7*time.Second, policy.NewCloudPolicy(), 0)
	if err != nil {
		t.Fatalf("NewSliceReconciler() error: %v", err)
	}
	rec := newDelayRecorder()
	r.requeueWithDelayFn = rec.record

	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	rec.wait(t, 1)
	// Give a stray second requeue a chance to land before checking there was only one.
	time.Sleep(50 * time.Millisecond)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 1 || rec.calls["tainted-group"] != 7*time.Second {
		t.Errorf("startup requeues = %v, want only tainted-group after 7s", rec.calls)
	}
}

var nodeCordoned = partitiontreev1.PartitionTaint{Key: corev1.TaintNodeUnschedulable, Effect: partitiontreev1.TaintEffectNoSchedule}

// probeFailedSlice is a bound slice whose health probe failed: it needs rescheduling, but
// CloudPolicy neither evicts its pods nor skips the backoff for it.
func probeFailedSlice(name string, partitionIDs ...string) *slicev1beta1.Slice {
	slice := taintTestSlice(name, TPU6e, interfaces.SliceSchedulerName, partitionIDs...)
	slice.Status.Conditions = []metav1.Condition{{
		Type:   policy.ConditionTypeReady,
		Status: metav1.ConditionUnknown,
		Reason: policy.ReasonSliceHealthProbeFailed,
	}}
	return slice
}

// TestTryScheduleSliceGroup_UnbindsMovedSlice checks that a bound slice the solver places
// on other partitions is marked for eviction, with no victims picked, instead of being
// bound there, which assignPartitionsWithRetry would refuse.
func TestTryScheduleSliceGroup_UnbindsMovedSlice(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T, tree *partitiontreev1.PartitionTree, timeout time.Duration, slices ...*slicev1beta1.Slice) *SliceReconciler {
		r := newTaintReconciler(t, true, tree, slices...)
		r.schedulerLib = taintAwareScheduler()
		r.rescheduleTimeout = timeout
		r.rescheduleHealthyDelay = time.Minute
		for _, s := range slices {
			r.updateGroupCache(ctx, s)
		}
		return r
	}
	expireBackoff := func(r *SliceReconciler, name string) {
		r.failureMutex.Lock()
		r.failureTimestamps[name] = time.Now().Add(-time.Hour)
		r.failureMutex.Unlock()
	}
	wantUnbinding := func(t *testing.T, r *SliceReconciler, requeue bool) {
		t.Helper()
		if requeue {
			t.Error("tryScheduleSliceGroup() requeued after marking the slice; the unbind requeues it")
		}
		got := getSlice(t, r, "owned")
		if got.Annotations[interfaces.EvictPodsAnnotation] != interfaces.TrueStr {
			t.Error("slice not marked for eviction")
		}
		if !slices.Equal(got.Spec.PartitionIds, []string{"cube-a"}) {
			t.Errorf("PartitionIds = %v, want unchanged [cube-a]", got.Spec.PartitionIds)
		}
		// The pods are still running there until the pod reconciler clears the partitions.
		if node, ok := r.cache.GetNode("cube-a"); !ok || node.InUseBy == nil || node.InUseBy.Name != "owned" {
			t.Error("cube-a not reserved for the slice being unbound")
		}
	}

	t.Run("cordoned: backs off while the backoff runs", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		requeue, delay := r.tryScheduleSliceGroup(ctx, "owned-group")
		if !requeue || delay != r.rescheduleHealthyDelay {
			t.Errorf("tryScheduleSliceGroup() = (%v, %v), want (true, %v)", requeue, delay, r.rescheduleHealthyDelay)
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("slice marked for eviction during the backoff")
		}
	})

	t.Run("cordoned: marked once the backoff times out", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		expireBackoff(r, "owned")
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})

	t.Run("cordoned: marked at once with the backoff disabled", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 0, probeFailedSlice("owned", "cube-a"))
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})

	t.Run("broken partition: marked without a backoff", func(t *testing.T) {
		tree := noExecuteTree()
		tree.Status.Root.ChildPartitions[0].State = partitiontreev1.PartitionStateUnhealthy
		r := setup(t, tree, 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
	})

	t.Run("no victim is preempted for the move", func(t *testing.T) {
		owned := probeFailedSlice("owned", "cube-a")
		owned.Annotations[SlicePriorityAnnotation] = "100"
		victim := taintTestSlice("victim", TPU6e, interfaces.SliceSchedulerName, "cube-b")
		r := setup(t, noExecuteTree(nodeCordoned), 0, owned, victim)
		requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group")
		wantUnbinding(t, r, requeue)
		if isMarkedForEviction(t, r, "victim") {
			t.Error("victim preempted for a placement that can't be bound")
		}
	})

	t.Run("untainted partition is rescheduled in place", func(t *testing.T) {
		r := setup(t, noExecuteTree(), 10*time.Minute, probeFailedSlice("owned", "cube-a"))
		expireBackoff(r, "owned")
		if requeue, _ := r.tryScheduleSliceGroup(ctx, "owned-group"); requeue {
			t.Error("tryScheduleSliceGroup() requeued instead of rescheduling in place")
		}
		if isMarkedForEviction(t, r, "owned") {
			t.Error("slice on an untainted partition marked for eviction")
		}
	})

	t.Run("healthy slice on a cordoned partition is left alone", func(t *testing.T) {
		r := setup(t, noExecuteTree(nodeCordoned), 0, taintTestSlice("owned", TPU6e, interfaces.SliceSchedulerName, "cube-a"))
		r.tryScheduleSliceGroup(ctx, "owned-group")
		if isMarkedForEviction(t, r, "owned") {
			t.Error("healthy slice marked for eviction by a cordon")
		}
	})
}

func TestMovedSlices(t *testing.T) {
	groupSlices := map[string]*slicev1beta1.Slice{
		"new":    taintTestSlice("new", TPU6e, interfaces.SliceSchedulerName),
		"stays":  taintTestSlice("stays", TPU6e, interfaces.SliceSchedulerName, "cube-a-0", "cube-a-1"),
		"moves":  taintTestSlice("moves", TPU6e, interfaces.SliceSchedulerName, "cube-b"),
		"absent": taintTestSlice("absent", TPU6e, interfaces.SliceSchedulerName, "cube-a"),
	}
	toReserve := []*interfaces.SliceInfo{
		{Name: "new", PartitionIds: []string{"cube-b-0"}},
		{Name: "stays", PartitionIds: []string{"cube-a-1", "cube-a-0"}},
		{Name: "moves", PartitionIds: []string{"cube-a"}},
	}
	moved := movedSlices(groupSlices, toReserve)
	if len(moved) != 1 || moved[0].Name != "moves" {
		t.Errorf("movedSlices() = %v, want [moves]", moved)
	}
}
