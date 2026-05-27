package util

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
)

// SampleDuringRollout calls sampleFn at every tick until done is closed or
// ctx is cancelled. Each sample runs inside its own gomega.Eventually-style
// assertion bucket — failures are reported via the passed gomega.Gomega
// instance, which the caller wires to the outer test's Fail handler.
//
// Returns the number of samples observed. Useful so the caller can assert
// a minimum sample count (i.e., the rollout actually took non-trivial
// time and the invariant was checked enough).
//
// Tick cadence is honored as a ticker, not a sleep loop, so a slow
// sampleFn doesn't queue ticks.
func SampleDuringRollout(ctx context.Context, done <-chan struct{}, interval time.Duration, sampleFn func(g gomega.Gomega)) int {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	g := gomega.NewWithT(failHandlerAdapter{})
	count := 0
	for {
		select {
		case <-ctx.Done():
			return count
		case <-done:
			return count
		case <-tick.C:
			sampleFn(g)
			count++
		}
	}
}

// StartSampler spawns a background sampler that runs sampleFn at the given
// interval. Returns a Stop function the caller MUST defer; Stop closes the
// done channel and blocks for one tick interval so the in-flight sample can
// finish. Returns the count of samples observed.
//
// The sampling goroutine wraps the loop in ginkgo.GinkgoRecover so a
// gomega failure inside sampleFn fails the surrounding It block instead of
// crashing the process — callers don't need their own GinkgoRecover.
//
// Typical usage:
//
//	stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
//	    snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns, "isvc-1")
//	    g.Expect(err).NotTo(HaveOccurred())
//	    g.Expect(snap.Serving(v1beta1.EngineComponent)).To(BeNumerically(">=", 4))
//	})
//	defer stop()
//	// trigger rollout, wait for re-Ready
func StartSampler(ctx context.Context, interval time.Duration, sampleFn func(g gomega.Gomega)) (stop func() int) {
	done := make(chan struct{})
	doneCount := make(chan int, 1)
	go func() {
		defer ginkgo.GinkgoRecover()
		doneCount <- SampleDuringRollout(ctx, done, interval, sampleFn)
	}()
	stopped := false
	return func() int {
		if stopped {
			return 0
		}
		stopped = true
		close(done)
		select {
		case n := <-doneCount:
			return n
		case <-time.After(2 * interval):
			return 0
		}
	}
}

// PodSnapshot bundles the live state of all OMENative-managed pods for one
// ISVC, grouped by component. Behavior-test invariants assert against this
// snapshot rather than re-querying the apiserver for each predicate.
type PodSnapshot struct {
	ByComponent map[v1beta1.ComponentType][]*corev1.Pod
}

// SnapshotPods lists every OMENative pod owned by isvcName in namespace and
// groups them by their `component` label. Returns the snapshot or any list
// error verbatim (caller decides whether to retry or fail the assertion).
func SnapshotPods(ctx context.Context, c client.Client, namespace, isvcName string) (*PodSnapshot, error) {
	list := &corev1.PodList{}
	if err := c.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingLabels{
			LabelManagedByKey:                     ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
		},
	); err != nil {
		return nil, fmt.Errorf("list OMENative pods for %s/%s: %w", namespace, isvcName, err)
	}
	out := &PodSnapshot{ByComponent: map[v1beta1.ComponentType][]*corev1.Pod{}}
	for i := range list.Items {
		pod := &list.Items[i]
		comp := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
		if comp == "" {
			continue
		}
		out.ByComponent[comp] = append(out.ByComponent[comp], pod)
	}
	return out, nil
}

// Serving returns the count of pods for component that are serving traffic:
// both ContainersReady=True AND the controller's serving gate is True.
// This is the count operators care about — pods actually receiving requests
// via kube-proxy's EndpointSlice rotation.
func (s *PodSnapshot) Serving(component v1beta1.ComponentType) int {
	if s == nil {
		return 0
	}
	n := 0
	for _, p := range s.ByComponent[component] {
		if podreadiness.IsContainersReady(p) && podreadiness.IsServing(p) {
			n++
		}
	}
	return n
}

// Alive returns the count of pods for component that exist (any phase, any
// revision, including those with DeletionTimestamp set). Used by tests that
// assert on surge bounds.
func (s *PodSnapshot) Alive(component v1beta1.ComponentType) int {
	if s == nil {
		return 0
	}
	return len(s.ByComponent[component])
}

// OnRevision returns the count of pods for component carrying the given
// revision-hash label. Used by tests that need to assert which revision pods
// belong to (e.g., for Sequential ordering or BlueGreen partition).
func (s *PodSnapshot) OnRevision(component v1beta1.ComponentType, revisionHash string) int {
	if s == nil {
		return 0
	}
	n := 0
	for _, p := range s.ByComponent[component] {
		if p.Labels["ome.io/revision-hash"] == revisionHash {
			n++
		}
	}
	return n
}

// failHandlerAdapter routes gomega failures inside SampleDuringRollout
// through ginkgo.Fail so a sample-level violation fails the surrounding
// It block. Callers running SampleDuringRollout in a goroutine should
// `defer ginkgo.GinkgoRecover()` so the Fail call lands on the right
// spec instead of crashing the process.
type failHandlerAdapter struct{}

func (failHandlerAdapter) Helper() {}
func (failHandlerAdapter) Fatalf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args...))
}
