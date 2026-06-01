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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
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
//
// The done/ctx channels are re-checked immediately before invoking sampleFn:
// if stop() raced a tick that was already ready, this short-circuits the
// queued sample so it can't fire an assertion against a snapshot taken AFTER
// the spec has wound down (e.g., during namespace teardown). Without this
// guard, a sampler whose sampleFn was mid-call when stop() ran would still
// complete the apiserver round-trip and assert against the post-teardown
// state — see the 2026-05-30 sampler-goroutine-leak bug report.
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
			// Re-check stop signals before sampling: stop() may have
			// closed done while a tick was concurrently ready, and Go's
			// select is non-deterministic between two ready channels.
			// Without this guard the sampler would invoke sampleFn after
			// the spec body returned and the namespace teardown began —
			// firing a stale assertion against the next spec's runtime.
			select {
			case <-ctx.Done():
				return count
			case <-done:
				return count
			default:
			}
			sampleFn(g)
			count++
		}
	}
}

// StartSampler spawns a background sampler that runs sampleFn at the given
// interval. Returns a Stop function the caller MUST defer; Stop closes the
// done channel and blocks UNCONDITIONALLY until the sampler goroutine
// exits. Returns the count of samples observed.
//
// The sampling goroutine wraps the loop in ginkgo.GinkgoRecover so a
// gomega failure inside sampleFn fails the surrounding It block instead of
// crashing the process — callers don't need their own GinkgoRecover.
//
// Stop blocks unconditionally (no timeout) so a sampler whose sampleFn is
// mid-apiserver-call cannot leak past the spec boundary. The previous
// implementation timed out after 2*interval and returned to the caller,
// leaving a live goroutine that would observe namespace-teardown state and
// queue a gomega.Fail against whichever spec was active next — see the
// 2026-05-30 sampler-goroutine-leak bug report attributing a
// recreate_pod_kind serving-floor failure to a later InPlaceOnly spec.
//
// Risk: a sampleFn that blocks forever (e.g., an apiserver call with no
// timeout) will now hang Stop forever. Mitigation: pass a ctx that the
// suite will cancel during teardown, and let sampleFn honor it.
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
		// Send count back on EVERY exit path — including the panic
		// that fires when sampleFn's gomega.Expect calls ginkgo.Fail.
		// Without a deferred send, a Fail-induced panic unwinds the
		// goroutine, GinkgoRecover catches it, and doneCount would
		// never receive — deadlocking the unconditional <-doneCount
		// in stop(). Defers run LIFO: inner GinkgoRecover catches
		// the panic first, then the outer deferred send delivers count.
		var count int
		defer func() {
			doneCount <- count
		}()
		defer ginkgo.GinkgoRecover()
		count = SampleDuringRollout(ctx, done, interval, sampleFn)
	}()
	stopped := false
	return func() int {
		if stopped {
			return 0
		}
		stopped = true
		close(done)
		// Block until the goroutine actually exits. No timeout: a
		// leaked sampler that fires a gomega.Fail post-stop lands on
		// the wrong spec (Ginkgo attributes Fail to the currently
		// active spec), which is exactly the bug this guard prevents.
		return <-doneCount
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
