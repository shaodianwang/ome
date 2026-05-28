package workload

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Deps is the manager-scoped wiring the workload reconciler needs to
// observe the cluster, materialize resources, and emit events. Unlike
// Source (which is per-workload), Deps is shared across every workload
// the same controller manages.
//
// Adapters construct Deps once at controller wiring time and pass it
// into workload.Reconcile on each call.
type Deps struct {
	// Client is the controller-runtime cached client used for the
	// vast majority of read/write operations.
	Client client.Client

	// APIReader is an optional live API reader, typically
	// mgr.GetAPIReader(). Used for the correctness-critical reads in
	// revision bookkeeping where stale cache contents could cause
	// spurious collisions. When nil the cached Client is used.
	APIReader client.Reader

	// Recorder emits K8s Events against Source.EventTarget() at
	// lifecycle transitions. nil-safe: when unset, event helpers
	// are no-ops.
	Recorder record.EventRecorder

	// Expectations is the create/delete bookkeeping cache the
	// reconciler uses to avoid re-issuing batches before the
	// controller-runtime watch has confirmed prior writes. Adapters
	// wire the same instance into the Pod event handler so observed
	// create / delete events release expectations immediately. When
	// nil the package-level DefaultExpectations singleton is used.
	Expectations *Expectations

	// RenderHook is an optional per-pod template mutator the
	// workload renderer invokes after composing the canonical pod
	// (name, hostname, labels, controller env, owner ref) but
	// before the adapter calls c.Create. Adapters use it to inject
	// adapter-specific overlays — the ISVC adapter wires this to
	// coordination.InjectPeerEnv so OME_<PEER>_ENDPOINT vars land on
	// every container; the IR adapter leaves it nil (peer-env
	// projection lives on the parent ISVC controller for v1).
	//
	// The hook MUST be idempotent — Render may be invoked twice in
	// the same reconcile (e.g., when a cache-lag retry resends the
	// pod after AlreadyExists).
	RenderHook RenderHook
}

// RenderHook is the optional adapter-specific render-time mutator.
// Receives the pod under construction after the workload renderer
// has stamped its canonical fields; may mutate any field except
// Name/Namespace/OwnerReferences (those are workload-controlled).
//
// hashFn returns the per-Component revision hash for cross-Component
// peer addressing. The hook MAY pass nil and use a fallback (the
// pod's own revision label) when cross-Component routing isn't in
// scope.
type RenderHook func(pod *corev1.Pod, runnerName string, ordinal int32, revisionHash string)

// Reader returns the live API reader when APIReader is set; otherwise
// the cached Client. Callers use this for revision bookkeeping and
// other reads that must not observe a stale cache.
func (d *Deps) Reader() client.Reader {
	if d.APIReader != nil {
		return d.APIReader
	}
	return d.Client
}

// ExpectationsCache returns the Expectations cache to use this
// reconcile — the per-controller instance threaded through Deps when
// set, or the package-level DefaultExpectations singleton as a
// fallback for tests and pre-wiring callers.
func (d *Deps) ExpectationsCache() *Expectations {
	if d.Expectations != nil {
		return d.Expectations
	}
	return DefaultExpectations
}
