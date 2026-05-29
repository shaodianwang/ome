package types

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Deps is the manager-scoped wiring the workload reconciler needs.
// Shared across every workload the same controller manages (unlike
// the per-workload ReconcileInput).
type Deps struct {
	// Client is the controller-runtime cached client used for the
	// vast majority of read/write operations.
	Client client.Client

	// APIReader is an optional live API reader (mgr.GetAPIReader()).
	// Used for revision bookkeeping where stale cache contents could
	// cause spurious collisions. When nil the cached Client is used.
	APIReader client.Reader

	// Recorder emits K8s Events at lifecycle transitions. nil-safe:
	// when unset, event helpers are no-ops.
	Recorder record.EventRecorder

	// Expectations is the create/delete bookkeeping cache used to
	// avoid re-issuing batches before the controller-runtime watch
	// has confirmed prior writes. Adapters wire the same instance
	// into the Pod event handler so observed create / delete events
	// release expectations immediately. nil falls back to
	// DefaultExpectations.
	Expectations *Expectations

	// RenderHook is an optional per-pod template mutator invoked
	// after composing the canonical pod (name, hostname, labels,
	// controller env, owner ref) but before c.Create. The ISVC
	// adapter wires this to coordination.InjectPeerEnv so
	// OME_<PEER>_ENDPOINT vars land on every container.
	//
	// The hook MUST be idempotent — Render may be invoked twice in
	// the same reconcile (e.g., cache-lag retry after AlreadyExists).
	RenderHook RenderHook
}

// RenderHook is the optional adapter-specific render-time mutator.
// Receives the pod under construction; may mutate any field except
// Name/Namespace/OwnerReferences (workload-controlled).
type RenderHook func(pod *corev1.Pod, runnerName string, ordinal int32, revisionHash string)

// Reader returns the live API reader when APIReader is set, otherwise
// the cached Client. Use for reads that must not observe a stale
// cache.
func (d *Deps) Reader() client.Reader {
	if d.APIReader != nil {
		return d.APIReader
	}
	return d.Client
}

// ExpectationsCache returns the per-controller cache threaded through
// Deps when set, otherwise the DefaultExpectations singleton.
func (d *Deps) ExpectationsCache() *Expectations {
	if d.Expectations != nil {
		return d.Expectations
	}
	return DefaultExpectations
}
