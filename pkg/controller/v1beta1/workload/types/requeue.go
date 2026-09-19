package types

import (
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// PassRequeue resolves the wake-up of one reconcile pass that still has
// work to do, from the operator's configured cadence plus every explicit
// wait the pass collected (a RetryBlock's due time, a promote-window
// remainder, a node-death deadline).
//
// Precedence: the earliest positive wait wins, and ANY explicit wait
// wins over the bare requeue. The bare form is reached only when the
// pass holds no wait at all — an unconfigured cadence and nothing else
// due — and asks for the controller's rate-limited backoff, the only
// spelling of "come back soon" that invents no interval.
func PassRequeue(cadence time.Duration, explicit ...time.Duration) ctrl.Result {
	earliest := cadence
	for _, wait := range explicit {
		if wait > 0 && (earliest <= 0 || wait < earliest) {
			earliest = wait
		}
	}
	if earliest > 0 {
		return ctrl.Result{RequeueAfter: earliest}
	}
	// The deprecated Requeue field is the only way to ask for the
	// controller's rate-limited backoff; every alternative spelling
	// would bake in an interval.
	return ctrl.Result{Requeue: true} //nolint:staticcheck // rate-limited backoff has no non-deprecated spelling
}

// FoldPassRequeue merges further explicit waits into a result a pass
// already resolved, under the same precedence PassRequeue applies: the
// earliest positive wait wins and supersedes a bare rate-limited
// requeue. A result that asked for no wake-up at all keeps asking for
// none — folding never invents one.
func FoldPassRequeue(res ctrl.Result, waits ...time.Duration) ctrl.Result {
	earliest := res.RequeueAfter
	for _, wait := range waits {
		if wait > 0 && (earliest <= 0 || wait < earliest) {
			earliest = wait
		}
	}
	if earliest <= 0 {
		return res
	}
	res.RequeueAfter = earliest
	res.Requeue = false //nolint:staticcheck // an explicit wait replaces the rate-limited backoff
	return res
}

// FloorPassRequeue raises a result's wake-up to at least wait. Used for
// a delay the apiserver itself asked for: unlike the fold's minimum, a
// server-suggested Retry-After is a FLOOR — waking sooner just re-earns
// the rejection, which the bare rate-limited backoff would do.
func FloorPassRequeue(res ctrl.Result, wait time.Duration) ctrl.Result {
	if wait <= 0 || wait <= res.RequeueAfter {
		return res
	}
	res.RequeueAfter = wait
	res.Requeue = false //nolint:staticcheck // the floor names a deadline the bare backoff cannot honor
	return res
}
