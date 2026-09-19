package types

import (
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// PassWake is the pass-scoped sink for a wake-up an op pass owes but has
// no result of its own to carry: a row held on operator configuration
// that no watch event will announce — nothing in the cluster changes
// when a ConfigMap key finally appears. The op deposits the wake-up here
// and the dispatcher folds it into the pass result, so the hold is
// re-consulted instead of waiting for an unrelated event or the resync.
// All methods are nil-safe.
type PassWake struct {
	pending time.Duration
	bare    bool
}

// Observe records that the pass must come back, after the given interval
// when the operator configured one — keeping the earliest seen this pass
// — or on the controller's rate-limited backoff when it did not.
func (w *PassWake) Observe(after time.Duration) {
	if w == nil {
		return
	}
	if after <= 0 {
		w.bare = true
		return
	}
	if w.pending == 0 || after < w.pending {
		w.pending = after
	}
}

// Fold merges the recorded wake-up into res, under the pass precedence:
// an explicit interval competes as the earliest wait wins, while a bare
// wake-up only applies when the result carries no explicit wait at all.
func (w *PassWake) Fold(res ctrl.Result) ctrl.Result {
	if w == nil {
		return res
	}
	if w.pending > 0 {
		res = FoldPassRequeue(res, w.pending)
	}
	if w.bare && res.RequeueAfter == 0 {
		res.Requeue = true //nolint:staticcheck // rate-limited backoff has no non-deprecated spelling
	}
	return res
}
