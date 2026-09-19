package types

import (
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// TestPassRequeue pins the precedence a pass's wake-up follows: the
// earliest positive wait wins, any explicit wait beats the bare
// rate-limited backoff, and the bare form is reached only when the pass
// holds no wait at all.
func TestPassRequeue(t *testing.T) {
	cases := []struct {
		name      string
		cadence   time.Duration
		explicit  []time.Duration
		wantAfter time.Duration
		wantBare  bool
	}{
		{name: "configured cadence, nothing else due", cadence: 5 * time.Second, wantAfter: 5 * time.Second},
		{name: "earlier explicit wait beats the cadence", cadence: 5 * time.Second,
			explicit: []time.Duration{2 * time.Second}, wantAfter: 2 * time.Second},
		{name: "later explicit wait loses to the cadence", cadence: 5 * time.Second,
			explicit: []time.Duration{time.Minute}, wantAfter: 5 * time.Second},
		{name: "explicit wait beats the bare backoff",
			explicit: []time.Duration{0, 90 * time.Second}, wantAfter: 90 * time.Second},
		{name: "earliest of several explicit waits", cadence: 0,
			explicit: []time.Duration{time.Minute, 3 * time.Second, time.Hour}, wantAfter: 3 * time.Second},
		{name: "no cadence and nothing due falls back to the backoff",
			explicit: []time.Duration{0, -time.Second}, wantBare: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PassRequeue(tc.cadence, tc.explicit...)
			if tc.wantBare {
				if got.RequeueAfter != 0 {
					t.Fatalf("RequeueAfter: got %v want 0 (bare rate-limited backoff)", got.RequeueAfter)
				}
				if !got.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
					t.Fatalf("a pass with no wait must ask for the rate-limited backoff; got %+v", got)
				}
				return
			}
			if got.RequeueAfter != tc.wantAfter {
				t.Errorf("RequeueAfter: got %v want %v", got.RequeueAfter, tc.wantAfter)
			}
			if got.Requeue { //nolint:staticcheck // an explicit wait must not also set the bare flag
				t.Errorf("an explicit wait must supersede the bare backoff; got %+v", got)
			}
		})
	}
}

// TestPassWakeFold pins the held-work sink: a configured interval
// competes as an ordinary explicit wait, an unconfigured one still
// wakes the pass on the rate-limited backoff, and neither overrides an
// earlier wait the pass already holds.
func TestPassWakeFold(t *testing.T) {
	var configured PassWake
	configured.Observe(5 * time.Second)
	if got := configured.Fold(ctrl.Result{}); got.RequeueAfter != 5*time.Second {
		t.Errorf("configured wake: got %+v want RequeueAfter 5s", got)
	}
	if got := configured.Fold(ctrl.Result{RequeueAfter: time.Second}); got.RequeueAfter != time.Second {
		t.Errorf("an earlier wait must win: got %+v", got)
	}

	var bare PassWake
	bare.Observe(0)
	got := bare.Fold(ctrl.Result{})
	if got.RequeueAfter != 0 || !got.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
		t.Errorf("unconfigured wake must ride the rate-limited backoff: got %+v", got)
	}
	if withWait := bare.Fold(ctrl.Result{RequeueAfter: time.Minute}); withWait.RequeueAfter != time.Minute || withWait.Requeue { //nolint:staticcheck // an explicit wait must stand alone
		t.Errorf("an explicit wait must stand: got %+v", withWait)
	}

	var empty *PassWake
	if got := empty.Fold(ctrl.Result{}); got.RequeueAfter != 0 || got.Requeue { //nolint:staticcheck // no wake was deposited
		t.Errorf("no deposit must leave the result alone: got %+v", got)
	}
}
