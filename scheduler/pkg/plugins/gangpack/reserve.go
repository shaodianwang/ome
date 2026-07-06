package gangpack

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Reserve is a no-op: placement is committed via the domain pin (PreFilter) and
// enforced by Filter, and the gang gate lives in Permit. A real per-gang capacity
// claim (so two gangs can't race into one domain) is future work; Reserve exists
// today only to pair with Unreserve, which does the unwind.
func (g *GangPack) Reserve(_ context.Context, _ *framework.CycleState, _ *v1.Pod, _ string) *framework.Status {
	return nil
}

// Unreserve unwinds a gang when one of its members fails to schedule — a Permit
// timeout, or a bind failure after the gate opened. It does two things:
//
//   - releases the domain pin, so the gang's retry re-runs best-fit instead of
//     reusing a stale commitment (the old domain may no longer be the best, or
//     may be why the member failed);
//   - rejects every sibling still waiting at the gate, so a half-formed gang
//     unwinds together in one shot rather than each member waiting out its own
//     timeout.
//
// It is idempotent: rejected siblings are themselves unreserved, re-entering here,
// but Release and Reject are both safe to repeat. A pod that is not a gang member
// is left alone.
func (g *GangPack) Unreserve(_ context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) {
	if g.pgReader == nil {
		return
	}
	gang, ok := resolveGang(pod, g.pgReader)
	if !ok {
		return
	}
	g.pins.Release(gang.key)
	g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		if sameGang(wp.GetPod(), gang.key) {
			wp.Reject(Name, "gang "+gang.key+" unwound after a member failed to schedule")
		}
	})
}
