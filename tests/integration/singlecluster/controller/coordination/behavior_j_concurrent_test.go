package coordination

import (
	"github.com/onsi/ginkgo/v2"
)

// Behavior tests J + K — concurrent spec changes and failure-revert.
//
// J covers bump-during-bump (operator bumps to v2, then to v3 before
// v2 rollout completes). K covers rollout deadline + operator-driven
// revert (we never auto-rollback).
//
// PIt while retarget + failure-handling paths stabilize.
var _ = ginkgo.Describe("behavior — concurrent spec changes + revert", func() {
	// Test J — bump during bump.

	ginkgo.PIt("J-1 no v2 stragglers after second bump — all pods end on v1 or v3 only", func() {
		// Config: 4 prefill + 2 decode, RollingUpdate, SurgeThenDrain.
		// Action: bump engine to v2. Wait until some pods are on v2.
		//         Then bump to v3 BEFORE v2 rollout completes.
		// Invariant: after the second bump, eventually no pod remains
		// on v2 (only v1 and v3 hashes observed).
	})

	ginkgo.PIt("J-2 capacity preserved through both bumps", func() {
		// Same config. Invariant (sampled): Serving(engine) >= 4
		// throughout both bump phases.
	})

	ginkgo.PIt("J-3 rollout converges on v3", func() {
		// Same config. Eventually all 4 prefill on v3 hash AND serving.
	})

	// Test K — rollout deadline + operator-driven revert.

	ginkgo.PIt("K-1 failure does NOT auto-roll-back already-migrated pods", func() {
		// Config: 4 prefill + 2 decode, RollingUpdate, SurgeThenDrain.
		// Action: bump engine to an image that won't become Ready.
		//         Some pods may transition to v2 before the failure is
		//         detected.
		// Invariant: when the group is marked Failed, pods that already
		// reached Serving on v2 STAY on v2. Controller does NOT
		// auto-rollback (that would destructively drain working pods).
	})

	ginkgo.PIt("K-2 capacity preserved during failure window", func() {
		// Same config. Invariant (sampled during failure window):
		//   Serving(engine)  >= 4 AND Serving(decoder) >= 2.
	})

	ginkgo.PIt("K-3 operator revert behaves as a new rollout in reverse", func() {
		// Same config. After failure, operator reverts spec back to v1.
		// All v2 pods roll back to v1 using the same RollingUpdate pacing
		// (treating the revert as a fresh rollout in reverse).
	})
})
