package coordination

import (
	"github.com/onsi/ginkgo/v2"
)

// Behavior test A — RollingUpdate + SurgeThenDrain (the default).
//
// PIt while SurgeThenDrain still delegates to recreate; the body
// documents the invariants the real implementation must satisfy.
var _ = ginkgo.Describe("behavior — RollingUpdate + SurgeThenDrain", func() {
	ginkgo.PIt("A-1 serving capacity never drops below replicas during a bump", func() {
		// Config: 4 prefill + 2 decode, RollingUpdate, sync: PerComponent,
		//         maxSurge: 1, maxUnavailable: 25%,
		//         updateStrategy.type: SurgeThenDrain (default).
		// Action: bump engine image v1 -> v2.
		// Invariant (sampled every 100ms throughout the rollout):
		//   util.SnapshotPods(...).Serving(engine)  >= 4
		//   util.SnapshotPods(...).Serving(decoder) >= 2
		// Rationale: SurgeThenDrain creates the new pod and waits for it
		// to be Serving before draining the old, so original capacity is
		// preserved at every instant.
	})

	ginkgo.PIt("A-2 alive pod count never exceeds replicas + maxSurge", func() {
		// Same config as A-1. maxSurge: 1.
		// Invariant (sampled): Alive(engine) <= 5 AND Alive(decoder) <= 3.
		// Rationale: SurgeThenDrain limits in-flight surge to the budget.
	})

	ginkgo.PIt("A-3 rollout converges to all pods on v2", func() {
		// Same config. Eventually all 4 prefill + 2 decode are on the v2
		// revision hash AND Serving.
	})
})
