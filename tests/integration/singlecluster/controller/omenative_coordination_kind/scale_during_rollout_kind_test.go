package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// scale-out of brand-new instance indices during an in-flight rollout.
//
// Pins the scale-during-rollout starvation fix: with instance-0 mid-
// update (a revision bump in flight), a concurrent scale-up of engine
// MinReplicas 1 → 3 must materialize the brand-new gangs (indices 1,2)
// immediately instead of serializing them behind instance-0's rollout.
// On TPU instance-0's rollout is a 20-40 min cold-compile, so a stalled
// scale-out is a real outage; the surge-free new indices legitimately
// bypass the dispatcher's skip-Create-while-updating gate (genuine
// ActiveOrdinal=0, no RunningRevision to mis-stamp).
//
// Invariants asserted:
//   - instances 1,2 reach Ready WITHOUT waiting on instance-0's rollout
//     to finish.
//   - instance-0 is never duplicated (its index-0 pod identity stays a
//     single live pod; no stray pod at a phantom ordinal).
//   - the whole Component converges to 3 Ready instances on v2.
//
// NOTE: authored to match the existing KIND spec patterns but UNRUN —
// this environment has no Docker, so `make test-kind` could not execute
// it. Run locally before relying on it as a regression guard.
var _ = ginkgo.Describe("permutation — scale-out new indices during in-flight rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-scale-rollout-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("indices 1,2 reach Ready without waiting on instance-0's rollout", func() {
		// Single-Component engine, SurgeThenDrain (the default), no
		// coordination group — the bypass under test is in the per-
		// Component dispatcher, independent of cross-Component pacing.
		fix := newCoordFixture(ns.Name, WithISVCName("perm-scale-rollout"),
			WithoutDecoder(),
			WithEngineReplicas(1),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the single v1 instance to converge.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-scale-rollout")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
			insts := instanceStatuses(g, ns.Name, "perm-scale-rollout", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-scale-rollout")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Combined update: bump the image (rollout on instance-0) AND
		// scale 1 → 3 in the SAME spec edit. The reconciler must not
		// stall the new indices behind instance-0's surge.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-scale-rollout")
			three := 3
			cur.Spec.Engine.MinReplicas = &three
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The load-bearing assertion: indices 1 and 2 reach Ready. With
		// the starvation bug they never get created until instance-0's
		// rollout completes; here they must converge regardless.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-scale-rollout")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "perm-scale-rollout", v1beta1.EngineComponent)
			seen := map[int32]v1beta1.OMENativeInstancePhase{}
			for _, s := range insts {
				seen[s.Index] = s.Phase
			}
			g.Expect(seen).To(HaveKeyWithValue(int32(1), v1beta1.OMENativeInstanceReady),
				"index 1 must reach Ready without waiting on instance-0's rollout; statuses=%v", seen)
			g.Expect(seen).To(HaveKeyWithValue(int32(2), v1beta1.OMENativeInstanceReady),
				"index 2 must reach Ready without waiting on instance-0's rollout; statuses=%v", seen)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Full convergence: 3 engine instances Ready on v2, group settled.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-scale-rollout")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)),
				"engine must converge to 3 serving replicas on v2")
			insts := instanceStatuses(g, ns.Name, "perm-scale-rollout", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(3))
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"instance %d must be Ready; got %q", s.Index, s.Phase)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pod-level invariant: exactly 3 live engine pods (one per Ready
		// instance) and instance-0's canonical index-0 pod is present.
		// A stale-ordinal DUPLICATE of instance-0 — one of the two
		// corruption modes the surge-free bypass must avoid — would push
		// the live count to 4.
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-scale-rollout")
			g.Expect(err).NotTo(HaveOccurred())
			alive := 0
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				if p.DeletionTimestamp != nil {
					continue
				}
				alive++
			}
			g.Expect(alive).To(Equal(3),
				"expected exactly 3 alive engine pods post-converge, got %d (a 4th = stale-ordinal duplicate of instance-0)", alive)
			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "perm-scale-rollout-engine-0-default-0",
			}, pod0)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
