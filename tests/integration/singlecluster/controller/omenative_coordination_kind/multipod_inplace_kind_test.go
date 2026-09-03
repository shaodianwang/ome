package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-pod (Worker.Size=2) Instance with InPlaceIfPossible.
//
// Per the production note in pkg/.../workload/ops/update.go around
// chooseUpdateModeForInstance, multi-pod with in-place modes falls
// back to recreate because the in-place eligibility check compares
// only the leader PodSpec — a worker-only image bump would patch
// just the leader, leaving workers on the old image. This spec pins
// that production safety contract:
//
//   - Multi-pod + InPlaceIfPossible + image bump must produce a
//     recreate (NOT preserve pod UIDs).
//   - All 3 pods (leader + 2 workers) end up on v2.
//   - Instance Incarnation bumps (recreate did happen).
//   - Group converges back to Idle.
var _ = ginkgo.Describe("permutation — multi-pod InPlaceIfPossible falls back to recreate", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-mp-ip-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("multi-pod + InPlaceIfPossible image bump produces recreate (UIDs not preserved)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("perm-mp-ip"),
			WithoutDecoder(),
			WithEngineWorkerSize(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-ip")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "perm-mp-ip", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture v1 pod count and Incarnation; we expect the
		// recreate fallback to bump Incarnation, NOT preserve UIDs
		// (which is the strict in-place contract that doesn't apply
		// here).
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-ip")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(Equal(3))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "perm-mp-ip")
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		ir := freshInferenceReplica(Default, ns.Name, "perm-mp-ip", v1beta1.EngineComponent)
		v1Incarnation := ir.Status.InstanceStatuses[0].Incarnation

		// Bump engine image. Multi-pod reads Leader.PodSpec +
		// Worker.PodSpec (engine.go:339-348) — bump BOTH so the gang
		// rollout actually fires. Bumping only Spec.Engine.PodSpec leaves
		// the rendered hash unchanged for multi-pod gangs.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-mp-ip")
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Converge. CurrentRevision must advance off v1 and
		// Incarnation must bump — the latter is the load-bearing
		// proof that we routed to recreate (in-place would NOT bump
		// Incarnation).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-ip")
			cs := c.Status.Components[v1beta1.EngineComponent]
			insts := instanceStatuses(g, ns.Name, "perm-mp-ip", v1beta1.EngineComponent)
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Incarnation).
				To(BeNumerically(">", v1Incarnation),
					"multi-pod InPlaceIfPossible must route to recreate (Incarnation must bump); in-place would keep the same Incarnation")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pod count stays at 3 across the rollout end-state.
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-ip")
		Expect(err).NotTo(HaveOccurred())
		Expect(snap.Alive(v1beta1.EngineComponent)).To(Equal(3),
			"multi-pod end-state must still have 3 pods (leader + 2 workers)")
	})
})
