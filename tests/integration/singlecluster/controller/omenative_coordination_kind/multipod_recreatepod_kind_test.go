package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-pod (Worker.Size=2) Instance, explicit RecreatePod.
//
// Pins the gang-recreate path for OMENative multi-pod Instances: a
// single Instance owns 1 leader + 2 worker pods (3 pods total). On
// image bump with RecreatePod strategy the rollout must:
//   - Bump the leader's image to v2.
//   - Bump the workers' images to v2.
//   - Keep the same Instance count (MinReplicas=1).
//   - Instance returns to Ready and Incarnation bumps.
//
// No coordination group is wired — this spec exercises the per-Component
// RecreatePod path, not coord policy. RollingUpdate requires ≥ 2
// Components (ValidateGroupShape, groups.go:227), and the gang-recreate
// behavior here is independent of coord policy.
//
// The unit test
// pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/reconciler_test.go
// covers the multi-pod dispatch wiring. This KIND spec covers the
// real-kubelet end-to-end roll.
var _ = ginkgo.Describe("permutation — multi-pod RecreatePod", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-mp-rp-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("multi-pod engine (leader + 2 workers) recreates on image bump", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("perm-mp-rp"),
			WithoutDecoder(),
			WithEngineWorkerSize(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the single multi-pod Instance to reach Ready. The
		// engine should have 3 alive pods (1 leader + 2 workers) for
		// the one Instance.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-rp")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"single Instance (MinReplicas=1) under multi-pod")
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady),
					"multi-pod Instance must reach Ready")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-rp")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(Equal(3),
				"multi-pod with Worker.Size=2 must produce 3 pods/Instance (1 leader + 2 workers)")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-mp-rp")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		v1Incarnation := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].Incarnation

		// Bump engine image. Multi-pod recreate must rebuild the gang
		// (Incarnation bumps). Multi-pod reads Leader.PodSpec +
		// Worker.PodSpec (engine.go:339-348) — bumping only the
		// top-level Spec.Engine.PodSpec leaves the rendered hash
		// unchanged and the rollout never triggers. Bump BOTH.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-mp-rp")
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-rp")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady),
					"multi-pod Instance must return to Ready on v2")
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Incarnation).
				To(BeNumerically(">", v1Incarnation),
					"RecreatePod must bump Incarnation (gang rebuilt)")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Post-bump: still 3 alive engine pods (the gang was recreated,
		// not surged).
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-rp")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(Equal(3),
				"post-bump multi-pod Instance must still have 3 pods (leader + 2 workers)")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
