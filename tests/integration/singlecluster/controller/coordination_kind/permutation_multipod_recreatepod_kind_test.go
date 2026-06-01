package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-11 — Multi-pod (Worker.Size=2) Instance, explicit RecreatePod.
//
// Pins the gang-recreate path for OMENative multi-pod Instances: a
// single Instance owns 1 leader + 2 worker pods (3 pods total). On
// image bump with RecreatePod strategy the rollout must:
//   - Bump the leader's image to v2.
//   - Bump the workers' images to v2.
//   - Keep the same Instance count (MinReplicas=1).
//   - Group converges back to Idle.
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

	ginkgo.It("KP-11 multi-pod engine (leader + 2 workers) recreates on image bump", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-mp-rp"),
			WithoutDecoder(),
			WithEngineWorkerSize(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
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
				"KP-11: single Instance (MinReplicas=1) under multi-pod")
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady),
					"KP-11: multi-pod Instance must reach Ready")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-rp")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(Equal(3),
				"KP-11: multi-pod with Worker.Size=2 must produce 3 pods/Instance (1 leader + 2 workers)")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-mp-rp")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		v1Incarnation := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].Incarnation

		// Bump engine image. Multi-pod recreate must rebuild the gang
		// (Incarnation bumps).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-mp-rp")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-rp")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"KP-11: engine CurrentRevision must advance off v1")
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady),
					"KP-11: multi-pod Instance must return to Ready on v2")
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Incarnation).
				To(BeNumerically(">", v1Incarnation),
					"KP-11: RecreatePod must bump Incarnation (gang rebuilt)")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Post-bump: still 3 alive engine pods (the gang was recreated,
		// not surged).
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-rp")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(Equal(3),
				"KP-11: post-bump multi-pod Instance must still have 3 pods (leader + 2 workers)")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
