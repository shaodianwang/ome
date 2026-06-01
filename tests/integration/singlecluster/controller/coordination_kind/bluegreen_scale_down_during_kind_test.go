package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// scale-down engine MinReplicas from 4 → 2 during a BlueGreen
// rollout.
//
// Symmetric to the scale-up-under-Sequential spec
// (sequential_scale_during_kind) but exercises the scale-down code path
// under BlueGreen pacing. The operator pushes a
// single spec change that both (a) drops engine MinReplicas and (b)
// bumps the image. The reconciler must converge to 2 engine pods on
// v2 — not leave orphan v1 pods around from the pre-scale-down
// baseline, and not refuse to delete v1 pods because the BlueGreen
// gate hasn't observed v2 surge.
var _ = ginkgo.Describe("permutation — BlueGreen + scale-down during rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-bg-scaledown-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("BlueGreen converges to 2 engine pods on v2 after a combined scale-down + bump", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-bg-scaledown"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-bg-scaledown")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(4)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-bg-scaledown")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Combined update: scale engine 4 → 2 + bump image. BlueGreen
		// has to (a) reach v2 on the (smaller) target replica count and
		// (b) drain the surplus v1 pods.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-bg-scaledown")
			two := 2
			cur.Spec.Engine.MinReplicas = &two
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-bg-scaledown")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)),
				"engine must converge to 2 serving replicas on v2 (not stuck at 4)")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pod-level invariant: only 2 alive engine pods (no v1 orphans).
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-bg-scaledown")
		Expect(err).NotTo(HaveOccurred())
		alive := 0
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			if p.DeletionTimestamp != nil {
				continue
			}
			alive++
		}
		Expect(alive).To(Equal(2),
			"expected exactly 2 alive engine pods post-converge, got %d (excess pods = v1 stragglers)", alive)
	})
})
