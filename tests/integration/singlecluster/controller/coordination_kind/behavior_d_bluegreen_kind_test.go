package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test D — BlueGreen convergence.
//
// D-1/D-2/D-3/D-4 stay PIt because they require real surge or
// controller-owned Service selector flips (HTTPRoute weighted cutover
// territory).
var _ = ginkgo.Describe("behavior — BlueGreen", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "bluegreen-d-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PIt: requires real SurgeThenDrain (current code path delegates to recreate).
	ginkgo.PIt("D-1 full surge observed", func() {})
	// PIt: requires real SurgeThenDrain so v1 + v2 can coexist.
	ginkgo.PIt("D-2 v1 drain ordering", func() {})
	// PIt: HTTPRoute weighted cutover owns traffic flips, not a controller-owned Service selector flip.
	ginkgo.PIt("D-3 traffic stays on v1 until all v2 serving", func() {})
	// PIt: same HTTPRoute cutover ownership as D-3.
	ginkgo.PIt("D-4 atomic flip is single-tick observable", func() {})

	ginkgo.It("D-5 rollout converges to all pods on v2", func() {
		// Config: 4 prefill + 2 decode, BlueGreen group.
		// Action: bump engine image v1 -> v2.
		// Invariant: engine CurrentRevision advances off v1, both
		// components return to full serving, group phase is Idle.
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("bluegreen-d5"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "bluegreen-d5")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(4)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "bluegreen-d5")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "bluegreen-d5")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "bluegreen-d5")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"D-5: engine CurrentRevision must advance off v1")
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(4)))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PIt: covered by behavior_x_bad_image X-1 which uses a real bad image tag.
	ginkgo.PIt("F-1 group marked Failed when a v2 pod can't reach Ready", func() {})
	// PIt: HTTPRoute weighted cutover ownership, same as D-3/D-4.
	ginkgo.PIt("F-2 no traffic flip on failure", func() {})
	// PIt: depends on F-1 unlock.
	ginkgo.PIt("F-3 v1 capacity preserved during failure", func() {})
})
