package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-13 — Sequential coord with EXPLICIT RecreatePod (not the
// SurgeThenDrain default).
//
// D-2 covers Sequential with default-strategy (which under
// single-pod is effectively recreate via the surge fallback in
// chooseUpdateModeForInstance). This spec pins the explicit
// RecreatePod selection: the strategy field is set to RecreatePod
// directly, not relying on the default. Sequential still serializes
// (decoder rolls before engine).
var _ = ginkgo.Describe("permutation — Sequential + explicit RecreatePod", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-seq-rp-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-13 Sequential + RecreatePod serializes Components and converges", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-seq-rp"),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-rp")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-seq-rp")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision

		// Sample for the decoder-done-while-engine-v1 handoff window.
		sawDecoderDoneWhileEngineV1 := false
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(_ Gomega) {
			c := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "perm-seq-rp"}, c); err != nil {
				return
			}
			dec := c.Status.Components[v1beta1.DecoderComponent].OMENative
			eng := c.Status.Components[v1beta1.EngineComponent].OMENative
			if dec == nil || eng == nil {
				return
			}
			if dec.CurrentRevision != v1DecoderRev && eng.CurrentRevision == v1EngineRev {
				sawDecoderDoneWhileEngineV1 = true
			}
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-rp")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-rp")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision).NotTo(Equal(v1EngineRev))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision).NotTo(Equal(v1DecoderRev))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		Expect(sawDecoderDoneWhileEngineV1).To(BeTrue(),
			"KP-13: never observed decoder rolled-off-v1 while engine still on v1 — Sequential not enforced under explicit RecreatePod")
	})
})
