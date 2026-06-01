package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-9 — scale-up engine MinReplicas from 2 → 4 during an in-flight
// Sequential rollout.
//
// The operator combines a "scale up + bump image" workflow in a
// single spec update. The Sequential gate serializes (decoder rolls
// before engine). When the engine's turn comes, it has to (a) reach
// the new replica count AND (b) be on the new revision. The
// invariant: the rollout converges to 4 engine pods on v2 without
// the gate refusing to advance forever.
//
// This pins the "scale during rollout" regression class — earlier
// implementations could deadlock the engine in scale-up because the
// gate was looking at the old replica count.
var _ = ginkgo.Describe("permutation — Sequential + scale-up during rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-seq-scaleup-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-9 Sequential converges when engine scales 2 → 4 mid-rollout", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-seq-scaleup"),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
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
			c := freshISVC(g, ns.Name, "perm-seq-scaleup")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].OMENative.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].OMENative.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-seq-scaleup")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision

		// Combined update: scale engine 2 → 4 + bump both images. The
		// Sequential gate must roll decoder first, then engine, and the
		// engine roll has to land on 4 replicas on v2.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-scaleup")
			four := 4
			cur.Spec.Engine.MinReplicas = &four
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Final convergence: 4 engine pods on v2, 2 decoder pods on v2,
		// group Idle. Use long timeout since scale-up + rollout doubles
		// the pod-lifecycle work.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-scaleup")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"KP-9: engine CurrentRevision must advance off v1")
			g.Expect(decCS.OMENative.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"KP-9: decoder CurrentRevision must advance off v1")
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(4)),
				"KP-9: engine must scale to 4 serving replicas on v2")
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"KP-9: rollout must return to Idle after combined scale + bump")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
