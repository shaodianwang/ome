package coordination_kind

import (
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// D-2 — Sequential coordination policy.
//
// Bumps BOTH images simultaneously. Sequential must roll them in
// declared Order (decoder first, then engine here). Real kubelet's
// seconds-long rollout window makes the active-Component window
// observable.
var _ = ginkgo.Describe("behavior — Sequential coordination", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "seq-d2-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("D-2 Sequential rolls components in declared order (decoder → engine)", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("seq-d2"),
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
			c := freshISVC(g, ns.Name, "seq-d2")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "seq-d2")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision

		// Bump BOTH images. Sequential should serialize: decoder first,
		// engine queues until decoder finishes.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "seq-d2")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture CompositePhase samples during the rollout. Sequential's
		// invariant: decoder.* phases appear BEFORE engine.* phases.
		// Sampler runs for the whole rollout window (~30s on a small
		// fleet under real kubelet).
		var observedComposite []string
		stop := util.StartSampler(suiteCtx, 200*time.Millisecond, func(g Gomega) {
			c := freshISVC(g, ns.Name, "seq-d2")
			if c.Status.RolloutCoordination == nil || len(c.Status.RolloutCoordination.Groups) == 0 {
				return
			}
			cp := c.Status.RolloutCoordination.Groups[0].CompositePhase
			if cp == "" {
				return
			}
			// Dedupe consecutive duplicates so the trace stays scannable.
			if n := len(observedComposite); n == 0 || observedComposite[n-1] != cp {
				observedComposite = append(observedComposite, cp)
			}
		})
		defer stop()

		// Wait for full convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "seq-d2")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"D-2: engine CurrentRevision must advance off v1")
			g.Expect(decCS.OMENative.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"D-2: decoder CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Assert Sequential order: every observed decoder.* CompositePhase
		// came before every observed engine.* CompositePhase.
		decoderActive := -1
		engineActive := -1
		for i, cp := range observedComposite {
			if strings.HasPrefix(cp, "decoder.") {
				decoderActive = i
			}
			if strings.HasPrefix(cp, "engine.") && engineActive == -1 {
				engineActive = i
			}
		}
		if decoderActive >= 0 && engineActive >= 0 {
			Expect(decoderActive).To(BeNumerically("<", engineActive),
				"D-2: decoder.* phases must come before engine.* phases; observed trace=%v", observedComposite)
		}
	})
})
