package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Behavior — P/D pairing protocol cohorts.
//
// spec.rollout.pairingProtocol rides every engine/decoder revision: the label
// surface (pods + per-revision routing Services), the routing contract
// (ComponentTrafficTarget.PairingProtocol), the pair-floor gate during a
// protocol transition, and the admission rule denying uncoordinated changes.
var _ = ginkgo.Describe("behavior — pairing protocol", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pairing-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// pairableTrafficPairExists is the consumer-view invariant: whenever both
	// pairing Components publish live traffic targets, some engine target and
	// some decoder target must be pairable — equal protocols, or either empty
	// (empty pairs with anything). An empty Traffic list is a status-lag /
	// availability question, not a pairing statement, and does not fail it.
	pairableTrafficPairExists := func(c *v1beta1.InferenceService) bool {
		eng := c.Status.Components[v1beta1.EngineComponent].Traffic
		dec := c.Status.Components[v1beta1.DecoderComponent].Traffic
		if len(eng) == 0 || len(dec) == 0 {
			return true
		}
		for _, e := range eng {
			if e.Percent <= 0 {
				continue
			}
			for _, d := range dec {
				if d.Percent <= 0 {
					continue
				}
				if e.PairingProtocol == "" || d.PairingProtocol == "" || e.PairingProtocol == d.PairingProtocol {
					return true
				}
			}
		}
		return false
	}

	// assertPairingSurface checks all three protocol surfaces converged on
	// `protocol`: every engine/decoder pod label, every live traffic target,
	// and every routing Service named by a live target.
	assertPairingSurface := func(g Gomega, namespace, isvcName, protocol string) {
		for _, pod := range listOMENativePods(g, namespace, isvcName) {
			comp := pod.Labels[constants.OMEComponentLabel]
			if comp != string(v1beta1.EngineComponent) && comp != string(v1beta1.DecoderComponent) {
				continue
			}
			g.Expect(pod.Labels[query.LabelPairingProtocol]).To(Equal(protocol),
				"pod %s must carry its revision's pairing protocol", pod.Name)
		}
		c := freshISVC(g, namespace, isvcName)
		for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			targets := c.Status.Components[comp].Traffic
			g.Expect(targets).NotTo(BeEmpty(), "%s must publish traffic targets", comp)
			for _, tgt := range targets {
				if tgt.Percent <= 0 {
					continue
				}
				g.Expect(tgt.PairingProtocol).To(Equal(protocol),
					"%s target %s protocol", comp, tgt.RevisionName)
				svc := &corev1.Service{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: tgt.RevisionName}, svc)).
					To(Succeed(), "routing Service %s", tgt.RevisionName)
				g.Expect(svc.Labels[query.LabelPairingProtocol]).To(Equal(protocol),
					"routing Service %s pairing label", tgt.RevisionName)
				g.Expect(svc.Spec.Selector).NotTo(HaveKey(query.LabelPairingProtocol),
					"pairing protocol must never enter a Service selector")
			}
		}
	}

	ginkgo.It("protocol rides pods, Services, and traffic targets; a protocol bump converges pair-alive", func() {
		// Config: 2 engine + 2 decoder in one blueGreen group, protocol v1.
		// Action: bump engine image AND pairing protocol in one update.
		// Invariants: pairable engine+decoder traffic pair at every sample;
		// full convergence onto the new protocol on all three surfaces.
		fix := newCoordFixture(ns.Name, WithISVCName("pairing-bg"),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithPairingProtocol("nixl-v1"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-bg")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The v1 protocol must be visible on every surface before the bump.
		Eventually(func(g Gomega) {
			assertPairingSurface(g, ns.Name, "pairing-bg", "nixl-v1")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Bump image + protocol together — the coordinated blueGreen shape the
		// admission rule steers operators to.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pairing-bg")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Rollout.PairingProtocol = ptr.To("nixl-v2")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pair floor through the early transition: at every sample the
		// consumer view must contain a pairable engine+decoder target pair.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-bg")
			g.Expect(pairableTrafficPairExists(c)).To(BeTrue(),
				"no pairable engine+decoder traffic pair: engine=%v decoder=%v",
				c.Status.Components[v1beta1.EngineComponent].Traffic,
				c.Status.Components[v1beta1.DecoderComponent].Traffic)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Full convergence: both Components fully serving on the new protocol,
		// pair intact, all three surfaces flipped.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-bg")
			g.Expect(pairableTrafficPairExists(c)).To(BeTrue(),
				"pair must stay alive to the end: engine=%v decoder=%v",
				c.Status.Components[v1beta1.EngineComponent].Traffic,
				c.Status.Components[v1beta1.DecoderComponent].Traffic)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			assertPairingSurface(g, ns.Name, "pairing-bg", "nixl-v2")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("asymmetric transition with a single-replica drain-first decoder converges", func() {
		// Config: 2 engines + 1 RecreatePod decoder in one blueGreen group,
		// protocol v1. Action: bump the protocol (both Components re-roll).
		// Invariant: the transition converges. The walk requires the gate's
		// asymmetric ordering: the engine's last old instance is held until
		// the decoder flips, and the single-replica decoder's drain-first hop
		// is admitted once the engine stages target capacity — its recreate
		// gap is the same one it has without pairing.
		fix := newCoordFixture(ns.Name, WithISVCName("pairing-asym"),
			WithEngineReplicas(2),
			WithDecoderReplicas(1),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithPairingProtocol("nixl-v1"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-asym")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			assertPairingSurface(g, ns.Name, "pairing-asym", "nixl-v1")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pairing-asym")
			cur.Spec.Rollout.PairingProtocol = ptr.To("nixl-v2")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Convergence IS the assertion: without the final-hop admission the
		// walk wedges permanently with the decoder still on the old cohort.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-asym")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(1)))
			assertPairingSurface(g, ns.Name, "pairing-asym", "nixl-v2")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("1x1 surge transition keeps the old pair serving until the new cohort serves on both sides", func() {
		// Config: 1 engine + 1 decoder, SurgeThenDrain, one blueGreen group,
		// protocol v1; the engine's containers start 45 s late, so the
		// decoder's replacement is Ready long before the engine's — the shape
		// in which the start-time gate must admit both first movers.
		// Invariants: the decoder's drain is held with gate=Pairing until the
		// engine's replacement serves; a pairable engine+decoder traffic pair
		// exists at every sample; full convergence on v2 with the hold gone.
		fix := newCoordFixture(ns.Name, WithISVCName("pairing-1x1"),
			WithEngineReplicas(1),
			WithDecoderReplicas(1),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithPairingProtocol("nixl-v1"),
			WithEngineImage("fake-serving:v1"),
			WithEngineStartupDelay(45))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-1x1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(1)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			assertPairingSurface(g, ns.Name, "pairing-1x1", "nixl-v1")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pairing-1x1")
			cur.Spec.Rollout.PairingProtocol = ptr.To("nixl-v2")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The decoder's replacement is Ready within seconds; its source may
		// not leave rotation while the only serving engine is still v1, and
		// the hold must say why.
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, "pairing-1x1", v1beta1.DecoderComponent)
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "the decoder drain must be held while the engine serves only v1")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGatePairing))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pair floor across the whole window the engine needs to come up.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-1x1")
			g.Expect(pairableTrafficPairExists(c)).To(BeTrue(),
				"no pairable engine+decoder traffic pair: engine=%v decoder=%v",
				c.Status.Components[v1beta1.EngineComponent].Traffic,
				c.Status.Components[v1beta1.DecoderComponent].Traffic)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pairing-1x1")
			g.Expect(pairableTrafficPairExists(c)).To(BeTrue(),
				"pair must stay alive to the end: engine=%v decoder=%v",
				c.Status.Components[v1beta1.EngineComponent].Traffic,
				c.Status.Components[v1beta1.DecoderComponent].Traffic)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(1)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(1)))
			assertPairingSurface(g, ns.Name, "pairing-1x1", "nixl-v2")
			ir := freshInferenceReplica(g, ns.Name, "pairing-1x1", v1beta1.DecoderComponent)
			if ir.Status.RolloutHold != nil {
				g.Expect(ir.Status.RolloutHold.Gate).NotTo(Equal(v1beta1.RolloutHoldGatePairing),
					"the Pairing hold must clear once the transition converged")
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// The admission rule denying uncoordinated protocol changes is covered by
	// the envtest webhook suite — the KIND environment runs no webhook server.
})
