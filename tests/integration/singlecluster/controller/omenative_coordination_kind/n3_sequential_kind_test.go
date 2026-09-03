package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Sequential coordination policy spanning N=3 Components.
//
// The existing N=2 Sequential spec (sequential_kind)
// asserts decoder → engine. The Sequential state machine has to chain a
// second handoff when N=3: after Component A finishes, Component B
// rolls; after B finishes, Component C rolls. The PD three-way suite
// (pd_three_way_kind) covers 3-Component PD topologies but uses NO
// coordination group, so it doesn't exercise the chained-handoff path.
//
// This spec asserts the chained-handoff invariant against the
// authoritative per-Component CurrentRevision (same shape as the N=2
// post-fix assertion — CompositePhase intentionally NOT consulted
// because it's the same product surface that flakes in the N=2 spec).
var _ = ginkgo.Describe("behavior — Sequential coordination (N=3)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "seq-n3-kcg1-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Sequential N=3 chains handoffs router → decoder → engine across all three", func() {
		// Order matters: router rolls first, then decoder, then engine.
		// Sequential's per-Component pacing is MaxUnavailable=1 so each
		// recreate takes one pod offline at a time.
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("seq-n3-kcg1"),
			WithStartupDelay(10),
			WithRouter(),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.RouterComponent,
					v1beta1.DecoderComponent,
					v1beta1.EngineComponent,
				},
				Policy: v1beta1.CoordinationPolicySequential,
				Order: []v1beta1.ComponentType{
					v1beta1.RouterComponent,
					v1beta1.DecoderComponent,
					v1beta1.EngineComponent,
				},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for all three Components to converge to Idle.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "seq-n3-kcg1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.RouterComponent,
				v1beta1.DecoderComponent,
				v1beta1.EngineComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), "Component %s missing OMENative status", comp)
				g.Expect(cs.Lifecycle.ServingReplicas).To(BeNumerically(">=", 1),
					"Component %s must have at least 1 serving replica before bump", comp)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture the pre-bump revisions for each of the three Components.
		// CurrentRevision is promoted on a later reconcile than the one that
		// flips an Instance to Ready, so poll: a bare read here races the
		// promotion and observes the empty pre-promotion value.
		var v1RouterRev, v1DecoderRev, v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "seq-n3-kcg1")
			g.Expect(preBump.Status.Components[v1beta1.RouterComponent].Lifecycle).NotTo(BeNil())
			v1RouterRev = preBump.Status.Components[v1beta1.RouterComponent].Lifecycle.CurrentRevision
			g.Expect(preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			v1DecoderRev = preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1RouterRev).NotTo(BeEmpty(), "router CurrentRevision must be promoted")
			g.Expect(v1DecoderRev).NotTo(BeEmpty(), "decoder CurrentRevision must be promoted")
			g.Expect(v1EngineRev).NotTo(BeEmpty(), "engine CurrentRevision must be promoted")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sample all three CurrentRevisions across the rollout. The
		// chained-handoff invariant produces two distinct windows we must
		// observe:
		//   (a) router rolled-off-v1 while decoder + engine still on v1
		//       (proves router went FIRST, and decoder/engine did NOT
		//       roll concurrently with it).
		//   (b) decoder rolled-off-v1 while engine still on v1 (proves
		//       decoder went BEFORE engine, and engine did NOT roll
		//       concurrently with decoder).
		// Both windows must exist for the chain to be enforced.
		sawRouterDoneWhileOthersV1 := false
		sawDecoderDoneWhileEngineV1 := false
		var lastRouterRev, lastDecoderRev, lastEngineRev string
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(_ Gomega) {
			// Tolerant Get: skip on any error so a sampler tick racing the
			// ISVC deletion in deferCleanup doesn't fail the spec.
			c := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "seq-n3-kcg1"}, c); err != nil {
				return
			}
			r := c.Status.Components[v1beta1.RouterComponent].Lifecycle
			d := c.Status.Components[v1beta1.DecoderComponent].Lifecycle
			e := c.Status.Components[v1beta1.EngineComponent].Lifecycle
			if r == nil || d == nil || e == nil {
				return
			}
			lastRouterRev, lastDecoderRev, lastEngineRev = r.CurrentRevision, d.CurrentRevision, e.CurrentRevision
			if r.CurrentRevision != v1RouterRev &&
				d.CurrentRevision == v1DecoderRev &&
				e.CurrentRevision == v1EngineRev {
				sawRouterDoneWhileOthersV1 = true
			}
			if d.CurrentRevision != v1DecoderRev && e.CurrentRevision == v1EngineRev {
				sawDecoderDoneWhileEngineV1 = true
			}
		})
		defer stop()

		// Bump all three images simultaneously. Sequential must serialize
		// the rollouts: router first, then decoder, then engine.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "seq-n3-kcg1")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Router.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the full chain to converge: all three CurrentRevisions
		// advanced off v1, group back to Idle.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "seq-n3-kcg1")
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1RouterRev),
				"router CurrentRevision must advance off v1")
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"decoder CurrentRevision must advance off v1")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"chained Sequential rollout must finish and return to Idle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Both handoff windows must have been observed. Either windowMiss
		// means the Sequential chain was not enforced (e.g. router +
		// decoder rolled in parallel, or decoder + engine rolled in
		// parallel).
		Expect(sawRouterDoneWhileOthersV1).To(BeTrue(),
			"never observed router rolled-off-v1 while decoder+engine still on v1 — "+
				"Sequential ordering not enforced at handoff 1 (router → decoder). "+
				"lastRouterRev=%s lastDecoderRev=%s lastEngineRev=%s",
			lastRouterRev, lastDecoderRev, lastEngineRev)
		Expect(sawDecoderDoneWhileEngineV1).To(BeTrue(),
			"never observed decoder rolled-off-v1 while engine still on v1 — "+
				"Sequential ordering not enforced at handoff 2 (decoder → engine). "+
				"lastDecoderRev=%s lastEngineRev=%s",
			lastDecoderRev, lastEngineRev)
	})
})
