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

// Sequential coordination policy.
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

	// The assertion samples the authoritative per-Component CurrentRevision
	// and requires the decoder-done-while-engine-still-v1 window. The
	// Sequential gate (CheckSequentialGate / state.go) derives "which
	// Component is in flight" from status, so the window between the decoder
	// converging and the engine being observed in flight is exactly where
	// the gate must keep holding the engine. A CompositePhase-based or
	// guarded assertion would pass vacuously here (the CompositePhase signal
	// stays "Idle" and never surfaces decoder.*/engine.*), so do not fall
	// back to one.
	ginkgo.It("Sequential rolls components in declared order (decoder → engine)", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("seq-d2"),
			WithStartupDelay(10),
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
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev, v1DecoderRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "seq-d2")
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			v1DecoderRev = preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			g.Expect(v1DecoderRev).NotTo(BeEmpty(),
				"decoder CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sequential's observable invariant: there is a window where the
		// decoder has fully rolled to v2 (its CurrentRevision advanced off
		// v1) while the engine is STILL on v1 — proving decoder finished
		// before engine started. Sample both Components' CurrentRevision
		// across the rollout, starting BEFORE the bump so the window can't
		// be missed.
		//
		// CompositePhase is deliberately NOT the signal: for this
		// single-replica Sequential scenario it never surfaces the
		// decoder.*/engine.* sub-phases — it stays "Idle" the entire
		// rollout, so an assertion guarded on it would pass vacuously,
		// never checking the contract. This spec asserts the ordering off
		// the authoritative per-Component CurrentRevision instead.
		sawDecoderDoneWhileEngineV1 := false
		var lastDecRev, lastEngRev string
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(_ Gomega) {
			// Tolerant Get: a sampler tick can race the ISVC deletion if
			// stop() is called concurrently with a Get in flight. Skip on
			// any error rather than using freshISVC, which Expect-Succeeds
			// and would fail the spec's DeferCleanup on a not-found ISVC.
			c := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "seq-d2"}, c); err != nil {
				return
			}
			dec := c.Status.Components[v1beta1.DecoderComponent].Lifecycle
			eng := c.Status.Components[v1beta1.EngineComponent].Lifecycle
			if dec == nil || eng == nil {
				return
			}
			lastDecRev, lastEngRev = dec.CurrentRevision, eng.CurrentRevision
			if dec.CurrentRevision != v1DecoderRev && eng.CurrentRevision == v1EngineRev {
				sawDecoderDoneWhileEngineV1 = true
			}
		})
		defer stop()

		// Bump BOTH images. Sequential should serialize: decoder first,
		// engine queues until decoder finishes.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "seq-d2")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for full convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "seq-d2")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"decoder CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Assert Sequential ordering: we must have observed the decoder
		// fully rolled to v2 while the engine was still on v1. If the two
		// Components rolled concurrently (Sequential gate broken) this
		// window would not exist — both CurrentRevisions would advance
		// together. A missed observation is a real failure, never a
		// silent skip.
		Expect(sawDecoderDoneWhileEngineV1).To(BeTrue(),
			"never observed decoder rolled-off-v1 while engine still on v1 — "+
				"Sequential ordering not enforced (Components rolled concurrently). "+
				"lastDecoderRev=%s lastEngineRev=%s", lastDecRev, lastEngRev)
	})
})
