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

	// PIt: PARKED — the assertion below is now ROBUST (it samples the
	// authoritative per-Component CurrentRevision and requires the
	// decoder-done-while-engine-still-v1 window), but it exposes a REAL
	// product bug: Sequential ordering is not reliably enforced. ~1 in 3
	// runs the engine rolls CONCURRENTLY with the decoder (no intermediate
	// window; both CurrentRevisions advance together). This is the SAME
	// root cause as G-4's soak bypass (behavior_g_sequential_soak): the
	// Sequential gate (CheckSequentialGate / state.go) derives "which
	// Component is in flight" from LAGGING status, so after the decoder
	// converges there is a window where the engine is not yet observed
	// in-flight and the gate returns "group idle", letting the engine roll
	// early. The original spec passed vacuously (its CompositePhase signal
	// never surfaced decoder.*/engine.* — it stays "Idle"), hiding this.
	// Un-park once the Sequential-gate reliability fix lands (shared with
	// G-4). DO NOT revert to a CompositePhase-based or guarded assertion.
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
		// rollout, which is exactly why the original guarded assertion
		// (`if decoderActive >= 0 && engineActive >= 0`) passed vacuously,
		// never checking the contract. The CompositePhase-surfacing gap is
		// a separate follow-up; this spec asserts the ordering off the
		// authoritative per-Component CurrentRevision instead.
		sawDecoderDoneWhileEngineV1 := false
		var lastDecRev, lastEngRev string
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(_ Gomega) {
			// Tolerant Get: a late sampler tick can race the ISVC deletion
			// during teardown (StartSampler.stop() has a 2*interval timeout
			// escape). Skip on any error rather than using freshISVC, which
			// Expect-Succeeds and would fail the spec's DeferCleanup on a
			// not-found ISVC.
			c := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "seq-d2"}, c); err != nil {
				return
			}
			dec := c.Status.Components[v1beta1.DecoderComponent].OMENative
			eng := c.Status.Components[v1beta1.EngineComponent].OMENative
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
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"D-2: engine CurrentRevision must advance off v1")
			g.Expect(decCS.OMENative.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"D-2: decoder CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Assert Sequential ordering: we must have observed the decoder
		// fully rolled to v2 while the engine was still on v1. If the two
		// Components rolled concurrently (Sequential gate broken) this
		// window would not exist — both CurrentRevisions would advance
		// together. A missed observation is now a real failure, not the
		// silent skip the original guarded assertion allowed.
		Expect(sawDecoderDoneWhileEngineV1).To(BeTrue(),
			"D-2: never observed decoder rolled-off-v1 while engine still on v1 — "+
				"Sequential ordering not enforced (Components rolled concurrently). "+
				"lastDecoderRev=%s lastEngineRev=%s", lastDecRev, lastEngRev)
	})
})
