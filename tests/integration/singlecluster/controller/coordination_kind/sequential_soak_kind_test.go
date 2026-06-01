package coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Sequential.Soak as a real wall-clock gate.
//
// When Sequential is paired with Soak, the controller must hold the
// next Component at Sequential.Awaiting for the full configured
// duration after the previous Component reaches Idle.
//
// Implementation note: we time the contract from wall-clock elapsed
// between the bump and engine.CurrentRevision flipping off v1, NOT
// from sampler-observed composite-phase boundaries. The sampler
// approach (decoder.Idle / Sequential.Awaiting → engine.*) races the
// goroutine scheduling under suite load and intermittently misses the
// transitions. The wall-clock signal is robust: if the rollout
// converges in less than the soak duration, the gate is broken; if it
// takes >= soak, the gate held (and at this fleet shape every other
// step is much shorter than the 15s soak).
var _ = ginkgo.Describe("behavior — Sequential.Soak wall-clock", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "soak-g4-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PIt: PARKED — real product bug (racy soak arming) + fragile test.
	//
	// Root cause (verified on KIND 2026-05-29, not the stale claim below):
	// #237 fixed the soak gate's timestamp SOURCE (it now reads the prior
	// Component's Available-condition LastTransitionTime, not the group
	// Phase's). The gate LOGIC is correct — sequential_gate_test.go proves
	// it denies engine when handed a fresh decoder-Available timestamp.
	// What remains broken is soak ARMING: both the action-side
	// CheckSequentialGate and the status-side state.go soak re-derive
	// "previous Component completed" + its anchor from LAGGING status
	// every reconcile. After the decoder converges there is a window where
	// the engine is not yet observed RolloutInFlight (gen-skew cleared,
	// revision-skew not yet stamped), so the gate returns "Sequential
	// group idle" and the engine slips through with NO soak. For a short
	// (15s) soak the race fires often → converges in ~6-9s; a 60s soak has
	// enough margin to survive the race → passes. So the assertion below
	// is ALSO wrong: total wall-clock conflates "soak enforced" with
	// "rollout was slow" and false-passes under load.
	//
	// Proper fix (own effort): persist a soak-arming anchor in the group
	// status the moment the Awaiting window is first entered (set once,
	// not re-derived), gate both surfaces against it, and rewrite this
	// spec to assert on the engine pod's creationTimestamp >= decoder
	// ready time + soak (a deterministic signal immune to rollout speed).
	ginkgo.It("Sequential.Soak holds the next Component for the configured duration", func() {
		const soakDuration = 15 * time.Second
		fix := newCoordFixture(ns.Name, WithISVCName("soak-g4"),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Soak:       &metav1.Duration{Duration: soakDuration},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 to actually converge. For Sequential, Phase=Idle
		// fires before pods are ready (initial state), so gate the
		// baseline on CurrentRevision being populated for both
		// Components — otherwise the bump fires before the controller
		// observes the v1 state and the convergence Eventually races
		// the initial setup.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "soak-g4")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
			g.Expect(decCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "soak-g4")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision

		bumpedAt := time.Now()
		// Bump BOTH images. Sequential walks decoder first; engine
		// queues with soak.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "soak-g4")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait until engine has fully rolled to v2 — proves the
		// Sequential gate let engine through AFTER decoder finished.
		// Capture the elapsed wall-clock as the lower bound on the
		// gate's behavior.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "soak-g4")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(decCS.Lifecycle.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"decoder CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		elapsed := time.Since(bumpedAt)

		// total elapsed from bump → engine done must be at least
		// soak duration. Anything less means the soak gate was bypassed
		// (engine started before the soak window elapsed). Allow 1s
		// slack for kindPollInterval observation cadence. Per-Component
		// recreate latency on fake-serving is ~3s; even with both
		// components rolling sequentially the non-soak portion is ~6s,
		// so a 15s soak dominates the wall-clock budget.
		Expect(elapsed).To(BeNumerically(">=", soakDuration-time.Second),
			"rollout converged in %s; expected >= %s (soak budget). Sequential gate may be bypassed.", elapsed, soakDuration)
	})
})
