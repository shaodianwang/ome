package coordination

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Unhappy-path specs for OEP-0007.1.
//
// Mix of pure-function tests (nil-arg defenses, degenerate
// observations, precedence ordering) and end-to-end PIts for cases
// that require the full reconciler running or external state we can't
// inject through the public surface.

var _ = ginkgo.Describe("Coordination unhappy paths (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-uh-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function defenses against bad inputs
	// ----------------------------------------------------------------

	ginkgo.It("UH-11 coordination.Reconcile with nil ISVC → error (not panic)", func() {
		ctx := context.Background()
		_, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
			ISVC:   nil,
			Client: k8sClient,
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nil ISVC"))
	})

	ginkgo.It("UH-12 coordination.Reconcile with nil Client → error (not panic)", func() {
		ctx := context.Background()
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "x"},
		}
		_, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
			ISVC:   isvc,
			Client: nil,
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nil client"))
	})

	ginkgo.It("UH-13 coordination.Reconcile with no rolloutCoordination block → graceful no-op", func() {
		// Opt-in semantics: nil/empty rolloutCoordination short-circuits
		// to a clean no-op Result. Defensive contract that existing
		// ISVCs (pre-OEP-0007.1) see zero coord behavior.
		ctx := context.Background()
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "uh13"},
		}
		result, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
			ISVC:   isvc,
			Client: k8sClient,
			Reader: k8sClient,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeNil())
		Expect(result.GroupStatuses).To(BeEmpty())
		Expect(result.PerRevisionServicesEnsured).To(BeZero())
	})

	ginkgo.It("UH-15 zero non-zero replicas on every Component → degenerate Idle", func() {
		// ComputeTransition treats DesiredReplicas=0 like any other
		// count — the state machine reads RolloutInFlight, NewRevisionPods,
		// etc. With zero pods + no rollout in flight, it's Idle.
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.DesiredReplicas = 0
				o.TotalPods = 0
				o.ReadyPods = 0
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.It("UH-16 first-reconcile observation (no pods yet) → Idle, not Surging", func() {
		// First reconcile sees an ISVC with no pods yet because the
		// Component reconciler hasn't created them. RolloutInFlight=false
		// → Idle. This codifies the bootstrap case so a future state
		// machine change doesn't accidentally Surge on bare observations.
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.TotalPods = 0
				o.ReadyPods = 0
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.It("UH-19 Atomic: 1 Component Failed + 1 Component Paused → Failed wins", func() {
		// atomicTransition order: Failed check FIRST, then Paused. So
		// when both are present on different Components, Failed wins.
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.Paused = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed),
				"Atomic precedence: Failed (any) > Paused (any)")
	})

	ginkgo.It("UH-20 PausedGlobal + per-Component Failed → PausedGlobal wins (outer short-circuit)", func() {
		// PausedGlobal sits at the OUTER ComputeTransition switch,
		// short-circuiting BEFORE the policy branch runs. So even
		// Failed Components return Paused when PausedGlobal=true.
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"PausedGlobal outer short-circuit beats inner Failed")
	})

	ginkgo.It("UH-21 Component declared in spec but missing from observation → state machine handles gracefully", func() {
		// Atomic with engine+decoder declared; only decoder observed.
		// The loops iterate over obs.Components (which only has
		// decoder) — they don't deref the missing engine.
		obs := atomicObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		// Drop engine from observations to simulate the "not yet
		// observed" case.
		delete(obs.Components, v1beta1.EngineComponent)
		// Decoder rolling.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		// allHaveSurge over only-decoder: NewRevisionPods==0 →
		// allHaveSurge=false → Surging. State machine doesn't panic
		// on missing entries.
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))
	})

	ginkgo.It("UH-24 Atomic Failed + PausedGlobal in same reconcile → Paused wins (outer beats inner)", func() {
		// Same as UH-20; explicit codification of "outer-switch precedence
		// over policy-internal precedence" so the order isn't lost in
		// future refactors.
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Paused = true
		})
		// Without PausedGlobal: Failed (atomicTransition checks Failed first).
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		// With PausedGlobal: outer short-circuit returns Paused.
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))
	})

	// ----------------------------------------------------------------
	// Pre-existing cross-refs (no new tests; named here for traceability)
	// ----------------------------------------------------------------

	// UH-17: rolloutCoordination with no Spec.<Component> → admission
	//         OrphanCoordinationGroup (already covered by W-22).
	// UH-22: All Components RawDeployment → admission CoordinationRequiresOMENative
	//         (already covered by W-4).

	// ----------------------------------------------------------------
	// G8-blocked PIt (Sequential failure recovery — same as S-9)
	// ----------------------------------------------------------------

	ginkgo.It("UH-18 operator-driven Sequential.Failed recovery is the stateless caller-clear pattern (cross-ref S-9, closes G8)", func() {
		// G8 closed: sequentialTransition has no sticky Failed state.
		// When the caller clears Component.Failed (typically after
		// observing a spec revert), the next ComputeTransition falls
		// through to Idle. This is the same proof as S-9 from the UH
		// perspective.
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Failed = false
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	// ----------------------------------------------------------------
	// G4-blocked PIt
	// ----------------------------------------------------------------

	ginkgo.PIt("G4: UH-23 per-revision Service for hash with zero pods → exists today; should be GC'd", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("uh23"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Today: per-revision Services are created for every observed
		// hash, never GC'd. After G4 lands, Services for hashes with
		// zero live pods should be deleted.
		Eventually(func(g Gomega) {
			svcs := listPerRevisionServices(g, ns.Name)
			g.Expect(svcs).To(BeEmpty(),
				"per-revision Service for a hash with no live pods must be GC'd")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// ----------------------------------------------------------------
	// G1-blocked / external-state PIts
	// ----------------------------------------------------------------

	ginkgo.PIt("G1: UH-1 corrupted ControllerRevision Data → Services alive, pods unrendered", func() {
		// Requires hand-crafting a ControllerRevision with malformed
		// Data field. controller-runtime's deep-copy guards against
		// most of this in production; out-of-scope without G1.
	})

	ginkgo.PIt("G1: UH-2 orphan pods (CR deleted) → Traffic includes orphan hash", func() {
		// Pre-G4 expected behavior. Tracking with UH-23 / SVC-12.
	})

	ginkgo.PIt("G1: UH-3 ISVC with finalizer → cascade-delete blocked → Services pile up", func() {
		// Finalizer-blocking + GC is OEP-0007's territory; coordination
		// just inherits the lifecycle. PIt'd until G4 closes the leak.
	})

	ginkgo.PIt("G1: UH-4 per-revision Service with foreign OwnerReference → coordination drift-corrects", func() {
		// Need a manually-created Service with a foreign owner ref.
		// Coordination's EnsurePerRevisionServices should not overwrite
		// foreign owners — pin once G1 lands.
	})

	ginkgo.PIt("G1: UH-5 user template stamps ome.io/revision-hash → coordination overwrites it", func() {
		// Pod labels are owned by coordination; user-set value should
		// be replaced. Requires the renderer to actually run (G1).
	})

	ginkgo.PIt("G1: UH-6 hand-stamped OMENative-labeled pods → coordination treats as native", func() {
		// Adoption semantics: any pod with the OMENative label set is
		// claimed by the controller. Requires E2E observability.
	})

	ginkgo.PIt("G1: UH-7 pod with hash referencing GC'd CR → traffic+Service inconsistency", func() {
		// Pre-G4 inconsistency. Tracking with UH-23.
	})

	ginkgo.PIt("G1: UH-8 CR retention < pod count → some pods at GC'd CR", func() {
		// Same as UH-7. Pre-G4 + needs OEP-0014 retention policy
		// settings exposed via the API.
	})

	ginkgo.PIt("UH-9 webhook down → ISVC creation succeeds without validation (bypass)", func() {
		// Requires deleting the ValidatingWebhookConfiguration mid-test
		// and re-creating it. Possible in envtest but flaky; PIt for now.
	})

	ginkgo.PIt("UH-10 defaulter down → group has no MaxSurge → state machine reads empty pacing", func() {
		// Similar to UH-9 — mutate webhook config mid-test. PIt for now.
	})

	ginkgo.PIt("G1: UH-25 phase oscillation (rapid bump-revert-bump) → events fire, no infinite loop", func() {
		// Requires E2E observation of event stream + status churn under
		// stress. Flaky in envtest; PIt'd.
	})
})
