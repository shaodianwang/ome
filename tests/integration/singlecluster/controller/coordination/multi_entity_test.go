package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-entity specs for OEP-0007.1.
//
// Covers two-or-more ISVCs in shared namespaces, multi-group single
// ISVCs, and multi-namespace fan-out. The state machine itself is
// per-group so this is largely about how the controller composes
// multiple ComputeTransition calls per reconcile. Most need G1.

var _ = ginkgo.Describe("Coordination multi-entity (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-multi-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs: multi-group composition
	// ----------------------------------------------------------------

	ginkgo.It("MULTI-3 two groups (Atomic + Sequential) → independent decisions per ComputeTransition call", func() {
		// One ISVC with two groups: Atomic{engine, decoder} + Sequential{router}.
		// The state machine is invoked once per group; this codifies that
		// they produce independent outputs from independent observations.
		atomicObs := atomicObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		for _, c := range atomicObs.Group.Components {
			mutate(&atomicObs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		// Sequential group (single Component as standalone).
		seqGroup := coordination.ResolvedGroup{
			Name:       "1",
			Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
			Order:      []v1beta1.ComponentType{v1beta1.RouterComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
		}
		seqObs := coordination.GroupObservation{
			Group: seqGroup,
			Components: map[v1beta1.ComponentType]coordination.ComponentObservation{
				v1beta1.RouterComponent: {
					Component:            v1beta1.RouterComponent,
					DesiredReplicas:      2,
					TotalPods:            2,
					ReadyPods:            2,
					NewRevisionPods:      2,
					NewRevisionReadyPods: 2,
					CurrentRevisionHash:  "rev1",
					TargetRevisionHash:   "rev1",
				},
			},
		}
		trA := coordination.ComputeTransition(atomicObs)
		trS := coordination.ComputeTransition(seqObs)
		Expect(trA.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(trS.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		// Mutating one group's observation does not affect the other.
		mutate(&atomicObs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(atomicObs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(coordination.ComputeTransition(seqObs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle),
				"failure in atomic group must not influence sequential group")
	})

	ginkgo.It("MULTI-4 one group paused (PausedGlobal effective for it) + other rolling → both reach independent phases", func() {
		// Same observation shape as MULTI-3 but one group gets
		// PausedGlobal=true. Both groups are evaluated independently.
		atomicObs := atomicObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		for _, c := range atomicObs.Group.Components {
			mutate(&atomicObs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		atomicObs.PausedGlobal = true
		Expect(coordination.ComputeTransition(atomicObs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))

		// Independent group on the same ISVC (different observation
		// constructed by the controller).
		indepObs := atomicObs // shallow copy; flip what differs
		indepObs.PausedGlobal = false
		indepObs.Group.Policy = v1beta1.CoordinationPolicyIndependent
		Expect(coordination.ComputeTransition(indepObs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"non-paused group should still progress")
	})

	ginkgo.It("MULTI-5 cross-ref: per-ISVC isolation already covered by CON-12 (independent observations)", func() {
		// CON-12 already pins that distinct observations produce
		// independent decisions. This `It` is a documentation anchor
		// so multi-entity reviewers can grep here.
		obsA := atomicObs()
		obsB := atomicObs()
		mutate(&obsA, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obsA).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(coordination.ComputeTransition(obsB).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.It("MULTI-9 5-group ISVC: each ComputeTransition call returns independently", func() {
		// 5 groups with mixed policies — independence guaranteed by
		// the per-call pure-function shape.
		policies := []v1beta1.CoordinationPolicy{
			v1beta1.CoordinationPolicyAtomic,
			v1beta1.CoordinationPolicyPairedMixable,
			v1beta1.CoordinationPolicyIndependent,
			v1beta1.CoordinationPolicySequential,
			v1beta1.CoordinationPolicyAtomic, // duplicate to make 5
		}
		for i, p := range policies {
			obs := pausedObs(p)
			obs.Group.Name = string(rune('0' + i))
			tr := coordination.ComputeTransition(obs)
			// Default observation is Idle for all policies.
			Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle),
				"group %d (policy=%s) must return Idle for the idle observation", i, p)
		}
	})

	// ----------------------------------------------------------------
	// G1-blocked end-to-end PIts
	// ----------------------------------------------------------------

	ginkgo.PIt("G1: MULTI-1 two ISVCs in same namespace with same Component names → Services scoped by inferenceservice label", func() {
		fixA := newCoordFixture(ns.Name, WithISVCName("multi1a"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fixB := newCoordFixture(ns.Name, WithCBMName("multi1b-cbm"), WithCSRName("multi1b-csr"), WithISVCName("multi1b"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fixA)
		applyFixture(Default, fixB)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixB.ISVC) })

		Eventually(func(g Gomega) {
			services := listPerRevisionServices(g, ns.Name)
			g.Expect(services).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MULTI-2 two ISVCs in different namespaces → no cross-talk", func() {
		// Setup: two namespaces with same-named ISVCs. controller-runtime
		// scopes watches to the right namespace; verify no cross-talk
		// via per-revision Service labels.
	})

	ginkgo.PIt("G1: MULTI-4 (E2E) two groups, one paused + other rolling → independent observation in status", func() {
		// Pure form covered above; this PIt is the E2E observation that
		// Status.RolloutCoordination.Groups[0].Phase = Paused and
		// Status.RolloutCoordination.Groups[1].Phase != Paused.
	})

	ginkgo.PIt("G1: MULTI-7 BaseModel deletion mid-rollout → ISVC rollout fails gracefully", func() {
		// CBM deleted while ISVC is rolling. ISVC should surface a Failed
		// condition (via the model-source resolver). Coord layer should
		// not crash.
	})

	// MULTI-6: cross-ref LIFE-8 (namespace delete cascade) — covered there.
	// MULTI-8: cross-ref W-2 (duplicate Component across groups rejected).
	// MULTI-10: covered by namespace delete cascade (kube-native).
})
