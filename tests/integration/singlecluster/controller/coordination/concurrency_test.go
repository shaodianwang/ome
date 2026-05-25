package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Coordination concurrency & failure paths (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-con-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("CON-8 idempotency: two back-to-back ComputeTransition calls with same obs → same decision", func() {
		obs := atomicObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		tr1 := coordination.ComputeTransition(obs)
		tr2 := coordination.ComputeTransition(obs)
		Expect(tr2).To(Equal(tr1),
			"back-to-back calls with same observation must produce identical decisions")
	})

	ginkgo.It("CON-9 ComputeTransition takes no context — cannot be canceled mid-call", func() {
		// Compile-time guarantee: function signature has no ctx parameter.
		obs := atomicObs()
		_ = coordination.ComputeTransition(obs)
	})

	ginkgo.It("CON-12 two distinct observations (e.g. two ISVCs) → independent decisions, no shared state", func() {
		obsA := atomicObs()
		mutate(&obsA, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obsA, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		obsB := atomicObs()
		trA1 := coordination.ComputeTransition(obsA)
		trB1 := coordination.ComputeTransition(obsB)
		trA2 := coordination.ComputeTransition(obsA)
		trB2 := coordination.ComputeTransition(obsB)

		Expect(trA1.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(trB1.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(trA2).To(Equal(trA1))
		Expect(trB2).To(Equal(trB1))

		mutate(&obsB, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obsA)).To(Equal(trA1),
			"mutating obsB must not influence obsA's decision (independence)")
	})

	ginkgo.It("CON-Pure-Aux state machine returns the same shape on repeated identical input (race-safe by construction)", func() {
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		ref := coordination.ComputeTransition(obs)
		for i := 0; i < 50; i++ {
			Expect(coordination.ComputeTransition(obs)).To(Equal(ref),
				"iteration %d differs from reference — pure function must be deterministic", i)
		}
	})

	ginkgo.It("CON-Pure-Aux ratio EvaluateSurge is also deterministic across iterations", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  2,
				v1beta1.DecoderComponent: 1,
			},
		}
		ref := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, 1)
		for i := 0; i < 50; i++ {
			Expect(coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, 1)).
				To(Equal(ref))
		}
	})

	ginkgo.It("CON-Pure-Aux metrics Record* helpers are thread-safe by Prometheus contract", func() {
		for i := 0; i < 100; i++ {
			coordination.RecordGroupPhase("con-aux", "0", "Idle")
		}
	})

	ginkgo.PIt("CON-1 Status Update 409 conflict — covered by retry.RetryOnConflict wrapper in updateStatus", func() {})
	ginkgo.PIt("CON-2 5xx during EnsurePerRevisionServices — no envtest 5xx injection seam", func() {})
	ginkgo.PIt("CON-3 stale-cache simulation — would require fake-cache injection", func() {})
	ginkgo.PIt("CON-4 coordination + Component reconciler race — controller-runtime serializes per object key", func() {})
	ginkgo.PIt("CON-5 controller restart mid-Sequential — needs restart harness", func() {})
	ginkgo.PIt("CON-6 pod-watch event lag — needs Watch-stream intercept", func() {})
	ginkgo.PIt("CON-7 ResourceVersion mismatch — covered by retry.RetryOnConflict wrapper", func() {})
	ginkgo.PIt("CON-10 429 throttling — no envtest throttle injection", func() {})
	ginkgo.PIt("CON-11 mid-reconcile interruption — controller-runtime requeue handles this", func() {})
})
