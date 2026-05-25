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
		obs := atomicObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		delete(obs.Components, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))
	})

	ginkgo.It("UH-24 Atomic Failed + PausedGlobal in same reconcile → Paused wins (outer beats inner)", func() {
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Paused = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))
	})

	ginkgo.It("UH-18 operator-driven Sequential.Failed recovery (caller clears Failed → next call returns Idle)", func() {
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

	ginkgo.PIt("UH-23 zero-pod Service test — GC sweeps before assertion window can observe it", func() {})
	ginkgo.PIt("UH-1 corrupted CR.Data — requires bypassing controller-runtime's deep-copy guard", func() {})
	ginkgo.PIt("UH-2 orphan pods (CR deleted) → Traffic includes orphan hash — correct behavior, no additional invariant", func() {})
	ginkgo.PIt("UH-3 finalizer-blocked delete — K8s-native finalizer semantic", func() {})
	ginkgo.PIt("UH-4 foreign OwnerReference on Service — coord intentionally overwrites; not a bug surface", func() {})
	ginkgo.PIt("UH-5 user template revision-hash label override — label injection happens in renderer, not coord", func() {})
	ginkgo.PIt("UH-6 hand-stamped OMENative-labeled pods — adoption logic belongs to OMENative reconciler", func() {})
	ginkgo.PIt("UH-7 pod with GC'd-CR hash — GC sweep removes the inconsistency next reconcile", func() {})
	ginkgo.PIt("UH-8 CR retention < pod count — CR retention is OEP-0014's concern", func() {})
	ginkgo.PIt("UH-9 webhook-down bypass — deleting shared webhook config breaks every other spec", func() {})
	ginkgo.PIt("UH-10 defaulter-down → empty pacing — same shared-webhook problem as UH-9", func() {})
	ginkgo.PIt("UH-25 phase oscillation stress test — belongs in a benchmark suite", func() {})
})
