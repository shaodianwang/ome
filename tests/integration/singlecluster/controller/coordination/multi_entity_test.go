package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

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

	ginkgo.It("MULTI-3 two groups (BlueGreen + Sequential) → independent decisions per ComputeTransition call", func() {
		blueGreenLocalObs := blueGreenObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		for _, c := range blueGreenLocalObs.Group.Components {
			mutate(&blueGreenLocalObs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
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
		trA := coordination.ComputeTransition(blueGreenLocalObs)
		trS := coordination.ComputeTransition(seqObs)
		Expect(trA.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(trS.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		mutate(&blueGreenLocalObs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(blueGreenLocalObs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(coordination.ComputeTransition(seqObs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle),
				"failure in atomic group must not influence sequential group")
	})

	ginkgo.It("MULTI-4 one group paused (PausedGlobal effective for it) + other rolling → both reach independent phases", func() {
		blueGreenLocalObs := blueGreenObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		for _, c := range blueGreenLocalObs.Group.Components {
			mutate(&blueGreenLocalObs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		blueGreenLocalObs.PausedGlobal = true
		Expect(coordination.ComputeTransition(blueGreenLocalObs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))

		indepObs := blueGreenLocalObs
		indepObs.PausedGlobal = false
		indepObs.Group.Policy = v1beta1.CoordinationPolicyIndependent
		Expect(coordination.ComputeTransition(indepObs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"non-paused group should still progress")
	})

	ginkgo.It("MULTI-5 per-ISVC isolation: distinct observations produce independent decisions", func() {
		obsA := blueGreenObs()
		obsB := blueGreenObs()
		mutate(&obsA, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obsA).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(coordination.ComputeTransition(obsB).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.It("MULTI-9 5-group ISVC: each ComputeTransition call returns independently", func() {
		policies := []v1beta1.CoordinationPolicy{
			v1beta1.CoordinationPolicyBlueGreen,
			v1beta1.CoordinationPolicyRollingUpdate,
			v1beta1.CoordinationPolicyIndependent,
			v1beta1.CoordinationPolicySequential,
			v1beta1.CoordinationPolicyBlueGreen,
		}
		for i, p := range policies {
			obs := pausedObs(p)
			obs.Group.Name = string(rune('0' + i))
			tr := coordination.ComputeTransition(obs)
			Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle),
				"group %d (policy=%s) must return Idle for the idle observation", i, p)
		}
	})

	ginkgo.It("MULTI-1 two ISVCs in same namespace with same Component names → Services scoped by inferenceservice label", func() {
		fixA := newCoordFixture(ns.Name, WithISVCName("multi1a"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fixB := newCoordFixture(ns.Name, WithISVCName("multi1b"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fixA)
		applyFixture(Default, fixB)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.CSR) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixB.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixB.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixB.CSR) })

		Eventually(func(g Gomega) {
			services := listPerRevisionServices(g, ns.Name)
			var aFound, bFound bool
			for _, s := range services {
				if isvcLabel := s.Spec.Selector["ome.io/inferenceservice"]; isvcLabel == "multi1a" {
					aFound = true
				} else if isvcLabel == "multi1b" {
					bFound = true
				}
			}
			g.Expect(aFound).To(BeTrue(), "no per-revision Service tagged for multi1a; got %s", describeServices(services))
			g.Expect(bFound).To(BeTrue(), "no per-revision Service tagged for multi1b; got %s", describeServices(services))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("MULTI-2 two ISVCs in different namespaces → no cross-talk", func() {
		nsB, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-multi-b-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, nsB.Name) })

		fixA := newCoordFixture(ns.Name, WithISVCName("multi2"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fixB := newCoordFixture(nsB.Name, WithISVCName("multi2"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		// CBM/CSR are cluster-scoped: reuse the same instances for both ISVCs.
		fixB.CBM = fixA.CBM
		fixB.CSR = fixA.CSR
		applyFixture(Default, fixA)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.CSR) })
		Expect(k8sClient.Create(suiteCtx, fixB.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixB.ISVC) })

		Eventually(func(g Gomega) {
			svcsA := listPerRevisionServices(g, ns.Name)
			svcsB := listPerRevisionServices(g, nsB.Name)
			g.Expect(svcsA).NotTo(BeEmpty(), "ns A should have services")
			g.Expect(svcsB).NotTo(BeEmpty(), "ns B should have services")
			for _, s := range svcsA {
				g.Expect(s.Namespace).To(Equal(ns.Name))
			}
			for _, s := range svcsB {
				g.Expect(s.Namespace).To(Equal(nsB.Name))
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("MULTI-4-E2E one paused + one rolling group — needs per-group pause API in v1beta1", func() {})

	ginkgo.It("MULTI-7 BaseModel deletion mid-rollout → ISVC rollout fails gracefully (coord layer does not crash)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("multi7"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			services := listPerRevisionServices(g, ns.Name)
			g.Expect(services).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, fix.CBM)).To(Succeed())

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "multi7")
			g.Expect(isvc).NotTo(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
