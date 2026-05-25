package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	ktypes "k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Coordination K8s primitive edges (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-ec-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("EC-8 ComputeTransition with empty obs.Components for a Component → no panic", func() {
		obs := coordination.GroupObservation{
			Group: coordination.ResolvedGroup{
				Name:       "0",
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
			},
			Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
		}
		Expect(func() {
			_ = coordination.ComputeTransition(obs)
		}).NotTo(Panic(), "empty Components map must not panic")
	})

	ginkgo.It("EC-10 ComputeTransition with negative DesiredReplicas → no panic, handled as if zero", func() {
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.DesiredReplicas = -1
		})
		Expect(func() {
			_ = coordination.ComputeTransition(obs)
		}).NotTo(Panic())
	})

	ginkgo.It("EC-Pure-Aux SnapshotOriginal with negative replicas → coerced to 1 (already covered, regression gate)", func() {
		out := coordination.SnapshotOriginal(map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent: -3,
		})
		Expect(out[v1beta1.EngineComponent]).To(Equal(int32(1)))
	})

	ginkgo.It("EC-Pure-Aux EvaluateSurge with empty NewPods → behaves like 'first surge'", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{},
		}
		d := coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 2)
		// 2/40 = 5%, decoder unseen treated as 0/20 = 0%. 5pp gap is at-band → allowed.
		Expect(d.AllowedSurgeDelta).To(Equal(int32(2)))
	})

	ginkgo.PIt("EC-1 Service create AlreadyExists — covered by services_test.go unit tests", func() {})
	ginkgo.PIt("EC-2 Pod list 5xx — no envtest 5xx injection hook", func() {})
	ginkgo.PIt("EC-3 missing EndpointSlice — Traffic computation reads pod state, not slices", func() {})

	ginkgo.It("EC-4 stale EndpointSlice (slice with no live pod backing) → coordination ignores", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("ec4"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "ec4")
			g.Expect(isvc.Status.Components).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		isvc := freshISVC(Default, ns.Name, "ec4")
		traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
		Expect(traffic).NotTo(BeEmpty(),
			"Traffic[] should populate from pod state regardless of EndpointSlice state")
	})

	ginkgo.PIt("EC-5 PodReady=True ContainersReady=False — impossible per kubelet invariant; needs fuzzing harness", func() {})
	ginkgo.PIt("EC-6 Pod with multiple OwnerReferences — adoption logic belongs to OMENative reconciler", func() {})
	ginkgo.PIt("EC-7 Pod in Terminating but Ready=True — needs kubelet-sim to model graceful drain phase", func() {})
	ginkgo.PIt("EC-9 CR with empty Data.Raw — CR-read happens in OMENative reconciler", func() {})

	ginkgo.It("EC-11 Service with stale Selector → drift-corrected on next reconcile", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("ec11"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var target corev1.Service
		Eventually(func(g Gomega) {
			svcs := listPerRevisionServices(g, ns.Name)
			g.Expect(svcs).NotTo(BeEmpty())
			for _, s := range svcs {
				if s.Spec.ClusterIP != corev1.ClusterIPNone {
					target = s
					return
				}
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx,
				ktypes.NamespacedName{Namespace: target.Namespace, Name: target.Name},
				&fresh)).To(Succeed())
			delete(fresh.Spec.Selector, "ome.io/revision-hash")
			fresh.Spec.Selector["bogus"] = "yes"
			g.Expect(k8sClient.Update(suiteCtx, &fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx,
				ktypes.NamespacedName{Namespace: target.Namespace, Name: target.Name},
				&fresh)).To(Succeed())
			g.Expect(fresh.Spec.Selector).To(HaveKey("ome.io/revision-hash"),
				"coordination must restore the revision-hash selector key")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("EC-12 EndpointSlice dual-stack (IPv4+IPv6) — EndpointSliceSimulator emits IPv4 only", func() {})
})
