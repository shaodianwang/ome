package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// PerComponent pacing specs for OEP-0007.1.
//
// PerComponent pacing applies per-Component MaxSurge / MaxUnavailable
// budgets independently. The conversion lives in coordination's
// MaxSurgeBudget and MaxUnavailableBudget — pure functions over the
// pacing spec + Component replica count. Most PC-* cases run as pure
// `It`s. End-to-end PIts are gated on G7 (state machine doesn't
// invoke the budget helpers before deciding how big a surge to do).
//
// Reference: pkg/.../coordination/status.go MaxSurgeBudget /
// MaxUnavailableBudget / surgeFromPercent / strPercent.

func surgePacing(maxSurge *intstr.IntOrString, maxUnavailable *intstr.IntOrString) v1beta1.CoordinationPacing {
	return v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingPerComponent,
		MaxSurge:       maxSurge,
		MaxUnavailable: maxUnavailable,
	}
}

func iosInt(n int) *intstr.IntOrString {
	v := intstr.FromInt(n)
	return &v
}

func iosString(s string) *intstr.IntOrString {
	v := intstr.FromString(s)
	return &v
}

var _ = ginkgo.Describe("Coordination PerComponent pacing (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-pc-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs against MaxSurgeBudget / MaxUnavailableBudget
	// ----------------------------------------------------------------

	ginkgo.It("PC-1 MaxSurge unset → defaults to 25% (ceil) for the Component's replicas", func() {
		// nil MaxSurge → 25%. 4 replicas → ceil(4 * 25 / 100) = 1.
		p := surgePacing(nil, nil)
		Expect(coordination.MaxSurgeBudget(p, 4)).To(Equal(int32(1)))
		// 10 replicas → ceil(10 * 25 / 100) = 3 (rounds up from 2.5).
		Expect(coordination.MaxSurgeBudget(p, 10)).To(Equal(int32(3)))
	})

	ginkgo.It("PC-2 MaxSurge=2 (int) → passes through unchanged regardless of replicas", func() {
		p := surgePacing(iosInt(2), nil)
		Expect(coordination.MaxSurgeBudget(p, 4)).To(Equal(int32(2)))
		Expect(coordination.MaxSurgeBudget(p, 100)).To(Equal(int32(2)),
			"int MaxSurge is an absolute count, not a percentage")
	})

	ginkgo.It("PC-3 MaxSurge=0 → no surge ever allowed", func() {
		p := surgePacing(iosInt(0), nil)
		Expect(coordination.MaxSurgeBudget(p, 100)).To(BeZero())
		// Also via string form.
		pStr := surgePacing(iosString("0%"), nil)
		Expect(coordination.MaxSurgeBudget(pStr, 100)).To(BeZero())
	})

	ginkgo.It("PC-4 MaxUnavailable=1 → absolute count returned independent of replicas", func() {
		p := surgePacing(nil, iosInt(1))
		Expect(coordination.MaxUnavailableBudget(p, 4)).To(Equal(int32(1)))
		Expect(coordination.MaxUnavailableBudget(p, 100)).To(Equal(int32(1)))
	})

	ginkgo.It("PC-5 MaxSurge=50% + MaxUnavailable=25% combined on the same pacing block", func() {
		p := surgePacing(iosString("50%"), iosString("25%"))
		// 4 replicas: surge = ceil(4*50/100) = 2, unavail = ceil(4*25/100) = 1.
		Expect(coordination.MaxSurgeBudget(p, 4)).To(Equal(int32(2)))
		Expect(coordination.MaxUnavailableBudget(p, 4)).To(Equal(int32(1)))
	})

	ginkgo.It("PC-6 fractional percent rounds up: 25% of 5 = ceil(1.25) = 2", func() {
		p := surgePacing(iosString("25%"), nil)
		Expect(coordination.MaxSurgeBudget(p, 5)).To(Equal(int32(2)),
			"surge math uses ceil so fractional results round up to nearest pod")
		// Boundary: 25% of 4 = exactly 1 (no rounding).
		Expect(coordination.MaxSurgeBudget(p, 4)).To(Equal(int32(1)))
	})

	ginkgo.It("PC-7 MaxSurge > 100% behavior: string clamps to 100%; int is taken as absolute count", func() {
		// String form: 200% is clamped to 100% in surgeFromPercent.
		// 4 replicas → 4 (full replacement).
		pStr := surgePacing(iosString("200%"), nil)
		Expect(coordination.MaxSurgeBudget(pStr, 4)).To(Equal(int32(4)),
			"string %% > 100 clamps to 100%% (returns full replicas)")
		// Int form: 200 is taken as an absolute count, no clamp.
		pInt := surgePacing(iosInt(200), nil)
		Expect(coordination.MaxSurgeBudget(pInt, 4)).To(Equal(int32(200)),
			"int MaxSurge is absolute — no clamp applied (caller's responsibility)")
	})

	ginkgo.It("PC-8 PerComponent budgets apply independently per Component (called with each replica count)", func() {
		// Same pacing block; different replica counts yield different
		// absolute surge budgets. Caller invokes MaxSurgeBudget once
		// per Component.
		p := surgePacing(iosString("25%"), nil)
		// 4 engine replicas → surge=1; 12 decoder replicas → surge=3.
		Expect(coordination.MaxSurgeBudget(p, 4)).To(Equal(int32(1)))
		Expect(coordination.MaxSurgeBudget(p, 12)).To(Equal(int32(3)))
	})

	ginkgo.It("PC-9 MaxUnavailable=0 → strict zero-downtime semantic", func() {
		p := surgePacing(iosInt(2), iosInt(0))
		Expect(coordination.MaxUnavailableBudget(p, 100)).To(BeZero(),
			"MaxUnavailable=0 means caller cannot terminate any old pod before its replacement is Ready")
		// nil MaxUnavailable also defaults to 0 (per spec defaulter).
		pNil := surgePacing(iosInt(2), nil)
		Expect(coordination.MaxUnavailableBudget(pNil, 100)).To(BeZero(),
			"nil MaxUnavailable defaults to 0 (matches zero-downtime intent)")
	})

	ginkgo.It("PC-10 PerComponent + multi-replica + asymmetric: each Component computed independently", func() {
		// 3-Component asymmetric: 5 engine, 3 decoder, 8 router.
		// MaxSurge=20% → engine=ceil(1.0)=1, decoder=ceil(0.6)=1,
		// router=ceil(1.6)=2.
		p := surgePacing(iosString("20%"), nil)
		Expect(coordination.MaxSurgeBudget(p, 5)).To(Equal(int32(1)))
		Expect(coordination.MaxSurgeBudget(p, 3)).To(Equal(int32(1)))
		Expect(coordination.MaxSurgeBudget(p, 8)).To(Equal(int32(2)))
	})

	ginkgo.It("PC-Pure-Aux negative int budgets clamp to 0 (defensive)", func() {
		pSurge := surgePacing(iosInt(-3), nil)
		Expect(coordination.MaxSurgeBudget(pSurge, 100)).To(BeZero(),
			"negative MaxSurge int clamps to 0")
		pUnavail := surgePacing(nil, iosInt(-1))
		Expect(coordination.MaxUnavailableBudget(pUnavail, 100)).To(BeZero(),
			"negative MaxUnavailable int clamps to 0")
	})

	ginkgo.It("PC-Pure-Aux 0 replicas → 0 budget regardless of percent", func() {
		// Edge case: a Component with no replicas can't surge.
		p := surgePacing(iosString("100%"), iosString("100%"))
		Expect(coordination.MaxSurgeBudget(p, 0)).To(BeZero())
		Expect(coordination.MaxUnavailableBudget(p, 0)).To(BeZero())
	})

	ginkgo.It("PC-Pure-Aux malformed string percent ('abc') → 0 (graceful, validated upstream)", func() {
		// strPercent returns 0 on parse failure — callers reading from
		// the validated webhook payload won't see this branch in
		// practice, but the math must not panic on bad input.
		p := surgePacing(iosString("abc"), nil)
		Expect(coordination.MaxSurgeBudget(p, 10)).To(BeZero())
		// String missing the % suffix is also rejected.
		pNoPct := surgePacing(iosString("50"), nil)
		Expect(coordination.MaxSurgeBudget(pNoPct, 10)).To(BeZero())
	})

	// ----------------------------------------------------------------
	// G7-blocked PIts (end-to-end: state machine never calls budgets)
	// ----------------------------------------------------------------

	ginkgo.PIt("G7: PC-E2E-1 MaxSurge=2 on Atomic group with MinReplicas=4 → exactly 2 surge pods observed", func() {
		// Pure math says surge=2; state machine should respect it and
		// create no more than 2 surge pods at a time. Today the budget
		// helpers are never invoked, so the controller surges however
		// many pods OMENative's per-Component logic decides — which
		// is independent of the group pacing block.
		fix := newCoordFixture(ns.Name, WithISVCName("pcE1"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithEngineReplicas(4))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:     v1beta1.CoordinationPacingPerComponent,
			MaxSurge: iosInt(2),
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "pcE1")
			// Bump trigger then check surge count.
			engineCount := 0
			for _, p := range pods {
				if p.Labels["component"] == string(v1beta1.EngineComponent) {
					engineCount++
				}
			}
			g.Expect(engineCount).To(BeNumerically("<=", 4+2),
				"engine pod count must not exceed MinReplicas + MaxSurge during rollout")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G7: PC-E2E-2 MaxUnavailable=0 (zero-downtime) → no observed period with ReadyPods < MinReplicas", func() {
		// MaxUnavailable=0 means the controller must keep all
		// MinReplicas Ready throughout the rollout. ReadyPods should
		// never dip below MinReplicas. Blocked on G7.
		fix := newCoordFixture(ns.Name, WithISVCName("pcE2"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			WithEngineReplicas(3))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:           v1beta1.CoordinationPacingPerComponent,
			MaxSurge:       iosInt(1),
			MaxUnavailable: iosInt(0),
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Consistently(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pcE2")
			cs := cur.Status.Components[v1beta1.EngineComponent]
			if cs.OMENative == nil {
				return // pre-reconcile
			}
			ready := 0
			for _, s := range cs.OMENative.InstanceStatuses {
				if s.Phase == v1beta1.OMENativeInstanceReady {
					ready++
				}
			}
			g.Expect(ready).To(BeNumerically(">=", 3),
				"with MaxUnavailable=0, ReadyPods must never dip below MinReplicas (3)")
		}, "30s", util.PollInterval).Should(Succeed())
	})
})
