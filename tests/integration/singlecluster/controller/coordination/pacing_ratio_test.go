package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RatioBalanced pacing specs for OEP-0007.1.
//
// RatioBalanced pacing keeps cross-Component pod ratios within a
// tolerance band as the rollout surges. The math lives in
// coordination.EvaluateSurge — a pure function — so most R-* cases
// run directly against that contract. End-to-end PIts cover:
//
//   - G3: state machine never invokes EvaluateSurge → RatioBalanced
//         has no effect on actual surge decisions
//   - G5: RatioSkewRejected event/counter never emitted
//   - G6: SnapshotOriginal never invoked + ObservedRatio status not
//         populated by the controller
//
// Reference: pkg/.../coordination/ratio.go

// pacing helpers
func ratioPacing(tolerance *int32) v1beta1.CoordinationPacing {
	return v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: tolerance,
	}
}

func perComponentPacing() v1beta1.CoordinationPacing {
	return v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingPerComponent}
}

func int32Ptr(n int32) *int32 { return &n }

var _ = ginkgo.Describe("Coordination RatioBalanced pacing (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-ratio-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs (call EvaluateSurge directly)
	// ----------------------------------------------------------------

	ginkgo.It("R-1 40:20 topology, in-band surge proposal → allowed unchanged", func() {
		// Original ratio: 40 engine : 20 decoder. Surge engine by 2
		// (5% of 40); surge decoder by 1 (5% of 20). With default
		// tolerance=5%, projected fractions are identical (0.05 each)
		// → within band.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  2, // already at 5%
				v1beta1.DecoderComponent: 1, // already at 5%
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.DecoderComponent, 1)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(1)),
			"a +1 surge that keeps decoder at 10% (vs engine at 5%) is within the 5% tolerance band")
		Expect(d.SkewRejected).To(BeFalse())
		Expect(d.Reason).To(ContainSubstring("within tolerance"))
	})

	ginkgo.It("R-2 out-of-band surge proposal → AllowedSurgeDelta truncated to fit band", func() {
		// Original: 40:20. Engine has 0 new pods, decoder has 0 new pods.
		// Proposing a huge surge on engine (20) blows out the ratio.
		// EvaluateSurge walks delta down to the largest value that fits.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  0,
				v1beta1.DecoderComponent: 0,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, 20)
		Expect(d.AllowedSurgeDelta).To(BeNumerically("<", int32(20)),
			"proposing 20 engine pods with 0 decoder progress must truncate to fit the 5%% band")
		Expect(d.AllowedSurgeDelta).To(BeNumerically(">=", int32(0)))
	})

	ginkgo.It("R-3 total skew rejection → AllowedSurgeDelta=0, SkewRejected=true", func() {
		// Even +1 engine surge with decoder at 0 blows the 0% tolerance.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  0,
				v1beta1.DecoderComponent: 0,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(int32Ptr(0)), state, v1beta1.EngineComponent, 1)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(0)))
		Expect(d.SkewRejected).To(BeTrue(),
			"tolerance=0 with asymmetric surge must SkewRejected")
		Expect(d.Reason).To(ContainSubstring("skew"))
	})

	ginkgo.It("R-4 RatioTolerance=10 widens the acceptable band vs default 5", func() {
		// Same ask: surge engine by 4 (10% of 40), decoder at 0 (0%).
		// Default tolerance=5 → rejected. tolerance=10 → allowed.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  0,
				v1beta1.DecoderComponent: 0,
			},
		}
		dDefault := coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 4)
		Expect(dDefault.AllowedSurgeDelta).To(BeNumerically("<", int32(4)),
			"5%% tolerance cannot fit a 10%% asymmetric surge")

		dWide := coordination.EvaluateSurge(ratioPacing(int32Ptr(10)), state, v1beta1.EngineComponent, 4)
		Expect(dWide.AllowedSurgeDelta).To(Equal(int32(4)),
			"10%% tolerance must accept the same proposal that default rejected")
	})

	ginkgo.It("R-5 single-Component group → trivially satisfied (no-op short-circuit)", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 5},
			NewPods:  map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 0},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, 99)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(99)),
			"single-Component group passes any proposed delta unchanged")
		Expect(d.Reason).To(ContainSubstring("single-component"))
	})

	ginkgo.It("R-7 all Components at 0 NewPods + proposed=0 → trivially satisfied", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  0,
				v1beta1.DecoderComponent: 0,
			},
		}
		// proposedDelta=0 short-circuits before band math runs.
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, 0)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(0)))
		Expect(d.Reason).To(ContainSubstring("no surge requested"))
	})

	ginkgo.It("R-8 Components with Original=0 are skipped in ratio computation", func() {
		// One Component declares 0 replicas (degenerate but accepted).
		// `respectsBand` skips Components with orig<=0, so the math
		// reduces to the remaining Components.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  4,
				v1beta1.DecoderComponent: 0, // skipped
				v1beta1.RouterComponent:  4,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  0,
				v1beta1.DecoderComponent: 0,
				v1beta1.RouterComponent:  0,
			},
		}
		// 50% engine vs 0% router blows the 5% band; with decoder=0
		// skipped, this still must fail.
		d := coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 2)
		Expect(d.AllowedSurgeDelta).To(BeNumerically("<", int32(2)),
			"Original=0 Component skipped but other peers still constrain")
		Expect(d.AllowedSurgeDelta).To(BeNumerically(">=", int32(0)))
	})

	ginkgo.It("R-9 3-way 40:20:10 ratio enforced pairwise (no degenerate biggest-only)", func() {
		// 40:20:10 topology. Surge engine (40) by 2 (5%). Router (10)
		// has 0 new (0%). Pairwise check: engine 5% vs router 0% = 5
		// percentage-point gap. With default 5% tolerance, this is
		// right at the edge — and the existing 5%-vs-5% pair (engine
		// vs decoder, both at 5%) is satisfied. Pairwise correctness
		// means the engine-vs-router pair gates this.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
				v1beta1.RouterComponent:  10,
			},
			NewPods: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  0,
				v1beta1.DecoderComponent: 1, // 5%
				v1beta1.RouterComponent:  0,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 2)
		// engine 2/40 = 5%, decoder 1/20 = 5%, router 0/10 = 0%.
		// engine-vs-router gap = 5pp = exactly at band (the band is
		// `>` not `>=`, so equality is accepted). 2 is allowed.
		Expect(d.AllowedSurgeDelta).To(Equal(int32(2)),
			"pairwise band check accepts 5%% gap at exactly 5%% tolerance")

		// Bumping by one more: engine=3/40=7.5% vs router=0% → 7.5pp
		// gap → rejected. AllowedSurgeDelta truncates to 2.
		d = coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 3)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(2)),
			"truncation walks delta down to the largest fit, not all-or-nothing")
	})

	ginkgo.It("R-10 proposedDelta=0 → allowed=0 with no-surge-requested reason", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  4,
				v1beta1.DecoderComponent: 4,
			},
			NewPods: map[v1beta1.ComponentType]int32{},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, 0)
		Expect(d.AllowedSurgeDelta).To(BeZero())
		Expect(d.Reason).To(Equal("no surge requested"))
	})

	ginkgo.It("R-11 proposedDelta negative → allowed=0 (defensive: <= 0 short-circuits)", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  4,
				v1beta1.DecoderComponent: 4,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, -5)
		Expect(d.AllowedSurgeDelta).To(BeZero())
		Expect(d.SkewRejected).To(BeFalse(),
			"negative delta is degenerate input, NOT a skew rejection")
	})

	ginkgo.It("R-12 pacing.Type != RatioBalanced → bypass: returns proposed delta unchanged", func() {
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 0, // would blow ratio
			},
			NewPods: map[v1beta1.ComponentType]int32{},
		}
		// PerComponent pacing skips ratio math entirely.
		d := coordination.EvaluateSurge(perComponentPacing(), state, v1beta1.EngineComponent, 99)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(99)))
		Expect(d.Reason).To(ContainSubstring("PerComponent"))
	})

	ginkgo.It("R-G3 atomic Surging with RatioBalanced + tight band zeroes SurgeBudget + sets RatioSkewRejected", func() {
		// Atomic group with RatioBalanced(tolerance=0). Asymmetric desired
		// replicas (40/20) — surging engine without decoder progress
		// would blow the ratio band. EvaluateSurge zeroes the engine
		// budget; ComputeTransition surfaces RatioSkewRejected=true.
		obs := atomicObs()
		obs.Group.Pacing = v1beta1.CoordinationPacing{
			Type:           v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerance: int32Ptr(0),
		}
		obs.OriginalReplicas = map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		}
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.DesiredReplicas = 40
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.DesiredReplicas = 20
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(tr.RatioSkewRejected).To(BeTrue(),
			"tolerance=0 + asymmetric replicas must trip the ratio gate")
	})

	ginkgo.It("R-G3b PerComponent pacing never sets RatioSkewRejected", func() {
		// Same shape as R-G3 but with PerComponent pacing → no ratio
		// gate runs. SkewRejected must stay false.
		obs := atomicObs()
		obs.Group.Pacing = v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingPerComponent,
		}
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).RatioSkewRejected).To(BeFalse())
	})

	ginkgo.It("R-Pure-Aux SnapshotOriginal coerces zero/negative to 1", func() {
		out := coordination.SnapshotOriginal(map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  3,
			v1beta1.DecoderComponent: 0,
			v1beta1.RouterComponent:  -2,
		})
		Expect(out[v1beta1.EngineComponent]).To(Equal(int32(3)))
		Expect(out[v1beta1.DecoderComponent]).To(Equal(int32(1)),
			"zero is coerced to 1 so the Component still anchors the ratio")
		Expect(out[v1beta1.RouterComponent]).To(Equal(int32(1)),
			"negative is also coerced to 1 (defensive)")
	})

	// ----------------------------------------------------------------
	// Known-gap PIts
	// ----------------------------------------------------------------

	ginkgo.PIt("G3+G5: R-3 RatioSkewRejected event + counter fire when EvaluateSurge returns SkewRejected", func() {
		// EvaluateSurge returns SkewRejected=true today, but neither
		// the state machine consults it (G3) nor does the metrics
		// layer emit RatioSkewRejected events / increment the counter
		// (G5). End-to-end this needs both fixes.
		fix := newCoordFixture(ns.Name, WithISVCName("r3"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:           v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerance: int32Ptr(0),
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events)).To(Succeed())
			var saw bool
			for _, e := range events.Items {
				if e.InvolvedObject.Name == "r3" && e.Reason == "RatioSkewRejected" {
					saw = true
				}
			}
			g.Expect(saw).To(BeTrue(),
				"RatioSkewRejected event must fire when surge is refused for ratio drift")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G6: R-6 ObservedRatio populated in RolloutCoordinationGroupStatus", func() {
		// SnapshotOriginal is never called in production today, so the
		// status writer can't fill ObservedRatio. Once G6 lands, this
		// should populate at the first surge observation.
		fix := newCoordFixture(ns.Name, WithISVCName("r6"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "r6")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedRatio).NotTo(BeNil(),
				"RatioBalanced must populate ObservedRatio at first surge")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
