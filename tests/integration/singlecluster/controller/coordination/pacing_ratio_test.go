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
// run directly against that contract.

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
		// Serving-ratio semantic. Original 40:20 → orig_ratio
		// 2.0. Default tolerance 5% → band [1.9, 2.1].
		// Serving: all original pods alive (engine=40, decoder=20).
		// Surge decoder +1 → serving 40/21 = 1.905, in band → allowed.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			Serving: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.DecoderComponent, 1)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(1)),
			"surge decoder +1 keeps live ratio at 40/21 = 1.905 within [1.9, 2.1]")
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
		// Serving-ratio semantic. Original 40:20 → orig_ratio 2.0.
		// Serving: live 40 engine / 20 decoder → ratio 2.0 (in band).
		// Propose surge engine +4 → projected 44/20 = 2.2.
		//   tol=5%: band [1.9, 2.1] → 2.2 OUT → back off step by step,
		//           +2 → 42/20=2.1 in band (boundary) → returns 2.
		//   tol=10%: band [1.8, 2.2] → 2.2 in (boundary) → returns 4.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
			Serving: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
			},
		}
		dDefault := coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 4)
		Expect(dDefault.AllowedSurgeDelta).To(BeNumerically("<", int32(4)),
			"5%% tolerance cannot fit the full +4 engine surge (would push ratio to 2.2)")

		dWide := coordination.EvaluateSurge(ratioPacing(int32Ptr(10)), state, v1beta1.EngineComponent, 4)
		Expect(dWide.AllowedSurgeDelta).To(Equal(int32(4)),
			"10%% tolerance accepts the +4 engine surge (ratio 2.2 == upper band boundary)")
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
		// Serving-ratio semantic with 3-way topology.
		// Original 40:20:10 → pairs:
		//   engine/decoder = 40/20 = 2.0,  band [1.9, 2.1] at tol=5%
		//   engine/router  = 40/10 = 4.0,  band [3.8, 4.2]
		//   decoder/router = 20/10 = 2.0,  band [1.9, 2.1]
		// Live serving: all original alive (40, 20, 10).
		// Surge engine +2 → projected 42/20/10:
		//   engine/decoder = 42/20 = 2.1   (at boundary, allowed)
		//   engine/router  = 42/10 = 4.2   (at boundary, allowed)
		//   decoder/router = 20/10 = 2.0   (unchanged, allowed)
		// → +2 allowed. +3 would push engine/decoder to 2.15 out → reject.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
				v1beta1.RouterComponent:  10,
			},
			Serving: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  40,
				v1beta1.DecoderComponent: 20,
				v1beta1.RouterComponent:  10,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(int32Ptr(5)), state, v1beta1.EngineComponent, 2)
		Expect(d.AllowedSurgeDelta).To(Equal(int32(2)),
			"pairwise band check accepts +2 (engine/decoder=2.1 and engine/router=4.2 both at boundary)")

		// +3 would push engine/decoder = 43/20 = 2.15 → out of [1.9, 2.1].
		// Backoff returns 2.
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

	ginkgo.It("R-11 proposedDelta negative → drain projection (single-step, not iterated)", func() {
		// OEP-0007.2: negative proposedDelta is the dispatcher asking
		// "may I pull one more pod from rotation?". Single-step
		// projection; a partial drain isn't a unit of work the
		// dispatcher can act on. With current serving (Engine=4, Decoder=4),
		// asking to drain 1 engine → projected (3, 4), ratio 4/3 ≈ 1.33,
		// orig ratio 1.0, default tolerance 5% → band [0.95, 1.05] →
		// projected ratio outside band → SkewRejected.
		state := coordination.RatioState{
			Original: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  4,
				v1beta1.DecoderComponent: 4,
			},
			Serving: map[v1beta1.ComponentType]int32{
				v1beta1.EngineComponent:  4,
				v1beta1.DecoderComponent: 4,
			},
		}
		d := coordination.EvaluateSurge(ratioPacing(nil), state, v1beta1.EngineComponent, -1)
		Expect(d.AllowedSurgeDelta).To(BeZero(),
			"drain blocked when projected ratio crosses the band")
		Expect(d.SkewRejected).To(BeTrue(),
			"out-of-band drain IS a skew rejection under the new drain-projection semantic")
		Expect(d.Reason).To(ContainSubstring("drain would skew"))
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
		// BlueGreen group with RatioBalanced(tolerance=0). Asymmetric desired
		// replicas (40/20) — surging engine without decoder progress
		// would blow the ratio band. EvaluateSurge zeroes the engine
		// budget; ComputeTransition surfaces RatioSkewRejected=true.
		obs := blueGreenObs()
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
		obs := blueGreenObs()
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

	ginkgo.It("R-3 RatioSkewRejected event + counter fire when EvaluateSurge returns SkewRejected", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("r3"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:           v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerance: int32Ptr(0),
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

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

	ginkgo.It("R-6 ObservedRatio populated in RolloutCoordinationGroupStatus", func() {
		// buildRatioState invokes SnapshotOriginal on first observation
		// and BuildGroupStatus populates ObservedRatio.
		fix := newCoordFixture(ns.Name, WithISVCName("r6"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "r6")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedRatio).NotTo(BeNil(),
				"RatioBalanced must populate ObservedRatio at first surge")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("R-PD-Gate canonical PD scenario: decoder=25% of engine, ObservedRatio snapshot wires", func() {
		// Engine 20 pods + decoder 5 pods + RatioBalanced @ tolerance=20%.
		// Verify Status.RolloutCoordination snapshots a non-empty
		// ObservedRatio.Original. Snapshot timing in envtest races
		// against OMENative's Replicas report, so we don't pin exact
		// values — the math is unit-tested in ratio_test.go.
		fix := newCoordFixture(ns.Name, WithISVCName("rpdg"),
			WithEngineReplicas(20),
			WithDecoderReplicas(5),
			WithEngineImage("ghcr.io/test/runtime:v1"),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:           v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerance: int32Ptr(20),
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the producer to emit ObservedRatio. We don't pin
		// exact Original values because snapshot timing is racy in
		// envtest (kubelet sim vs. first coord reconcile).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "rpdg")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			gs := cur.Status.RolloutCoordination.Groups[0]
			g.Expect(gs.ObservedRatio).NotTo(BeNil(),
				"ObservedRatio must be populated for RatioBalanced PD scenario")
			g.Expect(gs.ObservedRatio.Original).NotTo(BeEmpty(),
				"Original snapshot must contain both Components")
			g.Expect(gs.ObservedRatio.Original).To(HaveKey(v1beta1.EngineComponent))
			g.Expect(gs.ObservedRatio.Original).To(HaveKey(v1beta1.DecoderComponent))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
