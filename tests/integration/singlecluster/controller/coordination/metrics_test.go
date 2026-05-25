package coordination

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Metrics specs for OEP-0007.1.
//
// Six Prometheus collectors are registered at package init:
//
//   ome_omenative_rollout_group_phase_total       (Counter)
//   ome_omenative_rollout_group_transition_total  (Counter)
//   ome_omenative_rollout_group_failure_total     (Counter)
//   ome_omenative_ratio_skew_total                (Counter)
//   ome_omenative_per_revision_service_total      (Gauge)
//   ome_omenative_mixed_pairing_total             (Counter)
//
// Three Counters are NOT called from production code today (G5):
// RecordGroupTransition, RecordRatioSkew, RecordMixedPairing. These
// tests exercise the Record* helpers directly to prove the math works;
// end-to-end emission requires the G3/G5/G6 wiring fixes.
//
// Reference: pkg/.../coordination/metrics.go

// counterFor fetches the (isvc, group, ...) sample from a CounterVec
// by name from the controller-runtime registry. Test isolation: each
// spec uses a unique isvc/group label combo so increments don't bleed
// between tests.
func counterFor(metricName string, labels prometheus.Labels) float64 {
	g, err := metrics.Registry.Gather()
	if err != nil {
		return -1
	}
	for _, mf := range g {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for _, l := range m.GetLabel() {
				if v, want := labels[l.GetName()]; want && v != l.GetValue() {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
		}
	}
	return 0
}

var _ = ginkgo.Describe("Coordination metrics (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-metrics-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs: exercise Record* helpers directly and verify
	// the underlying counter/gauge moved.
	// ----------------------------------------------------------------

	ginkgo.It("M-1 RecordGroupPhase increments the phase counter for (isvc, group, phase)", func() {
		labels := prometheus.Labels{"isvc": "m1-isvc", "group": "g0", "phase": "Surging"}
		start := counterFor("ome_omenative_rollout_group_phase_total", labels)
		coordination.RecordGroupPhase("m1-isvc", "g0", "Surging")
		coordination.RecordGroupPhase("m1-isvc", "g0", "Surging")
		end := counterFor("ome_omenative_rollout_group_phase_total", labels)
		Expect(end-start).To(Equal(float64(2)),
			"two RecordGroupPhase calls must produce two counter increments")
	})

	ginkgo.It("M-2 SetPerRevisionServiceCount sets the gauge value (replaces, not accumulates)", func() {
		labels := prometheus.Labels{"isvc": "m2-isvc", "component": "engine"}
		coordination.SetPerRevisionServiceCount("m2-isvc", "engine", 3)
		Expect(counterFor("ome_omenative_per_revision_service_total", labels)).
			To(Equal(float64(3)))
		// Re-setting replaces (gauge semantics) — does NOT accumulate.
		coordination.SetPerRevisionServiceCount("m2-isvc", "engine", 1)
		Expect(counterFor("ome_omenative_per_revision_service_total", labels)).
			To(Equal(float64(1)),
				"gauge must be replaced on re-Set, not summed (mixed up with a counter)")
	})

	ginkgo.It("M-3 RecordGroupFailure increments per (isvc, group, reason)", func() {
		labelsA := prometheus.Labels{"isvc": "m3-isvc", "group": "g0", "reason": "ReadyTimeout"}
		labelsB := prometheus.Labels{"isvc": "m3-isvc", "group": "g0", "reason": "Other"}
		start := counterFor("ome_omenative_rollout_group_failure_total", labelsA)
		coordination.RecordGroupFailure("m3-isvc", "g0", "ReadyTimeout")
		end := counterFor("ome_omenative_rollout_group_failure_total", labelsA)
		Expect(end - start).To(Equal(float64(1)))
		// Different reason → different label combination → independent counter.
		coordination.RecordGroupFailure("m3-isvc", "g0", "Other")
		Expect(counterFor("ome_omenative_rollout_group_failure_total", labelsB)).
			To(Equal(float64(1)),
				"different `reason` label values must produce independent counters")
	})

	ginkgo.It("M-4 RecordGroupTransition increments", func() {
		labels := prometheus.Labels{"isvc": "m4-isvc", "group": "g0", "from": "Surging", "to": "Shifting"}
		start := counterFor("ome_omenative_rollout_group_transition_total", labels)
		coordination.RecordGroupTransition("m4-isvc", "g0", "Surging", "Shifting")
		Expect(counterFor("ome_omenative_rollout_group_transition_total", labels) - start).
			To(Equal(float64(1)))
	})

	ginkgo.It("M-4b RecordGroupTransition with from==to is a no-op", func() {
		labels := prometheus.Labels{"isvc": "m4b-isvc", "group": "g0", "from": "Idle", "to": "Idle"}
		start := counterFor("ome_omenative_rollout_group_transition_total", labels)
		coordination.RecordGroupTransition("m4b-isvc", "g0", "Idle", "Idle")
		Expect(counterFor("ome_omenative_rollout_group_transition_total", labels)).
			To(Equal(start), "same-phase transition must not increment (defensive)")
	})

	ginkgo.It("M-5 RecordRatioSkew increments", func() {
		labels := prometheus.Labels{"isvc": "m5-isvc", "group": "g0"}
		start := counterFor("ome_omenative_ratio_skew_total", labels)
		coordination.RecordRatioSkew("m5-isvc", "g0")
		coordination.RecordRatioSkew("m5-isvc", "g0")
		Expect(counterFor("ome_omenative_ratio_skew_total", labels) - start).
			To(Equal(float64(2)))
	})

	ginkgo.It("M-6 RecordMixedPairing increments", func() {
		labels := prometheus.Labels{"isvc": "m6-isvc", "group": "g0"}
		start := counterFor("ome_omenative_mixed_pairing_total", labels)
		coordination.RecordMixedPairing("m6-isvc", "g0")
		Expect(counterFor("ome_omenative_mixed_pairing_total", labels) - start).
			To(Equal(float64(1)))
	})

	ginkgo.It("M-7 Prometheus accepts the label values used in production (sanitization happens upstream)", func() {
		// Prometheus label values may contain any UTF-8. Callers must
		// not pass slashes or other delimiters because they'll be
		// surfaced verbatim to scrapers. Pin the well-formed shape:
		// isvc names are lowercase k8s names; group names are stringified
		// indices (0, 1, …); phase values are the CoordinationPhase enum.
		coordination.RecordGroupPhase("hyphen-name", "0", string(v1beta1.CoordinationPhaseSurging))
		coordination.RecordGroupPhase("a", "0", string(v1beta1.CoordinationPhaseFailed))
		// No panic, no error — Prometheus accepts these.
		// Negative coverage: a "/"-containing isvc would be surfaced
		// verbatim. Callers should validate upstream.
		coordination.RecordGroupPhase("foo/bar", "0", "Idle")
		labels := prometheus.Labels{"isvc": "foo/bar", "group": "0", "phase": "Idle"}
		Expect(counterFor("ome_omenative_rollout_group_phase_total", labels)).
			To(BeNumerically(">", float64(0)),
				"Prometheus accepts arbitrary label values; sanitization is the caller's job")
	})

	ginkgo.It("M-8 counters are monotonic across consecutive Record* calls", func() {
		labels := prometheus.Labels{"isvc": "m8-isvc", "group": "g0", "phase": "Idle"}
		var last float64
		for i := 0; i < 5; i++ {
			coordination.RecordGroupPhase("m8-isvc", "g0", "Idle")
			cur := counterFor("ome_omenative_rollout_group_phase_total", labels)
			Expect(cur).To(BeNumerically(">=", last),
				"counter must be non-decreasing across consecutive increments")
			last = cur
		}
		// Counters can also be inspected via testutil.ToFloat64 directly
		// if the package exposes the metric — for the test here we rely
		// on counterFor for label-keyed lookups.
		_ = testutil.CollectAndCount
	})

	ginkgo.It("M-9 gauge decrement reflects when SetPerRevisionServiceCount lowers the value (G4 dependency for prod usage)", func() {
		labels := prometheus.Labels{"isvc": "m9-isvc", "component": "decoder"}
		coordination.SetPerRevisionServiceCount("m9-isvc", "decoder", 5)
		Expect(counterFor("ome_omenative_per_revision_service_total", labels)).
			To(Equal(float64(5)))
		coordination.SetPerRevisionServiceCount("m9-isvc", "decoder", 2)
		Expect(counterFor("ome_omenative_per_revision_service_total", labels)).
			To(Equal(float64(2)),
				"gauge decrement is the right shape for tracking 'live' Service counts after G4 GC")
		coordination.SetPerRevisionServiceCount("m9-isvc", "decoder", 0)
		Expect(counterFor("ome_omenative_per_revision_service_total", labels)).
			To(BeZero(), "gauge must drop to 0 when all Services for a Component are GC'd")
	})

	ginkgo.It("M-11 Phase label values match the CoordinationPhase enum (recorded as-is)", func() {
		// Pin: the metric label gets the enum value as the string. If
		// a future refactor renames the enum (e.g. "Idle" → "AtRest"),
		// existing dashboards keyed on the old phase label break.
		// This test fails in that case, forcing a deliberate decision.
		for _, p := range []v1beta1.CoordinationPhase{
			v1beta1.CoordinationPhaseIdle,
			v1beta1.CoordinationPhaseSurging,
			v1beta1.CoordinationPhaseWaiting,
			v1beta1.CoordinationPhaseShifting,
			v1beta1.CoordinationPhaseDraining,
			v1beta1.CoordinationPhaseScalingDown,
			v1beta1.CoordinationPhaseFailed,
			v1beta1.CoordinationPhaseRollingBack,
			v1beta1.CoordinationPhasePaused,
		} {
			isvc := "m11-" + strings.ToLower(string(p))
			coordination.RecordGroupPhase(isvc, "0", string(p))
			labels := prometheus.Labels{"isvc": isvc, "group": "0", "phase": string(p)}
			Expect(counterFor("ome_omenative_rollout_group_phase_total", labels)).
				To(BeNumerically(">", float64(0)),
					"phase label %q must be writable as-is (regression gate for enum renames)", p)
		}
	})

	ginkgo.It("M-12 empty isvc/group/phase args → all Record* helpers no-op (no panic, no increment)", func() {
		// Defensive guards in each helper short-circuit when any
		// required label is empty.
		coordination.RecordGroupPhase("", "g", "Surging")
		coordination.RecordGroupPhase("i", "", "Surging")
		coordination.RecordGroupPhase("i", "g", "")

		coordination.RecordGroupTransition("", "g", "a", "b")
		coordination.RecordGroupTransition("i", "", "a", "b")

		coordination.RecordGroupFailure("", "g", "X")
		coordination.RecordGroupFailure("i", "", "X")

		coordination.RecordRatioSkew("", "g")
		coordination.RecordRatioSkew("i", "")

		coordination.SetPerRevisionServiceCount("", "c", 1)
		coordination.SetPerRevisionServiceCount("i", "", 1)

		coordination.RecordMixedPairing("", "g")
		coordination.RecordMixedPairing("i", "")

		// All calls returned without panic. To confirm no-op behavior
		// we check that an empty-isvc counter has no samples.
		labels := prometheus.Labels{"isvc": "", "group": "g", "phase": "Surging"}
		Expect(counterFor("ome_omenative_rollout_group_phase_total", labels)).
			To(BeZero(), "empty isvc must not have produced a counter sample")
	})

	ginkgo.It("M-4 E2E group_transition_total fires from reconciler on phase boundary", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("m4e"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			labels := prometheus.Labels{"isvc": "m4e", "group": "0"}
			val := counterFor("ome_omenative_rollout_group_transition_total", labels)
			g.Expect(val).To(BeNumerically(">", float64(0)),
				"group_transition_total must be incremented as the group walks phases")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("M-5 E2E ratio_skew_total fires when RatioBalanced rejects a surge", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("m5e"),
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
			labels := prometheus.Labels{"isvc": "m5e", "group": "0"}
			val := counterFor("ome_omenative_ratio_skew_total", labels)
			g.Expect(val).To(BeNumerically(">", float64(0)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
