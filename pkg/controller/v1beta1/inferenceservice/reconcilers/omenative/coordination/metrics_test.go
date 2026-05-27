package coordination

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Pure-function unit tests for the six Prometheus collectors registered
// at package init in metrics.go. Ported from the envtest sibling
// (formerly tests/integration/.../coordination/metrics_test.go M-1..M-12)
// — none of the assertions below depend on envtest, the OMENative
// reconciler, or any cluster scaffolding, so they belong here as a
// proper pkg-level suite.
//
// Each test uses a unique isvc-label combo so increments don't bleed.

// counterFor fetches the (isvc, group, ...) sample from a CounterVec or
// GaugeVec by name from the controller-runtime registry. Returns 0 when
// the metric exists but the label combo has no sample yet, and -1 only
// when Gather itself errors.
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

func TestRecordGroupPhase_IncrementsCounter(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m1-isvc", "group": "g0", "phase": "Surging"}
	start := counterFor("ome_omenative_rollout_group_phase_total", labels)
	RecordGroupPhase("m1-isvc", "g0", "Surging")
	RecordGroupPhase("m1-isvc", "g0", "Surging")
	end := counterFor("ome_omenative_rollout_group_phase_total", labels)
	if end-start != 2 {
		t.Errorf("two RecordGroupPhase calls: got delta %v want 2", end-start)
	}
}

func TestSetPerRevisionServiceCount_ReplacesValue(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m2-isvc", "component": "engine"}
	SetPerRevisionServiceCount("m2-isvc", "engine", 3)
	if v := counterFor("ome_omenative_per_revision_service_total", labels); v != 3 {
		t.Errorf("after Set(3): got %v want 3", v)
	}
	SetPerRevisionServiceCount("m2-isvc", "engine", 1)
	if v := counterFor("ome_omenative_per_revision_service_total", labels); v != 1 {
		t.Errorf("after Set(1): gauge must be replaced not summed; got %v want 1", v)
	}
}

func TestRecordGroupFailure_IncrementsPerReason(t *testing.T) {
	labelsA := prometheus.Labels{"isvc": "m3-isvc", "group": "g0", "reason": "ReadyTimeout"}
	labelsB := prometheus.Labels{"isvc": "m3-isvc", "group": "g0", "reason": "Other"}
	start := counterFor("ome_omenative_rollout_group_failure_total", labelsA)
	RecordGroupFailure("m3-isvc", "g0", "ReadyTimeout")
	if end := counterFor("ome_omenative_rollout_group_failure_total", labelsA); end-start != 1 {
		t.Errorf("ReadyTimeout: got delta %v want 1", end-start)
	}
	RecordGroupFailure("m3-isvc", "g0", "Other")
	if v := counterFor("ome_omenative_rollout_group_failure_total", labelsB); v != 1 {
		t.Errorf("Other reason: independent counter must be 1; got %v", v)
	}
}

func TestRecordGroupTransition_IncrementsCounter(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m4-isvc", "group": "g0", "from": "Surging", "to": "Shifting"}
	start := counterFor("ome_omenative_rollout_group_transition_total", labels)
	RecordGroupTransition("m4-isvc", "g0", "Surging", "Shifting")
	if end := counterFor("ome_omenative_rollout_group_transition_total", labels); end-start != 1 {
		t.Errorf("got delta %v want 1", end-start)
	}
}

func TestRecordGroupTransition_SamePhaseIsNoOp(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m4b-isvc", "group": "g0", "from": "Idle", "to": "Idle"}
	start := counterFor("ome_omenative_rollout_group_transition_total", labels)
	RecordGroupTransition("m4b-isvc", "g0", "Idle", "Idle")
	if end := counterFor("ome_omenative_rollout_group_transition_total", labels); end != start {
		t.Errorf("same-phase transition must not increment; got %v want %v", end, start)
	}
}

func TestRecordRatioSkew_IncrementsCounter(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m5-isvc", "group": "g0"}
	start := counterFor("ome_omenative_ratio_skew_total", labels)
	RecordRatioSkew("m5-isvc", "g0")
	RecordRatioSkew("m5-isvc", "g0")
	if end := counterFor("ome_omenative_ratio_skew_total", labels); end-start != 2 {
		t.Errorf("got delta %v want 2", end-start)
	}
}

func TestRecordMixedPairing_IncrementsCounter(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m6-isvc", "group": "g0"}
	start := counterFor("ome_omenative_mixed_pairing_total", labels)
	RecordMixedPairing("m6-isvc", "g0")
	if end := counterFor("ome_omenative_mixed_pairing_total", labels); end-start != 1 {
		t.Errorf("got delta %v want 1", end-start)
	}
}

func TestMetricsAcceptProductionLabelShapes(t *testing.T) {
	// Prometheus label values may contain any UTF-8; sanitization is the
	// caller's job. Verify the production shape (hyphenated names, enum
	// values) doesn't panic and lands in the registry.
	RecordGroupPhase("hyphen-name", "0", string(v1beta1.CoordinationPhaseSurging))
	RecordGroupPhase("a", "0", string(v1beta1.CoordinationPhaseFailed))
	// Negative coverage: a "/"-containing isvc surfaces verbatim.
	RecordGroupPhase("foo/bar", "0", "Idle")
	labels := prometheus.Labels{"isvc": "foo/bar", "group": "0", "phase": "Idle"}
	if v := counterFor("ome_omenative_rollout_group_phase_total", labels); v <= 0 {
		t.Errorf("production label shapes must be accepted; got %v", v)
	}
}

func TestCountersAreMonotonic(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m8-isvc", "group": "g0", "phase": "Idle"}
	var last float64
	for i := 0; i < 5; i++ {
		RecordGroupPhase("m8-isvc", "g0", "Idle")
		cur := counterFor("ome_omenative_rollout_group_phase_total", labels)
		if cur < last {
			t.Errorf("counter must be non-decreasing; got %v after %v", cur, last)
		}
		last = cur
	}
}

func TestGaugeDecrementOnLowerSet(t *testing.T) {
	labels := prometheus.Labels{"isvc": "m9-isvc", "component": "decoder"}
	SetPerRevisionServiceCount("m9-isvc", "decoder", 5)
	if v := counterFor("ome_omenative_per_revision_service_total", labels); v != 5 {
		t.Errorf("after Set(5): got %v want 5", v)
	}
	SetPerRevisionServiceCount("m9-isvc", "decoder", 2)
	if v := counterFor("ome_omenative_per_revision_service_total", labels); v != 2 {
		t.Errorf("decrement: got %v want 2", v)
	}
	SetPerRevisionServiceCount("m9-isvc", "decoder", 0)
	if v := counterFor("ome_omenative_per_revision_service_total", labels); v != 0 {
		t.Errorf("drop to zero: got %v want 0", v)
	}
}

func TestPhaseLabelMatchesEnumString(t *testing.T) {
	// Pin: dashboards keyed on the phase string break if the enum is
	// renamed. Force a deliberate decision by failing here when an
	// existing enum value stops being writable as-is.
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
		RecordGroupPhase(isvc, "0", string(p))
		labels := prometheus.Labels{"isvc": isvc, "group": "0", "phase": string(p)}
		if v := counterFor("ome_omenative_rollout_group_phase_total", labels); v <= 0 {
			t.Errorf("phase %q not writable as-is", p)
		}
	}
}

func TestEmptyArgsAreNoOp(t *testing.T) {
	RecordGroupPhase("", "g", "Surging")
	RecordGroupPhase("i", "", "Surging")
	RecordGroupPhase("i", "g", "")

	RecordGroupTransition("", "g", "a", "b")
	RecordGroupTransition("i", "", "a", "b")

	RecordGroupFailure("", "g", "X")
	RecordGroupFailure("i", "", "X")

	RecordRatioSkew("", "g")
	RecordRatioSkew("i", "")

	SetPerRevisionServiceCount("", "c", 1)
	SetPerRevisionServiceCount("i", "", 1)

	RecordMixedPairing("", "g")
	RecordMixedPairing("i", "")

	// Empty-isvc never lands a sample.
	labels := prometheus.Labels{"isvc": "", "group": "g", "phase": "Surging"}
	if v := counterFor("ome_omenative_rollout_group_phase_total", labels); v != 0 {
		t.Errorf("empty isvc must short-circuit; got %v", v)
	}
}
