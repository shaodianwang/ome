package obsmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// counterFor fetches the sample matching labels from a CounterVec by name
// from the controller-runtime registry. Returns 0 when the metric exists
// but the label combo has no sample yet, and -1 only when Gather errors.
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
		}
	}
	return 0
}

// histogramFor fetches the (sampleCount, sampleSum) of the histogram
// sample matching labels. Returns (0,0) when no sample yet.
func histogramFor(metricName string, labels prometheus.Labels) (uint64, float64) {
	g, err := metrics.Registry.Gather()
	if err != nil {
		return 0, 0
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
			if !matched || m.Histogram == nil {
				continue
			}
			return m.Histogram.GetSampleCount(), m.Histogram.GetSampleSum()
		}
	}
	return 0, 0
}

func TestRecordStatusUpdate_IncrementsPerResult(t *testing.T) {
	for _, result := range []string{ResultSuccess, ResultConflict, ResultNotFound, ResultError} {
		labels := prometheus.Labels{"controller": ControllerISVC, "result": result}
		start := counterFor("ome_isvc_status_update_total", labels)
		RecordStatusUpdate(ControllerISVC, result)
		RecordStatusUpdate(ControllerISVC, result)
		if end := counterFor("ome_isvc_status_update_total", labels); end-start != 2 {
			t.Errorf("result %q: got delta %v want 2", result, end-start)
		}
	}
}

func TestRecordStatusUpdate_ControllerLabelIsolated(t *testing.T) {
	ir := prometheus.Labels{"controller": ControllerIR, "result": ResultConflict}
	start := counterFor("ome_isvc_status_update_total", ir)
	RecordStatusUpdate(ControllerIR, ResultConflict)
	if end := counterFor("ome_isvc_status_update_total", ir); end-start != 1 {
		t.Errorf("IR conflict: got delta %v want 1", end-start)
	}
	// The IR increment must not bleed into the ISVC series.
	isvc := prometheus.Labels{"controller": ControllerISVC, "result": ResultConflict}
	before := counterFor("ome_isvc_status_update_total", isvc)
	RecordStatusUpdate(ControllerIR, ResultConflict)
	if after := counterFor("ome_isvc_status_update_total", isvc); after != before {
		t.Errorf("IR write must not touch ISVC series; got %v want %v", after, before)
	}
}

func TestRecordStatusUpdate_EmptyArgsNoOp(t *testing.T) {
	RecordStatusUpdate("", ResultSuccess)
	RecordStatusUpdate(ControllerISVC, "")
	// Empty-label combos never land a sample.
	if v := counterFor("ome_isvc_status_update_total", prometheus.Labels{"controller": "", "result": ResultSuccess}); v != 0 {
		t.Errorf("empty controller must short-circuit; got %v", v)
	}
}

func TestRecordRolloutPhaseDuration_Observes(t *testing.T) {
	labels := prometheus.Labels{"phase": "Surging"}
	startCount, startSum := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels)
	RecordRolloutPhaseDuration("Surging", 2.5)
	RecordRolloutPhaseDuration("Surging", 1.5)
	cnt, sum := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels)
	if cnt-startCount != 2 {
		t.Errorf("count delta: got %v want 2", cnt-startCount)
	}
	if d := sum - startSum; d < 3.99 || d > 4.01 {
		t.Errorf("sum delta: got %v want ~4.0", d)
	}
}

func TestRecordRolloutPhaseDuration_NonPositiveDropped(t *testing.T) {
	labels := prometheus.Labels{"phase": "Draining"}
	startCount, _ := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels)
	RecordRolloutPhaseDuration("Draining", 0)
	RecordRolloutPhaseDuration("Draining", -1)
	RecordRolloutPhaseDuration("", 5)
	if cnt, _ := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels); cnt != startCount {
		t.Errorf("non-positive/empty must be dropped; count moved from %v to %v", startCount, cnt)
	}
}

func TestRecordPodCreateToReady_Observes(t *testing.T) {
	labels := prometheus.Labels{"component": "engine"}
	startCount, startSum := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels)
	RecordPodCreateToReady("engine", 10)
	cnt, sum := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels)
	if cnt-startCount != 1 {
		t.Errorf("count delta: got %v want 1", cnt-startCount)
	}
	if d := sum - startSum; d < 9.99 || d > 10.01 {
		t.Errorf("sum delta: got %v want ~10.0", d)
	}
}

func TestRecordPodCreateToReady_NonPositiveDropped(t *testing.T) {
	labels := prometheus.Labels{"component": "decoder"}
	startCount, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels)
	RecordPodCreateToReady("decoder", 0)
	RecordPodCreateToReady("decoder", -3)
	RecordPodCreateToReady("", 5)
	if cnt, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels); cnt != startCount {
		t.Errorf("non-positive/empty must be dropped; count moved from %v to %v", startCount, cnt)
	}
}
