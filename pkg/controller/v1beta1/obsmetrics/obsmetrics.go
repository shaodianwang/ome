// Package obsmetrics holds cross-controller scale-operability Prometheus
// collectors shared by the ISVC reconciler, the IR reconciler, the
// OMENative coordination layer, and the workload op dispatcher. It is a
// leaf package (only depends on prometheus + controller-runtime metrics)
// so every controller can wire it without import cycles. Collectors are
// registered with the controller-runtime registry at package init so the
// operator's /metrics endpoint surfaces them with no extra wiring.
//
// Cardinality is deliberately controlled: the histograms carry only a
// component/result-style label (NOT per-isvc) so a 6000-pod, 10-cluster
// fleet does not explode the series count. The status-update counter
// keys on (controller, result) only.
package obsmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Status-update result label values for RecordStatusUpdate. Stable
// strings — dashboards and alerts key off them, so they must not change
// silently if the call-site classification is refactored.
const (
	// ResultSuccess is recorded when the status write committed.
	ResultSuccess = "success"
	// ResultConflict is recorded when RetryOnConflict exhausted its
	// budget on a 409 — the known OMENative status-churn hotspot.
	ResultConflict = "conflict"
	// ResultNotFound is recorded when the object vanished mid-flush
	// (raced a delete); the write is dropped cleanly, not an error.
	ResultNotFound = "notfound"
	// ResultError is recorded for any other terminal write failure.
	ResultError = "error"
)

// Controller label values for RecordStatusUpdate.
const (
	// ControllerISVC tags the parent InferenceService status flush.
	ControllerISVC = "isvc"
	// ControllerIR tags the per-Component InferenceReplica status flush.
	ControllerIR = "ir"
)

var (
	statusUpdateTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_isvc_status_update_total",
			Help: "Count of status-update attempts by controller and terminal result (success|conflict|notfound|error). Surfaces the OMENative 409 conflict hotspot.",
		},
		[]string{"controller", "result"},
	)

	// rolloutPhaseDurationSeconds records how long an OMENative
	// coordination group dwelt in a phase before transitioning out of
	// it. Labeled by the phase being left (NOT per-isvc) to keep
	// cardinality bounded by the small coordination-phase enum.
	rolloutPhaseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ome_omenative_rollout_phase_duration_seconds",
			Help: "Wall-clock seconds a coordination group spent in a phase before transitioning out, labeled by the phase being left.",
			// Upstream Prometheus default buckets — a well-known shared
			// constant, not an OME-baked tuning value. Bucket layout is a
			// dashboard concern, not a behavioral one (it never changes a
			// deployment outcome, timeout, or limit).
			Buckets: prometheus.DefBuckets,
		},
		[]string{"phase"},
	)

	// podCreateToReadySeconds records pod-create-to-ready latency for an
	// Instance: the gap between the earliest pod's creation timestamp and
	// the moment the Instance was first promoted to Ready. Labeled by
	// component only (NOT per-isvc) to bound cardinality.
	podCreateToReadySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ome_omenative_pod_create_to_ready_seconds",
			Help: "Wall-clock seconds from the earliest pod's creation to the Instance first reaching Ready, labeled by component.",
			// Upstream Prometheus default buckets (see above).
			Buckets: prometheus.DefBuckets,
		},
		[]string{"component"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		statusUpdateTotal,
		rolloutPhaseDurationSeconds,
		podCreateToReadySeconds,
	)
}

// RecordStatusUpdate increments the status-update result counter for one
// terminal status-write outcome. Empty args are a no-op so a misclassified
// call site never lands a blank-label series.
func RecordStatusUpdate(controller, result string) {
	if controller == "" || result == "" {
		return
	}
	statusUpdateTotal.WithLabelValues(controller, result).Inc()
}

// RecordRolloutPhaseDuration observes how long a coordination group spent
// in the phase it is leaving. Non-positive durations and empty phase
// labels are dropped so a clock skew or unobserved-prior-phase never
// pollutes the histogram.
func RecordRolloutPhaseDuration(phase string, seconds float64) {
	if phase == "" || seconds <= 0 {
		return
	}
	rolloutPhaseDurationSeconds.WithLabelValues(phase).Observe(seconds)
}

// RecordPodCreateToReady observes pod-create-to-ready latency for an
// Instance promotion. Non-positive durations and empty component labels
// are dropped.
func RecordPodCreateToReady(component string, seconds float64) {
	if component == "" || seconds <= 0 {
		return
	}
	podCreateToReadySeconds.WithLabelValues(component).Observe(seconds)
}
