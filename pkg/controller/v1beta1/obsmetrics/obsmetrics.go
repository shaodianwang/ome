// Package obsmetrics holds cross-controller scale-operability Prometheus
// collectors shared by the ISVC reconciler, the IR reconciler, the
// OMENative coordination layer, and the workload op dispatcher. It is a
// leaf package (only depends on prometheus + controller-runtime metrics)
// so every controller can wire it without import cycles. Collectors are
// registered with the controller-runtime registry at package init so the
// operator's /metrics endpoint surfaces them with no extra wiring.
//
// Cardinality is deliberately controlled: metrics avoid pod-level labels.
// The pod startup histogram and active scale-down gauges carry the stable ISVC
// identity so operators can compare workloads and locate deferred work. This
// assumes tens to low hundreds of ISVCs per cluster; pod identities are
// excluded because fleets may contain thousands of pods and pod names churn
// across rollouts.
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
	// Histogram buckets affect exported measurement resolution only; they do
	// not tune controller behavior. Pod-cost buckets cover large fleet changes,
	// while duration buckets retain resolution from one second through
	// multi-hour drains.
	scaleDownBatchPodBuckets = prometheus.ExponentialBuckets(1, 2, 12)
	scaleDownDurationBuckets = prometheus.ExponentialBuckets(1, 2, 15)

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
	// the moment the Instance was first promoted to Ready.
	podCreateToReadySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ome_omenative_pod_create_to_ready_seconds",
			Help: "Wall-clock seconds from the earliest pod's creation to the Instance first reaching Ready, labeled by namespace, ISVC, and component.",
			// Upstream Prometheus default buckets (see above).
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "isvc", "component"},
	)

	scaleDownBatchPods = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ome_omenative_scale_down_batch_pods",
			Help:    "Pod-equivalent cost selected in one OMENative scale-down wave, labeled by component.",
			Buckets: scaleDownBatchPodBuckets,
		},
		[]string{"component"},
	)

	scaleDownActivePods = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_scale_down_active_pods",
			Help: "Current Pod-equivalent cost selected for OMENative scale-down, labeled by namespace, ISVC, and component.",
		},
		[]string{"namespace", "isvc", "component"},
	)

	scaleDownDeferredInstances = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_scale_down_deferred_instances",
			Help: "Eligible OMENative scale-down Instances outside the active prefix, labeled by namespace, ISVC, and component.",
		},
		[]string{"namespace", "isvc", "component"},
	)

	scaleDownInstanceDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ome_omenative_scale_down_instance_duration_seconds",
			Help:    "Wall-clock seconds from OMENative Delete admission to confirmed Instance status removal, labeled by component.",
			Buckets: scaleDownDurationBuckets,
		},
		[]string{"component"},
	)

	scaleDownOversizedBatchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_scale_down_oversized_batch_total",
			Help: "Count of OMENative scale-down waves that admit one indivisible Instance above the configured Pod budget, labeled by component.",
		},
		[]string{"component"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		statusUpdateTotal,
		rolloutPhaseDurationSeconds,
		podCreateToReadySeconds,
		scaleDownBatchPods,
		scaleDownActivePods,
		scaleDownDeferredInstances,
		scaleDownInstanceDurationSeconds,
		scaleDownOversizedBatchTotal,
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
// Instance promotion. Non-positive durations and empty identity labels are
// dropped.
func RecordPodCreateToReady(namespace, isvc, component string, seconds float64) {
	if namespace == "" || isvc == "" || component == "" || seconds <= 0 {
		return
	}
	podCreateToReadySeconds.WithLabelValues(namespace, isvc, component).Observe(seconds)
}

// RecordScaleDownBatchPods observes the Pod-equivalent cost admitted into one
// scale-down wave. A wave always has positive cost; invalid observations and
// empty component labels are dropped.
func RecordScaleDownBatchPods(component string, pods int32) {
	if component == "" || pods <= 0 {
		return
	}
	scaleDownBatchPods.WithLabelValues(component).Observe(float64(pods))
}

// SetScaleDownActivePods publishes the current active scale-down wave cost.
// Zero resets a converged IR's series; negative values and incomplete identity
// labels are dropped.
func SetScaleDownActivePods(namespace, isvc, component string, pods int32) {
	if !validScaleDownIdentity(namespace, isvc, component) || pods < 0 {
		return
	}
	scaleDownActivePods.WithLabelValues(namespace, isvc, component).Set(float64(pods))
}

// SetScaleDownDeferredInstances publishes the eligible candidate count outside
// the active scale-down prefix. Zero resets a converged IR's series; negative
// values and incomplete identity labels are dropped.
func SetScaleDownDeferredInstances(namespace, isvc, component string, instances int) {
	if !validScaleDownIdentity(namespace, isvc, component) || instances < 0 {
		return
	}
	scaleDownDeferredInstances.WithLabelValues(namespace, isvc, component).Set(float64(instances))
}

// DeleteScaleDownSeries removes per-IR gauges when their owning IR disappears.
// Histograms and counters have bounded component-only cardinality and remain.
func DeleteScaleDownSeries(namespace, isvc, component string) {
	if !validScaleDownIdentity(namespace, isvc, component) {
		return
	}
	scaleDownActivePods.DeleteLabelValues(namespace, isvc, component)
	scaleDownDeferredInstances.DeleteLabelValues(namespace, isvc, component)
}

// RecordScaleDownInstanceDuration observes a completed Instance's time from
// durable Delete admission to confirmed status removal. Zero is valid for
// podless work completed within the timestamp precision; negative durations
// and empty component labels are dropped.
func RecordScaleDownInstanceDuration(component string, seconds float64) {
	if component == "" || seconds < 0 {
		return
	}
	scaleDownInstanceDurationSeconds.WithLabelValues(component).Observe(seconds)
}

// RecordScaleDownOversizedBatch increments once when an indivisible Instance
// is admitted above the configured Pod budget.
func RecordScaleDownOversizedBatch(component string) {
	if component == "" {
		return
	}
	scaleDownOversizedBatchTotal.WithLabelValues(component).Inc()
}

func validScaleDownIdentity(namespace, isvc, component string) bool {
	return namespace != "" && isvc != "" && component != ""
}
