package canary

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

var (
	canaryPhaseTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_phase_total",
		Help: "Count of canary reconcile passes by resulting RolloutPhase.",
	}, []string{"isvc", "phase"})

	canaryStepTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_step_total",
		Help: "Count of canary step advances per (isvc, component).",
	}, []string{"isvc", "component"})

	canaryCurrentStep = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_canary_current_step",
		Help: "Zero-based index of the active canary step per (isvc, component); 0 when no canary is in progress.",
	}, []string{"isvc", "component"})

	canaryTrafficWeight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_canary_traffic_weight",
		Help: "External traffic weight currently programmed for the canary revision per (isvc, component).",
	}, []string{"isvc", "component"})

	canaryCompleteTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_complete_total",
		Help: "Count of canary rollouts that reached a terminal state (Stable after promote or rollback).",
	}, []string{"isvc"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(canaryPhaseTotal, canaryStepTotal, canaryCurrentStep, canaryTrafficWeight, canaryCompleteTotal)
}

// recordDispatch emits canary metrics + Events for one reconcile pass. The
// counters increment only on edges (a step advance, a completion), the gauges
// are set idempotently, so re-reconciles of an unchanged step do not inflate.
func recordDispatch(rec record.EventRecorder, isvc *v1beta1.InferenceService, c v1beta1.ComponentType, res *Result) {
	if res == nil || !res.Active {
		return
	}
	// A completed canary keeps its status at the done sentinel (CurrentStep ==
	// len(steps)), so the step/weight gauges + step Event read live status here.
	cs := isvc.Status.Canary
	phase := isvc.Status.Components[c].RolloutPhase
	canaryPhaseTotal.WithLabelValues(isvc.Name, string(phase)).Inc()
	if cs != nil {
		canaryCurrentStep.WithLabelValues(isvc.Name, string(c)).Set(float64(cs.CurrentStep))
		canaryTrafficWeight.WithLabelValues(isvc.Name, string(c)).Set(float64(cs.ObservedTrafficWeight))
		if res.Stepped {
			canaryStepTotal.WithLabelValues(isvc.Name, string(c)).Inc()
			if rec != nil {
				// Report only the new step index — the new step's traffic weight is
				// programmed on the next reconcile, so pairing it with the
				// currently-programmed (previous step's) weight would mislead.
				rec.Eventf(isvc, corev1.EventTypeNormal, EventReasonCanaryStepAdvanced,
					"canary advanced to step %d", cs.CurrentStep)
			}
		}
	}
	if res.Complete {
		canaryCompleteTotal.WithLabelValues(isvc.Name).Inc()
		// Reset the in-progress gauges: the canary is done, no active step/split.
		canaryCurrentStep.WithLabelValues(isvc.Name, string(c)).Set(0)
		canaryTrafficWeight.WithLabelValues(isvc.Name, string(c)).Set(0)
		if rec != nil {
			rec.Eventf(isvc, corev1.EventTypeNormal, EventReasonCanaryCompleted,
				"canary rollout completed; %s is now stable at 100%%", c)
		}
	}
}
