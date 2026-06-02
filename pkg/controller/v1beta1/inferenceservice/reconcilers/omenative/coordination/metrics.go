package coordination

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Coordination layer Prometheus metrics. Registered with the
// controller-runtime metrics registry at package init so the operator's
// /metrics endpoint surfaces them without additional wiring.
var (
	groupPhaseTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_rollout_group_phase_total",
			Help: "Count of reconciles per (isvc, group, phase) for OMENative coordination groups.",
		},
		[]string{"isvc", "group", "phase"},
	)

	groupTransitionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_rollout_group_transition_total",
			Help: "Count of phase transitions per (isvc, group, from, to) for OMENative coordination groups.",
		},
		[]string{"isvc", "group", "from", "to"},
	)

	groupFailureTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_rollout_group_failure_total",
			Help: "Count of group entries into the Failed phase per (isvc, group, reason).",
		},
		[]string{"isvc", "group", "reason"},
	)

	ratioSkewTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ratio_skew_total",
			Help: "Count of reconciles that rejected a surge to avoid cross-Component ratio skew.",
		},
		[]string{"isvc", "group"},
	)

	perRevisionServiceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_per_revision_service_total",
			Help: "Number of live per-revision Services per (isvc, component).",
		},
		[]string{"isvc", "component"},
	)

	mixedPairingTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_mixed_pairing_total",
			Help: "Count of distinct cross-revision pairings observed in a RollingUpdate group.",
		},
		[]string{"isvc", "group"},
	)

	autoMigrationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_auto_migrations_total",
			Help: "Count of auto-triggered migrations the controller initiated to recover an Instance stuck past MigrationPolicy.ProgressDeadlineSeconds, per (isvc, component, reason).",
		},
		[]string{"isvc", "component", "reason"},
	)

	ratioGateBypassedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ratio_gate_bypassed_total",
			Help: "Number of times CheckRatioGate was bypassed for an in-place strategy because the drain is paired 1:1 with a same-pod return.",
		},
		[]string{"component", "strategy"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		groupPhaseTotal,
		groupTransitionTotal,
		groupFailureTotal,
		ratioSkewTotal,
		perRevisionServiceGauge,
		mixedPairingTotal,
		autoMigrationsTotal,
		ratioGateBypassedTotal,
	)
}

// RecordAutoMigrationTriggered increments the auto-migration counter
// when the controller writes a synthetic migration ledger entry to
// recover an Instance stuck past MigrationPolicy.ProgressDeadlineSeconds.
func RecordAutoMigrationTriggered(isvc, component, reason string) {
	if isvc == "" || component == "" || reason == "" {
		return
	}
	autoMigrationsTotal.WithLabelValues(isvc, component, reason).Inc()
}

// RecordGroupPhase increments the phase counter for one reconcile.
// Callers invoke this once per (group, reconcile).
func RecordGroupPhase(isvc, group, phase string) {
	if isvc == "" || group == "" || phase == "" {
		return
	}
	groupPhaseTotal.WithLabelValues(isvc, group, phase).Inc()
}

// RecordGroupTransition increments the transition counter when a
// group's phase changes between reconciles.
func RecordGroupTransition(isvc, group, from, to string) {
	if isvc == "" || group == "" || from == to {
		return
	}
	groupTransitionTotal.WithLabelValues(isvc, group, from, to).Inc()
}

// RecordGroupFailure increments the failure counter when a group
// enters the Failed phase.
func RecordGroupFailure(isvc, group, reason string) {
	if isvc == "" || group == "" {
		return
	}
	groupFailureTotal.WithLabelValues(isvc, group, reason).Inc()
}

// RecordRatioSkew increments the ratio-skew counter when RatioBalanced
// pacing refuses a surge.
func RecordRatioSkew(isvc, group string) {
	if isvc == "" || group == "" {
		return
	}
	ratioSkewTotal.WithLabelValues(isvc, group).Inc()
}

// SetPerRevisionServiceCount updates the gauge tracking the number of
// live per-revision Services for one (isvc, component).
func SetPerRevisionServiceCount(isvc, component string, count float64) {
	if isvc == "" || component == "" {
		return
	}
	perRevisionServiceGauge.WithLabelValues(isvc, component).Set(count)
}

// RecordMixedPairing increments the mixed-pairing counter when a
// RollingUpdate group sees a distinct revision-pair combination.
func RecordMixedPairing(isvc, group string) {
	if isvc == "" || group == "" {
		return
	}
	mixedPairingTotal.WithLabelValues(isvc, group).Inc()
}

// RecordRatioGateBypassed increments the bypass counter when the
// OMENative ops dispatcher skips CheckRatioGate for an in-place
// update strategy (InPlaceIfPossible / InPlaceOnly). See
// EventReasonRatioGateBypassed for the rationale.
func RecordRatioGateBypassed(component, strategy string) {
	if component == "" || strategy == "" {
		return
	}
	ratioGateBypassedTotal.WithLabelValues(component, strategy).Inc()
}
