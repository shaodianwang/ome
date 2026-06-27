// Package metricsobs reads the controller-runtime + OME prometheus
// collectors after a harness run and folds them into the recorder's
// summary shapes. It exists because three of the OMENative perf signals
// are not observable from the apiserver — they live only in the in-process
// prometheus registry the controllers increment:
//
//   - ome_isvc_status_update_total{controller,result}: the 409
//     status-conflict counter (the OMENative status-churn hotspot).
//   - controller_runtime_reconcile_total{controller,result}: reconcile
//     churn per controller.
//   - workqueue_adds_total / workqueue_depth {name,controller}: enqueue
//     churn and residual queue depth per controller.
//
// All collectors register into sigs.k8s.io/controller-runtime/pkg/metrics
// .Registry at package init, so the runner gathers from that single
// registry. Counters are read as absolute totals; the runner snapshots
// reconcile totals twice (post-convergence, a few seconds apart) to derive
// the steady-state idle reconcile rate — the storm guard.
//
// Nothing here bakes a behavioral value: the metric NAMES are the
// controller-runtime / OME wire contract (constants the collectors are
// registered under), not tunables, and every sampling duration is a runner
// flag.
package metricsobs

import (
	"fmt"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/tests/perf/omenative/runner/recorder"
)

// Metric family names. These are the wire-contract identifiers the
// collectors are registered under (controller-runtime internals + OME's
// obsmetrics package); they are not behavioral knobs.
const (
	statusUpdateTotalName  = "ome_isvc_status_update_total"
	reconcileTotalName     = "controller_runtime_reconcile_total"
	workqueueAddsTotalName = "workqueue_adds_total"
	workqueueDepthName     = "workqueue_depth"

	// Label keys on those families.
	labelController = "controller"
	labelResult     = "result"

	// Result label value classifying a 409-exhausted status write. Mirrors
	// obsmetrics.ResultConflict; redeclared (not imported) so this reader
	// stays a thin registry consumer with no controller-package dependency.
	resultConflict = "conflict"
)

// StatusUpdates reads ome_isvc_status_update_total and folds it into a
// per-controller {total, conflict} map. Empty when the counter was never
// incremented (no status writes happened).
func StatusUpdates() (map[string]*recorder.StatusUpdateSummary, error) {
	fams, err := gather()
	if err != nil {
		return nil, err
	}
	out := map[string]*recorder.StatusUpdateSummary{}
	fam := fams[statusUpdateTotalName]
	if fam == nil {
		return out, nil
	}
	for _, m := range fam.GetMetric() {
		ctrl := labelValue(m, labelController)
		if ctrl == "" {
			continue
		}
		su, ok := out[ctrl]
		if !ok {
			su = &recorder.StatusUpdateSummary{}
			out[ctrl] = su
		}
		v := int64(m.GetCounter().GetValue())
		su.Total += v
		if labelValue(m, labelResult) == resultConflict {
			su.Conflict += v
		}
	}
	return out, nil
}

// Churn reads controller_runtime_reconcile_total + workqueue_adds_total +
// workqueue_depth and folds them into a per-controller ChurnSummary,
// keyed by the `controller` label (shared across all three families).
func Churn() (map[string]*recorder.ChurnSummary, error) {
	fams, err := gather()
	if err != nil {
		return nil, err
	}
	out := map[string]*recorder.ChurnSummary{}
	get := func(ctrl string) *recorder.ChurnSummary {
		c, ok := out[ctrl]
		if !ok {
			c = &recorder.ChurnSummary{}
			out[ctrl] = c
		}
		return c
	}

	if fam := fams[reconcileTotalName]; fam != nil {
		for _, m := range fam.GetMetric() {
			if ctrl := labelValue(m, labelController); ctrl != "" {
				get(ctrl).ReconcileTotal += int64(m.GetCounter().GetValue())
			}
		}
	}
	if fam := fams[workqueueAddsTotalName]; fam != nil {
		for _, m := range fam.GetMetric() {
			if ctrl := labelValue(m, labelController); ctrl != "" {
				get(ctrl).WorkqueueAdds += int64(m.GetCounter().GetValue())
			}
		}
	}
	if fam := fams[workqueueDepthName]; fam != nil {
		for _, m := range fam.GetMetric() {
			if ctrl := labelValue(m, labelController); ctrl != "" {
				get(ctrl).WorkqueueDepth += int64(m.GetGauge().GetValue())
			}
		}
	}
	return out, nil
}

// TotalReconciles returns the sum of controller_runtime_reconcile_total
// across every controller + result. Used to derive the idle reconcile
// rate from two snapshots taken a known interval apart.
func TotalReconciles() (int64, error) {
	fams, err := gather()
	if err != nil {
		return 0, err
	}
	var total int64
	if fam := fams[reconcileTotalName]; fam != nil {
		for _, m := range fam.GetMetric() {
			total += int64(m.GetCounter().GetValue())
		}
	}
	return total, nil
}

// gather pulls the current metric families from the controller-runtime
// registry, keyed by family name for O(1) lookup.
func gather() (map[string]*dto.MetricFamily, error) {
	raw, err := metrics.Registry.Gather()
	if err != nil {
		return nil, fmt.Errorf("gather prometheus registry: %w", err)
	}
	out := make(map[string]*dto.MetricFamily, len(raw))
	for _, f := range raw {
		out[f.GetName()] = f
	}
	return out, nil
}

// labelValue returns the value of label key on metric m, or "".
func labelValue(m *dto.Metric, key string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key {
			return strings.TrimSpace(lp.GetValue())
		}
	}
	return ""
}
