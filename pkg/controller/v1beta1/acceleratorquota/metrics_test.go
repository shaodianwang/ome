package acceleratorquota

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/quota/capacity"
)

// Unit tests for the collectors registered at package init in metrics.go.
// Nothing here needs envtest or a reconciler: the recorders are pure functions
// of a capacity.Result or an int, which is the whole reason instrumentation
// lives at the controller call site rather than inside pkg/quota.

// sampleFor reads one sample out of the controller-runtime registry by metric
// name and label subset. Returns 0 when the family exists but no sample
// matches, and -1 only when Gather itself fails, so a missing metric is
// distinguishable from a metric sitting at zero.
func sampleFor(t *testing.T, name string, labels prometheus.Labels) float64 {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		return -1
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for _, l := range m.GetLabel() {
				if want, ok := labels[l.GetName()]; ok && want != l.GetValue() {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}

// The registration itself is the contract the scrape path depends on: a metric
// that is declared but never registered is served by nothing, which is the
// failure this whole package exists to stop repeating.
func TestMetricsAreRegistered(t *testing.T) {
	recordCapacity(capacity.Result{
		Capacities: []capacity.Capacity{{
			ResourceName: "example.com/accelerator", ResourceFlavor: "flavor-a",
			Allocatable: qty("1"),
		}},
		Unattributed: []capacity.Unattributed{{
			Node: "node-a", ResourceName: "example.com/accelerator",
			Quantity: qty("1"), Reason: capacity.ReasonNoFlavor,
		}},
	})
	recordApplied(1)
	recordSwept(SweepTriggerMaterialize, 1)

	for _, name := range []string{
		"ome_quota_capacity_allocatable",
		"ome_quota_capacity_unavailable",
		"ome_quota_capacity_nodes",
		"ome_quota_capacity_unattributed",
		"ome_quota_capacity_unattributed_nodes",
		"ome_quota_backend_applied_total",
		"ome_quota_backend_swept_total",
	} {
		families, err := ctrlmetrics.Registry.Gather()
		if err != nil {
			t.Fatalf("gathering: %v", err)
		}
		found := false
		for _, mf := range families {
			if mf.GetName() == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s is not registered in the controller-runtime registry", name)
		}
	}
}

func TestRecordCapacitySplitsSchedulableFromParked(t *testing.T) {
	recordCapacity(capacity.Result{Capacities: []capacity.Capacity{{
		ResourceName:     "example.com/gpu",
		ResourceFlavor:   "flavor-split",
		Allocatable:      qty("24"),
		Unavailable:      qty("8"),
		Nodes:            3,
		UnavailableNodes: 1,
	}}})

	l := prometheus.Labels{"resource": "example.com/gpu", "flavor": "flavor-split"}
	if got := sampleFor(t, "ome_quota_capacity_allocatable", l); got != 24 {
		t.Errorf("allocatable chips = %v, want 24", got)
	}
	if got := sampleFor(t, "ome_quota_capacity_unavailable", l); got != 8 {
		t.Errorf("unavailable chips = %v, want 8", got)
	}

	sched := prometheus.Labels{"resource": "example.com/gpu", "flavor": "flavor-split", "state": NodeStateSchedulable}
	if got := sampleFor(t, "ome_quota_capacity_nodes", sched); got != 3 {
		t.Errorf("schedulable nodes = %v, want 3", got)
	}
	parked := prometheus.Labels{"resource": "example.com/gpu", "flavor": "flavor-split", "state": NodeStateUnavailable}
	if got := sampleFor(t, "ome_quota_capacity_nodes", parked); got != 1 {
		t.Errorf("unavailable nodes = %v, want 1", got)
	}
}

// Unattributed carries one entry per (node, resource). Chips sum across those
// entries; nodes must not, or a node advertising two unclaimed resources is
// counted twice and the fleet looks bigger than it is.
func TestRecordCapacityCountsNodesOnceAcrossResources(t *testing.T) {
	recordCapacity(capacity.Result{Unattributed: []capacity.Unattributed{
		{Node: "dual", ResourceName: "example.com/a", Quantity: qty("4"), Reason: capacity.ReasonNoFlavor},
		{Node: "dual", ResourceName: "example.com/b", Quantity: qty("2"), Reason: capacity.ReasonNoFlavor},
		{Node: "single", ResourceName: "example.com/a", Quantity: qty("4"), Reason: capacity.ReasonNoFlavor},
	}})

	if got := sampleFor(t, "ome_quota_capacity_unattributed_nodes",
		prometheus.Labels{"reason": capacity.ReasonNoFlavor}); got != 2 {
		t.Errorf("unattributed nodes = %v, want 2 (dual counted once)", got)
	}
	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/a", "reason": capacity.ReasonNoFlavor}); got != 8 {
		t.Errorf("unattributed chips for resource a = %v, want 8 (summed across nodes)", got)
	}
}

// The two reasons must not be merged: a missing ResourceFlavor and an ambiguous
// one need different fixes, and the whole point of the metric is to say which.
func TestRecordCapacitySeparatesUnattributedReasons(t *testing.T) {
	recordCapacity(capacity.Result{Unattributed: []capacity.Unattributed{
		{Node: "n1", ResourceName: "example.com/r", Quantity: qty("1"), Reason: capacity.ReasonNoFlavor},
		{Node: "n2", ResourceName: "example.com/r", Quantity: qty("5"), Reason: capacity.ReasonAmbiguous},
	}})

	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/r", "reason": capacity.ReasonNoFlavor}); got != 1 {
		t.Errorf("no-flavor chips = %v, want 1", got)
	}
	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/r", "reason": capacity.ReasonAmbiguous}); got != 5 {
		t.Errorf("ambiguous chips = %v, want 5", got)
	}
}

// A flavor being added has to drive the series it used to own back to zero. A
// gauge that merely stops updating keeps reporting unattributed chips on a
// cluster that has just been fixed.
func TestRecordCapacityClearsResolvedSeries(t *testing.T) {
	recordCapacity(capacity.Result{Unattributed: []capacity.Unattributed{
		{Node: "n1", ResourceName: "example.com/transient", Quantity: qty("9"), Reason: capacity.ReasonNoFlavor},
	}})
	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/transient", "reason": capacity.ReasonNoFlavor}); got != 9 {
		t.Fatalf("precondition: chips = %v, want 9", got)
	}

	// The operator adds the flavor; the next pass attributes everything.
	recordCapacity(capacity.Result{Capacities: []capacity.Capacity{{
		ResourceName: "example.com/transient", ResourceFlavor: "now-claimed", Allocatable: qty("9"),
	}}})

	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/transient", "reason": capacity.ReasonNoFlavor}); got != 0 {
		t.Errorf("chips after the flavor was added = %v, want the series gone", got)
	}
}

// A sweep that finds nothing is the steady state. Recording it would make a
// counter that never moves indistinguishable from one whose sweep never ran.
func TestRecordSweptIgnoresEmptySweeps(t *testing.T) {
	l := prometheus.Labels{"trigger": SweepTriggerFinalize}
	before := sampleFor(t, "ome_quota_backend_swept_total", l)

	recordSwept(SweepTriggerFinalize, 0)
	if got := sampleFor(t, "ome_quota_backend_swept_total", l); got != before {
		t.Errorf("an empty sweep moved the counter: %v -> %v", before, got)
	}

	recordSwept(SweepTriggerFinalize, 3)
	if got := sampleFor(t, "ome_quota_backend_swept_total", l); got != before+3 {
		t.Errorf("swept counter = %v, want %v", got, before+3)
	}
}

// The two triggers answer different questions -- routine orphan collection
// versus a node being deleted -- so they must not share a series.
func TestRecordSweptSeparatesTriggers(t *testing.T) {
	mat := prometheus.Labels{"trigger": SweepTriggerMaterialize}
	fin := prometheus.Labels{"trigger": SweepTriggerFinalize}
	beforeMat := sampleFor(t, "ome_quota_backend_swept_total", mat)
	beforeFin := sampleFor(t, "ome_quota_backend_swept_total", fin)

	recordSwept(SweepTriggerMaterialize, 2)

	if got := sampleFor(t, "ome_quota_backend_swept_total", mat); got != beforeMat+2 {
		t.Errorf("materialize sweeps = %v, want %v", got, beforeMat+2)
	}
	if got := sampleFor(t, "ome_quota_backend_swept_total", fin); got != beforeFin {
		t.Errorf("a materialize sweep leaked into the finalize series: %v -> %v", beforeFin, got)
	}
}

func TestRecordAppliedIgnoresEmptyPasses(t *testing.T) {
	before := sampleFor(t, "ome_quota_backend_applied_total", nil)

	recordApplied(0)
	if got := sampleFor(t, "ome_quota_backend_applied_total", nil); got != before {
		t.Errorf("an empty pass moved the counter: %v -> %v", before, got)
	}

	recordApplied(7)
	if got := sampleFor(t, "ome_quota_backend_applied_total", nil); got != before+7 {
		t.Errorf("applied counter = %v, want %v", got, before+7)
	}
}
