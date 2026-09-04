package acceleratorquota

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/quota/capacity"
)

// Collectors for the numbers the capacity and materialization passes already
// compute. Every one is observed at the controller call site that produced it,
// so pkg/quota stays a library with no metrics dependency of its own.
//
// None of these carry a plane label. Capacity derivation and materialization
// both run in workload mode only -- r.Capacity and r.Materialize are populated
// on that branch alone -- so a mode label could only ever hold one value, and a
// constant label is a column that costs cardinality and answers nothing.
//
// The capacity gauges are a full snapshot of the last pass, so each is Reset
// before it is repopulated: a flavor being added has to drive the series it
// used to own back to zero, and a stale series that merely stops updating
// would read as a fleet that still has unattributed chips.
var (
	capacityAllocatable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_allocatable",
		Help: "Allocatable accelerator chips on nodes that could accept work at the last capacity pass, by resource and ResourceFlavor. Allocatable, not capacity, so it will not reconcile against kube_node_status_capacity.",
	}, []string{"resource", "flavor"})

	capacityUnavailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_unavailable",
		Help: "Accelerator chips on nodes that matched a ResourceFlavor but were cordoned or not Ready. Budgets are sized against allocatable, so this is the parked remainder.",
	}, []string{"resource", "flavor"})

	capacityNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_nodes",
		Help: "Nodes behind the chip totals, by resource, ResourceFlavor and whether they can accept work. This is what separates a one-machine shortfall from a rack.",
	}, []string{"resource", "flavor", "state"})

	capacityUnattributed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_unattributed",
		Help: "Allocatable accelerator chips on nodes that no ResourceFlavor claims, so they contribute zero capacity to every budget. Non-zero means a missing or ambiguous ResourceFlavor, not a shrinking fleet.",
	}, []string{"resource", "reason"})

	capacityUnattributedNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_unattributed_nodes",
		Help: "Distinct nodes whose accelerators could not be attributed to a ResourceFlavor. Counts nodes, not node-resource pairs, so a node advertising two unclaimed resources is counted once.",
	}, []string{"reason"})

	backendAppliedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ome_quota_backend_applied_total",
		Help: "Kueue objects written by the materializer. Writes are server-side applies issued unconditionally, so this tracks pass frequency times render size, not how often the tree changed.",
	})

	backendSweptTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_quota_backend_swept_total",
		Help: "Kueue objects reaped as orphans, by what triggered the sweep: a materialization pass finding objects the tree no longer names, or a node being deleted.",
	}, []string{"trigger"})
)

// Sweep triggers. Stable strings -- dashboards and alerts key off them.
const (
	// SweepTriggerMaterialize is a sweep at the end of a materialization pass,
	// removing objects the current tree no longer names.
	SweepTriggerMaterialize = "materialize"
	// SweepTriggerFinalize is a sweep while releasing a deleted node's
	// finalizer, removing that node's own objects.
	SweepTriggerFinalize = "finalize"
)

// Node states behind a capacity total. Stable strings.
const (
	// NodeStateSchedulable counts nodes that could accept work.
	NodeStateSchedulable = "schedulable"
	// NodeStateUnavailable counts nodes that matched the flavor but were
	// cordoned or not Ready.
	NodeStateUnavailable = "unavailable"
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		capacityAllocatable,
		capacityUnavailable,
		capacityNodes,
		capacityUnattributed,
		capacityUnattributedNodes,
		backendAppliedTotal,
		backendSweptTotal,
	)
}

// recordCapacity publishes one whole capacity pass.
//
// Takes the full result rather than the two slices separately, because the
// gauges are a snapshot: partial publication would leave the unattributed
// series describing one pass and the allocatable series another.
func recordCapacity(observed capacity.Result) {
	capacityAllocatable.Reset()
	capacityUnavailable.Reset()
	capacityNodes.Reset()
	capacityUnattributed.Reset()
	capacityUnattributedNodes.Reset()

	for _, c := range observed.Capacities {
		capacityAllocatable.WithLabelValues(c.ResourceName, c.ResourceFlavor).
			Set(c.Allocatable.AsApproximateFloat64())
		capacityUnavailable.WithLabelValues(c.ResourceName, c.ResourceFlavor).
			Set(c.Unavailable.AsApproximateFloat64())
		capacityNodes.WithLabelValues(c.ResourceName, c.ResourceFlavor, NodeStateSchedulable).
			Set(float64(c.Nodes))
		capacityNodes.WithLabelValues(c.ResourceName, c.ResourceFlavor, NodeStateUnavailable).
			Set(float64(c.UnavailableNodes))
	}

	// Unattributed carries one entry per (node, resource). Chips sum across
	// those entries, but nodes must not: a node advertising two unclaimed
	// resources is one node, and counting entries would double it.
	chips := map[[2]string]float64{}
	nodes := map[string]map[string]struct{}{}
	for _, u := range observed.Unattributed {
		chips[[2]string{u.ResourceName, u.Reason}] += u.Quantity.AsApproximateFloat64()
		if nodes[u.Reason] == nil {
			nodes[u.Reason] = map[string]struct{}{}
		}
		nodes[u.Reason][u.Node] = struct{}{}
	}
	for key, total := range chips {
		capacityUnattributed.WithLabelValues(key[0], key[1]).Set(total)
	}
	for reason, set := range nodes {
		capacityUnattributedNodes.WithLabelValues(reason).Set(float64(len(set)))
	}
}

// recordApplied counts the objects one materialization pass wrote.
func recordApplied(objects int) {
	if objects <= 0 {
		return
	}
	backendAppliedTotal.Add(float64(objects))
}

// recordSwept counts the objects one sweep reaped. A sweep that finds nothing
// is the steady state, so it is not recorded -- the counter would otherwise be
// indistinguishable from one that never ran.
func recordSwept(trigger string, objects int) {
	if objects <= 0 {
		return
	}
	backendSweptTotal.WithLabelValues(trigger).Add(float64(objects))
}
