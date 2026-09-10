package quotatree

import (
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// layout is the column set for one render. It is chosen once per invocation
// from two facts — whether the caller asked for wide output, and whether the
// tree spans more than one (resource, flavor) pair — and then applied to every
// node, so the table cannot grow a column halfway down.
type layout struct {
	wide bool

	// showPair adds the resource and flavor columns. Suppressed when the whole
	// tree uses one pair, which the caption states instead.
	showPair bool

	// split draws each budget's per-cluster shares as a tier below the node.
	split bool

	// now is what AGE is measured against, fixed for the whole render so two
	// rows of one table cannot disagree about the current time.
	now time.Time
}

func newLayout(wide, singlePair, split bool, now time.Time) layout {
	return layout{wide: wide, showPair: !singlePair, split: split, now: now}
}

// columnNames is the single description of the column set. The header and the
// padding of a row that stops early both read it, so a row can never be one
// cell out of step with the heading above it.
func (l layout) columnNames() []string {
	names := []string{"NAME", "ROLE"}
	if l.showPair {
		names = append(names, "RESOURCE", "FLAVOR")
	}
	if l.wide {
		names = append(names, "POLICY")
	}
	names = append(names, "NOMINAL", "ADMITTED")
	if l.wide {
		names = append(names,
			"BORROWED", "ALLOCATABLE", "PEAK",
			"READY", "DEGRADED", "MATERIALIZED",
			"TIER",
		)
	} else {
		names = append(names, "READY")
	}
	return append(names, "AGE")
}

func (l layout) header() row {
	names := l.columnNames()
	cells := make([]cell, 0, len(names))
	for _, n := range names {
		cells = append(cells, cell{text: n})
	}
	return row{cells: cells, style: styleBold}
}

// pad fills a row out to the full column set with empty cells, so a row that
// says nothing past a certain column still aligns with the rest of the table.
func (l layout) pad(cells []cell) []cell {
	n := len(l.columnNames())
	for len(cells) < n {
		cells = append(cells, cell{})
	}
	return cells
}

func (l layout) sectionRow(text string) row {
	return row{span: strings.ToUpper(text), style: styleYellow}
}

// nodeRows renders one node. A node carrying several budgets emits one row per
// (resource, flavor) pair, with the name drawn once and the rest carrying only
// the branch art: the alternative is summing across flavors, which would add up
// two different kinds of accelerator.
//
// Under each budget row come that budget's per-cluster shares. Every budget's
// shares belong to one sibling list below the node, so the tier's line is kept
// open until the last of them and the count of what is left to draw decides
// where it closes.
func (l layout) nodeRows(n *NodeView, art branches) []row {
	budgets := n.Budgets
	if len(budgets) == 0 {
		// A Cohort may carry no budget at all, and a filter can leave a node
		// with none of the selected pair. Either way the node still occupies a
		// position in the tree, so it gets a row of dashes rather than vanishing.
		budgets = []BudgetView{{}}
	}

	remaining := 0
	if l.split {
		for _, b := range budgets {
			remaining += len(b.PerCluster)
		}
	}

	rows := make([]row, 0, len(budgets)+remaining)
	for i, b := range budgets {
		if i == 0 {
			rows = append(rows, l.budgetRow(n, b, art.node, n.Name, true))
		} else {
			rows = append(rows, l.budgetRow(n, b, art.cont(remaining > 0), "", false))
		}
		if !l.split {
			continue
		}
		for _, c := range b.PerCluster {
			remaining--
			rows = append(rows, l.memberRow(n, b, c, art.tier(remaining == 0)))
		}
	}
	return rows
}

// memberRow draws one workload cluster's share of a budget.
//
// The columns describing the node rather than the share are left empty: a
// cluster holds no position in the quota tree, binds no namespaces, and has no
// conditions or age of its own. The policy is left to the budget row above,
// because one policy produced the whole split rather than any single share.
func (l layout) memberRow(n *NodeView, b BudgetView, c ClusterBudgetView, prefix string) row {
	cells := []cell{{prefix: prefix, prefixStyle: styleDim, text: c.Cluster}}
	cells = append(cells, dim(roleWorkloadCluster))
	if l.showPair {
		cells = append(cells, dim(orUnset(b.ResourceName)), dim(orUnset(b.ResourceFlavor)))
	}
	if l.wide {
		cells = append(cells, cell{})
	}
	cells = append(cells, quantityCell(c.Nominal), quantityCell(c.Admitted))
	if l.wide {
		cells = append(cells, quantityCell(c.Borrowed))
	}
	return row{cells: l.pad(cells), dim: n.contextDim()}
}

// roleWorkloadCluster is what a per-cluster row puts in the ROLE column. Not a
// role the API defines: the column says what a row is, and this row is one
// WorkloadCluster's share. Naming the kind is what keeps it from being read as
// a ClusterQueue, which a bare "Cluster" would not.
const roleWorkloadCluster = "WorkloadCluster"

// clusterRows draws a workload cluster's own row, one per (resource, flavor)
// pair it carries.
//
// The name is bold because on this axis the cluster is the top of its own tree,
// which is what bold marks. The columns describing a node are empty for the
// same reason they are on a per-cluster row one axis over: a cluster holds no
// position in the quota tree, and the policy that produced its shares belongs
// to the leaves below, not to the total.
func (l layout) clusterRows(c *ClusterView) []row {
	budgets := c.Budgets
	if len(budgets) == 0 {
		budgets = []BudgetView{{}}
	}

	rows := make([]row, 0, len(budgets))
	for i, b := range budgets {
		first := i == 0
		cells := []cell{{text: firstOnly(first, c.Name), style: styleBold}}
		cells = append(cells, dim(firstOnly(first, roleWorkloadCluster)))
		if l.showPair {
			cells = append(cells, text(orUnset(b.ResourceName)), text(orUnset(b.ResourceFlavor)))
		}
		if l.wide {
			cells = append(cells, cell{})
		}
		cells = append(cells, quantityCell(b.Nominal), quantityCell(b.Admitted))
		if l.wide {
			cells = append(cells, quantityCell(b.Borrowed))
		}
		rows = append(rows, row{cells: l.pad(cells)})
	}
	return rows
}

func (l layout) budgetRow(n *NodeView, b BudgetView, prefix, name string, first bool) row {
	// Branch art recedes so the names it leads to carry the eye down the tree.
	cells := []cell{{
		prefix:      prefix,
		prefixStyle: styleDim,
		text:        name,
		style:       nameStyle(n),
	}}
	cells = append(cells, dim(firstOnly(first, n.Role)))
	if l.showPair {
		cells = append(cells, text(orUnset(b.ResourceName)), text(orUnset(b.ResourceFlavor)))
	}
	if l.wide {
		// Per budget rather than per node, so it repeats down a node's flavors:
		// a leaf can be Explicit on a reserved flavor and Proportional on a
		// fungible one.
		cells = append(cells, text(orUnset(b.Policy)))
	}
	cells = append(cells, nominalCell(b), quantityCell(b.Admitted))

	if !l.wide {
		cells = append(cells, cell{
			text:  firstOnly(first, unhealthyReady(n)),
			style: conditionStyle(n, v1beta1.AcceleratorQuotaReady),
		}, l.ageCell(n, first))
		return row{cells: cells, dim: n.contextDim()}
	}

	observed := n.capacityFor(b.ResourceName, b.ResourceFlavor)
	cells = append(cells,
		quantityCell(b.Borrowed),
		quantityCell(observed.Allocatable),
		quantityCell(observed.HighWaterMark),
	)
	for _, ct := range conditionTypes {
		cells = append(cells, cell{
			text:  firstOnly(first, n.condition(ct)),
			style: conditionStyle(n, ct),
		})
	}
	cells = append(cells,
		text(firstOnly(first, orUnset(n.PriorityTier))),
		l.ageCell(n, first),
	)
	return row{cells: cells, dim: n.contextDim()}
}

// ageCell renders how long the node has existed, in kubectl's own abbreviated
// form so the two tables read alike. It recedes: age is the least of what a row
// says, and it is the same width on every row, so drawn plain it would pull the
// eye across the table for nothing.
func (l layout) ageCell(n *NodeView, first bool) cell {
	if n.CreationTimestamp.IsZero() {
		return dim(firstOnly(first, unset))
	}
	return dim(firstOnly(first, duration.HumanDuration(l.now.Sub(n.CreationTimestamp.Time))))
}

// nameStyle picks how a node's name is drawn.
//
// Only a node that broke an invariant itself is colored. A frozen node merely
// loses the bold that a live one carries, because freezing propagates to every
// descendant: a violation on the root freezes the whole fleet, and coloring all
// of them would bury the single node that caused it under its own blast radius.
// Cause is loud, consequence is quiet.
func nameStyle(n *NodeView) style {
	switch {
	case n.Violation != nil:
		return styleRed
	case n.FrozenBy != nil:
		return styleNone
	default:
		return styleBold
	}
}

// contextDim reports whether the whole row should recede as filter context.
// A node that broke an invariant never does, even when the filter kept it only
// as somebody else's ancestor: dimming the one node at fault would be the
// filter concealing a fault, which is the thing the view must never do.
func (n *NodeView) contextDim() bool { return n.Context && n.Violation == nil }

// conditionStyle colors a condition cell by what it means rather than by its
// literal value: Degraded=True is bad where Ready=True is good.
func conditionStyle(n *NodeView, condType string) style {
	status, ok := n.Conditions[condType]
	if !ok {
		return styleDim
	}
	good := "True"
	if condType == v1beta1.AcceleratorQuotaDegraded {
		good = "False"
	}
	switch {
	case status == good:
		return styleGreen
	case status == "Unknown":
		return styleYellow
	default:
		return styleRed
	}
}

// text is an unstyled cell, except that a placeholder dash recedes: it marks
// the absence of a value and should not read as one.
func text(s string) cell {
	if s == unset {
		return cell{text: s, style: styleDim}
	}
	return cell{text: s}
}

func dim(s string) cell { return cell{text: s, style: styleDim} }

func quantityCell(q *resource.Quantity) cell { return text(quantity(q)) }

// nominalCell renders the allowance, marked where the number needs qualifying.
// Both marks are drawn in color as well, so they read at a glance and still
// survive a copy-paste into a plain terminal.
//
// The two cannot collide: a split is only ever resolved alongside the status
// nominal it divides, so a value read from spec has none.
func nominalCell(b BudgetView) cell {
	if b.Nominal == nil {
		return text(unset)
	}
	if b.FromSpec {
		return cell{text: b.Nominal.String() + "*", style: styleYellow}
	}
	if _, whole := splitTotal(b); !whole {
		return cell{text: b.Nominal.String() + "!", style: styleRed}
	}
	return cell{text: b.Nominal.String()}
}

// unhealthyReady is the default view's Ready cell: blank when the node is
// Ready. The table exists to surface problems, so a healthy fleet renders as a
// column of nothing and an unhealthy node is the only thing in it.
func unhealthyReady(n *NodeView) string {
	if s, ok := n.Conditions[v1beta1.AcceleratorQuotaReady]; ok {
		if s == "True" {
			return ""
		}
		return s
	}
	return unset
}

func (n *NodeView) condition(t string) string {
	if s, ok := n.Conditions[t]; ok {
		return s
	}
	return unset
}

func (n *NodeView) capacityFor(resourceName, flavor string) CapacityView {
	for _, c := range n.Capacity {
		if c.ResourceName == resourceName && c.ResourceFlavor == flavor {
			return c
		}
	}
	return CapacityView{}
}

func quantity(q *resource.Quantity) string {
	if q == nil {
		return unset
	}
	return q.String()
}

func orUnset(s string) string {
	if s == "" {
		return unset
	}
	return s
}

// firstOnly blanks a per-node value on a budget continuation row, so the value
// is not repeated once per flavor.
func firstOnly(first bool, s string) string {
	if !first {
		return ""
	}
	return s
}
