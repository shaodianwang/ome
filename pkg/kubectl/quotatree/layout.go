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

	// now is what AGE is measured against, fixed for the whole render so two
	// rows of one table cannot disagree about the current time.
	now time.Time
}

func newLayout(wide, singlePair bool, now time.Time) layout {
	return layout{wide: wide, showPair: !singlePair, now: now}
}

func (l layout) header() row {
	names := []string{"NAME", "ROLE"}
	if l.showPair {
		names = append(names, "RESOURCE", "FLAVOR")
	}
	names = append(names, "NOMINAL", "ADMITTED")
	if l.wide {
		names = append(names,
			"BORROWED", "ALLOCATABLE", "PEAK",
			"READY", "DEGRADED", "MATERIALIZED",
			"TIER", "NAMESPACES", "PATH",
		)
	} else {
		names = append(names, "READY")
	}
	names = append(names, "AGE")

	cells := make([]cell, 0, len(names))
	for _, n := range names {
		cells = append(cells, cell{text: n})
	}
	return row{cells: cells, style: styleBold}
}

func (l layout) sectionRow(text string) row {
	return row{span: strings.ToUpper(text), style: styleYellow}
}

// nodeRows renders one node. A node carrying several budgets emits one row per
// (resource, flavor) pair, with the name drawn once and the rest carrying only
// the branch art: the alternative is summing across flavors, which would add up
// two different kinds of accelerator.
//
// prefix is the branch art for the node's own row; cont is the art for its
// continuation rows, which sit between the node and its children and so must
// keep both the ancestors' lines and the node's own line open.
func (l layout) nodeRows(n *NodeView, prefix, cont string) []row {
	budgets := n.Budgets
	if len(budgets) == 0 {
		// A Cohort may carry no budget at all, and a filter can leave a node
		// with none of the selected pair. Either way the node still occupies a
		// position in the tree, so it gets a row of dashes rather than vanishing.
		budgets = []BudgetView{{}}
	}

	rows := make([]row, 0, len(budgets))
	for i, b := range budgets {
		if i == 0 {
			rows = append(rows, l.budgetRow(n, b, prefix, n.Name, true))
			continue
		}
		rows = append(rows, l.budgetRow(n, b, cont, "", false))
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
		text(firstOnly(first, orUnset(SortedNamespaces(n)))),
		dim(firstOnly(first, orUnset(n.Path))),
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

// nominalCell renders the allowance. A value with no resolved counterpart in
// status is drawn in the provisional color as well as marked, so it reads as
// unsettled at a glance and still survives a copy-paste into a plain terminal.
func nominalCell(b BudgetView) cell {
	if b.Nominal == nil {
		return text(unset)
	}
	if b.FromSpec {
		return cell{text: b.Nominal.String() + "*", style: styleYellow}
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
