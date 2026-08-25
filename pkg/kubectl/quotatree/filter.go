package quotatree

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// roleClusterQueue is the leaf role as it appears in a NodeView.
var roleClusterQueue = string(v1beta1.AcceleratorQuotaRoleClusterQueue)

// Filter narrows what a View renders. The zero value keeps everything.
type Filter struct {
	// Node re-roots the render at one node, which may be an orphan. Empty
	// renders the whole set.
	Node string

	// LeavesOnly keeps the nodes that carry a budget.
	LeavesOnly bool

	// Namespace keeps the leaves binding this serving namespace. A namespace
	// charges exactly one leaf fleet-wide, so a result with two is a
	// NamespaceConflict the violation list also reports.
	Namespace string

	// ResourceName and ResourceFlavor narrow which budgets are shown. They
	// filter rows within a node, never which nodes appear: a Cohort carrying no
	// budget for the pair is still the parent of leaves that do.
	ResourceName   string
	ResourceFlavor string
}

// Empty reports whether the filter would keep the view unchanged.
func (f Filter) Empty() bool {
	return f.Node == "" && !f.LeavesOnly && f.Namespace == "" &&
		f.ResourceName == "" && f.ResourceFlavor == ""
}

// Apply narrows v in place and returns it.
//
// Node selection prunes rather than flattens: a kept node drags its ancestors
// along, so the answer to "which leaf charges namespace X" arrives with the path
// that leads to it. Ancestors kept only as context are marked so the renderer
// can dim them.
//
// Returns an error only when a selector matches nothing, which is a question
// answered "no" rather than an empty tree that reads like a healthy one.
func (f Filter) Apply(v *View) error {
	if v == nil || f.Empty() {
		return nil
	}

	if f.Node != "" {
		found := findNode(v, f.Node)
		if found == nil {
			return fmt.Errorf("no AcceleratorQuota named %q in the assembled tree", f.Node)
		}
		v.Root, v.Orphans = found, nil
	}

	if f.ResourceName != "" || f.ResourceFlavor != "" {
		eachNode(v, func(n *NodeView) { f.narrowBudgets(n) })
	}

	keep := f.nodePredicate()
	if keep != nil {
		matched := false
		if v.Root != nil {
			v.Root = prune(v.Root, keep, &matched)
		}
		kept := v.Orphans[:0]
		for _, o := range v.Orphans {
			if p := prune(o, keep, &matched); p != nil {
				kept = append(kept, p)
			}
		}
		v.Orphans = kept
		if !matched {
			return errors.New(f.noMatchMessage())
		}
	}
	return nil
}

// nodePredicate is the node-level selector, or nil when the filter selects on
// budgets only.
func (f Filter) nodePredicate() func(*NodeView) bool {
	switch {
	case f.Namespace != "":
		// Only a leaf binds namespaces, so the namespace selector subsumes
		// LeavesOnly rather than intersecting with it.
		return func(n *NodeView) bool { return bindsNamespace(n, f.Namespace) }
	case f.LeavesOnly:
		return func(n *NodeView) bool { return n.Role == roleClusterQueue }
	default:
		return nil
	}
}

func (f Filter) noMatchMessage() string {
	if f.Namespace != "" {
		return fmt.Sprintf("no AcceleratorQuota leaf binds namespace %q", f.Namespace)
	}
	return "no AcceleratorQuota leaf matched"
}

// narrowBudgets drops the budget and capacity rows outside the selected
// (resource, flavor) pair. An empty selector half matches every value.
func (f Filter) narrowBudgets(n *NodeView) {
	budgets := n.Budgets[:0]
	for _, b := range n.Budgets {
		if matches(f.ResourceName, b.ResourceName) && matches(f.ResourceFlavor, b.ResourceFlavor) {
			budgets = append(budgets, b)
		}
	}
	n.Budgets = budgets

	capacity := n.Capacity[:0]
	for _, c := range n.Capacity {
		if matches(f.ResourceName, c.ResourceName) && matches(f.ResourceFlavor, c.ResourceFlavor) {
			capacity = append(capacity, c)
		}
	}
	n.Capacity = capacity
}

func matches(selector, value string) bool { return selector == "" || selector == value }

func bindsNamespace(n *NodeView, ns string) bool {
	for _, have := range n.Namespaces {
		if have == ns {
			return true
		}
	}
	return false
}

// prune keeps n when it matches or when any descendant does, and reports
// through matched whether anything matched at all. A node kept only because a
// descendant matched is marked Context so the renderer can show it as the path
// to the answer rather than part of it.
func prune(n *NodeView, keep func(*NodeView) bool, matched *bool) *NodeView {
	children := n.Children[:0]
	for _, c := range n.Children {
		if p := prune(c, keep, matched); p != nil {
			children = append(children, p)
		}
	}
	n.Children = children

	self := keep(n)
	if self {
		*matched = true
		n.Context = false
		return n
	}
	if len(n.Children) > 0 {
		n.Context = true
		return n
	}
	return nil
}

// findNode locates a node anywhere in the view, tree or orphan.
func findNode(v *View, name string) *NodeView {
	var found *NodeView
	eachNode(v, func(n *NodeView) {
		if found == nil && n.Name == name {
			found = n
		}
	})
	return found
}

// eachNode visits every node in the view, parents before children.
func eachNode(v *View, fn func(*NodeView)) {
	var walk func(*NodeView)
	walk = func(n *NodeView) {
		fn(n)
		for _, c := range n.Children {
			walk(c)
		}
	}
	if v.Root != nil {
		walk(v.Root)
	}
	for _, o := range v.Orphans {
		walk(o)
	}
}

// visibleNodes is the set of node names a filtered view still renders. The
// violation list is deliberately not filtered alongside the nodes — a view that
// hid "this tree contains a cycle" because the cycle is outside the current
// --node would be a diagnostic tool concealing a fault. The renderer uses this
// to show the in-view violations in full and count the rest.
func visibleNodes(v *View) map[string]bool {
	names := map[string]bool{}
	eachNode(v, func(n *NodeView) { names[n.Name] = true })
	return names
}

// SortedNamespaces renders a leaf's namespaces for a single cell.
func SortedNamespaces(n *NodeView) string {
	if len(n.Namespaces) == 0 {
		return ""
	}
	ns := append([]string(nil), n.Namespaces...)
	sort.Strings(ns)
	return strings.Join(ns, ",")
}
