package quotatree

import (
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

// View is one rendering of a quota tree: the nodes reachable from the display
// root, the nodes that have no position in the tree, and every invariant the
// set violates.
//
// The three are kept together deliberately. A node whose parentRef does not
// resolve is absent from the tree yet present in the cluster, and it is usually
// the node being looked for; dropping it would make the plugin agree with a
// broken tree instead of exposing it.
type View struct {
	// Root is the node the render is anchored at. Nil when no node claimed the
	// reserved root name and no --node was given, which is itself a finding.
	Root *NodeView `json:"root,omitempty"`

	// Orphans are the nodes with no position in the tree, each rendered with
	// whatever subtree hangs off it. Sorted by name.
	Orphans []*NodeView `json:"orphans,omitempty"`

	// Violations is every invariant the set breaks, in the order
	// pkg/quota/tree reports them: by node, then by which cause to act on first.
	Violations []ViolationView `json:"violations,omitempty"`
}

// NodeView is one node as rendered. Quantities stay resource.Quantity so JSON
// output matches how the same numbers appear on the CR.
type NodeView struct {
	Name string `json:"name"`
	Role string `json:"role"`

	// Path is the root-to-node path the controller computed, empty on a node
	// with no position in the tree.
	Path string `json:"path,omitempty"`

	// Depth is the distance from the root in edges, -1 when unreachable.
	Depth int `json:"depth"`

	// Parent is the declared parent name, present even when it does not
	// resolve — that gap is the diagnosis.
	Parent string `json:"parent,omitempty"`

	PriorityTier string   `json:"priorityTier,omitempty"`
	Namespaces   []string `json:"namespaces,omitempty"`

	// Budgets is the union of spec and status budgets, keyed by
	// (resource, flavor). See BudgetView.FromSpec.
	Budgets []BudgetView `json:"budgets,omitempty"`

	// Capacity is observed capacity, reported on the root node only.
	Capacity []CapacityView `json:"capacity,omitempty"`

	// Conditions maps condition type to status for the conditions that are set.
	Conditions map[string]string `json:"conditions,omitempty"`

	// Violation is the node's own primary violation.
	Violation *ViolationView `json:"violation,omitempty"`

	// FrozenBy is the ancestor violation that freezes this node when it has no
	// violation of its own. Set from tree.Frozen, so a subtree below a broken
	// parent reads as one cause rather than many symptoms.
	FrozenBy *ViolationView `json:"frozenBy,omitempty"`

	// Context marks a node retained by a filter only because a descendant
	// matched. It is the path to the answer, not part of it, and the renderer
	// dims it accordingly.
	Context bool `json:"context,omitempty"`

	Children []*NodeView `json:"children,omitempty"`
}

// BudgetView is one (resource, flavor) pair's numbers.
type BudgetView struct {
	ResourceName   string `json:"resourceName"`
	ResourceFlavor string `json:"resourceFlavor"`

	// Nominal is the allowance. Prefer the controller-resolved value in status;
	// fall back to spec when no status entry exists for this pair.
	Nominal *resource.Quantity `json:"nominal,omitempty"`

	// FromSpec reports that Nominal was read from spec because status.budgets
	// holds no entry for this pair. On a management plane the two differ: spec
	// is the fleet total, status is what was resolved for this plane. Rendering
	// them identically without saying so would report a fleet-wide number as
	// though it were this plane's resolved share.
	//
	// It says nothing about when a resolved value will appear. status.budgets
	// is written by whatever resolves budgets on the plane being read, and a
	// plane where nothing does will report FromSpec indefinitely — a steady
	// state, not a pending one.
	FromSpec bool `json:"fromSpec,omitempty"`

	// EffectiveCeiling is the relaxed ceiling a bursting leaf can reach. Set
	// only when a borrowingLimit is configured.
	EffectiveCeiling *resource.Quantity `json:"effectiveCeiling,omitempty"`

	Admitted *resource.Quantity `json:"admitted,omitempty"`
	Borrowed *resource.Quantity `json:"borrowed,omitempty"`
}

// CapacityView is observed capacity for one (resource, flavor) pair.
type CapacityView struct {
	ResourceName   string             `json:"resourceName"`
	ResourceFlavor string             `json:"resourceFlavor"`
	Allocatable    *resource.Quantity `json:"allocatable,omitempty"`
	HighWaterMark  *resource.Quantity `json:"highWaterMark,omitempty"`
}

// ViolationView is one broken invariant, flattened for output.
type ViolationView struct {
	Node    string `json:"node,omitempty"`
	Reason  string `json:"reason"`
	Subject string `json:"subject,omitempty"`
	Message string `json:"message"`
}

// conditionTypes are rendered in the order an operator triages them: whether
// the node is usable, then why not, then whether its output is current.
var conditionTypes = []string{
	v1beta1.AcceleratorQuotaReady,
	v1beta1.AcceleratorQuotaDegraded,
	v1beta1.AcceleratorQuotaMaterialized,
}

// BuildView turns an assembled tree and its violations into a renderable view.
//
// Every node in t appears exactly once: reachable ones under Root, the rest
// under Orphans. Nodes inside a parentRef cycle are reachable from each other
// but not from the root, so they surface as orphans with their cycle rendered
// once — the walk stops at the first repeat rather than looping.
func BuildView(t *tree.Tree, vs tree.Violations) *View {
	v := &View{Violations: violationViews(vs)}
	if t == nil {
		return v
	}

	own := make(map[string]tree.Violation, len(vs))
	for _, name := range vs.Nodes() {
		own[name] = vs.For(name).Primary()
	}
	frozen := t.Frozen(vs)

	// A node is emitted at most once even though cycles make the child edges
	// re-enter themselves.
	seen := map[string]bool{}
	var build func(*tree.Node) *NodeView
	build = func(n *tree.Node) *NodeView {
		if seen[n.Name()] {
			return nil
		}
		seen[n.Name()] = true

		nv := newNodeView(n)
		if o, ok := own[n.Name()]; ok {
			nv.Violation = violationView(o)
		} else if f, ok := frozen[n.Name()]; ok {
			nv.FrozenBy = violationView(f)
		}
		for _, c := range n.Children {
			if cv := build(c); cv != nil {
				nv.Children = append(nv.Children, cv)
			}
		}
		return nv
	}

	if t.Root != nil {
		v.Root = build(t.Root)
	}
	for _, n := range t.Nodes() {
		if seen[n.Name()] {
			continue
		}
		if ov := build(n); ov != nil {
			v.Orphans = append(v.Orphans, ov)
		}
	}
	return v
}

func newNodeView(n *tree.Node) *NodeView {
	q := n.Quota
	nv := &NodeView{
		Name:         n.Name(),
		Role:         string(n.Role()),
		Path:         n.Path,
		Depth:        n.Depth,
		PriorityTier: q.Spec.PriorityTier,
		Namespaces:   append([]string(nil), q.Spec.Namespaces...),
		Budgets:      budgetViews(q),
		Capacity:     capacityViews(q),
	}
	if q.Spec.ParentRef != nil {
		nv.Parent = q.Spec.ParentRef.Name
	}
	for _, ct := range conditionTypes {
		if c := meta.FindStatusCondition(q.Status.Conditions, ct); c != nil {
			if nv.Conditions == nil {
				nv.Conditions = map[string]string{}
			}
			nv.Conditions[ct] = string(c.Status)
		}
	}
	return nv
}

// budgetViews merges spec and status budgets on the (resource, flavor) key.
//
// Presence of a status entry, not whether its nominal is non-zero, decides
// which side wins: a leaf deliberately budgeted zero is a real state, and
// treating it as "unresolved" would silently show the fleet total instead.
func budgetViews(q *v1beta1.AcceleratorQuota) []BudgetView {
	type key struct{ resourceName, flavor string }

	views := map[key]*BudgetView{}
	order := []key{}
	at := func(k key) *BudgetView {
		if bv, ok := views[k]; ok {
			return bv
		}
		bv := &BudgetView{ResourceName: k.resourceName, ResourceFlavor: k.flavor}
		views[k] = bv
		order = append(order, k)
		return bv
	}

	for i := range q.Spec.Budgets {
		b := &q.Spec.Budgets[i]
		bv := at(key{b.ResourceName, b.ResourceFlavor})
		bv.Nominal = quantityPtr(b.Nominal)
		bv.FromSpec = true
	}
	for i := range q.Status.Budgets {
		b := &q.Status.Budgets[i]
		bv := at(key{b.ResourceName, b.ResourceFlavor})
		bv.Nominal = quantityPtr(b.Nominal)
		bv.FromSpec = false
		bv.Admitted = quantityPtr(b.Admitted)
		bv.Borrowed = quantityPtr(b.Borrowed)
		if b.EffectiveCeiling != nil {
			bv.EffectiveCeiling = quantityPtr(*b.EffectiveCeiling)
		}
	}

	sort.Slice(order, func(i, j int) bool {
		if order[i].resourceName != order[j].resourceName {
			return order[i].resourceName < order[j].resourceName
		}
		return order[i].flavor < order[j].flavor
	})
	out := make([]BudgetView, 0, len(order))
	for _, k := range order {
		out = append(out, *views[k])
	}
	return out
}

func capacityViews(q *v1beta1.AcceleratorQuota) []CapacityView {
	if len(q.Status.Capacity) == 0 {
		return nil
	}
	out := make([]CapacityView, 0, len(q.Status.Capacity))
	for i := range q.Status.Capacity {
		c := &q.Status.Capacity[i]
		out = append(out, CapacityView{
			ResourceName:   c.ResourceName,
			ResourceFlavor: c.ResourceFlavor,
			Allocatable:    quantityPtr(c.Allocatable),
			HighWaterMark:  quantityPtr(c.HighWaterMark),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ResourceName != out[j].ResourceName {
			return out[i].ResourceName < out[j].ResourceName
		}
		return out[i].ResourceFlavor < out[j].ResourceFlavor
	})
	return out
}

func quantityPtr(q resource.Quantity) *resource.Quantity {
	c := q.DeepCopy()
	return &c
}

func violationView(v tree.Violation) *ViolationView {
	return &ViolationView{Node: v.Node, Reason: v.Reason, Subject: v.Subject, Message: v.Message}
}

func violationViews(vs tree.Violations) []ViolationView {
	if len(vs) == 0 {
		return nil
	}
	out := make([]ViolationView, 0, len(vs))
	for _, v := range vs {
		out = append(out, *violationView(v))
	}
	return out
}
