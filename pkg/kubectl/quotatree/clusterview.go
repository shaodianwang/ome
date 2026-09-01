package quotatree

import (
	"errors"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ViewAxis selects what the render is anchored on.
type ViewAxis string

const (
	// ViewTenant anchors on the quota tree, the shape an admin authored.
	ViewTenant ViewAxis = "tenant"

	// ViewCluster anchors on the fleet: one workload cluster at the top of each
	// tree, carrying its slice of the same numbers.
	ViewCluster ViewAxis = "cluster"
)

// ParseViewAxis validates a --view value.
func ParseViewAxis(s string) (ViewAxis, error) {
	switch ViewAxis(s) {
	case ViewTenant, ViewCluster:
		return ViewAxis(s), nil
	default:
		return "", errors.New("invalid --view " + s + ": want tenant or cluster")
	}
}

// ClusterView is one workload cluster's slice of the quota tree.
type ClusterView struct {
	Name string `json:"name"`

	// Budgets is what this cluster carries in total: the leaf shares below it,
	// added up per (resource, flavor). Derived here rather than read from any
	// object — nothing publishes a per-cluster total.
	Budgets []BudgetView `json:"budgets,omitempty"`

	// Nodes is the top of this cluster's slice, each with the subtree that
	// holds a share here.
	Nodes []*NodeView `json:"nodes,omitempty"`
}

// ByCluster re-anchors a view on the fleet, in place.
//
// The same numbers read along the other axis. A tenant-anchored tree answers
// "where does this budget live"; this answers "what is this cluster carrying",
// which is the question a capacity owner has and the one the tree shape buries:
// a cluster hosting none of a tenant shows up only as a row absent several
// tiers down.
//
// Derived entirely from status.budgets[].perCluster, so it reads nothing the
// plugin does not already list. That is also its limit — a WorkloadCluster
// registered but holding no share anywhere is in no budget and so appears
// nowhere here.
func ByCluster(v *View) error {
	if v == nil {
		return nil
	}

	var clusters []*ClusterView
	for _, name := range clusterNames(v) {
		c := &ClusterView{Name: name}
		for _, top := range anchorsOf(v.Root) {
			if sliced := sliceFor(top, name); sliced != nil {
				c.Nodes = append(c.Nodes, sliced)
			}
		}
		if len(c.Nodes) == 0 {
			continue
		}
		c.Budgets = rollUp(c.Nodes)
		clusters = append(clusters, c)
	}
	if len(clusters) == 0 {
		return errors.New("no AcceleratorQuota reports a per-cluster split, so there is " +
			"no fleet to anchor on; only a management plane resolves one")
	}

	// Root and Clusters are the same tree read two ways, so exactly one of them
	// is ever set. Orphans are left alone: a node with no position in the tree
	// has no position in a cluster's slice of it either, and it is a fault that
	// has to stay visible whichever axis is in use.
	v.Clusters, v.Root = clusters, nil
	return nil
}

// clusterNames is every cluster the view mentions a share on, sorted.
func clusterNames(v *View) []string {
	seen := map[string]bool{}
	eachNode(v, func(n *NodeView) {
		for _, b := range n.Budgets {
			for _, c := range b.PerCluster {
				seen[c.Cluster] = true
			}
		}
	})
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// anchorsOf is where a cluster's slice starts.
//
// The reserved root is dropped: it sits under every cluster and says the same
// thing each time, so repeating it costs a row per cluster and carries nothing.
// A view re-rooted by --node is kept, because that node is what the caller
// asked to be shown.
func anchorsOf(root *NodeView) []*NodeView {
	switch {
	case root == nil:
		return nil
	case root.Name == v1beta1.AcceleratorQuotaRootName:
		return root.Children
	default:
		return []*NodeView{root}
	}
}

// sliceFor keeps the part of a subtree holding a share on one cluster, with
// every budget rewritten to that cluster's numbers.
//
// A node with no share of its own survives when a descendant has one, the same
// way an ancestor survives --for-namespace: it is the path to the answer. The
// copy is fresh, so the tenant-anchored view it was taken from is unchanged and
// a caller may still render both.
func sliceFor(n *NodeView, cluster string) *NodeView {
	cp := *n
	cp.Children = nil
	for _, child := range n.Children {
		if sliced := sliceFor(child, cluster); sliced != nil {
			cp.Children = append(cp.Children, sliced)
		}
	}

	cp.Budgets = sharesOn(n.Budgets, cluster)
	if len(cp.Budgets) == 0 {
		if len(cp.Children) == 0 {
			return nil
		}
		// A Cohort is apportioned nothing: it travels to a member as pure
		// topology. What its tenants hold between them on this cluster is the
		// only figure that means anything on this axis, and a row of dashes at
		// every tier would bury the ones that do.
		cp.Budgets = rollUp(cp.Children)
	}
	return &cp
}

// sharesOn is one node's budgets as one cluster sees them.
//
// The per-cluster breakdown is dropped from the result rather than carried: on
// this axis the cluster is already named by the tree the row sits in, and a
// second copy of it below every leaf would restate the anchor.
func sharesOn(budgets []BudgetView, cluster string) []BudgetView {
	var out []BudgetView
	for _, b := range budgets {
		for _, s := range b.PerCluster {
			if s.Cluster != cluster {
				continue
			}
			out = append(out, BudgetView{
				ResourceName:   b.ResourceName,
				ResourceFlavor: b.ResourceFlavor,
				Policy:         b.Policy,
				Nominal:        s.Nominal,
				Admitted:       s.Admitted,
				Borrowed:       s.Borrowed,
			})
		}
	}
	return out
}

// rollUp totals the leaf shares below a set of nodes, per (resource, flavor).
//
// Leaves only. A Cohort's own row on this axis is itself a roll-up, so counting
// both tiers would double every number on the way up.
//
// Never summed across flavors, for the reason the tree never is: two flavors
// are two different kinds of accelerator and their total is not a quantity of
// anything.
func rollUp(nodes []*NodeView) []BudgetView {
	type key struct{ resourceName, flavor string }
	totals := map[key]*BudgetView{}
	var order []key

	var walk func(*NodeView)
	walk = func(n *NodeView) {
		if len(n.Children) > 0 {
			for _, c := range n.Children {
				walk(c)
			}
			return
		}
		for _, b := range n.Budgets {
			k := key{b.ResourceName, b.ResourceFlavor}
			t, ok := totals[k]
			if !ok {
				t = &BudgetView{
					ResourceName:   k.resourceName,
					ResourceFlavor: k.flavor,
					Nominal:        &resource.Quantity{},
					Admitted:       &resource.Quantity{},
					Borrowed:       &resource.Quantity{},
				}
				totals[k] = t
				order = append(order, k)
			}
			addTo(t.Nominal, b.Nominal)
			addTo(t.Admitted, b.Admitted)
			addTo(t.Borrowed, b.Borrowed)
		}
	}
	for _, n := range nodes {
		walk(n)
	}

	sort.Slice(order, func(i, j int) bool {
		if order[i].resourceName != order[j].resourceName {
			return order[i].resourceName < order[j].resourceName
		}
		return order[i].flavor < order[j].flavor
	})
	out := make([]BudgetView, 0, len(order))
	for _, k := range order {
		out = append(out, *totals[k])
	}
	return out
}

func addTo(total *resource.Quantity, add *resource.Quantity) {
	if add != nil {
		total.Add(*add)
	}
}
