package projection

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/quota/shares"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

// Capacity is one cluster's allocatable for one (resource, flavor) pair.
//
// It comes from that member's own root node, which its local controller derives
// from the hardware actually there. The hub measures nothing itself — it has no
// credential to read a member's nodes and no business holding one.
type Capacity struct {
	Cluster        string
	ResourceName   string
	ResourceFlavor string
	Allocatable    resource.Quantity
}

// ResolveOptions carry the fleet-level defaults a node may fall back on.
type ResolveOptions struct {
	// DefaultPolicy applies to a budget whose node declares no distribution.
	// Empty is not a policy: a node with nowhere left to fall back to is
	// reported unresolved rather than split by a guess.
	DefaultPolicy v1beta1.AcceleratorQuotaDistributionPolicy
}

// Resolution is one pass of the split.
type Resolution struct {
	// ByCluster is what to project, keyed by cluster name.
	ByCluster map[string][]Allowance
	// Unresolved names the leaves whose split could not be computed, and why.
	// A leaf appears here instead of in ByCluster, never in both: projecting a
	// half-resolved leaf would put a real budget on some clusters and silently
	// omit it on others, which reads as a working tenant that is quietly
	// missing capacity.
	Unresolved map[string]string
}

// Resolve computes every leaf's per-cluster allowance.
//
// Nothing here is fatal to the pass. A leaf whose policy cannot be determined,
// whose explicit split does not add up, or whose flavor no cluster reports yet
// is recorded as unresolved and skipped; the rest of the fleet is still
// projected. One misauthored tenant must not hold every other tenant's quota.
//
// The distinction that matters most is between "cannot" and "not yet".
// Proportional splits against capacity a member reports on its own root, so
// before members are up there is nothing to be proportional to — a startup
// ordering, not a misconfiguration. Both land in Unresolved, but the message
// says which, because one clears itself and the other needs an admin.
func Resolve(t *tree.Tree, capacity []Capacity, opts ResolveOptions) Resolution {
	out := Resolution{
		ByCluster:  map[string][]Allowance{},
		Unresolved: map[string]string{},
	}

	byPair := map[key][]shares.Weight{}
	for _, c := range capacity {
		k := key{c.ResourceName, c.ResourceFlavor}
		byPair[k] = append(byPair[k], shares.Weight{Cluster: c.Cluster, Capacity: c.Allocatable})
	}
	for k := range byPair {
		// Sorted so a split's tiebreak sees the same order however the caller
		// gathered the readings.
		w := byPair[k]
		sort.Slice(w, func(i, j int) bool { return w[i].Cluster < w[j].Cluster })
		byPair[k] = w
	}

	for _, leaf := range t.Leaves() {
		resolved, reason := resolveLeaf(leaf, byPair, opts)
		if reason != "" {
			out.Unresolved[leaf.Name()] = reason
			continue
		}
		for _, a := range resolved {
			out.ByCluster[a.cluster] = append(out.ByCluster[a.cluster], a.Allowance)
		}
	}

	for c := range out.ByCluster {
		a := out.ByCluster[c]
		sort.Slice(a, func(i, j int) bool {
			if a[i].Node != a[j].Node {
				return a[i].Node < a[j].Node
			}
			if a[i].ResourceName != a[j].ResourceName {
				return a[i].ResourceName < a[j].ResourceName
			}
			return a[i].ResourceFlavor < a[j].ResourceFlavor
		})
		out.ByCluster[c] = a
	}
	return out
}

// placed is an allowance together with the cluster it landed on.
type placed struct {
	cluster string
	Allowance
}

// resolveLeaf splits every budget on one leaf, or explains why it could not.
func resolveLeaf(leaf *tree.Node, byPair map[key][]shares.Weight, opts ResolveOptions) ([]placed, string) {
	var out []placed
	for _, b := range leaf.Quota.Spec.Budgets {
		policy := effectivePolicy(leaf.Quota, b, opts)
		switch policy {
		case v1beta1.AcceleratorQuotaDistributionExplicit:
			split, reason := explicit(b)
			if reason != "" {
				return nil, reason
			}
			out = append(out, attach(leaf.Name(), b, split)...)

		case v1beta1.AcceleratorQuotaDistributionProportional:
			weights := byPair[key{b.ResourceName, b.ResourceFlavor}]
			if len(weights) == 0 {
				return nil, fmt.Sprintf(
					"no cluster has reported capacity for %s/%s yet, so a proportional split has nothing to divide by",
					b.ResourceName, b.ResourceFlavor)
			}
			split, err := shares.Proportional(b.Nominal, weights)
			if err != nil {
				return nil, fmt.Sprintf("splitting %s/%s: %v", b.ResourceName, b.ResourceFlavor, err)
			}
			out = append(out, attach(leaf.Name(), b, split)...)

		default:
			return nil, fmt.Sprintf(
				"budget %s/%s has no distribution policy, and none is configured fleet-wide",
				b.ResourceName, b.ResourceFlavor)
		}
	}
	return out, ""
}

// effectivePolicy is the budget's own policy, then the node's, then the fleet
// default. Each is a deliberate override of the one behind it.
func effectivePolicy(q *v1beta1.AcceleratorQuota, b v1beta1.AcceleratorBudget,
	opts ResolveOptions,
) v1beta1.AcceleratorQuotaDistributionPolicy {
	if b.Policy != "" {
		return b.Policy
	}
	if q.Spec.Distribution != nil && q.Spec.Distribution.Policy != "" {
		return q.Spec.Distribution.Policy
	}
	return opts.DefaultPolicy
}

// explicit takes the admin's split verbatim, once it adds up.
//
// The sum is checked here rather than trusted from admission, because a hub
// re-splits on every pass and a source can be edited between them. A split that
// is short leaves capacity stranded; one that is over hands the fleet more than
// was authorized, which is the failure the tree exists to prevent.
func explicit(b v1beta1.AcceleratorBudget) ([]shares.Share, string) {
	if len(b.PerCluster) == 0 {
		return nil, fmt.Sprintf(
			"budget %s/%s is Explicit but names no per-cluster shares",
			b.ResourceName, b.ResourceFlavor)
	}
	out := make([]shares.Share, 0, len(b.PerCluster))
	seen := map[string]struct{}{}
	for _, p := range b.PerCluster {
		if _, dup := seen[p.Cluster]; dup {
			return nil, fmt.Sprintf("budget %s/%s names cluster %s twice",
				b.ResourceName, b.ResourceFlavor, p.Cluster)
		}
		seen[p.Cluster] = struct{}{}
		out = append(out, shares.Share{Cluster: p.Cluster, Nominal: p.Nominal})
	}
	if sum := shares.Sum(out); sum.Cmp(b.Nominal) != 0 {
		return nil, fmt.Sprintf("budget %s/%s is %s but its per-cluster shares sum to %s",
			b.ResourceName, b.ResourceFlavor, b.Nominal.String(), sum.String())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cluster < out[j].Cluster })
	return out, ""
}

// attach pairs a split with the budget it resolves.
func attach(node string, b v1beta1.AcceleratorBudget, split []shares.Share) []placed {
	out := make([]placed, 0, len(split))
	for _, s := range split {
		out = append(out, placed{
			cluster: s.Cluster,
			Allowance: Allowance{
				Node:           node,
				ResourceName:   b.ResourceName,
				ResourceFlavor: b.ResourceFlavor,
				Nominal:        s.Nominal.DeepCopy(),
			},
		})
	}
	return out
}
