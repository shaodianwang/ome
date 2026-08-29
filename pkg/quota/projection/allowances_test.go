package projection

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

func cap3(cluster, nominal string) Capacity {
	return Capacity{Cluster: cluster, ResourceName: tpu, ResourceFlavor: flavor, Allocatable: qty(nominal)}
}

func allow(node, nominal string) Allowance {
	return Allowance{Node: node, ResourceName: tpu, ResourceFlavor: flavor, Nominal: qty(nominal)}
}

// oneLeaf is root -> team, the smallest tree that resolves a split.
func oneLeaf(t *testing.T, budgets ...v1beta1.AcceleratorBudget) *tree.Tree {
	t.Helper()
	nodes := []v1beta1.AcceleratorQuota{
		node("root", "", v1beta1.AcceleratorQuotaRoleCohort, nil),
		node("team", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, []string{"ns"}, budgets...),
	}
	built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}
	return built
}

func withPolicy(p v1beta1.AcceleratorQuotaDistributionPolicy) func(*v1beta1.AcceleratorBudget) {
	return func(b *v1beta1.AcceleratorBudget) { b.Policy = p }
}

func withPerCluster(pairs ...[2]string) func(*v1beta1.AcceleratorBudget) {
	return func(b *v1beta1.AcceleratorBudget) {
		for _, p := range pairs {
			b.PerCluster = append(b.PerCluster, v1beta1.AcceleratorClusterShare{
				Cluster: p[0], Nominal: qty(p[1]),
			})
		}
	}
}

// How a fleet total becomes per-cluster numbers, under each policy and each way
// of arriving at one.
func TestResolve(t *testing.T) {
	tests := []struct {
		name     string
		budgets  []v1beta1.AcceleratorBudget
		capacity []Capacity
		opts     ResolveOptions
		want     map[string][]Allowance
	}{
		{
			name: "an explicit split is taken verbatim",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "60"}, [2]string{"member-b", "40"}))},
			want: map[string][]Allowance{
				"member-a": {allow("team", "60")},
				"member-b": {allow("team", "40")},
			},
		},
		{
			name: "a proportional split follows reported capacity",
			budgets: []v1beta1.AcceleratorBudget{budget("120",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			capacity: []Capacity{cap3("member-a", "3"), cap3("member-b", "1")},
			want: map[string][]Allowance{
				"member-a": {allow("team", "90")},
				"member-b": {allow("team", "30")},
			},
		},
		{
			// A cluster reporting none of the flavor takes zero, which is what
			// keeps the projector from creating a queue that admits nothing.
			name: "a cluster without the flavor is apportioned zero",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			capacity: []Capacity{cap3("member-a", "8"), cap3("member-b", "0")},
			want: map[string][]Allowance{
				"member-a": {allow("team", "100")},
				"member-b": {allow("team", "0")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Resolve(oneLeaf(t, tc.budgets...), tc.capacity, tc.opts)
			if len(got.Unresolved) != 0 {
				t.Fatalf("Resolve() left nodes unresolved: %v", got.Unresolved)
			}
			if diff := cmp.Diff(tc.want, got.ByCluster, cmpQuantity); diff != "" {
				t.Errorf("Resolve() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Where the policy comes from, in precedence order. Each layer is a deliberate
// override of the one behind it, and a budget silently taking the wrong one
// would split a tenant's quota by a rule nobody chose.
func TestResolvePolicyPrecedence(t *testing.T) {
	capacity := []Capacity{cap3("member-a", "3"), cap3("member-b", "1")}
	explicitSplit := withPerCluster([2]string{"member-a", "10"}, [2]string{"member-b", "90"})

	tests := []struct {
		name    string
		budget  v1beta1.AcceleratorBudget
		nodePol v1beta1.AcceleratorQuotaDistributionPolicy
		opts    ResolveOptions
		want    map[string]string // cluster -> nominal
	}{
		{
			// Explicit on the budget beats Proportional on the node: 10/90, not
			// the 75/25 the capacities would give.
			name:    "the budget's own policy wins",
			budget:  budget("100", withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit), explicitSplit),
			nodePol: v1beta1.AcceleratorQuotaDistributionProportional,
			want:    map[string]string{"member-a": "10", "member-b": "90"},
		},
		{
			name:    "the node's policy applies when the budget declares none",
			budget:  budget("100"),
			nodePol: v1beta1.AcceleratorQuotaDistributionProportional,
			want:    map[string]string{"member-a": "75", "member-b": "25"},
		},
		{
			name:   "the fleet default applies when neither does",
			budget: budget("100"),
			opts:   ResolveOptions{DefaultPolicy: v1beta1.AcceleratorQuotaDistributionProportional},
			want:   map[string]string{"member-a": "75", "member-b": "25"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []v1beta1.AcceleratorQuota{
				node("root", "", v1beta1.AcceleratorQuotaRoleCohort, nil),
				node("team", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, []string{"ns"}, tc.budget),
			}
			if tc.nodePol != "" {
				nodes[1].Spec.Distribution = &v1beta1.AcceleratorQuotaDistribution{Policy: tc.nodePol}
			}
			built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
			if err != nil {
				t.Fatalf("tree.Build() error = %v", err)
			}

			got := Resolve(built, capacity, tc.opts)
			if len(got.Unresolved) != 0 {
				t.Fatalf("Resolve() left nodes unresolved: %v", got.Unresolved)
			}
			for cluster, nominal := range tc.want {
				a := got.ByCluster[cluster]
				if len(a) != 1 {
					t.Fatalf("cluster %s got %d allowances, want 1", cluster, len(a))
				}
				if a[0].Nominal.Cmp(qty(nominal)) != 0 {
					t.Errorf("cluster %s got %s, want %s", cluster, a[0].Nominal.String(), nominal)
				}
			}
		})
	}
}

// A leaf that cannot be split is skipped and explained, never half-projected.
// Putting a real budget on some clusters and silently omitting it on others
// reads as a working tenant that is quietly missing capacity.
func TestResolveUnresolved(t *testing.T) {
	tests := []struct {
		name     string
		budgets  []v1beta1.AcceleratorBudget
		capacity []Capacity
		opts     ResolveOptions
		wantWhy  string
	}{
		{
			name:    "no policy anywhere",
			budgets: []v1beta1.AcceleratorBudget{budget("100")},
			wantWhy: "no distribution policy",
		},
		{
			name: "explicit with no shares named",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit))},
			wantWhy: "names no per-cluster shares",
		},
		{
			// Re-checked on every pass rather than trusted from admission: a
			// hub re-splits continuously and the source can be edited between
			// passes. Short strands capacity; over hands out more than the
			// admin authorized.
			name: "explicit shares that do not add up",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "60"}, [2]string{"member-b", "30"}))},
			wantWhy: "sum to 90",
		},
		{
			name: "explicit naming one cluster twice",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "60"}, [2]string{"member-a", "40"}))},
			wantWhy: "twice",
		},
		{
			// Not a misconfiguration: members report capacity on their own
			// roots, so before they are up there is nothing to divide by. The
			// message says so, because this one clears itself.
			name: "proportional before any member has reported",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			wantWhy: "has reported capacity",
		},
		{
			name: "proportional where every reported capacity is zero",
			budgets: []v1beta1.AcceleratorBudget{budget("100",
				withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))},
			capacity: []Capacity{cap3("member-a", "0"), cap3("member-b", "0")},
			wantWhy:  "nothing to be proportional to",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Resolve(oneLeaf(t, tc.budgets...), tc.capacity, tc.opts)
			why, ok := got.Unresolved["team"]
			if !ok {
				t.Fatalf("Resolve() resolved a leaf it should not have: %v", got.ByCluster)
			}
			if !strings.Contains(why, tc.wantWhy) {
				t.Errorf("reason = %q, want it to name %q", why, tc.wantWhy)
			}
			if len(got.ByCluster) != 0 {
				t.Errorf("an unresolved leaf was still projected somewhere: %v", got.ByCluster)
			}
		})
	}
}

// One misauthored tenant must not cost every other tenant its quota, so a pass
// carries the good leaves and reports the bad one.
func TestResolveIsolatesAFailedLeaf(t *testing.T) {
	nodes := []v1beta1.AcceleratorQuota{
		node("root", "", v1beta1.AcceleratorQuotaRoleCohort, nil),
		node("good", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, []string{"ns-good"},
			budget("100", withPolicy(v1beta1.AcceleratorQuotaDistributionProportional))),
		// Explicit, and the shares do not add up.
		node("bad", "root", v1beta1.AcceleratorQuotaRoleClusterQueue, []string{"ns-bad"},
			budget("100", withPolicy(v1beta1.AcceleratorQuotaDistributionExplicit),
				withPerCluster([2]string{"member-a", "1"}))),
	}
	built, _, err := tree.Build(nodes, tree.Options{RootName: "root", MaxDepth: 5})
	if err != nil {
		t.Fatalf("tree.Build() error = %v", err)
	}

	got := Resolve(built, []Capacity{cap3("member-a", "1")}, ResolveOptions{})

	if _, bad := got.Unresolved["bad"]; !bad {
		t.Error("the misauthored leaf was not reported")
	}
	if _, stillProjected := got.Unresolved["good"]; stillProjected {
		t.Error("a sound leaf was dropped because a sibling was misauthored")
	}
	a := got.ByCluster["member-a"]
	if len(a) != 1 || a[0].Node != "good" {
		t.Errorf("member-a got %v, want only the sound leaf's allowance", a)
	}
}
