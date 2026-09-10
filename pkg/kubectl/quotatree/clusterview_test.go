package quotatree

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// byCluster builds a view and re-anchors it, failing the test if it cannot.
func byCluster(t *testing.T, quotas []v1beta1.AcceleratorQuota) *View {
	t.Helper()
	v := buildView(t, quotas, 0)
	if err := ByCluster(v); err != nil {
		t.Fatalf("ByCluster: %v", err)
	}
	return v
}

func TestByClusterAnchorsOnTheFleet(t *testing.T) {
	got := render(t, byCluster(t, fleetSplitAcrossClusters()), RenderOptions{})

	want := strings.Join([]string{
		"nvidia.com/gpu on flavor a100",
		"",
		"NAME                ROLE             NOMINAL  ADMITTED  READY  AGE",
		"member-a            WorkloadCluster  448      248",
		"├─ org-product      Cohort           256      128              3h",
		"│  └─ team-serving  ClusterQueue     256      128              3h",
		"└─ org-research     Cohort           192      120              3h",
		"   ├─ team-nlp      ClusterQueue     96       72               3h",
		"   └─ team-vision   ClusterQueue     96       48               3h",
		"member-b            WorkloadCluster  64       40",
		"└─ org-research     Cohort           64       40               3h",
		"   ├─ team-nlp      ClusterQueue     32       24               3h",
		"   └─ team-vision   ClusterQueue     32       16               3h",
		"",
		"Cohort and WorkloadCluster rows total the leaf shares below them; " +
			"only a ClusterQueue row is a resolved budget",
		"",
	}, "\n")
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("cluster view mismatch (-want +got):\n%s", diff)
	}
}

func TestByClusterRollsUpLeavesOnce(t *testing.T) {
	// A Cohort's row on this axis is itself a total, so a roll-up that counted
	// both tiers would double every number on the way to the cluster.
	v := byCluster(t, fleetSplitAcrossClusters())

	a := v.Clusters[0]
	if a.Name != "member-a" {
		t.Fatalf("first cluster = %q, want member-a (sorted)", a.Name)
	}
	// 256 (team-serving) + 96 (team-nlp) + 96 (team-vision).
	if got := a.Budgets[0].Nominal.String(); got != "448" {
		t.Errorf("member-a nominal = %s, want 448", got)
	}
	if got := a.Budgets[0].Admitted.String(); got != "248" {
		t.Errorf("member-a admitted = %s, want 248 (128+72+48)", got)
	}

	research := clusterNode(t, a, "org-research")
	if got := research.Budgets[0].Nominal.String(); got != "192" {
		t.Errorf("org-research on member-a = %s, want 192 (96+96)", got)
	}
}

func TestByClusterDropsATenantAClusterDoesNotHost(t *testing.T) {
	// team-serving is Explicit on member-a alone. In the tenant view that is a
	// row quietly absent under one leaf; here the whole branch is gone, which
	// is the thing this axis exists to make obvious.
	v := byCluster(t, fleetSplitAcrossClusters())

	b := v.Clusters[1]
	if b.Name != "member-b" {
		t.Fatalf("second cluster = %q, want member-b", b.Name)
	}
	if got := nodeNames(b.Nodes); !cmp.Equal(got, []string{"org-research"}) {
		t.Errorf("member-b hosts %v, want only org-research", got)
	}
}

func TestByClusterElidesTheReservedRootButNotARerooting(t *testing.T) {
	// root is under every cluster and says the same thing each time. A node the
	// caller selected is the opposite: dropping it would drop what they asked
	// to see.
	v := byCluster(t, fleetSplitAcrossClusters())
	for _, c := range v.Clusters {
		for _, n := range c.Nodes {
			if n.Name == v1beta1.AcceleratorQuotaRootName {
				t.Errorf("%s still carries the reserved root", c.Name)
			}
		}
	}

	rerooted := buildView(t, fleetSplitAcrossClusters(), 0)
	if err := (Filter{Node: "org-research"}).Apply(rerooted); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := ByCluster(rerooted); err != nil {
		t.Fatalf("ByCluster: %v", err)
	}
	for _, c := range rerooted.Clusters {
		if got := nodeNames(c.Nodes); !cmp.Equal(got, []string{"org-research"}) {
			t.Errorf("%s anchors on %v, want the node --node selected", c.Name, got)
		}
	}
}

func TestByClusterNarrowsToWhatAFilterKept(t *testing.T) {
	// Asking for one tenant and then for the fleet axis should answer "which
	// clusters is this tenant on", not "every cluster there is".
	v := buildView(t, fleetSplitAcrossClusters(), 0)
	if err := (Filter{Node: "team-serving"}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := ByCluster(v); err != nil {
		t.Fatalf("ByCluster: %v", err)
	}

	var names []string
	for _, c := range v.Clusters {
		names = append(names, c.Name)
	}
	if !cmp.Equal(names, []string{"member-a"}) {
		t.Errorf("clusters = %v, want only member-a; no other cluster hosts team-serving", names)
	}
}

func TestByClusterClearsTheOtherAxis(t *testing.T) {
	// One set of numbers read two ways, so exactly one anchor is ever set. A
	// consumer that found both would have to guess which it was looking at.
	v := byCluster(t, fleetSplitAcrossClusters())
	if v.Root != nil {
		t.Errorf("Root = %q, want nil once the view is anchored on the fleet", v.Root.Name)
	}
	if len(v.Clusters) == 0 {
		t.Error("no clusters after ByCluster")
	}
}

func TestByClusterLeavesTheTenantViewIntact(t *testing.T) {
	// The slices are copies, so a caller may render both axes from one read.
	v := buildView(t, fleetSplitAcrossClusters(), 0)
	nlp := childNamed(childNamed(v.Root, "org-research"), "team-nlp")
	before := len(nlp.Budgets[0].PerCluster)

	if err := ByCluster(v); err != nil {
		t.Fatalf("ByCluster: %v", err)
	}
	if got := len(nlp.Budgets[0].PerCluster); got != before {
		t.Errorf("the original node's split is now %d entries, want %d untouched", got, before)
	}
}

func TestByClusterDrawsNoSplitTier(t *testing.T) {
	// The cluster is already named by the tree a row sits in, so restating it
	// under every leaf would repeat the anchor. Dropped from the data rather
	// than suppressed at render time, so --split cannot bring it back.
	v := byCluster(t, fleetSplitAcrossClusters())
	eachNode(v, func(n *NodeView) {
		for _, b := range n.Budgets {
			if len(b.PerCluster) > 0 {
				t.Errorf("%s still carries a split on the fleet axis", n.Name)
			}
		}
	})

	// An anchor row carries no branch art; a tier row would.
	for _, line := range strings.Split(render(t, v, RenderOptions{Split: true}), "\n") {
		if strings.Contains(line, roleWorkloadCluster) && strings.ContainsAny(line, "├└│") {
			t.Errorf("a per-cluster tier was drawn below a leaf: %q", line)
		}
	}
}

func TestByClusterKeepsFlavorsApart(t *testing.T) {
	// Two flavors are two kinds of accelerator; their sum is a quantity of
	// nothing. A roll-up must not produce one.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "96"), specBudget(gpu, h100, "48"))),
		leaf("team", "root",
			withSpec(specBudget(gpu, a100, "96"), specBudget(gpu, h100, "48")),
			withStatus(
				splitAcross(statusBudget(gpu, a100, "96", "0"), share("member-a", "96", "0")),
				splitAcross(statusBudget(gpu, h100, "48", "0"), share("member-a", "48", "0"))),
			ready()),
	}
	v := byCluster(t, quotas)

	got := map[string]string{}
	for _, b := range v.Clusters[0].Budgets {
		got[b.ResourceFlavor] = b.Nominal.String()
	}
	if diff := cmp.Diff(map[string]string{a100: "96", h100: "48"}, got); diff != "" {
		t.Errorf("roll-up merged two flavors (-want +got):\n%s", diff)
	}
}

func TestByClusterKeepsOrphansAndViolations(t *testing.T) {
	// A node with no position in the tree has none in a cluster's slice of it
	// either, and it is a fault that must stay visible on both axes.
	quotas := append(fleetSplitAcrossClusters(),
		leaf("stranded", "deleted-cohort", withSpec(specBudget(gpu, a100, "4"))))
	v := byCluster(t, quotas)

	if got := nodeNames(v.Orphans); !cmp.Equal(got, []string{"stranded"}) {
		t.Errorf("orphans = %v, want [stranded]", got)
	}
	got := render(t, v, RenderOptions{})
	if !strings.Contains(got, "NODES WITH NO POSITION IN THE TREE") {
		t.Errorf("orphan section dropped by the fleet axis:\n%s", got)
	}
	if !strings.Contains(got, "ParentMissing") {
		t.Errorf("violation dropped by the fleet axis:\n%s", got)
	}
}

func TestByClusterRefusesAPlaneWithNoSplit(t *testing.T) {
	// A workload cluster resolves none, and rendering an empty table there
	// would read as "this fleet holds nothing".
	v := buildView(t, healthyFleet(), 0)
	err := ByCluster(v)
	if err == nil {
		t.Fatal("ByCluster succeeded on a plane that resolves no split")
	}
	if !strings.Contains(err.Error(), "management plane") {
		t.Errorf("error = %q, want it to say where a split comes from", err)
	}
	if v.Root == nil {
		t.Error("the tenant view was torn down by a failed re-anchor")
	}
}

func TestByClusterOnNil(t *testing.T) {
	if err := ByCluster(nil); err != nil {
		t.Errorf("ByCluster(nil) = %v, want nil", err)
	}
}

func TestParseViewAxis(t *testing.T) {
	for _, s := range []string{"tenant", "cluster"} {
		if _, err := ParseViewAxis(s); err != nil {
			t.Errorf("ParseViewAxis(%q) = %v, want nil", s, err)
		}
	}
	if _, err := ParseViewAxis("fleet"); err == nil {
		t.Error("ParseViewAxis(\"fleet\") succeeded, want an error naming the valid axes")
	}
}

// clusterNode finds one node at the top of a cluster's slice.
func clusterNode(t *testing.T, c *ClusterView, name string) *NodeView {
	t.Helper()
	for _, n := range c.Nodes {
		if n.Name == name {
			return n
		}
	}
	t.Fatalf("%s has no node %q", c.Name, name)
	return nil
}
