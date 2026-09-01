package quotatree

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestBuildViewShapesTheTree(t *testing.T) {
	v := buildView(t, healthyFleet(), 0)

	if v.Root == nil {
		t.Fatal("no root in view")
	}
	if len(v.Orphans) != 0 {
		t.Errorf("orphans = %v, want none", nodeNames(v.Orphans))
	}
	if len(v.Violations) != 0 {
		t.Errorf("violations = %v, want none", v.Violations)
	}

	want := []string{"org-product", "org-research"}
	if got := nodeNames(v.Root.Children); !cmp.Equal(got, want) {
		t.Errorf("root children = %v, want %v (sorted by name)", got, want)
	}
	research := childNamed(v.Root, "org-research")
	if got, want := nodeNames(research.Children), []string{"team-nlp", "team-vision"}; !cmp.Equal(got, want) {
		t.Errorf("org-research children = %v, want %v", got, want)
	}
	if got, want := research.Depth, 1; got != want {
		t.Errorf("org-research depth = %d, want %d", got, want)
	}
}

func TestBuildViewPrefersStatusBudgetOverSpec(t *testing.T) {
	// On a management plane spec is the fleet total and status is what was
	// resolved for the plane, so the two legitimately differ.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "512"))),
		leaf("team", "root",
			withSpec(specBudget(gpu, a100, "400")),
			withStatus(statusBudget(gpu, a100, "100", "40"))),
	}
	v := buildView(t, quotas, 0)

	team := childNamed(v.Root, "team")
	b := team.Budgets[0]
	if got, want := b.Nominal.String(), "100"; got != want {
		t.Errorf("nominal = %s, want %s (status wins over spec)", got, want)
	}
	if b.FromSpec {
		t.Error("FromSpec = true, want false when a status budget exists")
	}
	if got, want := b.Admitted.String(), "40"; got != want {
		t.Errorf("admitted = %s, want %s", got, want)
	}
}

func TestBuildViewFallsBackToSpecAndSaysSo(t *testing.T) {
	// A tree applied before the controller has reconciled has no status at all.
	// Rendering nothing would make a freshly-authored tree look empty.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "512"))),
		leaf("team", "root", withSpec(specBudget(gpu, a100, "128"))),
	}
	v := buildView(t, quotas, 0)

	b := childNamed(v.Root, "team").Budgets[0]
	if got, want := b.Nominal.String(), "128"; got != want {
		t.Errorf("nominal = %s, want %s", got, want)
	}
	if !b.FromSpec {
		t.Error("FromSpec = false, want true so the renderer can mark it unresolved")
	}
	if b.Admitted != nil {
		t.Errorf("admitted = %v, want nil; usage is only knowable from status", b.Admitted)
	}
}

func TestBuildViewKeepsAZeroStatusBudget(t *testing.T) {
	// A leaf resolved to zero on this plane is a real state, distinct from
	// "not resolved yet". Treating zero as absent would show the fleet total.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "512"))),
		leaf("drained", "root",
			withSpec(specBudget(gpu, a100, "128")),
			withStatus(statusBudget(gpu, a100, "0", "0"))),
	}
	v := buildView(t, quotas, 0)

	b := childNamed(v.Root, "drained").Budgets[0]
	if got, want := b.Nominal.String(), "0"; got != want {
		t.Errorf("nominal = %s, want %s", got, want)
	}
	if b.FromSpec {
		t.Error("FromSpec = true, want false; a resolved zero is not a fallback")
	}
}

func TestBuildViewMergesBudgetsAcrossFlavorsInOrder(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		leaf("team", "root",
			withSpec(specBudget(tpu, tpu7x, "8"), specBudget(gpu, h100, "16"), specBudget(gpu, a100, "32")),
			withStatus(statusBudget(gpu, h100, "16", "4"))),
	}
	v := buildView(t, quotas, 0)

	team := childNamed(v.Root, "team")
	var got []string
	for _, b := range team.Budgets {
		got = append(got, b.ResourceName+"/"+b.ResourceFlavor)
	}
	want := []string{"google.com/tpu/tpu7x", "nvidia.com/gpu/a100", "nvidia.com/gpu/h100"}
	if !cmp.Equal(got, want) {
		t.Errorf("budget order = %v, want %v (by resource then flavor)", got, want)
	}
	if team.Budgets[2].FromSpec {
		t.Error("h100 FromSpec = true, want false; only that pair has status")
	}
	if !team.Budgets[1].FromSpec {
		t.Error("a100 FromSpec = false, want true; that pair has no status")
	}
}

func TestBuildViewCarriesTheSplitSortedByCluster(t *testing.T) {
	// Status is controller-owned and already sorted, but the view sorts anyway:
	// the order a member appears in is part of what a reader compares between
	// two runs, and it must not depend on how the list was assembled.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "120"))),
		leaf("team", "root",
			withSpec(specBudget(gpu, a100, "120")),
			withStatus(splitAcross(statusBudget(gpu, a100, "120", "40"),
				share("member-b", "30", "10"),
				share("member-a", "90", "30")))),
	}
	v := buildView(t, quotas, 0)

	b := childNamed(v.Root, "team").Budgets[0]
	var got []string
	for _, c := range b.PerCluster {
		got = append(got, c.Cluster+"="+c.Nominal.String()+"/"+c.Admitted.String())
	}
	want := []string{"member-a=90/30", "member-b=30/10"}
	if !cmp.Equal(got, want) {
		t.Errorf("split = %v, want %v", got, want)
	}
}

func TestBuildViewLeavesTheSplitEmptyOnAWorkloadCluster(t *testing.T) {
	// A projected copy carries one already-resolved share and no trace of the
	// arithmetic behind it, so there is nothing to break down.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		leaf("team", "root",
			withSpec(specBudget(gpu, a100, "90")),
			withStatus(statusBudget(gpu, a100, "90", "30"))),
	}
	v := buildView(t, quotas, 0)

	if got := childNamed(v.Root, "team").Budgets[0].PerCluster; len(got) != 0 {
		t.Errorf("split = %v, want none", got)
	}
}

func TestBuildViewReportsOnlyADeclaredPolicy(t *testing.T) {
	// The remaining fallback is an operator-level default held in the manager's
	// configuration, which is on no object the plugin reads. Naming one would
	// report a policy the cluster may not be using.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "300"))),
		leaf("on-budget", "root", withSpec(v1beta1.AcceleratorBudget{
			ResourceName:   gpu,
			ResourceFlavor: a100,
			Nominal:        qty("100"),
			Policy:         v1beta1.AcceleratorQuotaDistributionExplicit,
		})),
		leaf("on-node", "root",
			withSpec(specBudget(gpu, a100, "100")),
			withPolicy(v1beta1.AcceleratorQuotaDistributionProportional)),
		leaf("inherited", "root", withSpec(specBudget(gpu, a100, "100"))),
	}
	v := buildView(t, quotas, 0)

	for _, tc := range []struct{ node, want string }{
		{"on-budget", "Explicit"},
		{"on-node", "Proportional"},
		{"inherited", ""},
	} {
		if got := childNamed(v.Root, tc.node).Budgets[0].Policy; got != tc.want {
			t.Errorf("%s policy = %q, want %q", tc.node, got, tc.want)
		}
	}
}

func TestSplitTotal(t *testing.T) {
	nominal := qty("120")
	for _, tc := range []struct {
		name      string
		budget    BudgetView
		wantTotal string
		wantWhole bool
	}{
		{
			"adds up",
			BudgetView{Nominal: &nominal, PerCluster: []ClusterBudgetView{
				{Cluster: "a", Nominal: quantityPtr(qty("90"))},
				{Cluster: "b", Nominal: quantityPtr(qty("30"))},
			}},
			"120", true,
		},
		{
			"short by a member the hub could not place",
			BudgetView{Nominal: &nominal, PerCluster: []ClusterBudgetView{
				{Cluster: "a", Nominal: quantityPtr(qty("90"))},
			}},
			"90", false,
		},
		// A budget nobody split is whole by definition: there is no gap where
		// there was no division.
		{"never split", BudgetView{Nominal: &nominal}, "0", true},
		{"no allowance to divide", BudgetView{PerCluster: []ClusterBudgetView{{Cluster: "a"}}}, "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total, whole := splitTotal(tc.budget)
			if got := total.String(); got != tc.wantTotal {
				t.Errorf("total = %s, want %s", got, tc.wantTotal)
			}
			if whole != tc.wantWhole {
				t.Errorf("whole = %v, want %v", whole, tc.wantWhole)
			}
		})
	}
}

func TestBuildViewSurfacesOrphansRatherThanDroppingThem(t *testing.T) {
	// A node whose parent was deleted is exactly the node an operator is
	// hunting for, so it must not disappear with the edge.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		leaf("adopted", "root", withSpec(specBudget(gpu, a100, "8"))),
		leaf("stranded", "gone-cohort", withSpec(specBudget(gpu, a100, "8"))),
	}
	v := buildView(t, quotas, 0)

	if got, want := nodeNames(v.Orphans), []string{"stranded"}; !cmp.Equal(got, want) {
		t.Fatalf("orphans = %v, want %v", got, want)
	}
	orphan := v.Orphans[0]
	if got, want := orphan.Parent, "gone-cohort"; got != want {
		t.Errorf("orphan parent = %q, want %q; the dangling ref is the diagnosis", got, want)
	}
	if orphan.Violation == nil {
		t.Fatal("orphan carries no violation")
	}
	if got, want := orphan.Violation.Reason, v1beta1.AcceleratorQuotaReasonParentMissing; got != want {
		t.Errorf("violation reason = %q, want %q", got, want)
	}
}

func TestBuildViewRendersACycleOnceWithoutHanging(t *testing.T) {
	// Cyclic nodes keep their parent edges, so a naive child walk never
	// terminates and a naive emit prints each member repeatedly.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		cohort("a", "b"),
		cohort("b", "c"),
		cohort("c", "a"),
	}
	v := buildView(t, quotas, 0)

	seen := map[string]int{}
	eachNode(v, func(n *NodeView) { seen[n.Name]++ })
	for _, name := range []string{"a", "b", "c"} {
		if seen[name] != 1 {
			t.Errorf("node %q emitted %d times, want 1", name, seen[name])
		}
	}
	if len(v.Violations) == 0 {
		t.Error("no violations reported for a parentRef cycle")
	}
}

func TestBuildViewAttributesFreezeToTheNearestCause(t *testing.T) {
	// Containment is attributed to the parent whose budget is too small, not to
	// the children that jointly overflow it: two children can each be valid on
	// their own. The descendants are frozen by it and should read that one root
	// cause rather than a subtree of identical complaints.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "100"))),
		cohort("mid", "root", withSpec(specBudget(gpu, a100, "500"))),
		leaf("below", "mid", withSpec(specBudget(gpu, a100, "10"))),
	}
	v := buildView(t, quotas, 0)

	if v.Root.Violation == nil {
		t.Fatal("root carries no violation; its children total more than its budget")
	}
	if got, want := v.Root.Violation.Reason, v1beta1.AcceleratorQuotaReasonContainmentViolated; got != want {
		t.Errorf("root violation = %q, want %q", got, want)
	}

	mid := childNamed(v.Root, "mid")
	below := childNamed(mid, "below")
	for _, n := range []*NodeView{mid, below} {
		if n.Violation != nil {
			t.Errorf("%s has its own violation %v, want none", n.Name, n.Violation)
		}
		if n.FrozenBy == nil {
			t.Fatalf("%s is not marked frozen by an ancestor", n.Name)
		}
		if got, want := n.FrozenBy.Node, "root"; got != want {
			t.Errorf("%s frozen by %q, want %q", n.Name, got, want)
		}
	}
}

func TestBuildViewReportsConditionsThatAreSet(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "",
			withCondition(v1beta1.AcceleratorQuotaReady, "True"),
			withCondition(v1beta1.AcceleratorQuotaDegraded, "False")),
	}
	v := buildView(t, quotas, 0)

	want := map[string]string{"Ready": "True", "Degraded": "False"}
	if diff := cmp.Diff(want, v.Root.Conditions); diff != "" {
		t.Errorf("conditions mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildViewToleratesNoRoot(t *testing.T) {
	// Every node is an orphan when the reserved root is missing, and that is a
	// finding rather than an empty cluster.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("org", ""),
		leaf("team", "org", withSpec(specBudget(gpu, a100, "8"))),
	}
	v := buildView(t, quotas, 0)

	if v.Root != nil {
		t.Errorf("root = %q, want nil", v.Root.Name)
	}
	if got, want := nodeNames(v.Orphans), []string{"org"}; !cmp.Equal(got, want) {
		t.Errorf("orphans = %v, want %v with team nested under it", got, want)
	}
	if got, want := nodeNames(v.Orphans[0].Children), []string{"team"}; !cmp.Equal(got, want) {
		t.Errorf("org children = %v, want %v", got, want)
	}
}

func TestBuildViewOnNilTree(t *testing.T) {
	if v := BuildView(nil, nil); v == nil || v.Root != nil || len(v.Orphans) != 0 {
		t.Errorf("BuildView(nil, nil) = %+v, want an empty view", v)
	}
}

func nodeNames(ns []*NodeView) []string {
	var out []string
	for _, n := range ns {
		out = append(out, n.Name)
	}
	return out
}

func childNamed(parent *NodeView, name string) *NodeView {
	for _, c := range parent.Children {
		if c.Name == name {
			return c
		}
	}
	return &NodeView{}
}
