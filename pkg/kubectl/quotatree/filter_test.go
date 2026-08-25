package quotatree

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestFilterEmptyKeepsEverything(t *testing.T) {
	v := buildView(t, healthyFleet(), 0)
	before := nodeNames(v.Root.Children)

	if err := (Filter{}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := nodeNames(v.Root.Children); !cmp.Equal(got, before) {
		t.Errorf("children = %v, want %v unchanged", got, before)
	}
}

func TestFilterNodeReRootsTheRender(t *testing.T) {
	v := buildView(t, healthyFleet(), 0)

	if err := (Filter{Node: "org-research"}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, want := v.Root.Name, "org-research"; got != want {
		t.Errorf("root = %q, want %q", got, want)
	}
	if got, want := nodeNames(v.Root.Children), []string{"team-nlp", "team-vision"}; !cmp.Equal(got, want) {
		t.Errorf("children = %v, want %v", got, want)
	}
	if names := visibleNodes(v); names["org-product"] {
		t.Error("a sibling subtree survived --node")
	}
}

func TestFilterNodeCanSelectAnOrphan(t *testing.T) {
	// A node with no position in the tree is the one most worth focusing on.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		cohort("stranded", "gone"),
		leaf("under-stranded", "stranded", withSpec(specBudget(gpu, a100, "4"))),
	}
	v := buildView(t, quotas, 0)

	if err := (Filter{Node: "stranded"}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, want := v.Root.Name, "stranded"; got != want {
		t.Errorf("root = %q, want %q", got, want)
	}
	if got, want := nodeNames(v.Root.Children), []string{"under-stranded"}; !cmp.Equal(got, want) {
		t.Errorf("children = %v, want %v", got, want)
	}
}

func TestFilterNodeUnknownNameIsAnError(t *testing.T) {
	v := buildView(t, healthyFleet(), 0)

	err := (Filter{Node: "nope"}).Apply(v)
	if err == nil {
		t.Fatal("Apply succeeded for an unknown node; an empty tree reads as a healthy one")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q does not name the node asked for", err)
	}
}

func TestFilterLeavesOnlyKeepsThePathToEachLeaf(t *testing.T) {
	// Answering "which leaves exist" with a flat list loses where they sit,
	// which is half the question.
	v := buildView(t, healthyFleet(), 0)

	if err := (Filter{LeavesOnly: true}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if v.Root.Name != "root" || !v.Root.Context {
		t.Errorf("root = %q context=%v, want it kept as dimmed context", v.Root.Name, v.Root.Context)
	}
	research := childNamed(v.Root, "org-research")
	if !research.Context {
		t.Error("org-research kept but not marked as context")
	}
	nlp := childNamed(research, "team-nlp")
	if nlp.Name == "" || nlp.Context {
		t.Errorf("team-nlp = %+v, want it kept as a match rather than context", nlp)
	}
}

func TestFilterForNamespaceFindsTheChargingLeaf(t *testing.T) {
	v := buildView(t, healthyFleet(), 0)

	if err := (Filter{Namespace: "nlp-dev"}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	names := visibleNodes(v)
	for _, want := range []string{"root", "org-research", "team-nlp"} {
		if !names[want] {
			t.Errorf("%q missing; the answer needs the path that leads to it", want)
		}
	}
	for _, unwanted := range []string{"team-vision", "org-product", "team-serving"} {
		if names[unwanted] {
			t.Errorf("%q survived --for-namespace but binds nothing relevant", unwanted)
		}
	}
}

func TestFilterForNamespaceUnboundIsAnError(t *testing.T) {
	v := buildView(t, healthyFleet(), 0)

	err := (Filter{Namespace: "not-bound"}).Apply(v)
	if err == nil {
		t.Fatal("Apply succeeded for an unbound namespace")
	}
	if !strings.Contains(err.Error(), "not-bound") {
		t.Errorf("error %q does not name the namespace asked about", err)
	}
}

func TestFilterForNamespaceShowsBothLeavesOnAConflict(t *testing.T) {
	// A namespace charges exactly one leaf. When two bind it the tool has to
	// show both, since picking one would hide the conflict.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "64"))),
		leaf("first", "root", withSpec(specBudget(gpu, a100, "32")), withNamespaces("shared")),
		leaf("second", "root", withSpec(specBudget(gpu, a100, "32")), withNamespaces("shared")),
	}
	v := buildView(t, quotas, 0)

	if err := (Filter{Namespace: "shared"}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, want := nodeNames(v.Root.Children), []string{"first", "second"}; !cmp.Equal(got, want) {
		t.Errorf("leaves = %v, want %v", got, want)
	}
	if len(v.Violations) == 0 {
		t.Error("no NamespaceConflict reported for two leaves binding one namespace")
	}
}

func TestFilterNarrowsBudgetsWithoutDroppingNodes(t *testing.T) {
	// Restricting to one accelerator must not delete the Cohorts that carry no
	// budget for it — they are still the parents of the leaves that do.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "64"), specBudget(tpu, tpu7x, "8"))),
		cohort("mid", "root", withSpec(specBudget(tpu, tpu7x, "8"))),
		leaf("team", "mid", withSpec(specBudget(gpu, a100, "16"), specBudget(tpu, tpu7x, "8"))),
	}
	v := buildView(t, quotas, 0)

	if err := (Filter{ResourceName: gpu, ResourceFlavor: a100}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, want := len(v.Root.Budgets), 1; got != want {
		t.Errorf("root budgets = %d, want %d", got, want)
	}
	mid := childNamed(v.Root, "mid")
	if mid.Name == "" {
		t.Fatal("mid dropped; a Cohort without the selected pair still holds a position")
	}
	if got, want := len(mid.Budgets), 0; got != want {
		t.Errorf("mid budgets = %d, want %d", got, want)
	}
	team := childNamed(mid, "team")
	if got, want := len(team.Budgets), 1; got != want {
		t.Fatalf("team budgets = %d, want %d", got, want)
	}
	if got, want := team.Budgets[0].ResourceFlavor, a100; got != want {
		t.Errorf("team flavor = %q, want %q", got, want)
	}
}

func TestFilterNarrowsCapacityToo(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "",
			withSpec(specBudget(gpu, a100, "64")),
			withCapacity(gpu, a100, "64", "64"),
			withCapacity(tpu, tpu7x, "8", "8")),
	}
	v := buildView(t, quotas, 0)

	if err := (Filter{ResourceName: gpu}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, want := len(v.Root.Capacity), 1; got != want {
		t.Fatalf("capacity rows = %d, want %d", got, want)
	}
	if got, want := v.Root.Capacity[0].ResourceName, gpu; got != want {
		t.Errorf("capacity resource = %q, want %q", got, want)
	}
}

func TestFilterNamespaceOverridesLeavesOnly(t *testing.T) {
	// Only a leaf binds namespaces, so combining the two must not intersect
	// into nothing.
	v := buildView(t, healthyFleet(), 0)

	if err := (Filter{Namespace: "vision-prod", LeavesOnly: true}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if names := visibleNodes(v); !names["team-vision"] {
		t.Error("team-vision missing when both selectors are set")
	}
}

func TestFilterEmptyReportsItself(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    Filter
		want bool
	}{
		{"zero", Filter{}, true},
		{"node", Filter{Node: "x"}, false},
		{"leaves", Filter{LeavesOnly: true}, false},
		{"namespace", Filter{Namespace: "x"}, false},
		{"resource", Filter{ResourceName: gpu}, false},
		{"flavor", Filter{ResourceFlavor: a100}, false},
	} {
		if got := tc.f.Empty(); got != tc.want {
			t.Errorf("%s: Empty() = %v, want %v", tc.name, got, tc.want)
		}
	}
}
