package quotatree

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func render(t *testing.T, v *View, opts RenderOptions) string {
	t.Helper()
	if opts.Color == "" {
		opts.Color = ColorNever
	}
	var b strings.Builder
	if err := Render(&b, v, opts); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return b.String()
}

func TestRenderDrawsTheTreeWithAlignedColumns(t *testing.T) {
	got := render(t, buildView(t, healthyFleet(), 0), RenderOptions{})

	want := strings.Join([]string{
		"nvidia.com/gpu on flavor a100",
		"",
		"NAME                ROLE          NOMINAL  ADMITTED  READY",
		"root                Cohort        512      288",
		"├─ org-product      Cohort        256      128",
		"│  └─ team-serving  ClusterQueue  256      128       False",
		"└─ org-research     Cohort        256      160",
		"   ├─ team-nlp      ClusterQueue  128      96",
		"   └─ team-vision   ClusterQueue  128      64",
		"",
	}, "\n")
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("rendered tree mismatch (-want +got):\n%s", diff)
	}
}

func TestRenderHidesReadyWhenHealthy(t *testing.T) {
	// The table exists to surface problems: a fleet that is entirely Ready
	// should render an empty column, not a wall of True.
	got := render(t, buildView(t, healthyFleet(), 0), RenderOptions{})

	if strings.Contains(got, "True") {
		t.Error("default output spells out Ready=True; it should be blank when healthy")
	}
	if !strings.Contains(got, "team-serving  ClusterQueue  256      128       False") {
		t.Error("the one not-Ready node is not called out")
	}
}

func TestRenderCollapsesThePairColumnsWhenTheTreeHasOne(t *testing.T) {
	single := render(t, buildView(t, healthyFleet(), 0), RenderOptions{})
	if strings.Contains(single, "FLAVOR") {
		t.Error("FLAVOR column present for a single-pair tree; the caption already says it")
	}
	if !strings.HasPrefix(single, "nvidia.com/gpu on flavor a100\n") {
		t.Errorf("missing caption naming the only pair, got:\n%s", single)
	}

	multi := render(t, buildView(t, twoFlavorFleet(), 0), RenderOptions{})
	if !strings.Contains(multi, "RESOURCE") || !strings.Contains(multi, "FLAVOR") {
		t.Errorf("multi-pair tree must name the pair per row, got:\n%s", multi)
	}
	if strings.Contains(multi, "on flavor") {
		t.Error("multi-pair tree should not carry a single-pair caption")
	}
}

func TestRenderKeepsBranchLinesOpenAcrossBudgetRows(t *testing.T) {
	// A node with two budgets emits two rows. The extra row sits between the
	// node and its children, so the branch line has to survive it.
	got := render(t, buildView(t, twoFlavorFleet(), 0), RenderOptions{})

	lines := strings.Split(got, "\n")
	var rootCont string
	for i, l := range lines {
		if strings.HasPrefix(l, "root ") {
			rootCont = lines[i+1]
			break
		}
	}
	if !strings.HasPrefix(rootCont, "│") {
		t.Errorf("root's continuation row = %q, want it to keep the line to its children open", rootCont)
	}
}

func TestRenderMarksSpecSourcedBudgets(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "64"))),
	}
	got := render(t, buildView(t, quotas, 0), RenderOptions{})

	if !strings.Contains(got, "64*") {
		t.Errorf("unresolved nominal not marked, got:\n%s", got)
	}
	if !strings.Contains(got, "* nominal read from spec") {
		t.Errorf("no footnote explaining the mark, got:\n%s", got)
	}
}

func TestRenderOmitsTheFootnoteWhenEverythingIsResolved(t *testing.T) {
	got := render(t, buildView(t, healthyFleet(), 0), RenderOptions{})
	if strings.Contains(got, "nominal read from spec") {
		t.Error("footnote shown for a fully resolved tree; a legend for an absent mark is noise")
	}
}

func TestRenderSectionsOffNodesWithNoPosition(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		leaf("stranded", "deleted-cohort", withSpec(specBudget(gpu, a100, "4"))),
	}
	got := render(t, buildView(t, quotas, 0), RenderOptions{})

	if !strings.Contains(got, "NODES WITH NO POSITION IN THE TREE") {
		t.Errorf("orphan section heading missing, got:\n%s", got)
	}
	if !strings.Contains(got, "stranded") {
		t.Errorf("orphaned node not rendered, got:\n%s", got)
	}
	if !strings.Contains(got, "does not resolve") {
		t.Errorf("orphan cause not explained, got:\n%s", got)
	}
}

func TestRenderCountsViolationsHiddenByAFilter(t *testing.T) {
	v := buildView(t, cycleAndContainmentFleet(), 0)
	if err := (Filter{Node: "root"}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got := render(t, v, RenderOptions{})

	if !strings.Contains(got, "ContainmentViolated") {
		t.Errorf("in-view violation not reported, got:\n%s", got)
	}
	if strings.Contains(got, "loop-a: parentRef forms a cycle") {
		t.Error("spelled out a violation for a node the filter hid")
	}
	if !strings.Contains(got, "outside this view") {
		t.Errorf("hidden violations not counted; a filter must not conceal a fault, got:\n%s", got)
	}
}

func TestRenderASCIIGlyphs(t *testing.T) {
	got := render(t, buildView(t, healthyFleet(), 0), RenderOptions{Glyphs: ASCIIGlyphs})

	for _, box := range []string{"├", "└", "│"} {
		if strings.Contains(got, box) {
			t.Errorf("box-drawing character %q survived --ascii", box)
		}
	}
	if !strings.Contains(got, "|- ") || !strings.Contains(got, "`- ") {
		t.Errorf("ASCII branches missing, got:\n%s", got)
	}
}

func TestRenderEmptyView(t *testing.T) {
	got := render(t, &View{}, RenderOptions{})
	if want := "No AcceleratorQuota resources found.\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderColorWrapsWithoutBreakingAlignment(t *testing.T) {
	// Widths are measured on unstyled text, so the escape must not shift a
	// column. Comparing the two renders with escapes stripped proves it.
	v1 := buildView(t, healthyFleet(), 0)
	v2 := buildView(t, healthyFleet(), 0)
	plain := render(t, v1, RenderOptions{Color: ColorNever})
	colored := render(t, v2, RenderOptions{Color: ColorAlways})

	if !strings.Contains(colored, "\x1b[") {
		t.Fatal("--color=always emitted no escapes")
	}
	if diff := cmp.Diff(plain, stripANSI(colored)); diff != "" {
		t.Errorf("colored output does not match plain once stripped (-plain +stripped):\n%s", diff)
	}
}

func TestRenderWideAddsTheDetailColumns(t *testing.T) {
	got := render(t, buildView(t, healthyFleet(), 0), RenderOptions{Wide: true})

	for _, col := range []string{"BORROWED", "CEILING", "ALLOCATABLE", "PEAK", "DEGRADED", "MATERIALIZED", "TIER", "NAMESPACES", "PATH"} {
		if !strings.Contains(got, col) {
			t.Errorf("wide output missing %s column", col)
		}
	}
	if !strings.Contains(got, "nlp-dev,nlp-prod") {
		t.Error("wide output does not list a leaf's bound namespaces, sorted")
	}
	if !strings.Contains(got, "/root/org-research/team-nlp") {
		t.Error("wide output does not show the computed path")
	}
	if !strings.Contains(got, "544") {
		t.Error("wide output does not show the capacity high-water mark")
	}
	// Wide states Ready literally; only the default view blanks it.
	if !strings.Contains(got, "True") {
		t.Error("wide output should spell out Ready rather than blanking it")
	}
}

// stripANSI removes SGR escapes so a styled render can be compared to a plain one.
func stripANSI(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '\x1b' {
			for i < len(s) && s[i] != 'm' {
				i++
			}
			i++ // consume the 'm'
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func twoFlavorFleet() []v1beta1.AcceleratorQuota {
	return []v1beta1.AcceleratorQuota{
		cohort("root", "",
			withSpec(specBudget(gpu, a100, "64"), specBudget(gpu, h100, "32")),
			withStatus(statusBudget(gpu, a100, "64", "16"), statusBudget(gpu, h100, "32", "8")),
			ready()),
		leaf("team", "root",
			withSpec(specBudget(gpu, a100, "64"), specBudget(gpu, h100, "32")),
			withStatus(statusBudget(gpu, a100, "64", "16"), statusBudget(gpu, h100, "32", "8")),
			ready()),
	}
}

func cycleAndContainmentFleet() []v1beta1.AcceleratorQuota {
	return []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "10"))),
		leaf("big", "root", withSpec(specBudget(gpu, a100, "99"))),
		cohort("loop-a", "loop-b"),
		cohort("loop-b", "loop-a"),
	}
}
