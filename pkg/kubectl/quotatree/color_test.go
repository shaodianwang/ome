package quotatree

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// styledLine finds the rendered line whose plain text contains want, and
// returns it with escapes intact.
func styledLine(t *testing.T, out, want string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(stripANSI(line), want) {
			return line
		}
	}
	t.Fatalf("no line containing %q in:\n%s", want, out)
	return ""
}

// styleOf returns the SGR parameters wrapping the first occurrence of word.
func styleOf(line, word string) string {
	re := regexp.MustCompile(`\x1b\[([0-9;]+)m` + regexp.QuoteMeta(word) + `\x1b\[0m`)
	if m := re.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

func colored(t *testing.T, v *View, opts RenderOptions) string {
	t.Helper()
	opts.Color = ColorAlways
	var b strings.Builder
	if err := Render(&b, v, opts); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return b.String()
}

func TestColorMarksTheCauseNotTheBlastRadius(t *testing.T) {
	// A violation on the root freezes every descendant. Coloring all of them
	// would bury the one node at fault under its own consequences, so only the
	// cause is colored and the frozen nodes simply lose their bold.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "10"))),
		cohort("mid", "root", withSpec(specBudget(gpu, a100, "500"))),
		leaf("below", "mid", withSpec(specBudget(gpu, a100, "500"))),
	}
	out := colored(t, buildView(t, quotas, 0), RenderOptions{})

	if got, want := styleOf(styledLine(t, out, "root"), "root"), string(styleRed); got != want {
		t.Errorf("root style = %q, want %q; it is the node that broke containment", got, want)
	}
	for _, frozen := range []string{"mid", "below"} {
		line := styledLine(t, out, frozen)
		if s := styleOf(line, frozen); s != "" {
			t.Errorf("%s style = %q, want unstyled; it is frozen, not at fault", frozen, s)
		}
	}
}

func TestColorBoldsHealthyNames(t *testing.T) {
	out := colored(t, buildView(t, healthyFleet(), 0), RenderOptions{})

	if got, want := styleOf(styledLine(t, out, "team-nlp"), "team-nlp"), string(styleBold); got != want {
		t.Errorf("healthy node style = %q, want bold %q", got, want)
	}
}

func TestColorRecedesTheBranchArt(t *testing.T) {
	// The glyphs are structure, not content: they should lead the eye without
	// competing with the names.
	out := colored(t, buildView(t, healthyFleet(), 0), RenderOptions{})
	line := styledLine(t, out, "team-nlp")

	if !strings.Contains(line, "\x1b["+string(styleDim)+"m") {
		t.Errorf("branch art is not dimmed:\n%q", line)
	}
	// Art and name are separately styled, so the name is not inside the dim run.
	if styleOf(line, "team-nlp") == string(styleDim) {
		t.Error("the node name was dimmed along with the branch art")
	}
}

func TestColorConditionsByMeaningNotLiteral(t *testing.T) {
	// Degraded=True is bad where Ready=True is good, so the two cannot be
	// colored off the same string.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "",
			withSpec(specBudget(gpu, a100, "8")),
			withCondition(v1beta1.AcceleratorQuotaReady, "True"),
			withCondition(v1beta1.AcceleratorQuotaDegraded, "True")),
	}
	out := colored(t, buildView(t, quotas, 0), RenderOptions{Wide: true})
	line := styledLine(t, out, "root")

	if !strings.Contains(line, "\x1b["+string(styleGreen)+"mTrue") {
		t.Errorf("Ready=True is not green:\n%q", line)
	}
	if !strings.Contains(line, "\x1b["+string(styleRed)+"mTrue") {
		t.Errorf("Degraded=True is not red:\n%q", line)
	}
}

func TestColorMarksUnresolvedBudgetsAsProvisional(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "64"))),
	}
	out := colored(t, buildView(t, quotas, 0), RenderOptions{})

	if got, want := styleOf(styledLine(t, out, "64*"), "64*"), string(styleYellow); got != want {
		t.Errorf("spec-sourced nominal style = %q, want %q", got, want)
	}
}

func TestColorMarksASplitThatDoesNotAddUp(t *testing.T) {
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "120"))),
		leaf("team", "root",
			withSpec(specBudget(gpu, a100, "120")),
			withStatus(splitAcross(statusBudget(gpu, a100, "120", "30"),
				share("member-a", "90", "30")))),
	}
	out := colored(t, buildView(t, quotas, 0), RenderOptions{Split: true})

	if got, want := styleOf(styledLine(t, out, "120!"), "120!"), string(styleRed); got != want {
		t.Errorf("short-split nominal style = %q, want %q", got, want)
	}
}

func TestColorSetsAPerClusterRowApartFromTheNodes(t *testing.T) {
	// A cluster holds no position in the quota tree, so it must not carry the
	// bold a live node does — otherwise an eye scanning the column for tenants
	// finds members among them.
	out := colored(t, buildView(t, splitFleet(), 0), RenderOptions{Split: true})

	if got := styleOf(styledLine(t, out, "member-a"), "member-a"); got == string(styleBold) {
		t.Error("a per-cluster row is bolded like a node in the tree")
	}
	if got, want := styleOf(styledLine(t, out, "member-a"), roleWorkloadCluster), string(styleDim); got != want {
		t.Errorf("WorkloadCluster role style = %q, want dim %q", got, want)
	}
	if got, want := styleOf(styledLine(t, out, "serving"), "serving"), string(styleBold); got != want {
		t.Errorf("the node above it lost its bold: %q, want %q", got, want)
	}
}

func TestColorSeparatesFilterMatchesFromTheirPath(t *testing.T) {
	// --leaves-only keeps the ancestors so you can see where each leaf sits.
	// They are the path to the answer, not the answer, and have to read that
	// way — otherwise the flag looks like it did nothing on a tree it pruned
	// nothing from.
	v := buildView(t, healthyFleet(), 0)
	if err := (Filter{LeavesOnly: true}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	out := colored(t, v, RenderOptions{})

	for _, context := range []string{"root", "org-research"} {
		if got, want := styleOf(styledLine(t, out, context), context), string(styleDim); got != want {
			t.Errorf("%s style = %q, want dim %q; it is context, not an answer", context, got, want)
		}
	}
	if got, want := styleOf(styledLine(t, out, "team-nlp"), "team-nlp"), string(styleBold); got != want {
		t.Errorf("team-nlp style = %q, want bold %q; it is the match", got, want)
	}
}

func TestColorNeverDimsAFaultAwayAsContext(t *testing.T) {
	// root is both the node at fault and, under this filter, merely the path to
	// the match. Receding would be the filter concealing the fault.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", "", withSpec(specBudget(gpu, a100, "10"))),
		leaf("team", "root", withSpec(specBudget(gpu, a100, "8"))),
		leaf("hog", "root", withSpec(specBudget(gpu, a100, "99"))),
	}
	v := buildView(t, quotas, 0)
	if err := (Filter{LeavesOnly: true}).Apply(v); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	out := colored(t, v, RenderOptions{})

	if got, want := styleOf(styledLine(t, out, "root"), "root"), string(styleRed); got != want {
		t.Errorf("root style = %q, want %q; a filter must not dim away the node at fault", got, want)
	}
}

func TestColorAutoStaysOffWhenNotATerminal(t *testing.T) {
	// A strings.Builder is not a terminal, so auto must produce clean text that
	// survives a pipe into grep or jq.
	var b strings.Builder
	if err := Render(&b, buildView(t, healthyFleet(), 0), RenderOptions{Color: ColorAuto}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if strings.Contains(b.String(), "\x1b[") {
		t.Error("--color=auto emitted escapes into a non-terminal writer")
	}
}

func TestColorNeverBeatsEverything(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	var b strings.Builder
	if err := Render(&b, buildView(t, healthyFleet(), 0), RenderOptions{Color: ColorNever}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if strings.Contains(b.String(), "\x1b[") {
		t.Error("--color=never emitted escapes")
	}
}

func TestNewPaletteHonorsNoColor(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("NO_COLOR", "1")

	if newPalette(ColorAuto, nil).on {
		t.Error("auto ignored NO_COLOR")
	}
	// An explicit --color=always is the operator overriding their own
	// environment, so it still wins.
	if !newPalette(ColorAlways, nil).on {
		t.Error("--color=always was suppressed by NO_COLOR")
	}
}

func TestNewPaletteHonorsDumbTerminals(t *testing.T) {
	for _, term := range []string{"", "dumb"} {
		t.Run("TERM="+term, func(t *testing.T) {
			t.Setenv("TERM", term)
			if newPalette(ColorAuto, nil).on {
				t.Errorf("auto enabled color for TERM=%q", term)
			}
		})
	}
}
