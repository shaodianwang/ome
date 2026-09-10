package quotatree

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// fakeLister serves a fixed set, standing in for an apiserver.
type fakeLister struct {
	quotas []v1beta1.AcceleratorQuota
	err    error
}

func (f fakeLister) List(context.Context) ([]v1beta1.AcceleratorQuota, error) {
	return f.quotas, f.err
}

func runWith(t *testing.T, o *Options) (string, error) {
	t.Helper()
	var out bytes.Buffer
	o.Out = &out
	if o.Output == "" {
		o.Output = OutputTree
	}
	if o.Color == "" {
		o.Color = ColorNever
	}
	err := o.Run(context.Background())
	return out.String(), err
}

func TestRunRendersTheTree(t *testing.T) {
	got, err := runWith(t, &Options{Lister: fakeLister{quotas: healthyFleet()}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, want := range []string{"root", "org-research", "team-nlp", "├─", "512"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunPropagatesAListError(t *testing.T) {
	sentinel := errors.New("connection refused")
	_, err := runWith(t, &Options{Lister: fakeLister{err: sentinel}})
	if !errors.Is(err, sentinel) {
		t.Errorf("Run error = %v, want it to wrap %v", err, sentinel)
	}
}

func TestRunOnAnEmptyCluster(t *testing.T) {
	got, err := runWith(t, &Options{Lister: fakeLister{}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(got, "No AcceleratorQuota resources found") {
		t.Errorf("unexpected output for an empty cluster:\n%s", got)
	}
}

func TestRunAppliesTheFilter(t *testing.T) {
	got, err := runWith(t, &Options{
		Lister: fakeLister{quotas: healthyFleet()},
		Filter: Filter{Node: "org-product"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(got, "team-nlp") {
		t.Errorf("--node did not exclude the sibling subtree:\n%s", got)
	}
	if !strings.Contains(got, "team-serving") {
		t.Errorf("--node dropped the selected subtree:\n%s", got)
	}
}

func TestRunReportsDepthOnlyWhenAsked(t *testing.T) {
	// The plugin cannot see the operator's quota config, so it must not invent
	// a depth limit and report a violation the controller never raised.
	quotas := []v1beta1.AcceleratorQuota{
		cohort("root", ""),
		cohort("l1", "root"),
		cohort("l2", "l1"),
		leaf("l3", "l2", withSpec(specBudget(gpu, a100, "8"))),
	}

	unlimited, err := runWith(t, &Options{Lister: fakeLister{quotas: quotas}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(unlimited, "DepthExceeded") {
		t.Errorf("depth reported with --max-depth unset:\n%s", unlimited)
	}

	limited, err := runWith(t, &Options{Lister: fakeLister{quotas: quotas}, MaxDepth: 2})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(limited, "DepthExceeded") {
		t.Errorf("--max-depth 2 did not flag a node three edges down:\n%s", limited)
	}
}

func TestRunDrawsTheSplitWhenAsked(t *testing.T) {
	got, err := runWith(t, &Options{Lister: fakeLister{quotas: splitFleet()}, Split: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, want := range []string{"member-a", "member-b", "WorkloadCluster", "128", "64"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunAnchorsOnTheFleetWhenAsked(t *testing.T) {
	got, err := runWith(t, &Options{
		Lister: fakeLister{quotas: fleetSplitAcrossClusters()},
		Axis:   ViewCluster,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.HasPrefix(strings.SplitN(got, "\n", 4)[3], "member-a") {
		t.Errorf("a cluster is not the first row:\n%s", got)
	}
	// The reserved root is under every cluster and says the same thing each
	// time, so it is not repeated.
	if strings.Contains(got, "\nroot ") {
		t.Errorf("the reserved root was repeated under a cluster:\n%s", got)
	}
}

func TestRunReportsAFleetAxisWithNoFleet(t *testing.T) {
	_, err := runWith(t, &Options{
		Lister: fakeLister{quotas: healthyFleet()},
		Axis:   ViewCluster,
	})
	if err == nil {
		t.Fatal("Run succeeded with --view cluster on a plane that resolves no split")
	}
	if !strings.Contains(err.Error(), "no fleet to anchor on") {
		t.Errorf("error = %q, want it to say why there is nothing to show", err)
	}
}

func TestRunASCII(t *testing.T) {
	got, err := runWith(t, &Options{Lister: fakeLister{quotas: healthyFleet()}, ASCII: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(got, "├") {
		t.Errorf("--ascii still emitted box-drawing characters:\n%s", got)
	}
}

func TestCompleteValidatesFlags(t *testing.T) {
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}

	for _, tc := range []struct {
		name    string
		o       Options
		output  string
		color   string
		axis    string
		wantErr string
	}{
		{"bad output", Options{Lister: fakeLister{}}, "table", "auto", "tenant", "invalid -o"},
		{"bad color", Options{Lister: fakeLister{}}, "tree", "sometimes", "tenant", "invalid --color"},
		{"bad view", Options{Lister: fakeLister{}}, "tree", "auto", "fleet", "invalid --view"},
		{"negative depth", Options{Lister: fakeLister{}, MaxDepth: -1}, "tree", "auto", "tenant", "invalid --max-depth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.o.Complete(tc.output, tc.color, tc.axis, loading, overrides)
			if err == nil {
				t.Fatalf("Complete succeeded, want an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCompleteKeepsAnInjectedLister(t *testing.T) {
	// Without this the command would reach for a kubeconfig in a unit test.
	o := Options{Lister: fakeLister{}}
	if err := o.Complete("json", "never", "cluster", clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if o.Output != OutputJSON || o.Color != ColorNever || o.Axis != ViewCluster {
		t.Errorf("Complete did not apply the parsed flags: %+v", o)
	}
}

func TestNewCommandRegistersTheExpectedFlags(t *testing.T) {
	cmd := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})

	for _, name := range []string{
		"output", "color", "ascii", "split", "view",
		"node", "leaves-only", "resource", "flavor", "max-depth",
		"request-timeout",
		// The standard connection flags, so the plugin behaves like kubectl.
		"kubeconfig", "context", "cluster", "user", "server",
	} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("flag --%s is not registered", name)
		}
	}

	// The split is the whole of what a management plane adds over a flat list,
	// so it is on unless an operator says otherwise. A workload cluster
	// resolves none, so the default costs nothing there.
	if got := cmd.Flags().Lookup("split").DefValue; got != "true" {
		t.Errorf("--split default = %q, want %q", got, "true")
	}
	for _, short := range []string{"o", "c"} {
		if cmd.Flags().ShorthandLookup(short) == nil {
			t.Errorf("-%s shorthand is not registered", short)
		}
	}

	// AcceleratorQuota is cluster-scoped, and tenancy is not namespace-scoped
	// either, so -n has nothing to select. Accepting it and doing nothing would
	// be worse than rejecting it.
	if cmd.Flags().Lookup("namespace") != nil || cmd.Flags().ShorthandLookup("n") != nil {
		t.Error("--namespace/-n is registered but would have no effect on a cluster-scoped resource")
	}
	if !strings.HasPrefix(cmd.Use, "kubectl-qt") {
		t.Errorf("Use = %q, want it to name the plugin binary", cmd.Use)
	}
}

func TestNewCommandRejectsPositionalArgs(t *testing.T) {
	cmd := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
	cmd.SetArgs([]string{"some-node"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})

	if err := cmd.Execute(); err == nil {
		t.Error("a bare positional argument was accepted; --node is the selector")
	}
}

func TestNewCommandVersionSkipsTheCluster(t *testing.T) {
	// --version has to work without a kubeconfig; cobra short-circuits before
	// RunE, which is why the flag is not handled in the run path.
	var out bytes.Buffer
	cmd := NewCommand(&out, &bytes.Buffer{})
	cmd.SetArgs([]string{"--version"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("--version: %v", err)
	}
	if !strings.HasPrefix(out.String(), "kubectl-qt version ") {
		t.Errorf("--version output = %q", out.String())
	}
}

func TestNewCommandWritesHelpToTheGivenStream(t *testing.T) {
	// A plugin that printed help straight to os.Stdout would be untestable and
	// would ignore a caller's redirection.
	var out bytes.Buffer
	cmd := NewCommand(&out, &bytes.Buffer{})
	cmd.SetArgs([]string{"--help"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("--help: %v", err)
	}
	if !strings.Contains(out.String(), "--leaves-only") {
		t.Errorf("help did not reach the provided writer:\n%s", out.String())
	}
}
