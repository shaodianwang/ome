package quotatree

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	clientset "github.com/ome-projects/ome/pkg/client/clientset/versioned"
	"github.com/ome-projects/ome/pkg/quota/tree"
	"github.com/ome-projects/ome/pkg/version"
)

// Lister fetches the AcceleratorQuota set to render. It is an interface so the
// command is testable without an apiserver.
type Lister interface {
	List(ctx context.Context) ([]v1beta1.AcceleratorQuota, error)
}

// Options is the resolved invocation.
type Options struct {
	Filter Filter

	// MaxDepth is the greatest permitted distance from the root, in edges.
	// Zero disables the check, which is pkg/quota/tree's own contract: the
	// plugin reads a cluster whose operator config it cannot see, so it must
	// not invent a limit and report a DepthExceeded the controller never raised.
	MaxDepth int

	Output OutputFormat
	Color  ColorMode
	ASCII  bool

	// Split draws each budget's per-cluster shares below the node. NewCommand
	// turns it on; the zero value leaves a caller driving Run directly with the
	// tree alone.
	Split bool

	// Axis is what the render is anchored on, from --view. Empty is ViewTenant.
	Axis ViewAxis

	// Timeout bounds Run when set. The cluster path leaves it zero and relies
	// on --request-timeout, which clientcmd applies to the rest config; it is
	// here so a caller driving Run with an injected Lister can still bound it.
	Timeout time.Duration

	Out    io.Writer
	ErrOut io.Writer

	// Lister is the source of nodes. Set by Complete from the kubeconfig, or
	// injected by a test.
	Lister Lister
}

// NewCommand builds the kubectl-qt root command.
//
// The name is the plugin's own: kubectl dispatches `kubectl qt` to a
// kubectl-qt binary on PATH, and the usage string has to match how the operator
// invoked it or the help text tells them to run something that does not exist.
func NewCommand(out, errOut io.Writer) *cobra.Command {
	o := &Options{Out: out, ErrOut: errOut}

	var (
		output string
		color  string
		axis   string
	)

	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}

	cmd := &cobra.Command{
		Use:   "kubectl-qt",
		Short: "Show the AcceleratorQuota hierarchy as a tree",
		Long: `Render every AcceleratorQuota in the cluster as the quota tree they describe.

An AcceleratorQuota is one node of a single-rooted tree: a Cohort groups other
nodes, a ClusterQueue is a leaf that binds serving namespaces and carries the
accelerator budget. The edges live in spec.parentRef on each node, so the shape
is only visible once the whole set is assembled — which is what this does.

Where the tree is authored the fleet total is divided into a share per workload
cluster, and those shares are drawn as a tier of WorkloadCluster rows below the node
that carries the budget. A projected copy holds one already-resolved share and
no split, so on a workload cluster there is nothing extra to draw.

Nodes with no position in the tree are reported separately rather than hidden,
and every invariant the set violates is listed with the node it is attributed
to. A budget shown with a trailing * was read from spec because status.budgets
carries no resolved value for it; one with a trailing ! was divided into shares
that do not add up to it.

--view cluster reads the same numbers along the other axis: one workload
cluster at the top of each tree, carrying the slice of the tenants it hosts.
A Cohort's figure there is what its tenants hold on that cluster between them.`,
		Example: `  # The whole tree, with each budget's per-cluster split.
  kubectl qt

  # The tree alone.
  kubectl qt --split=false

  # Anchored on the fleet: what is each cluster carrying?
  kubectl qt --view cluster

  # Every column, including the split policy, capacity, and all conditions.
  kubectl qt -o wide

  # One subtree.
  kubectl qt --node org-research

  # Which leaf charges a serving namespace, and where it sits.
  kubectl qt --for-namespace team-nlp-prod

  # One accelerator type across the fleet.
  kubectl qt --resource nvidia.com/gpu --flavor a100

  # Machine-readable, including orphans and violations.
  kubectl qt -o json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		// Cobra registers --version itself once this is set, which keeps the
		// flag out of the run path entirely.
		Version: fmt.Sprintf("%s (%s)", version.GitVersion, version.GitCommit),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := o.Complete(output, color, axis, loading, overrides); err != nil {
				return err
			}
			return o.Run(cmd.Context())
		},
	}
	// Help, usage, and --version all follow the streams the caller passed,
	// rather than defaulting to the process's own.
	cmd.SetOut(out)
	cmd.SetErr(errOut)

	flags := cmd.Flags()
	flags.StringVarP(&output, "output", "o", string(OutputTree),
		"Output format: tree, wide, json, or yaml.")
	flags.StringVarP(&color, "color", "c", string(ColorAuto),
		"When to colorize: auto, always, or never. auto colorizes only when writing "+
			"to a terminal, and is overridden by the NO_COLOR environment variable.")
	flags.BoolVar(&o.ASCII, "ascii", false,
		"Draw branches with ASCII instead of box-drawing characters.")
	flags.BoolVar(&o.Split, "split", true,
		"Show how each budget was divided across the fleet, as a tier of per-cluster "+
			"rows below the node. Pass --split=false for the tree alone.")
	flags.StringVar(&axis, "view", string(ViewTenant),
		"What to anchor the render on: tenant, the quota tree as authored; or "+
			"cluster, one workload cluster at the top of each tree carrying its slice of it.")

	flags.StringVar(&o.Filter.Node, "node", "",
		"Render only this node and its descendants.")
	flags.BoolVar(&o.Filter.LeavesOnly, "leaves-only", false,
		"Show only budget-carrying leaves, with the chain that leads to each.")
	flags.StringVar(&o.Filter.ResourceName, "resource", "",
		"Show budgets for this resource only, such as nvidia.com/gpu.")
	flags.StringVar(&o.Filter.ResourceFlavor, "flavor", "",
		"Show budgets on this Kueue ResourceFlavor only.")
	flags.IntVar(&o.MaxDepth, "max-depth", 0,
		"Report nodes deeper than this many edges from the root. 0 disables the check.")

	// The standard connection flags, so the plugin behaves like kubectl:
	// --kubeconfig, --context, --cluster, --user, --server, --request-timeout,
	// and the auth and TLS overrides. --request-timeout comes from this set and
	// is applied to the rest config by clientcmd, so the plugin does not define
	// a second one.
	flags.StringVar(&loading.ExplicitPath, clientcmd.RecommendedConfigPathFlag, "",
		"Path to the kubeconfig file to use.")
	overrideFlags := clientcmd.RecommendedConfigOverrideFlags("")
	// AcceleratorQuota is cluster-scoped, so -n would be accepted and then do
	// nothing — and it is the flag an operator would reach for when they mean
	// --for-namespace. Blanking LongName is how clientcmd suppresses a binding.
	overrideFlags.ContextOverrideFlags.Namespace.LongName = ""
	overrideFlags.ContextOverrideFlags.Namespace.ShortName = ""
	clientcmd.BindOverrideFlags(overrides, flags, overrideFlags)

	return cmd
}

// Complete validates the flags and, unless a Lister was injected, builds one
// from the kubeconfig.
func (o *Options) Complete(output, color, axis string, loading clientcmd.ClientConfigLoader, overrides *clientcmd.ConfigOverrides) error {
	format, err := ParseOutputFormat(output)
	if err != nil {
		return err
	}
	o.Output = format

	mode, err := ParseColorMode(color)
	if err != nil {
		return err
	}
	o.Color = mode

	anchor, err := ParseViewAxis(axis)
	if err != nil {
		return err
	}
	o.Axis = anchor

	if o.MaxDepth < 0 {
		return fmt.Errorf("invalid --max-depth %d: want a non-negative number of edges", o.MaxDepth)
	}
	if o.Lister != nil {
		return nil
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	// A deprecation warning from the apiserver would otherwise land in the
	// middle of the table and break the alignment the tree depends on.
	cfg.WarningHandler = rest.NoWarnings{}
	o.Lister, err = newClusterLister(cfg)
	return err
}

// Run assembles the tree and writes it.
func (o *Options) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if o.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Timeout)
		defer cancel()
	}

	quotas, err := o.Lister.List(ctx)
	if err != nil {
		return err
	}

	// RootName is the API's reserved name, not a deployment tunable, so it is
	// read from the type rather than exposed as a flag.
	t, violations, err := tree.Build(quotas, tree.Options{
		RootName: v1beta1.AcceleratorQuotaRootName,
		MaxDepth: o.MaxDepth,
	})
	if err != nil {
		return fmt.Errorf("assemble quota tree: %w", err)
	}

	view := BuildView(t, violations)
	// Filtered first, so --view sees the set the caller narrowed to: asking for
	// one tenant and then for the fleet axis should show the clusters that
	// tenant is on, not every cluster there is.
	if err := o.Filter.Apply(view); err != nil {
		return err
	}
	if o.Axis == ViewCluster {
		if err := ByCluster(view); err != nil {
			return err
		}
	}

	glyphs := UnicodeGlyphs
	if o.ASCII {
		glyphs = ASCIIGlyphs
	}
	return Print(o.Out, view, o.Output, RenderOptions{Glyphs: glyphs, Color: o.Color, Split: o.Split})
}

// clusterLister reads the nodes from an apiserver.
type clusterLister struct{ client clientset.Interface }

func newClusterLister(cfg *rest.Config) (Lister, error) {
	c, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build OME client: %w", err)
	}
	return &clusterLister{client: c}, nil
}

func (l *clusterLister) List(ctx context.Context) ([]v1beta1.AcceleratorQuota, error) {
	list, err := l.client.OmeV1beta1().AcceleratorQuotas().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list AcceleratorQuotas: %w", err)
	}
	return list.Items, nil
}
