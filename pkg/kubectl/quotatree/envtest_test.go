package quotatree

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	clientset "github.com/ome-projects/ome/pkg/client/clientset/versioned"
)

// TestAgainstLiveAPIServer exercises the path the unit tests cannot: the
// generated clientset against a real apiserver enforcing the real CRD, with
// status written through the status subresource.
//
// The rendering assertions stay deliberately thin — the table layout is covered
// against fixtures elsewhere. What this proves is that what the plugin lists
// back off the wire is what it was given, including the status the CRD stores
// separately from spec.
//
// Skipped without KUBEBUILDER_ASSETS:
//
//	export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
func TestAgainstLiveAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset; see the comment above for how to set it")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "config", "crd", "full", "ome.io_acceleratorquotas.yaml"),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	ctx := context.Background()
	client, err := clientset.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("build clientset: %v", err)
	}
	seedFleet(ctx, t, client)

	t.Run("tree", func(t *testing.T) {
		got := runAgainst(ctx, t, cfg, Options{Output: OutputTree, Color: ColorNever})

		// The tree is only visible once the whole set is assembled, which is
		// the thing being proved here: the CRs went in flat and came back as a
		// hierarchy.
		for _, want := range []string{
			"nvidia.com/gpu on flavor a100",
			"root",
			"├─ org-product",
			"└─ org-research",
			"   ├─ team-nlp",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("missing %q in:\n%s", want, got)
			}
		}
	})

	t.Run("status survives the round trip", func(t *testing.T) {
		got := runAgainst(ctx, t, cfg, Options{Output: OutputWide, Color: ColorNever})

		// Admitted only exists in status. Seeing it back means the plugin read
		// the subresource rather than falling back to spec.
		if !strings.Contains(got, "288") {
			t.Errorf("root's admitted quantity from status is missing:\n%s", got)
		}
		if strings.Contains(got, "*") {
			t.Errorf("a budget was marked unresolved though every node has status:\n%s", got)
		}
		if !strings.Contains(got, "/root/org-research/team-nlp") {
			t.Errorf("status.path missing from wide output:\n%s", got)
		}
	})

	t.Run("orphan is reported not hidden", func(t *testing.T) {
		orphan := leaf("stranded", "deleted-cohort", withSpec(specBudget(gpu, a100, "4")))
		if _, err := client.OmeV1beta1().AcceleratorQuotas().Create(ctx, &orphan, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create orphan: %v", err)
		}
		t.Cleanup(func() {
			_ = client.OmeV1beta1().AcceleratorQuotas().Delete(ctx, "stranded", metav1.DeleteOptions{})
		})

		got := runAgainst(ctx, t, cfg, Options{Output: OutputTree, Color: ColorNever})
		if !strings.Contains(got, "NODES WITH NO POSITION IN THE TREE") {
			t.Errorf("orphan section missing:\n%s", got)
		}
		if !strings.Contains(got, "ParentMissing") {
			t.Errorf("dangling parentRef not diagnosed:\n%s", got)
		}
	})

	t.Run("namespace lookup", func(t *testing.T) {
		got := runAgainst(ctx, t, cfg, Options{
			Output: OutputTree, Color: ColorNever,
			Filter: Filter{Namespace: "nlp-prod"},
		})
		if !strings.Contains(got, "team-nlp") {
			t.Errorf("charging leaf not found:\n%s", got)
		}
		if strings.Contains(got, "team-vision") {
			t.Errorf("unrelated leaf survived the namespace filter:\n%s", got)
		}
	})

	t.Run("json", func(t *testing.T) {
		got := runAgainst(ctx, t, cfg, Options{Output: OutputJSON, Color: ColorNever})
		if !strings.HasPrefix(strings.TrimSpace(got), "{") {
			t.Errorf("not JSON:\n%s", got)
		}
		if !strings.Contains(got, `"path": "/root/org-research/team-nlp"`) {
			t.Errorf("JSON missing the computed path:\n%s", got)
		}
	})
}

// runAgainst drives the real cluster path, including the generated clientset.
func runAgainst(ctx context.Context, t *testing.T, cfg *rest.Config, o Options) string {
	t.Helper()
	lister, err := newClusterLister(cfg)
	if err != nil {
		t.Fatalf("build lister: %v", err)
	}
	var out bytes.Buffer
	o.Lister, o.Out = lister, &out
	if err := o.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out.String()
}

// seedFleet writes the fixture through the apiserver, spec and status
// separately because the CRD declares a status subresource: a Create carrying
// status silently drops it, which would make every budget look unresolved.
func seedFleet(ctx context.Context, t *testing.T, client clientset.Interface) {
	t.Helper()
	api := client.OmeV1beta1().AcceleratorQuotas()

	for _, q := range healthyFleet() {
		spec := q.DeepCopy()
		spec.Status = v1beta1.AcceleratorQuotaStatus{}
		created, err := api.Create(ctx, spec, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create %s: %v", q.Name, err)
		}
		created.Status = *q.Status.DeepCopy()
		if _, err := api.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update status for %s: %v", q.Name, err)
		}
	}
}
