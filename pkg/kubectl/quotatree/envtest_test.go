package quotatree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/google/go-cmp/cmp"
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

	// The CRD's printer columns are the other reader of this data, and they are
	// held to the plugin's vocabulary: same names, same order, so `kubectl get
	// aq` and `kubectl qt` describe a node the same way.
	t.Run("printer columns", func(t *testing.T) {
		tbl := fetchTable(ctx, t, cfg)

		wantDefault := "NAME ROLE PARENT RESOURCE FLAVOR NOMINAL ADMITTED READY DEGRADED AGE"
		wantWide := "NAME ROLE PARENT RESOURCE FLAVOR NOMINAL ADMITTED BORROWED READY DEGRADED AGE PATH"
		if got := strings.Join(columnNames(tbl, false), " "); got != wantDefault {
			t.Errorf("default columns:\n got %s\nwant %s", got, wantDefault)
		}
		if got := strings.Join(columnNames(tbl, true), " "); got != wantWide {
			t.Errorf("wide columns:\n got %s\nwant %s", got, wantWide)
		}
		t.Logf("kubectl get aq\n%s", renderTable(tbl, false))
		t.Logf("kubectl get aq -o wide\n%s", renderTable(tbl, true))

		// Every node carries a budget, so a leaf reports its own allowance and
		// usage with no -o wide.
		nlp := cellsOf(t, tbl, "team-nlp")
		if nlp["NOMINAL"] != "128" || nlp["ADMITTED"] != "96" {
			t.Errorf("a leaf's budget is missing from the default view: %v", nlp)
		}
		// Observed capacity is the root's alone, so it is no column here: in a
		// flat list it would be blank on every row but one. The plugin keeps
		// it, having a tree to put it in.
		for _, col := range []string{"ALLOCATABLE", "PEAK"} {
			if _, ok := nlp[col]; ok {
				t.Errorf("%s is a column again, and only the root can fill it", col)
			}
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

	// Last, because it writes a split onto a node the subtests above read.
	//
	// status.budgets[].perCluster is a list-map keyed on cluster, which only a
	// real apiserver enforces: a fixture can hold two rows for one cluster and
	// a unit test would never know. The shares are made to sum, so the tree
	// this leaves behind is still the healthy one.
	t.Run("split survives the round trip", func(t *testing.T) {
		api := client.OmeV1beta1().AcceleratorQuotas()
		nlp, err := api.Get(ctx, "team-nlp", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get team-nlp: %v", err)
		}
		nlp.Status.Budgets = []v1beta1.AcceleratorBudgetStatus{
			splitAcross(statusBudget(gpu, a100, "128", "96"),
				share("member-a", "96", "72"),
				share("member-b", "32", "24")),
		}
		if _, err := api.UpdateStatus(ctx, nlp, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("write the split: %v", err)
		}

		// The leading │ is load-bearing: team-vision follows this leaf, so the
		// tier must not close its parent's line on the way past.
		got := runAgainst(ctx, t, cfg, Options{Output: OutputTree, Color: ColorNever, Split: true})
		for cluster, want := range map[string][]string{
			"member-a": {"│", "├─", "member-a", "WorkloadCluster", "96", "72"},
			"member-b": {"│", "└─", "member-b", "WorkloadCluster", "32", "24"},
		} {
			if diff := cmp.Diff(want, strings.Fields(lineWith(t, got, cluster))); diff != "" {
				t.Errorf("per-cluster row mismatch (-want +got):\n%s\nin:\n%s", diff, got)
			}
		}
		if strings.Contains(got, "128!") {
			t.Errorf("a split that adds up was marked short:\n%s", got)
		}

		// Absent, not empty: the leaves beside it resolve no split, and a
		// consumer must be able to tell that from a split of nothing.
		asJSON := runAgainst(ctx, t, cfg, Options{Output: OutputJSON, Color: ColorNever})
		if !strings.Contains(asJSON, `"cluster": "member-a"`) {
			t.Errorf("the split is missing from JSON:\n%s", asJSON)
		}
		if strings.Contains(asJSON, `"perCluster": []`) {
			t.Errorf("a leaf with no split emitted an empty list:\n%s", asJSON)
		}
	})
}

// lineWith is the one rendered line containing want. Column padding depends on
// the widest name in the whole table, so an assertion that spelled a row out
// verbatim would break whenever an unrelated node was renamed.
func lineWith(t *testing.T, out, want string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, want) {
			return line
		}
	}
	t.Fatalf("no line containing %q in:\n%s", want, out)
	return ""
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

// fetchTable asks the apiserver for the same server-side Table kubectl renders.
// That is where a printer column's JSONPath is actually evaluated, and a path
// matching nothing yields an empty cell rather than an error — so nothing short
// of the rendered table shows whether a column works.
func fetchTable(ctx context.Context, t *testing.T, cfg *rest.Config) *metav1.Table {
	t.Helper()
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatalf("build http client: %v", err)
	}
	url := strings.TrimSuffix(cfg.Host, "/") + "/apis/ome.io/v1beta1/acceleratorquotas"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("list as table: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list as table %s: %s: %s", url, resp.Status, body)
	}
	var tbl metav1.Table
	if err := json.Unmarshal(body, &tbl); err != nil {
		t.Fatalf("decode table: %v", err)
	}
	return &tbl
}

// keptColumns applies kubectl's own rule: a column carrying a non-zero priority
// is dropped unless -o wide was asked for, and the rest keep their declared
// order.
func keptColumns(tbl *metav1.Table, wide bool) []int {
	var out []int
	for i, c := range tbl.ColumnDefinitions {
		if c.Priority != 0 && !wide {
			continue
		}
		out = append(out, i)
	}
	return out
}

func columnNames(tbl *metav1.Table, wide bool) []string {
	kept := keptColumns(tbl, wide)
	out := make([]string, 0, len(kept))
	for _, i := range kept {
		out = append(out, strings.ToUpper(tbl.ColumnDefinitions[i].Name))
	}
	return out
}

func renderTable(tbl *metav1.Table, wide bool) string {
	var b bytes.Buffer
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(columnNames(tbl, wide), "\t"))
	for _, r := range tbl.Rows {
		cells := []string{}
		for _, i := range keptColumns(tbl, wide) {
			cells = append(cells, cellText(r.Cells, i))
		}
		fmt.Fprintln(w, strings.Join(cells, "\t"))
	}
	_ = w.Flush()
	return b.String()
}

func cellText(cells []interface{}, i int) string {
	if i >= len(cells) || cells[i] == nil {
		return ""
	}
	return fmt.Sprintf("%v", cells[i])
}

// cellsOf indexes one object's row by column name, including the columns -o
// wide would have hidden.
func cellsOf(t *testing.T, tbl *metav1.Table, name string) map[string]string {
	t.Helper()
	for _, r := range tbl.Rows {
		if cellText(r.Cells, 0) != name {
			continue
		}
		out := map[string]string{}
		for i, c := range tbl.ColumnDefinitions {
			out[strings.ToUpper(c.Name)] = cellText(r.Cells, i)
		}
		return out
	}
	t.Fatalf("no row for %q", name)
	return nil
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
