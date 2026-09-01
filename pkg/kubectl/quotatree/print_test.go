package quotatree

import (
	"encoding/json"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestParseOutputFormat(t *testing.T) {
	for _, s := range []string{"tree", "wide", "json", "yaml"} {
		if _, err := ParseOutputFormat(s); err != nil {
			t.Errorf("ParseOutputFormat(%q) = %v, want nil", s, err)
		}
	}
	if _, err := ParseOutputFormat("table"); err == nil {
		t.Error("ParseOutputFormat(\"table\") succeeded, want an error naming the valid formats")
	}
}

func TestParseColorMode(t *testing.T) {
	for _, s := range []string{"auto", "always", "never"} {
		if _, err := ParseColorMode(s); err != nil {
			t.Errorf("ParseColorMode(%q) = %v, want nil", s, err)
		}
	}
	if _, err := ParseColorMode("yes"); err == nil {
		t.Error("ParseColorMode(\"yes\") succeeded, want an error")
	}
}

func TestPrintJSONNestsTheTree(t *testing.T) {
	var b strings.Builder
	if err := Print(&b, buildView(t, healthyFleet(), 0), OutputJSON, RenderOptions{}); err != nil {
		t.Fatalf("Print: %v", err)
	}

	var got struct {
		Root struct {
			Name    string `json:"name"`
			Role    string `json:"role"`
			Budgets []struct {
				ResourceName string `json:"resourceName"`
				Nominal      string `json:"nominal"`
				Admitted     string `json:"admitted"`
			} `json:"budgets"`
			Children []struct {
				Name     string `json:"name"`
				Children []struct {
					Name       string   `json:"name"`
					Namespaces []string `json:"namespaces"`
				} `json:"children"`
			} `json:"children"`
		} `json:"root"`
	}
	if err := json.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, b.String())
	}

	if got.Root.Name != "root" {
		t.Errorf("root name = %q, want %q", got.Root.Name, "root")
	}
	// Quantities marshal as strings, the same way they appear on the CR.
	if got.Root.Budgets[0].Nominal != "512" || got.Root.Budgets[0].Admitted != "288" {
		t.Errorf("root budget = %+v, want nominal 512 admitted 288", got.Root.Budgets[0])
	}
	if len(got.Root.Children) != 2 {
		t.Fatalf("root children = %d, want 2", len(got.Root.Children))
	}
	if got.Root.Children[0].Name != "org-product" {
		t.Errorf("first child = %q, want org-product (sorted)", got.Root.Children[0].Name)
	}
}

func TestPrintJSONCarriesTheSplitWhateverTheTreeDrew(t *testing.T) {
	// The machine formats are not width-constrained, so they carry the split
	// the same way they already carry capacity and namespaces the default tree
	// keeps behind -o wide.
	var b strings.Builder
	if err := Print(&b, buildView(t, splitFleet(), 0), OutputJSON, RenderOptions{}); err != nil {
		t.Fatalf("Print: %v", err)
	}

	var got View
	if err := json.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, b.String())
	}
	serving := got.Root.Children[0].Children[0]
	if serving.Name != "serving" {
		t.Fatalf("leaf = %q, want serving", serving.Name)
	}
	if want := "Proportional"; serving.Budgets[0].Policy != want {
		t.Errorf("policy = %q, want %q", serving.Budgets[0].Policy, want)
	}

	shares := serving.Budgets[0].PerCluster
	if len(shares) != 2 {
		t.Fatalf("per-cluster shares = %d, want 2", len(shares))
	}
	// Quantities marshal as strings, the same way they appear on the CR.
	if shares[0].Cluster != "member-a" || shares[0].Nominal.String() != "128" {
		t.Errorf("first share = %+v, want member-a at 128", shares[0])
	}
	if shares[1].Admitted.String() != "8" {
		t.Errorf("second share admitted = %s, want 8", shares[1].Admitted)
	}
}

func TestPrintJSONKeepsOrphansAndViolations(t *testing.T) {
	// A consumer reading only .root must not be able to conclude a broken tree
	// is healthy, so both have to survive serialization.
	var b strings.Builder
	v := buildView(t, cycleAndContainmentFleet(), 0)
	if err := Print(&b, v, OutputJSON, RenderOptions{}); err != nil {
		t.Fatalf("Print: %v", err)
	}

	var got View
	if err := json.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Orphans) == 0 {
		t.Error("orphans dropped from JSON output")
	}
	if len(got.Violations) == 0 {
		t.Fatal("violations dropped from JSON output")
	}
	for _, vi := range got.Violations {
		if vi.Reason == "" || vi.Message == "" {
			t.Errorf("violation %+v is missing reason or message", vi)
		}
	}
}

func TestPrintJSONOnEmptyEmitsAnObject(t *testing.T) {
	// "null" would be indistinguishable from a failed request.
	var b strings.Builder
	if err := Print(&b, nil, OutputJSON, RenderOptions{}); err != nil {
		t.Fatalf("Print: %v", err)
	}
	if got := strings.TrimSpace(b.String()); got != "{}" {
		t.Errorf("got %q, want %q", got, "{}")
	}
}

func TestPrintYAMLRoundTrips(t *testing.T) {
	var b strings.Builder
	if err := Print(&b, buildView(t, healthyFleet(), 0), OutputYAML, RenderOptions{}); err != nil {
		t.Fatalf("Print: %v", err)
	}

	var got View
	if err := yaml.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, b.String())
	}
	if got.Root == nil || got.Root.Name != "root" {
		t.Fatalf("root = %+v, want the root node", got.Root)
	}
	if len(got.Root.Children) != 2 {
		t.Errorf("root children = %d, want 2", len(got.Root.Children))
	}
}

func TestPrintWideSetsTheWideLayout(t *testing.T) {
	var b strings.Builder
	// Wide must win even when the caller passed RenderOptions with Wide unset.
	if err := Print(&b, buildView(t, healthyFleet(), 0), OutputWide, RenderOptions{Color: ColorNever}); err != nil {
		t.Fatalf("Print: %v", err)
	}
	if !strings.Contains(b.String(), "MATERIALIZED") {
		t.Errorf("-o wide did not select the wide layout:\n%s", b.String())
	}
}

func TestPrintRejectsAnUnknownFormat(t *testing.T) {
	var b strings.Builder
	if err := Print(&b, &View{}, OutputFormat("toml"), RenderOptions{}); err == nil {
		t.Error("Print accepted an unknown format")
	}
}
