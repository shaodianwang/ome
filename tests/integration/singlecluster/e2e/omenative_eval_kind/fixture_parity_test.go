package omenative_eval_kind

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestMigratedFixturesMatchYAML(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		buildGo func(string) *v1beta1.InferenceService
	}{
		{
			name:    "p19",
			path:    "hack/omenative-testing/perm/isvcs/rollout/p19.yaml",
			buildGo: p19InferenceService,
		},
		{
			name:    "p322",
			path:    "hack/omenative-testing/perm/isvcs/reject/p322.yaml",
			buildGo: p322InferenceService,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := readEvalFixture(t, tt.path)
			got := tt.buildGo("fixture-namespace")
			if diff := cmp.Diff(want.Name, got.Name); diff != "" {
				t.Fatalf("metadata.name mismatch (-YAML +Go):\n%s", diff)
			}
			if diff := cmp.Diff(want.Spec, got.Spec); diff != "" {
				t.Fatalf("spec mismatch (-YAML +Go):\n%s", diff)
			}
		})
	}
}

func readEvalFixture(t *testing.T, relativePath string) *v1beta1.InferenceService {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve fixture test source path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../.."))
	contents, err := os.ReadFile(filepath.Join(repoRoot, relativePath))
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	isvc := &v1beta1.InferenceService{}
	if err := yaml.Unmarshal(contents, isvc); err != nil {
		t.Fatalf("decode %s: %v", relativePath, err)
	}
	return isvc
}
