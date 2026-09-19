package v1beta1

import (
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func TestTrafficMapCRD_EntryWeightBounds(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "config", "crd", "full", "ome.io_trafficmaps.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Schema == nil || crd.Spec.Versions[0].Schema.OpenAPIV3Schema == nil {
		t.Fatalf("%s: expected exactly one served version with a schema", path)
	}

	root := *crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	spec := requireSchemaProperty(t, root, "spec")
	entries := requireSchemaProperty(t, spec, "entries")
	if entries.Items == nil || entries.Items.Schema == nil {
		t.Fatal("spec.entries has no item schema")
	}
	weight := requireSchemaProperty(t, *entries.Items.Schema, "weight")
	if weight.Minimum == nil || *weight.Minimum != 0 {
		t.Errorf("spec.entries[].weight minimum = %v, want 0", weight.Minimum)
	}
	if weight.Maximum == nil || *weight.Maximum != 1_000_000 {
		t.Errorf("spec.entries[].weight maximum = %v, want 1000000", weight.Maximum)
	}
}
