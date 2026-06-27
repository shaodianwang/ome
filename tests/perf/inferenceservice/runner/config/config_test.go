package config

import (
	"os"
	"path/filepath"
	"testing"
)

const validYAML = `
catalog:
  servingRuntimes: 2
  baseModels: 3
  modelFormat: safetensors
  modelArchitecture: LlamaForCausalLM
  modelFramework: Transformers
  storageUri: oci://n/perf/b/models/o/llama
classes:
  - className: small
    count: 10
    replicas: 1
    creationIntervalMs: 20
    readyAfterMs: 500
    image: ghcr.io/perf/runtime:small
  - className: large
    count: 5
    replicas: 4
    creationIntervalMs: 100
    readyAfterMs: 5000
    image: ghcr.io/perf/runtime:large
`

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gen.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write temp: %s", err)
	}
	return p
}

func TestLoadConfigValid(t *testing.T) {
	cfg, err := LoadConfig(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("LoadConfig: %s", err)
	}
	if cfg.Total() != 15 {
		t.Fatalf("Total = %d, want 15", cfg.Total())
	}
	if cfg.Catalog.ServingRuntimes != 2 || cfg.Catalog.BaseModels != 3 {
		t.Fatalf("catalog mis-parsed: %+v", cfg.Catalog)
	}
}

func TestLoadConfigRejectsMissingFields(t *testing.T) {
	cases := map[string]string{
		"no classes": `
catalog:
  servingRuntimes: 1
  baseModels: 1
  modelFormat: safetensors
  modelArchitecture: A
  modelFramework: F
  storageUri: oci://x
classes: []
`,
		"zero servingRuntimes": `
catalog:
  servingRuntimes: 0
  baseModels: 1
  modelFormat: safetensors
  modelArchitecture: A
  modelFramework: F
  storageUri: oci://x
classes:
  - className: small
    count: 1
    replicas: 1
    image: img
`,
		"missing image": `
catalog:
  servingRuntimes: 1
  baseModels: 1
  modelFormat: safetensors
  modelArchitecture: A
  modelFramework: F
  storageUri: oci://x
classes:
  - className: small
    count: 1
    replicas: 1
`,
		"duplicate class": `
catalog:
  servingRuntimes: 1
  baseModels: 1
  modelFormat: safetensors
  modelArchitecture: A
  modelFramework: F
  storageUri: oci://x
classes:
  - className: small
    count: 1
    replicas: 1
    image: img
  - className: small
    count: 1
    replicas: 1
    image: img
`,
		"missing storageUri": `
catalog:
  servingRuntimes: 1
  baseModels: 1
  modelFormat: safetensors
  modelArchitecture: A
  modelFramework: F
classes:
  - className: small
    count: 1
    replicas: 1
    image: img
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(writeTemp(t, body)); err == nil {
				t.Fatalf("expected error for %q, got nil", name)
			}
		})
	}
}

func TestLoadConfigUnknownFieldRejected(t *testing.T) {
	// Strict parsing must reject a typo'd field so a misnamed knob can't
	// silently fall back to a zero value.
	body := validYAML + "\nbogusField: 1\n"
	if _, err := LoadConfig(writeTemp(t, body)); err == nil {
		t.Fatalf("expected strict-parse error for unknown field, got nil")
	}
}
