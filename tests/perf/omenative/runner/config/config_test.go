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
  - className: single-small
    count: 10
    replicas: 1
    workerSize: 0
    creationIntervalMs: 20
    image: ghcr.io/perf/runtime:small
  - className: gang
    count: 2
    replicas: 1
    workerSize: 3
    creationIntervalMs: 100
    image: ghcr.io/perf/runtime:gang
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
	if cfg.Total() != 12 {
		t.Fatalf("Total = %d, want 12", cfg.Total())
	}
	if cfg.Catalog.ServingRuntimes != 2 || cfg.Catalog.BaseModels != 3 {
		t.Fatalf("catalog mis-parsed: %+v", cfg.Catalog)
	}
	// single-small: 10 ISVCs * 1 replica * 1 pod = 10.
	// gang: 2 ISVCs * 1 replica * (3 workers + 1 leader) = 8.
	if got := cfg.TotalPods(); got != 18 {
		t.Fatalf("TotalPods = %d, want 18", got)
	}
}

func TestPodsPerISVC(t *testing.T) {
	cases := []struct {
		name     string
		cls      SizeClass
		wantPods int
	}{
		{"single pod", SizeClass{Replicas: 1, WorkerSize: 0}, 1},
		{"three single-pod instances", SizeClass{Replicas: 3, WorkerSize: 0}, 3},
		{"one gang of leader+2", SizeClass{Replicas: 1, WorkerSize: 2}, 3},
		{"two gangs of leader+3", SizeClass{Replicas: 2, WorkerSize: 3}, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cls.PodsPerISVC(); got != tc.wantPods {
				t.Fatalf("PodsPerISVC = %d, want %d", got, tc.wantPods)
			}
		})
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
  - className: single
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
  - className: single
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
  - className: single
    count: 1
    replicas: 1
    image: img
  - className: single
    count: 1
    replicas: 1
    image: img
`,
		"negative workerSize": `
catalog:
  servingRuntimes: 1
  baseModels: 1
  modelFormat: safetensors
  modelArchitecture: A
  modelFramework: F
  storageUri: oci://x
classes:
  - className: single
    count: 1
    replicas: 1
    workerSize: -1
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
  - className: single
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
