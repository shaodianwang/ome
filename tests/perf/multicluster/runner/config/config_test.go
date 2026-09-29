package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const validYAML = `
members:
  - name: member-fast
    admitDelayMs: 100
  - name: member-slow
    admitDelayMs: 1500
placement:
  acceleratorLabel: gpu
  acceleratorValue: test-accel
  controlPlaneId: perf-cp
  dispatcherMode: AllAtOnce
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 120000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
  resyncIntervalMs: 250
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 10
    creationIntervalMs: 20
    image: ghcr.io/perf/runtime:small
  - className: large
    runtimeName: runtime-large
    minReplicas: 1
    count: 5
    creationIntervalMs: 100
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
	if len(cfg.Members) != 2 {
		t.Fatalf("members = %d, want 2", len(cfg.Members))
	}
	if cfg.Placement.DispatcherMode != DispatcherAllAtOnce {
		t.Fatalf("dispatcherMode = %q, want AllAtOnce", cfg.Placement.DispatcherMode)
	}
	if cfg.Funnel.EnabledSafetyRequeueMs != 120000 || cfg.Funnel.DisabledSafetyRequeueMs != 1000 {
		t.Fatalf("funnel windows mis-parsed: %+v", cfg.Funnel)
	}
}

func TestLoadConfigRejectsMissingFields(t *testing.T) {
	cases := map[string]string{
		"no members": `
members: []
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: AllAtOnce
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 1
    image: img
`,
		"duplicate member": `
members:
  - name: m
  - name: m
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: AllAtOnce
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 1
    image: img
`,
		"bad dispatcher mode": `
members:
  - name: m
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: Bogus
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 1
    image: img
`,
		"zero requeue": `
members:
  - name: m
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: AllAtOnce
  requeueMs: 0
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 1
    image: img
`,
		"missing funnel disabled requeue": `
members:
  - name: m
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: AllAtOnce
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 1
    image: img
`,
		"missing class image": `
members:
  - name: m
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: AllAtOnce
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes:
  - className: small
    runtimeName: runtime-small
    minReplicas: 1
    count: 1
`,
		"no classes": `
members:
  - name: m
placement:
  acceleratorLabel: gpu
  acceleratorValue: a
  controlPlaneId: cp
  dispatcherMode: AllAtOnce
  requeueMs: 500
  clusterHealthIntervalMs: 500
funnel:
  enabledSafetyRequeueMs: 1000
  disabledSafetyRequeueMs: 1000
  batchPeriodMs: 100
classes: []
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

func TestLoadConfigRequiresMemberPolicy(t *testing.T) {
	for _, tt := range []struct{ name, from, to, want string }{
		{name: "runtime", from: "runtimeName: runtime-small", to: "runtimeName: ''", want: "runtimeName must be set"},
		{name: "zero floor", from: "minReplicas: 1", to: "minReplicas: 0", want: "minReplicas must be positive"},
		{name: "negative floor", from: "minReplicas: 1", to: "minReplicas: -1", want: "minReplicas must be positive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadConfig(writeTemp(t, strings.Replace(validYAML, tt.from, tt.to, 1)))
			got := err != nil && strings.Contains(err.Error(), tt.want)
			if diff := cmp.Diff(true, got); diff != "" {
				t.Fatalf("validation (-want +got): %s; error: %v", diff, err)
			}
		})
	}
}

func TestPlacementClientTuning(t *testing.T) {
	for _, tt := range []struct {
		name    string
		qps     float32
		burst   int
		wantErr bool
	}{
		{name: "client defaults"},
		{name: "explicit budget", qps: 50, burst: 100},
		{name: "missing burst", qps: 50, wantErr: true},
		{name: "missing rate", burst: 100, wantErr: true},
		{name: "negative rate", qps: -1, burst: 100, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeTemp(t, validYAML))
			if err != nil {
				t.Fatal(err)
			}
			cfg.Placement.ClientQPS, cfg.Placement.ClientBurst = tt.qps, tt.burst
			if diff := cmp.Diff(tt.wantErr, cfg.validate() != nil); diff != "" {
				t.Error(diff)
			}
		})
	}
}
