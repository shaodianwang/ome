package config

import (
	"os"
	"path/filepath"
	"testing"
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
    count: 10
    creationIntervalMs: 20
    image: ghcr.io/perf/runtime:small
  - className: large
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
