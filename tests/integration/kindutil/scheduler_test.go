package kindutil

import (
	"context"
	"testing"
)

func TestWithSchedulerCRDsAddsThePodGroupCRDOnce(t *testing.T) {
	base := []string{"config/crd/full", "tests/integration/dep-crds/keda"}
	if got := withSchedulerCRDs(base, nil); len(got) != len(base) {
		t.Fatalf("nil scheduler must not change the paths, got %v", got)
	}
	s := &SchedulerInstall{Name: "ome-scheduler", Image: "ome-scheduler:kind-local"}
	got := withSchedulerCRDs(base, s)
	if len(got) != len(base)+1 || got[len(got)-1] != schedulerCRDPath {
		t.Fatalf("expected the scheduler-plugins CRD path appended, got %v", got)
	}
	if len(base) != 2 {
		t.Fatalf("input slice was mutated: %v", base)
	}
	again := withSchedulerCRDs(got, s)
	if len(again) != len(got) {
		t.Fatalf("path must not be appended twice, got %v", again)
	}
	trailing := withSchedulerCRDs([]string{schedulerCRDPath + "/"}, s)
	if len(trailing) != 1 {
		t.Fatalf("a trailing slash still counts as present, got %v", trailing)
	}
}

func TestInstallSchedulerValidation(t *testing.T) {
	if err := installScheduler(context.Background(), "any", nil); err != nil {
		t.Fatalf("nil install must be a no-op, got %v", err)
	}
	for _, s := range []*SchedulerInstall{
		{Name: "", Image: "ome-scheduler:kind-local"},
		{Name: "ome-scheduler", Image: ""},
		{Name: "ome-scheduler", Image: "no-tag"},
		{Name: "ome-scheduler", Image: "registry:5000/ome-scheduler"},
	} {
		if err := installScheduler(context.Background(), "any", s); err == nil {
			t.Fatalf("expected a validation error for %+v", s)
		}
	}
	s := &SchedulerInstall{}
	if s.chartPath() != "charts/ome-scheduler" || s.namespace() != "ome" {
		t.Fatalf("defaults: chart %q namespace %q", s.chartPath(), s.namespace())
	}
	if s.topologyKey() != "topology.kubernetes.io/zone" || s.topologyDomain() != "ome-kind" {
		t.Fatalf("topology defaults: key %q domain %q", s.topologyKey(), s.topologyDomain())
	}
	if s.minWorkers() != 6 || (&SchedulerInstall{MinWorkers: 3}).minWorkers() != 3 {
		t.Fatalf("worker defaults: %d", s.minWorkers())
	}
}
