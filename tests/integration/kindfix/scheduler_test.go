package kindfix

import "testing"

func TestSchedulerFromEnv(t *testing.T) {
	t.Setenv(EnvScheduler, "")
	t.Setenv(EnvSchedulerImage, "")
	if SchedulerFromEnv() != nil || SchedulerName() != "" {
		t.Fatal("no scheduler requested must yield nil")
	}
	if got := StandardClusterServingRuntime("rt").Spec.SchedulerName; got != "" {
		t.Fatalf("runtime must not name a scheduler by default, got %q", got)
	}

	t.Setenv(EnvScheduler, " ome-scheduler ")
	s := SchedulerFromEnv()
	if s == nil || s.Name != "ome-scheduler" || s.Image != defaultSchedulerImage {
		t.Fatalf("unexpected install %+v", s)
	}
	if got := StandardClusterServingRuntime("rt").Spec.SchedulerName; got != "ome-scheduler" {
		t.Fatalf("runtime must select the requested scheduler, got %q", got)
	}

	t.Setenv(EnvSchedulerImage, "ome-scheduler:abc12345")
	if got := SchedulerFromEnv().Image; got != "ome-scheduler:abc12345" {
		t.Fatalf("image override ignored, got %q", got)
	}
	t.Setenv(EnvSchedulerWorkers, "9")
	if got := SchedulerFromEnv().MinWorkers; got != 9 {
		t.Fatalf("worker override ignored, got %d", got)
	}
}
