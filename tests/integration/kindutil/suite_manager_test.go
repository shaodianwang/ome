package kindutil

import (
	"context"
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"
)

// stubManager is a non-nil ctrl.Manager so tests can observe that stopping
// clears the field rather than asserting on a value that started out nil.
type stubManager struct{ ctrl.Manager }

func TestStopManagerWithoutManagerIsNoop(t *testing.T) {
	s := &SuiteState{}
	if err := s.StopManager(); err != nil {
		t.Fatalf("StopManager on an idle state: %v", err)
	}
}

func TestStartManagerWithoutSetupErrors(t *testing.T) {
	s := &SuiteState{}
	err := s.StartManager()
	if err == nil {
		t.Fatal("StartManager without a setup callback must error")
	}
	if got, want := err.Error(), "SuiteState has no manager setup callback"; got != want {
		t.Fatalf("StartManager error = %q, want %q", got, want)
	}
}

func TestStartManagerRefusesWhenRunning(t *testing.T) {
	s := &SuiteState{
		managerSetup: func(context.Context, ctrl.Manager) error { return nil },
		managerStop:  func() {},
	}
	err := s.StartManager()
	if err == nil {
		t.Fatal("StartManager with a running manager must error")
	}
	if got, want := err.Error(), "manager is already running"; got != want {
		t.Fatalf("StartManager error = %q, want %q", got, want)
	}
}

func TestStopManagerStopsOnceAndClears(t *testing.T) {
	calls := 0
	s := &SuiteState{
		Manager:     stubManager{},
		managerStop: func() { calls++ },
	}
	if err := s.StopManager(); err != nil {
		t.Fatalf("first StopManager: %v", err)
	}
	if calls != 1 {
		t.Fatalf("managerStop calls after first StopManager = %d, want 1", calls)
	}
	if s.managerStop != nil {
		t.Fatal("managerStop must be cleared after StopManager")
	}
	if s.Manager != nil {
		t.Fatal("Manager must be nil after StopManager")
	}
	if err := s.StopManager(); err != nil {
		t.Fatalf("second StopManager: %v", err)
	}
	if calls != 1 {
		t.Fatalf("managerStop calls after second StopManager = %d, want 1", calls)
	}
}

func TestNilSuiteStateErrors(t *testing.T) {
	var s *SuiteState
	if err := s.StopManager(); err == nil {
		t.Error("StopManager on a nil SuiteState must error")
	}
	if err := s.StartManager(); err == nil {
		t.Error("StartManager on a nil SuiteState must error")
	}
	if err := s.RestartManager(); err == nil {
		t.Error("RestartManager on a nil SuiteState must error")
	}
}
