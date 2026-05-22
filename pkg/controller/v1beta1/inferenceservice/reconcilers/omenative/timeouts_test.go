package omenative

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func pastDeadline() metav1.Time { return metav1.NewTime(time.Now().Add(-1 * time.Hour)) }
func futureDeadline() metav1.Time {
	return metav1.NewTime(time.Now().Add(1 * time.Hour))
}

func TestOperationDeadlinePassed(t *testing.T) {
	if operationDeadlinePassed(nil, time.Now()) {
		t.Errorf("nil status should not be expired")
	}
	if operationDeadlinePassed(&v1beta1.OMENativeInstanceStatus{}, time.Now()) {
		t.Errorf("nil Operation should not be expired")
	}
	zero := &v1beta1.OMENativeInstanceStatus{Operation: &v1beta1.InstanceOperation{}}
	if operationDeadlinePassed(zero, time.Now()) {
		t.Errorf("zero Deadline must be treated as no-expiry")
	}
	future := &v1beta1.OMENativeInstanceStatus{Operation: &v1beta1.InstanceOperation{Deadline: futureDeadline()}}
	if operationDeadlinePassed(future, time.Now()) {
		t.Errorf("future Deadline should not be expired")
	}
	past := &v1beta1.OMENativeInstanceStatus{Operation: &v1beta1.InstanceOperation{Deadline: pastDeadline()}}
	if !operationDeadlinePassed(past, time.Now()) {
		t.Errorf("past Deadline should be expired")
	}
}

func TestExpireOperationsIfDeadlinePassed_NilClient(t *testing.T) {
	err := ExpireOperationsIfDeadlinePassed(context.Background(), ReconcileParams{}, ComponentPlan{Component: v1beta1.EngineComponent})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestExpireOperationsIfDeadlinePassed_InPlaceFallsBackToRecreate(t *testing.T) {
	// In-place Update past Deadline -> Operation cleared, Phase=Ready.
	// detectUpdateTrigger refires next reconcile, chooseUpdateMode
	// routes through recreate.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "old-rev",
		TargetRevision:  "new-rev",
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Step:     updateStepInPlace,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation != nil {
		t.Errorf("Operation must be cleared after in-place fallback; got %+v", s.Operation)
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.TargetRevision != "new-rev" {
		t.Errorf("TargetRevision must be preserved for detectUpdateTrigger; got %q", s.TargetRevision)
	}
	if s.RunningRevision != "old-rev" {
		t.Errorf("RunningRevision must be preserved (mismatch is the trigger); got %q", s.RunningRevision)
	}
}

func TestExpireOperationsIfDeadlinePassed_RecreateUpdateStampsStuck(t *testing.T) {
	// Recreate-mode Update past Deadline -> mark stuck, do NOT clear.
	// (Drain/Recreate steps may have committed pod-delete side effects;
	// force-progress would risk worse states.)
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "old-rev",
		TargetRevision:  "new-rev",
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Step:     updateStepDrain,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil {
		t.Fatalf("Operation must be preserved for recreate; got nil")
	}
	if s.Operation.Reason != timeoutReasonExceeded {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, timeoutReasonExceeded)
	}
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: must stay Updating; got %q", s.Phase)
	}
}

func TestExpireOperationsIfDeadlinePassed_FutureDeadlineNoOp(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	original := v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Step:     updateStepInPlace,
			Deadline: futureDeadline(),
		},
	}
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = original
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.Step != updateStepInPlace {
		t.Errorf("future Deadline must leave Operation untouched; got %+v", s.Operation)
	}
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
}

func TestExpireOperationsIfDeadlinePassed_StuckIsIdempotent(t *testing.T) {
	// Already-stuck Operation must not get re-written every reconcile
	// (would churn LastTransitionTime on the eventual condition).
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceMigrating,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationMigrate,
			Step:     "CreateSurge",
			Reason:   timeoutReasonExceeded,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	// The mutate function's skip-write guard means the second call is a
	// no-op. Running twice should not raise an error.
	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("second call: %v", err)
	}
}

func TestExpireOperationsIfDeadlinePassed_PerInstanceIndependent(t *testing.T) {
	// One Instance past Deadline (in-place), another still in progress
	// (recreate, deadline in the future) — only the first should change.
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{
						Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
						RunningRevision: "old-rev", TargetRevision: "new-rev",
						Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: updateStepInPlace, Deadline: pastDeadline()},
					},
					{
						Index: 1, Phase: v1beta1.OMENativeInstanceUpdating,
						Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: updateStepDrain, Deadline: futureDeadline()},
					},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	for _, s := range fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses {
		switch s.Index {
		case 0:
			if s.Operation != nil {
				t.Errorf("Instance 0 (in-place + past deadline) must clear Operation; got %+v", s.Operation)
			}
			if s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("Instance 0 Phase: got %q want Ready", s.Phase)
			}
		case 1:
			if s.Operation == nil {
				t.Fatalf("Instance 1 (recreate + future deadline) must preserve Operation")
			}
			if s.Operation.Reason == timeoutReasonExceeded {
				t.Errorf("Instance 1 must not be stamped stuck; got Reason=%q", s.Operation.Reason)
			}
		}
	}
}
