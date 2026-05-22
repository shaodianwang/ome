package omenative

import (
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestAvailableCondition(t *testing.T) {
	cases := []struct {
		name       string
		cs         *v1beta1.OMENativeComponentStatus
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "no replicas yet",
			cs:         &v1beta1.OMENativeComponentStatus{},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNoReplicas,
		},
		{
			name: "all available",
			cs: &v1beta1.OMENativeComponentStatus{
				Replicas:          2,
				AvailableReplicas: 2,
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonReady,
		},
		{
			name: "partially available",
			cs: &v1beta1.OMENativeComponentStatus{
				Replicas:          3,
				AvailableReplicas: 1,
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonNotAllAvailable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conditions := computeComponentConditions(tc.cs, 7)
			available := apimeta.FindStatusCondition(conditions, ConditionAvailable)
			if available == nil {
				t.Fatalf("Available condition missing")
			}
			if available.Status != tc.wantStatus {
				t.Errorf("Status: got %s want %s", available.Status, tc.wantStatus)
			}
			if available.Reason != tc.wantReason {
				t.Errorf("Reason: got %q want %q", available.Reason, tc.wantReason)
			}
			if available.ObservedGeneration != 7 {
				t.Errorf("ObservedGeneration not stamped: got %d", available.ObservedGeneration)
			}
		})
	}
}

func TestProgressingCondition_RolloutAndPhaseTransitions(t *testing.T) {
	cases := []struct {
		name       string
		cs         *v1beta1.OMENativeComponentStatus
		wantStatus metav1.ConditionStatus
	}{
		{
			name:       "no rollout",
			cs:         &v1beta1.OMENativeComponentStatus{CurrentRevision: "r1", UpdateRevision: "r1"},
			wantStatus: metav1.ConditionFalse,
		},
		{
			name: "revision diff",
			cs: &v1beta1.OMENativeComponentStatus{
				CurrentRevision: "r1", UpdateRevision: "r2",
				Replicas: 2, UpdatedReadyReplicas: 1,
			},
			wantStatus: metav1.ConditionTrue,
		},
		{
			name: "transient phase",
			cs: &v1beta1.OMENativeComponentStatus{
				CurrentRevision: "r1", UpdateRevision: "r1",
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceMigrating},
				},
			},
			wantStatus: metav1.ConditionTrue,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conditions := computeComponentConditions(tc.cs, 1)
			progressing := apimeta.FindStatusCondition(conditions, ConditionProgressing)
			if progressing == nil {
				t.Fatalf("Progressing condition missing")
			}
			if progressing.Status != tc.wantStatus {
				t.Errorf("Status: got %s want %s", progressing.Status, tc.wantStatus)
			}
		})
	}
}

func TestOperationStuckCondition_LiftsDeadlineExceededBreadcrumb(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Operation: &v1beta1.InstanceOperation{Type: "Update", Reason: timeoutReasonExceeded}},
			{Index: 1, Operation: &v1beta1.InstanceOperation{Type: "Update"}},
			{Index: 2}, // No operation.
		},
	}
	conditions := computeComponentConditions(cs, 1)
	stuck := apimeta.FindStatusCondition(conditions, ConditionOperationStuck)
	if stuck == nil {
		t.Fatalf("OperationStuck condition missing")
	}
	if stuck.Status != metav1.ConditionTrue {
		t.Errorf("Status: got %s want True", stuck.Status)
	}
	if stuck.Reason != ReasonOperationDeadlineExceeded {
		t.Errorf("Reason: got %q want %q", stuck.Reason, ReasonOperationDeadlineExceeded)
	}
	if stuck.Message != "Instances with stuck operations: 0" {
		t.Errorf("Message: got %q", stuck.Message)
	}
}

func TestOperationStuckCondition_FalseWhenClean(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Operation: &v1beta1.InstanceOperation{Type: "Update"}}, // No DeadlineExceeded.
		},
	}
	conditions := computeComponentConditions(cs, 1)
	stuck := apimeta.FindStatusCondition(conditions, ConditionOperationStuck)
	if stuck.Status != metav1.ConditionFalse {
		t.Errorf("Status: got %s want False", stuck.Status)
	}
}

func TestComputeInstanceConditions_AllPodsAggregates(t *testing.T) {
	s := &v1beta1.OMENativeInstanceStatus{
		Index:             0,
		PodCount:          2,
		ScheduledPodCount: 2,
		ReadyPodCount:     1,
		AvailablePodCount: 1,
	}
	conditions := computeInstanceConditions(s, 1)

	scheduled := apimeta.FindStatusCondition(conditions, ConditionAllPodsScheduled)
	if scheduled.Status != metav1.ConditionTrue {
		t.Errorf("AllPodsScheduled: got %s want True", scheduled.Status)
	}
	ready := apimeta.FindStatusCondition(conditions, ConditionAllPodsReady)
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("AllPodsReady: got %s want False (1/2)", ready.Status)
	}
	available := apimeta.FindStatusCondition(conditions, ConditionAllPodsAvailable)
	if available.Status != metav1.ConditionFalse {
		t.Errorf("AllPodsAvailable: got %s want False (1/2)", available.Status)
	}
}

func TestComputeInstanceConditions_PreservesInPlaceFallbackBreadcrumb(t *testing.T) {
	// fallbackInPlaceToRecreate (timeouts.go) writes
	// InPlaceUpdateFallback=True; subsequent AggregateAndWriteStatus
	// passes must preserve it rather than overwriting back to False.
	s := &v1beta1.OMENativeInstanceStatus{
		Index:    0,
		PodCount: 1,
		Conditions: []metav1.Condition{{
			Type:    ConditionInPlaceUpdateFallback,
			Status:  metav1.ConditionTrue,
			Reason:  ReasonInPlaceTimeoutFallback,
			Message: "previous in-place attempt timed out",
		}},
	}
	conditions := computeInstanceConditions(s, 1)
	got := apimeta.FindStatusCondition(conditions, ConditionInPlaceUpdateFallback)
	if got.Status != metav1.ConditionTrue {
		t.Errorf("breadcrumb not preserved: got Status=%s", got.Status)
	}
	if got.Reason != ReasonInPlaceTimeoutFallback {
		t.Errorf("breadcrumb Reason not preserved: got %q", got.Reason)
	}
}

func TestComputeInstanceConditions_DefaultsInPlaceFallbackToFalse(t *testing.T) {
	s := &v1beta1.OMENativeInstanceStatus{Index: 0, PodCount: 1}
	conditions := computeInstanceConditions(s, 1)
	fallback := apimeta.FindStatusCondition(conditions, ConditionInPlaceUpdateFallback)
	if fallback == nil || fallback.Status != metav1.ConditionFalse {
		t.Errorf("InPlaceUpdateFallback should default to False, got %+v", fallback)
	}
}

func TestApplyComponentConditions_NoLastTransitionTimeChurn(t *testing.T) {
	// Second call with the same condition shape must NOT bump
	// LastTransitionTime. SetStatusCondition handles this.
	cs := &v1beta1.OMENativeComponentStatus{
		Replicas:          1,
		AvailableReplicas: 1,
	}
	first := computeComponentConditions(cs, 1)
	applyComponentConditions(cs, first)
	firstTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	second := computeComponentConditions(cs, 1)
	applyComponentConditions(cs, second)
	secondTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	if !firstTime.Equal(&secondTime) {
		t.Errorf("LastTransitionTime churned across identical applies: %v -> %v", firstTime, secondTime)
	}
}

func TestApplyComponentConditions_TransitionUpdatesLastTransitionTime(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{
		Replicas:          1,
		AvailableReplicas: 0,
	}
	applyComponentConditions(cs, computeComponentConditions(cs, 1))
	beforeTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	// Transition False -> True.
	cs.AvailableReplicas = 1
	applyComponentConditions(cs, computeComponentConditions(cs, 1))
	afterTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	if afterTime.Equal(&beforeTime) {
		t.Errorf("LastTransitionTime should bump on Status transition; both = %v", afterTime)
	}
}

func TestComputeComponentConditions_NilStatusIsSafe(t *testing.T) {
	got := computeComponentConditions(nil, 1)
	if got != nil {
		t.Errorf("expected nil for nil status, got %v", got)
	}
}

func TestComputeInstanceConditions_NilStatusIsSafe(t *testing.T) {
	got := computeInstanceConditions(nil, 1)
	if got != nil {
		t.Errorf("expected nil for nil status, got %v", got)
	}
}
