package status

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
			conditions := ComputeComponentConditions(tc.cs, 7)
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
		// Bug #52 follow-up: a stuck-pod escalation lands Phase=Failed on
		// the per-Instance status while UpdateRevision is still ahead of
		// CurrentRevision (the rollout never finished). Without the
		// failed-instance carve-out the Progressing condition reports
		// True/RolloutInProgress, lying to the operator about a rollout
		// that is permanently wedged. The per-Instance Failed phase MUST
		// short-circuit the revision-diff branch so the condition reflects
		// reality: Progressing=False, Reason=Failed.
		{
			name: "rollout failed: revision diff but every Instance is Phase=Failed",
			cs: &v1beta1.OMENativeComponentStatus{
				CurrentRevision: "r1", UpdateRevision: "r2",
				Replicas: 1, UpdatedReadyReplicas: 0,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
				},
			},
			wantStatus: metav1.ConditionFalse,
		},
		{
			name: "mixed: any Failed Instance suppresses Progressing even when peers are mid-update",
			cs: &v1beta1.OMENativeComponentStatus{
				CurrentRevision: "r1", UpdateRevision: "r2",
				Replicas: 2, UpdatedReadyReplicas: 0,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
					{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating},
				},
			},
			wantStatus: metav1.ConditionFalse,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conditions := ComputeComponentConditions(tc.cs, 1)
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

// TestProgressingCondition_FailedReasonAndMessage pins the operator-
// facing string surface for the failed-instance carve-out: Reason
// MUST be the constant `Failed` and Message MUST name the failed
// Instance index so a `kubectl describe isvc` line is actionable.
// Without these the operator sees Status=False but has to dig into
// the per-Instance breadcrumbs to find out why.
func TestProgressingCondition_FailedReasonAndMessage(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{
		CurrentRevision: "rev-old", UpdateRevision: "rev-new",
		Replicas: 2,
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
			{Index: 2, Phase: v1beta1.OMENativeInstanceFailed},
		},
	}
	conditions := ComputeComponentConditions(cs, 1)
	progressing := apimeta.FindStatusCondition(conditions, ConditionProgressing)
	if progressing == nil {
		t.Fatalf("Progressing condition missing")
	}
	if progressing.Status != metav1.ConditionFalse {
		t.Errorf("Status: got %s want False", progressing.Status)
	}
	if progressing.Reason != ReasonFailed {
		t.Errorf("Reason: got %q want %q", progressing.Reason, ReasonFailed)
	}
	if progressing.Message == "" {
		t.Errorf("Message must not be empty when surfacing a failed rollout")
	}
	// Operators want to see WHICH Instance failed. We index by sparse
	// integers, so the message MUST name at least one of the failed
	// indices.
	if !contains(progressing.Message, "0") || !contains(progressing.Message, "2") {
		t.Errorf("Message must name failed Instance indices 0 and 2; got %q", progressing.Message)
	}
}

func TestOperationStuckCondition_LiftsDeadlineExceededBreadcrumb(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Operation: &v1beta1.InstanceOperation{Type: "Update", Reason: ReasonDeadlineExceeded}},
			{Index: 1, Operation: &v1beta1.InstanceOperation{Type: "Update"}},
			{Index: 2}, // No operation.
		},
	}
	conditions := ComputeComponentConditions(cs, 1)
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
	conditions := ComputeComponentConditions(cs, 1)
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
	conditions := ComputeInstanceConditions(s, 1)

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
	// InPlaceUpdateFallback=True; while the Instance is still mid-recreate
	// (Operation in flight or Phase != Ready) subsequent
	// AggregateAndWriteStatus passes must preserve the breadcrumb.
	s := &v1beta1.OMENativeInstanceStatus{
		Index:    0,
		PodCount: 1,
		Phase:    v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate,
			Step: "Drain",
		},
		Conditions: []metav1.Condition{{
			Type:    ConditionInPlaceUpdateFallback,
			Status:  metav1.ConditionTrue,
			Reason:  ReasonInPlaceTimeoutFallback,
			Message: "previous in-place attempt timed out",
		}},
	}
	conditions := ComputeInstanceConditions(s, 1)
	got := apimeta.FindStatusCondition(conditions, ConditionInPlaceUpdateFallback)
	if got.Status != metav1.ConditionTrue {
		t.Errorf("breadcrumb not preserved: got Status=%s", got.Status)
	}
	if got.Reason != ReasonInPlaceTimeoutFallback {
		t.Errorf("breadcrumb Reason not preserved: got %q", got.Reason)
	}
}

func TestComputeInstanceConditions_FlipsInPlaceFallbackBackOnRecovery(t *testing.T) {
	// Once the Instance returns to steady-Ready (Phase=Ready, Operation=nil),
	// the recreate triggered by the fallback has converged. The breadcrumb
	// must flip back to False — otherwise the condition is sticky-True
	// forever and operators can't tell current state from history.
	s := &v1beta1.OMENativeInstanceStatus{
		Index:           0,
		PodCount:        1,
		ReadyPodCount:   1,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: "rev-recovered",
		Conditions: []metav1.Condition{{
			Type:    ConditionInPlaceUpdateFallback,
			Status:  metav1.ConditionTrue,
			Reason:  ReasonInPlaceTimeoutFallback,
			Message: "previous in-place attempt timed out",
		}},
	}
	conditions := ComputeInstanceConditions(s, 1)
	got := apimeta.FindStatusCondition(conditions, ConditionInPlaceUpdateFallback)
	if got == nil || got.Status != metav1.ConditionFalse {
		t.Errorf("breadcrumb should flip to False on recovery, got %+v", got)
	}
	if got.Reason != ReasonNoInPlaceFallback {
		t.Errorf("Reason should reset to %q, got %q", ReasonNoInPlaceFallback, got.Reason)
	}
}

func TestComputeInstanceConditions_DefaultsInPlaceFallbackToFalse(t *testing.T) {
	s := &v1beta1.OMENativeInstanceStatus{Index: 0, PodCount: 1}
	conditions := ComputeInstanceConditions(s, 1)
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
	first := ComputeComponentConditions(cs, 1)
	ApplyComponentConditions(cs, first)
	firstTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	second := ComputeComponentConditions(cs, 1)
	ApplyComponentConditions(cs, second)
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
	ApplyComponentConditions(cs, ComputeComponentConditions(cs, 1))
	beforeTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	// Transition False -> True.
	cs.AvailableReplicas = 1
	ApplyComponentConditions(cs, ComputeComponentConditions(cs, 1))
	afterTime := apimeta.FindStatusCondition(cs.Conditions, ConditionAvailable).LastTransitionTime

	if afterTime.Equal(&beforeTime) {
		t.Errorf("LastTransitionTime should bump on Status transition; both = %v", afterTime)
	}
}

func TestComputeComponentConditions_NilStatusIsSafe(t *testing.T) {
	got := ComputeComponentConditions(nil, 1)
	if got != nil {
		t.Errorf("expected nil for nil status, got %v", got)
	}
}

func TestComputeInstanceConditions_NilStatusIsSafe(t *testing.T) {
	got := ComputeInstanceConditions(nil, 1)
	if got != nil {
		t.Errorf("expected nil for nil status, got %v", got)
	}
}

// TestGangSchedulingUnavailableCondition_Matrix covers the 4 input
// combinations of (crdPresent, anyMultiPodInstance) the helper accepts.
// Only the (crdMissing AND multi-pod) cell yields Status=True with
// reason `PodGroupCRDNotInstalled`; the other three are False with
// reason `GangSchedulingAvailable`.
func TestGangSchedulingUnavailableCondition_Matrix(t *testing.T) {
	cases := []struct {
		name           string
		crdPresent     bool
		anyMultiPod    bool
		wantStatus     metav1.ConditionStatus
		wantReason     string
		messageContain string
	}{
		{
			name:           "no CRD + multi-pod -> True (degraded)",
			crdPresent:     false,
			anyMultiPod:    true,
			wantStatus:     metav1.ConditionTrue,
			wantReason:     ReasonPodGroupCRDNotInstalled,
			messageContain: "PodGroup CRD is not installed",
		},
		{
			name:           "CRD present + multi-pod -> False (gang available)",
			crdPresent:     true,
			anyMultiPod:    true,
			wantStatus:     metav1.ConditionFalse,
			wantReason:     ReasonGangSchedulingAvailable,
			messageContain: "available or not required",
		},
		{
			name:           "no CRD + single-pod -> False (not required)",
			crdPresent:     false,
			anyMultiPod:    false,
			wantStatus:     metav1.ConditionFalse,
			wantReason:     ReasonGangSchedulingAvailable,
			messageContain: "available or not required",
		},
		{
			name:           "CRD present + single-pod -> False (not required)",
			crdPresent:     true,
			anyMultiPod:    false,
			wantStatus:     metav1.ConditionFalse,
			wantReason:     ReasonGangSchedulingAvailable,
			messageContain: "available or not required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cond := GangSchedulingUnavailableCondition(tc.crdPresent, tc.anyMultiPod, 42)
			if cond.Type != ConditionGangSchedulingUnavailable {
				t.Errorf("Type: got %q want %q", cond.Type, ConditionGangSchedulingUnavailable)
			}
			if cond.Status != tc.wantStatus {
				t.Errorf("Status: got %s want %s", cond.Status, tc.wantStatus)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf("Reason: got %q want %q", cond.Reason, tc.wantReason)
			}
			if cond.ObservedGeneration != 42 {
				t.Errorf("ObservedGeneration: got %d want 42", cond.ObservedGeneration)
			}
			if !contains(cond.Message, tc.messageContain) {
				t.Errorf("Message: got %q want substring %q", cond.Message, tc.messageContain)
			}
		})
	}
}

// TestGangSchedulingUnavailableCondition_Idempotent — applying the
// same condition twice via ApplyComponentConditions does not bump
// LastTransitionTime (only real status transitions do, per
// apimeta.SetStatusCondition semantics).
func TestGangSchedulingUnavailableCondition_Idempotent(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{}
	first := GangSchedulingUnavailableCondition(false, true, 1)
	ApplyComponentConditions(cs, []metav1.Condition{first})
	beforeTime := apimeta.FindStatusCondition(cs.Conditions, ConditionGangSchedulingUnavailable).LastTransitionTime

	// Second pass with identical inputs (same generation pair) — the
	// helper always builds a fresh metav1.Now(), so the
	// LastTransitionTime in the desired-condition is newer, but
	// ApplyComponentConditions' SetStatusCondition only stamps it onto
	// the live status when Status / Reason / Message changed.
	second := GangSchedulingUnavailableCondition(false, true, 1)
	ApplyComponentConditions(cs, []metav1.Condition{second})
	afterTime := apimeta.FindStatusCondition(cs.Conditions, ConditionGangSchedulingUnavailable).LastTransitionTime

	if !afterTime.Equal(&beforeTime) {
		t.Errorf("LastTransitionTime should NOT bump on no-op apply; before=%v after=%v", beforeTime, afterTime)
	}
}

// contains is a tiny substring check; avoids importing strings just
// for this file.
func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
