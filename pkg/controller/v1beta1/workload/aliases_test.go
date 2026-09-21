package workload

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// The aliases exist so external callers keep the workload.X spelling
// across the types split. What can go wrong is a forwarder that stops
// agreeing with the declaration it forwards to, so each one is checked
// against types directly rather than re-testing the behavior.

func TestPredicateAliasesAgreeWithTypes(t *testing.T) {
	ops := []*InstanceOperation{
		nil,
		{},
		{Waiting: types.WaitingReasonPaused},
		{Waiting: types.WaitingReasonUnschedulable},
		{Waiting: types.WaitingReasonPodGroupTerminating},
		{Waiting: types.WaitingReasonNodeUnknown},
	}
	for _, op := range ops {
		for name, pair := range map[string][2]bool{
			"OperationUnschedulable":  {OperationUnschedulable(op), types.OperationUnschedulable(op)},
			"OperationGangHeld":       {OperationGangHeld(op), types.OperationGangHeld(op)},
			"OperationPaused":         {OperationPaused(op), types.OperationPaused(op)},
			"OperationExternallyHeld": {OperationExternallyHeld(op), types.OperationExternallyHeld(op)},
		} {
			if pair[0] != pair[1] {
				t.Errorf("%s(%v): alias says %v, types says %v", name, op, pair[0], pair[1])
			}
		}
	}
	for _, reason := range []string{"ImagePullBackOff", "CrashLoopBackOff", ""} {
		if IsWorkloadCausedReason(reason) != types.IsWorkloadCausedReason(reason) {
			t.Errorf("IsWorkloadCausedReason(%q) disagrees with types", reason)
		}
	}
}

func TestReadAliasesAgreeWithTypes(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodFailed,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "no fit", LastTransitionTime: now}},
		},
	}
	gotMsg, gotAt, gotOK := PodUnschedulable(pod)
	wantMsg, wantAt, wantOK := types.PodUnschedulable(pod)
	if gotMsg != wantMsg || !gotAt.Equal(&wantAt) || gotOK != wantOK {
		t.Errorf("PodUnschedulable disagrees with types: (%q,%v,%v) vs (%q,%v,%v)", gotMsg, gotAt, gotOK, wantMsg, wantAt, wantOK)
	}

	instances := []InstanceStatus{{Index: 0}, {Index: 2}}
	if AllocateSurgeIndex(instances) != types.AllocateSurgeIndex(instances) {
		t.Error("AllocateSurgeIndex disagrees with types")
	}
	records := []MigrationRecord{{RequestUUID: "a", Trigger: types.MigrationTriggerManual, StartedAt: now}}
	if got, want := FindMigrationRecord(records, "a"), types.FindMigrationRecord(records, "a"); got != want {
		t.Error("FindMigrationRecord disagrees with types")
	}
	if got, want := NextManualMigration(records), types.NextManualMigration(records); got != want {
		t.Error("NextManualMigration disagrees with types")
	}
	if got, want := PodTermination(pod, now), types.PodTermination(pod, now); (got == nil) != (want == nil) || (got != nil && *got != *want) {
		t.Error("PodTermination disagrees with types")
	}
	if got, want := PodTerminationWithReason(pod, "Evicted", now), types.PodTerminationWithReason(pod, "Evicted", now); (got == nil) != (want == nil) || (got != nil && *got != *want) {
		t.Error("PodTerminationWithReason disagrees with types")
	}
	if NewGangObservations() == nil || NewExpectations() == nil {
		t.Error("the constructor aliases must construct")
	}
}

// The singleton is mirrored in two packages, so reseating it through the
// alias has to move both or Deps.ExpectationsCache() keeps the old one.
func TestSetDefaultExpectationsReseatsBothMirrors(t *testing.T) {
	prior := DefaultExpectations
	t.Cleanup(func() { SetDefaultExpectations(prior) })

	fresh := NewExpectations()
	SetDefaultExpectations(fresh)
	if DefaultExpectations != fresh || types.DefaultExpectations != fresh {
		t.Fatal("both the alias mirror and the types singleton must move")
	}
	if (&Deps{}).ExpectationsCache() != fresh {
		t.Error("an unwired Deps must pick up the reseated cache")
	}
}
