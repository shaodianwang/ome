package ops

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// stuckInstanceFixture builds an ISVC plus its engine InferenceReplica whose
// Instance is stuck in the given Phase with an Operation.StartedAt of
// now-stuckDuration. The per-instance detail lives on the IR (the authoritative
// source the detector reads via ComponentIRStatus); the ISVC carries only an
// empty aggregate Lifecycle. Optionally writes a live pod for the Instance with
// the given node name so the auto-trigger detector's source-node resolution
// succeeds. Callers seed both the ISVC and the returned IR into the fake client.
func stuckInstanceFixture(t *testing.T, name, ns string, phase v1beta1.OMENativeInstancePhase, stuckDuration time.Duration, withPod bool, podNode string, policy *v1beta1.MigrationPolicy) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, *corev1.Pod) {
	t.Helper()
	isvc := minimalISVC(name, ns, 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{MigrationPolicy: policy}
	stuckSince := time.Now().Add(-stuckDuration)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{}},
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      isvc.Name + "-" + string(v1beta1.EngineComponent),
		},
		Status: v1beta1.InferenceReplicaStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
				{
					Index:       0,
					Incarnation: 1,
					Phase:       phase,
					Operation: &v1beta1.InstanceOperation{
						ID:        "op-0",
						Type:      v1beta1.InstanceOperationCreate,
						Step:      "CreatePods",
						StartedAt: metav1.NewTime(stuckSince),
						Deadline:  metav1.NewTime(stuckSince.Add(30 * time.Minute)),
					},
				},
			},
		},
	}
	var pod *corev1.Pod
	if withPod {
		pod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      query.PodName(isvc.Name, "engine", 0, "default", 0),
				Namespace: ns,
				UID:       types.UID(name + "-pod-0"),
				Labels: map[string]string{
					constants.InferenceServicePodLabelKey: isvc.Name,
					constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
					query.LabelInstanceIdx:                "0",
					query.LabelInstanceIncarnation:        "1",
					query.LabelRunner:                     "default",
					query.LabelManagedBy:                  query.ManagedByOMENative,
					query.LabelPodOrdinal:                 "0",
				},
			},
			Spec: corev1.PodSpec{
				NodeName:   podNode,
				Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
			},
		}
	}
	return isvc, ir, pod
}

func policyWithDeadline(deadline int32, maxRetries *int32) *v1beta1.MigrationPolicy {
	return &v1beta1.MigrationPolicy{
		Mode:                    v1beta1.MigrationPolicyModeAuto,
		ProgressDeadlineSeconds: &deadline,
		MaxAutoRetries:          maxRetries,
	}
}

func TestDetectStuckInstancesForAutoMigration_NilPolicy_NoTrigger(t *testing.T) {
	resetExpectations(t)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, time.Hour, true, "node-a", nil)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, err := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if len(ledger.Entries) != 0 {
		t.Errorf("nil MigrationPolicy must not trigger; got %d ledger entries", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_ModeNever_NoTrigger(t *testing.T) {
	resetExpectations(t)
	deadline := int32(30)
	policy := &v1beta1.MigrationPolicy{
		Mode:                    v1beta1.MigrationPolicyModeNever,
		ProgressDeadlineSeconds: &deadline,
	}
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, time.Hour, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 0 {
		t.Errorf("Mode=Never must not trigger; got %d ledger entries", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_DeadlineUnset_NoTrigger(t *testing.T) {
	resetExpectations(t)
	policy := &v1beta1.MigrationPolicy{Mode: v1beta1.MigrationPolicyModeAuto}
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, time.Hour, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 0 {
		t.Errorf("ProgressDeadlineSeconds=nil must not trigger; got %d ledger entries", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_PhaseUpdatingUnderDeadline_NoTrigger(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(60, nil)
	// 5s stuck, 60s deadline → no trigger yet
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 5*time.Second, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 0 {
		t.Errorf("stuck < deadline must not trigger; got %d ledger entries", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_PhaseUpdatingOverDeadline_Triggers(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(30, nil)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 2*time.Minute, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, err := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("expected 1 ledger entry; got %d (%+v)", len(ledger.Entries), ledger.Entries)
	}
	e := ledger.Entries[0]
	if e.Reason != AutoMigrateReason {
		t.Errorf("Reason: got %q want %q", e.Reason, AutoMigrateReason)
	}
	if e.Phase != audit.PhaseStarted {
		t.Errorf("Phase: got %q want Started", e.Phase)
	}
	if e.Component != "engine" {
		t.Errorf("Component: got %q want engine", e.Component)
	}
	if e.SourceInstance != 0 {
		t.Errorf("SourceInstance: got %d want 0", e.SourceInstance)
	}
	if e.FromNode != "node-a" {
		t.Errorf("FromNode: got %q want node-a", e.FromNode)
	}
}

func TestDetectStuckInstancesForAutoMigration_PhaseCreatingOverDeadline_Triggers(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(30, nil)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceCreating, 2*time.Minute, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 1 {
		t.Fatalf("Phase=Creating stuck past deadline should trigger; got %d entries", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_PhaseReady_NoTrigger(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(30, nil)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceReady, time.Hour, true, "node-a", policy)
	// Phase=Ready has no Operation by convention; strip it to mirror prod state.
	ir.Status.InstanceStatuses[0].Operation = nil
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 0 {
		t.Errorf("Phase=Ready must not trigger; got %d ledger entries", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_NoPod_NoTrigger(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(30, nil)
	// withPod=false → resolveLiveSourceNode returns ""; trigger skipped.
	isvc, ir, _ := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 2*time.Minute, false, "", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 0 {
		t.Errorf("no live pod (nothing to migrate off of) must not trigger; got %d", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_NoOperationAnchor_NoTrigger(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(30, nil)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 2*time.Minute, true, "node-a", policy)
	// Clear Operation so stuckSinceForInstance returns ok=false.
	ir.Status.InstanceStatuses[0].Operation = nil
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if len(ledger.Entries) != 0 {
		t.Errorf("no Operation anchor must not trigger; got %d", len(ledger.Entries))
	}
}

func TestDetectStuckInstancesForAutoMigration_RetryCapNotReached_Triggers(t *testing.T) {
	resetExpectations(t)
	max := int32(3)
	policy := policyWithDeadline(30, &max)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 2*time.Minute, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	// Seed 2 prior auto-migration entries (1 Failed + 1 Completed).
	prior := &audit.Ledger{Entries: []audit.Entry{
		{RequestUUID: "u1", Component: "engine", SourceInstance: 0, Phase: audit.PhaseFailed, Reason: AutoMigrateReason, Outcome: "fail"},
		{RequestUUID: "u2", Component: "engine", SourceInstance: 0, Phase: audit.PhaseCompleted, Reason: AutoMigrateReason},
	}}
	if err := internalsource.PersistAuditLedgerForISVC(context.Background(), c, isvc, prior); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	startedCount := 0
	for _, e := range ledger.Entries {
		if e.Phase == audit.PhaseStarted && e.Reason == AutoMigrateReason {
			startedCount++
		}
	}
	if startedCount != 1 {
		t.Errorf("retry < cap should add one Started entry; got %d (entries=%+v)", startedCount, ledger.Entries)
	}
}

func TestDetectStuckInstancesForAutoMigration_RetryCapReached_NoTrigger(t *testing.T) {
	resetExpectations(t)
	max := int32(2)
	policy := policyWithDeadline(30, &max)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 2*time.Minute, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	// Seed 2 prior auto-migration entries — at the cap.
	prior := &audit.Ledger{Entries: []audit.Entry{
		{RequestUUID: "u1", Component: "engine", SourceInstance: 0, Phase: audit.PhaseFailed, Reason: AutoMigrateReason, Outcome: "fail"},
		{RequestUUID: "u2", Component: "engine", SourceInstance: 0, Phase: audit.PhaseFailed, Reason: AutoMigrateReason, Outcome: "fail"},
	}}
	if err := internalsource.PersistAuditLedgerForISVC(context.Background(), c, isvc, prior); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	// At the cap, no new Started entry is filed.
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	for _, e := range ledger.Entries {
		if e.Phase == audit.PhaseStarted && e.Reason == AutoMigrateReason {
			t.Fatalf("retry cap reached should NOT write a Started entry; got %+v", e)
		}
	}
	// The cap-reached path no longer stamps Failed/condition onto the ISVC copy
	// (the IR controller owns escalating the stuck instance to Failed on the
	// authoritative InferenceReplica; the ISVC surfaces it via aggregation).
}

func TestDetectStuckInstancesForAutoMigration_InFlightMigrationExists_NoTrigger(t *testing.T) {
	resetExpectations(t)
	policy := policyWithDeadline(30, nil)
	isvc, ir, pod := stuckInstanceFixture(t, "iv", "ns", v1beta1.OMENativeInstanceUpdating, 2*time.Minute, true, "node-a", policy)
	c := newFakeClientWithDiscovery(t, isvc, ir, pod)
	// Seed an in-flight (Started, no terminal) migration for the same Instance.
	prior := &audit.Ledger{Entries: []audit.Entry{
		{RequestUUID: "in-flight", Component: "engine", SourceInstance: 0, Phase: audit.PhaseStarted, Reason: AutoMigrateReason, StartedAt: time.Now().UTC().Format(time.RFC3339)},
	}}
	if err := internalsource.PersistAuditLedgerForISVC(context.Background(), c, isvc, prior); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	p := minimalParams(t, isvc, c)

	if err := detectStuckInstancesForAutoMigration(context.Background(), p, v1beta1.EngineComponent); err != nil {
		t.Fatalf("detect: %v", err)
	}
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	startedCount := 0
	for _, e := range ledger.Entries {
		if e.Phase == audit.PhaseStarted && e.Reason == AutoMigrateReason {
			startedCount++
		}
	}
	if startedCount != 1 {
		t.Errorf("in-flight already-existing Started entry should suppress new trigger; got %d started entries", startedCount)
	}
}

func TestIsStuckPhase(t *testing.T) {
	cases := map[v1beta1.OMENativeInstancePhase]bool{
		v1beta1.OMENativeInstanceCreating:   true,
		v1beta1.OMENativeInstanceUpdating:   true,
		v1beta1.OMENativeInstanceReady:      false,
		v1beta1.OMENativeInstanceMigrating:  false,
		v1beta1.OMENativeInstanceFailed:     false,
		v1beta1.OMENativeInstanceDeleting:   false,
		v1beta1.OMENativeInstanceRestarting: false,
		v1beta1.OMENativeInstancePending:    false,
	}
	for phase, want := range cases {
		if got := isStuckPhase(phase); got != want {
			t.Errorf("isStuckPhase(%q): got %v want %v", phase, got, want)
		}
	}
}

func TestCountAutoMigrationRetries(t *testing.T) {
	ledger := &audit.Ledger{Entries: []audit.Entry{
		// Match (engine, 0, AutoRecover) — 3 entries spanning Started, Failed, Completed.
		{Component: "engine", SourceInstance: 0, Reason: AutoMigrateReason, Phase: audit.PhaseFailed},
		{Component: "engine", SourceInstance: 0, Reason: AutoMigrateReason, Phase: audit.PhaseCompleted},
		{Component: "engine", SourceInstance: 0, Reason: AutoMigrateReason, Phase: audit.PhaseStarted},
		// Different instance, same component — not counted.
		{Component: "engine", SourceInstance: 1, Reason: AutoMigrateReason, Phase: audit.PhaseCompleted},
		// Different component — not counted.
		{Component: "decoder", SourceInstance: 0, Reason: AutoMigrateReason, Phase: audit.PhaseCompleted},
		// Same instance, different Reason (operator-driven) — not counted.
		{Component: "engine", SourceInstance: 0, Reason: "fragmentation", Phase: audit.PhaseCompleted},
	}}
	got := countAutoMigrationRetries(ledger, v1beta1.EngineComponent, 0)
	if got != 3 {
		t.Errorf("countAutoMigrationRetries: got %d want 3", got)
	}
}

func TestInstanceHasInFlightMigration(t *testing.T) {
	ledger := &audit.Ledger{Entries: []audit.Entry{
		// In-flight Started; matches (engine, 0).
		{RequestUUID: "u1", Component: "engine", SourceInstance: 0, Phase: audit.PhaseStarted, Reason: AutoMigrateReason},
		// Started but terminal counterpart present → not in-flight.
		{RequestUUID: "u2", Component: "engine", SourceInstance: 1, Phase: audit.PhaseStarted, Reason: AutoMigrateReason},
		{RequestUUID: "u2", Component: "engine", SourceInstance: 1, Phase: audit.PhaseCompleted, Reason: AutoMigrateReason},
	}}
	if !instanceHasInFlightMigration(ledger, v1beta1.EngineComponent, 0) {
		t.Errorf("Started without terminal counterpart should be in-flight")
	}
	// UpsertEntry deduplicates by UUID, but we built the slice directly — adjust expectations:
	// the lookup walks all entries, so u2 has both Started and Completed for the same UUID.
	// In production UpsertEntry replaces in place; here the Started entry was overwritten by Completed.
	// Either way, the terminal counterpart suppresses in-flight detection.
	if instanceHasInFlightMigration(ledger, v1beta1.EngineComponent, 1) {
		t.Errorf("Started + Completed for same UUID should NOT be in-flight")
	}
	if instanceHasInFlightMigration(ledger, v1beta1.EngineComponent, 2) {
		t.Errorf("no Started entry for instance 2 should NOT be in-flight")
	}
}

func TestStuckSinceForInstance(t *testing.T) {
	now := time.Now()
	got, ok := stuckSinceForInstance(&v1beta1.OMENativeInstanceStatus{Operation: &v1beta1.InstanceOperation{StartedAt: metav1.NewTime(now)}})
	if !ok {
		t.Errorf("Operation with StartedAt should return ok=true")
	}
	if !got.Equal(now) {
		t.Errorf("StartedAt: got %v want %v", got, now)
	}
	if _, ok := stuckSinceForInstance(&v1beta1.OMENativeInstanceStatus{Operation: nil}); ok {
		t.Errorf("nil Operation should return ok=false")
	}
	if _, ok := stuckSinceForInstance(&v1beta1.OMENativeInstanceStatus{Operation: &v1beta1.InstanceOperation{}}); ok {
		t.Errorf("zero StartedAt should return ok=false")
	}
}

func TestUpsertInstanceCondition(t *testing.T) {
	now := metav1.Now()
	s := &v1beta1.OMENativeInstanceStatus{
		Conditions: []metav1.Condition{
			{Type: "Other", Status: metav1.ConditionTrue, Reason: "x", Message: "y", LastTransitionTime: now},
		},
	}
	cond := metav1.Condition{Type: ConditionProgressDeadlineExceeded, Status: metav1.ConditionTrue, Reason: "r", Message: "m"}
	if !upsertInstanceCondition(s, cond) {
		t.Errorf("first append should report mutation")
	}
	if len(s.Conditions) != 2 {
		t.Fatalf("expected 2 conditions; got %d", len(s.Conditions))
	}

	// No-op when Status / Reason / Message all match.
	cond2 := cond
	cond2.LastTransitionTime = metav1.NewTime(now.Add(time.Hour))
	if upsertInstanceCondition(s, cond2) {
		t.Errorf("same-payload condition should be a no-op")
	}
	// LastTransitionTime preserved across the no-op.
	for _, c := range s.Conditions {
		if c.Type == ConditionProgressDeadlineExceeded && c.LastTransitionTime.Equal(&cond2.LastTransitionTime) {
			t.Errorf("LastTransitionTime should NOT have been bumped on no-op write")
		}
	}

	// Mutate Status — should write.
	cond3 := cond
	cond3.Status = metav1.ConditionFalse
	cond3.LastTransitionTime = metav1.NewTime(now.Add(2 * time.Hour))
	if !upsertInstanceCondition(s, cond3) {
		t.Errorf("Status change should report mutation")
	}
}
