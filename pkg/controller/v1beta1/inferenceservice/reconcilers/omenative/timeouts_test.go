package omenative

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// --- tryAbandonStuckMigration / surgeReady ---

func TestSurgeReady_EmptyReturnsFalse(t *testing.T) {
	if surgeReady(nil) {
		t.Errorf("nil pod slice must report not-ready (no observation == no health claim)")
	}
}

func TestSurgeReady_AllReadyReturnsTrue(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod1 := podAtIncarnation(isvc, 1, 1, true /* ready */, true)
	pod2 := podAtIncarnation(isvc, 1, 1, true, true)
	if !surgeReady([]*corev1.Pod{pod1, pod2}) {
		t.Errorf("two ContainersReady pods should return ready")
	}
}

func TestSurgeReady_AnyNotReadyReturnsFalse(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod1 := podAtIncarnation(isvc, 1, 1, true, true)
	pod2 := podAtIncarnation(isvc, 1, 1, false /* not ready */, true)
	if surgeReady([]*corev1.Pod{pod1, pod2}) {
		t.Errorf("any not-ContainersReady pod must drop the gate")
	}
}

func TestExpireOperationsIfDeadlinePassed_MigrateAbandonsStuckSurge(t *testing.T) {
	// Migrate source past Deadline + surge pod NOT ContainersReady ->
	// abandon path runs: surge pod deleted, source restored to Ready,
	// surge InstanceStatus removed, audit Failed row written, request
	// annotation cleared.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	surgeIdx := int32(1)
	uuid := "req-abandon"
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating,
			RunningRevision: "rev-source",
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationMigrate, Step: "CreateSurge",
				RequestUUID: uuid, FromNode: "node5", SurgeIndex: &surgeIdx,
				Deadline: pastDeadline(),
			},
		},
		{
			Index: surgeIdx, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationMigrate, Step: "CreateSurge",
				RequestUUID: uuid, FromNode: "node5",
				Deadline: pastDeadline(),
			},
		},
	}
	addMigrationAnnotation(t, isvc, uuid, migrationRequestJSON("engine", 0, "node5"))

	sourcePod := podAtIncarnation(isvc, 0, 1, true /* ready */, true)
	surgePod := podAtIncarnation(isvc, surgeIdx, 1, false /* NOT ready */, false)
	c := newFakeClientWithDiscovery(t, isvc, sourcePod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	var src *v1beta1.OMENativeInstanceStatus
	for i := range cs.OMENative.InstanceStatuses {
		s := &cs.OMENative.InstanceStatuses[i]
		if s.Index == 0 {
			src = s
		}
		if s.Index == surgeIdx {
			t.Errorf("surge InstanceStatus must be removed after abandon; got %+v", s)
		}
	}
	if src == nil {
		t.Fatal("source InstanceStatus must survive abandon")
	}
	if src.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("source Phase: got %q want Ready", src.Phase)
	}
	if src.Operation != nil {
		t.Errorf("source Operation must be cleared; got %+v", src.Operation)
	}

	// Surge pod should have been deleted (best-effort).
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(surgePod), got); err == nil && got.DeletionTimestamp == nil {
		t.Errorf("surge pod should have been deleted; still present without DeletionTimestamp")
	}

	// Ledger should carry a Failed entry for the UUID.
	ledger, _ := loadAuditLedger(context.Background(), c, fresh)
	if !ledger.hasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger should record Failed entry for uuid=%s; got %+v", uuid, ledger.Entries)
	}

	// Migration request annotation should be cleared.
	if _, present := fresh.Annotations[migrationRequestAnnotationPrefix+uuid]; present {
		t.Errorf("migration request annotation must be cleared after abandon")
	}
}

func TestExpireOperationsIfDeadlinePassed_MigrateLeavesReadySurgeStuck(t *testing.T) {
	// Migrate past Deadline but surge pod IS ContainersReady -> abandon
	// does NOT run; falls through to the generic stuck stamp. The wedge
	// is downstream of surge (drain stalled, kube-proxy lag, etc.).
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	surgeIdx := int32(1)
	uuid := "req-stuck"
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationMigrate, Step: "Drain",
				RequestUUID: uuid, FromNode: "node5", SurgeIndex: &surgeIdx,
				Deadline: pastDeadline(),
			},
		},
		{
			Index: surgeIdx, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationMigrate, Step: "Drain",
				RequestUUID: uuid, Deadline: pastDeadline(),
			},
		},
	}

	surgePod := podAtIncarnation(isvc, surgeIdx, 1, true /* ContainersReady */, true)
	c := newFakeClientWithDiscovery(t, isvc, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	var src *v1beta1.OMENativeInstanceStatus
	for i := range cs.OMENative.InstanceStatuses {
		s := &cs.OMENative.InstanceStatuses[i]
		if s.Index == 0 {
			src = s
		}
	}
	if src == nil || src.Operation == nil {
		t.Fatalf("source Operation must be preserved when surge ready; got %+v", src)
	}
	if src.Operation.Reason != timeoutReasonExceeded {
		t.Errorf("source Operation.Reason: got %q want %q", src.Operation.Reason, timeoutReasonExceeded)
	}
}

func TestTryAbandonStuckMigration_SourceWithoutSurgeIndexNoOp(t *testing.T) {
	// Source-side stamp missing SurgeIndex is malformed (or this is the
	// surge side, which the abandon helper deliberately skips).
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	source := v1beta1.OMENativeInstanceStatus{
		Index: 0,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationMigrate,
			// SurgeIndex deliberately nil
		},
	}
	abandoned, err := tryAbandonStuckMigration(context.Background(), p, v1beta1.EngineComponent, source)
	if err != nil {
		t.Fatalf("tryAbandonStuckMigration: %v", err)
	}
	if abandoned {
		t.Errorf("abandon must not run without SurgeIndex pointer")
	}
}
