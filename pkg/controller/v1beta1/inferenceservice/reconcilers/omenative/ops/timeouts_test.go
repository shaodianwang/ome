package ops

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
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
	err := ExpireOperationsIfDeadlinePassed(context.Background(), core.ReconcileParams{}, core.ComponentPlan{Component: internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent)})
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
	plan, _ := core.BuildPlan(p)

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

func TestExpireOperationsIfDeadlinePassed_RecreateUpdateStampsStuckAndEscalatesPhase(t *testing.T) {
	// Recreate-mode Update past Deadline -> mark stuck (Operation
	// preserved with Reason=DeadlineExceeded) AND escalate Phase to
	// Failed so the coordination layer's per-Component Failed flag
	// flips. Drain/Recreate steps may have committed pod-delete side
	// effects; the controller refuses to force-progress them, but the
	// per-Instance Phase=Failed marker is what lifts the wedge into
	// Component.Failed so the BlueGreen/Sequential state machines can
	// transition to Phase=Failed instead of sitting at Surging forever.
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
	plan, _ := core.BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil {
		t.Fatalf("Operation must be preserved for recreate; got nil")
	}
	if s.Operation.Reason != status.ReasonDeadlineExceeded {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, status.ReasonDeadlineExceeded)
	}
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: must escalate to Failed (so coord layer fails the group); got %q", s.Phase)
	}
}

// TestExpireOperationsIfDeadlinePassed_MirrorsPhaseToInMemoryISVC pins
// the in-memory mirror added to keep the escalation from racing the
// rest of the same reconcile pass. dispatch.go's Update / Restart
// passes read `p.ISVC.Status` (the reconcile-start snapshot), so an
// apiserver-only write to Phase=Failed leaves them seeing
// Phase=Updating, firing recreate, and immediately clobbering the
// escalation. The mirror keeps the snapshot in sync.
func TestExpireOperationsIfDeadlinePassed_MirrorsPhaseToInMemoryISVC(t *testing.T) {
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
	plan, _ := core.BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	// Read from the in-memory ISVC pointer, NOT a fresh client.Get.
	// The whole point of the mirror is that the rest of the reconcile
	// pass sees the new Phase without needing a re-read.
	got := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Phase
	if got != v1beta1.OMENativeInstanceFailed {
		t.Errorf("in-memory ISVC Phase: must mirror the just-committed Phase=Failed; got %q", got)
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
	plan, _ := core.BuildPlan(p)

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
			Reason:   status.ReasonDeadlineExceeded,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

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
	plan, _ := core.BuildPlan(p)

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
			if s.Operation.Reason == status.ReasonDeadlineExceeded {
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
	plan, _ := core.BuildPlan(p)

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
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, fresh)
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger should record Failed entry for uuid=%s; got %+v", uuid, ledger.Entries)
	}

	// Migration request annotation should be cleared.
	if _, present := fresh.Annotations[audit.MigrationRequestAnnotationPrefix+uuid]; present {
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
	plan, _ := core.BuildPlan(p)

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
	if src.Operation.Reason != status.ReasonDeadlineExceeded {
		t.Errorf("source Operation.Reason: got %q want %q", src.Operation.Reason, status.ReasonDeadlineExceeded)
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

// --- stampInstanceFailed (Phase escalation helper) ---

func TestIsEscalatablePhase(t *testing.T) {
	cases := []struct {
		phase v1beta1.OMENativeInstancePhase
		want  bool
	}{
		{v1beta1.OMENativeInstancePending, false}, // never observed yet — nothing to escalate
		{v1beta1.OMENativeInstanceCreating, true},
		{v1beta1.OMENativeInstanceReady, false}, // settled, healthy
		{v1beta1.OMENativeInstanceUpdating, true},
		{v1beta1.OMENativeInstanceRestarting, true},
		{v1beta1.OMENativeInstanceMigrating, true},
		{v1beta1.OMENativeInstanceFailed, false},   // already terminal
		{v1beta1.OMENativeInstanceDeleting, false}, // tearing down
	}
	for _, c := range cases {
		if got := isEscalatablePhase(c.phase); got != c.want {
			t.Errorf("phase %q: got %v want %v", c.phase, got, c.want)
		}
	}
}

func TestStampInstanceFailed_EscalatesPhase(t *testing.T) {
	// Phase=Updating + Operation present -> Phase=Failed.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Step:     updateStepDrain,
			Reason:   status.ReasonDeadlineExceeded,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	if err := stampInstanceFailed(context.Background(), p, v1beta1.EngineComponent, 0); err != nil {
		t.Fatalf("stampInstanceFailed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed", s.Phase)
	}
	if s.Operation == nil {
		t.Errorf("Operation must be preserved as breadcrumb; got nil")
	}
}

func TestStampInstanceFailed_AlreadyFailedIsNoop(t *testing.T) {
	// Phase already Failed -> mutate returns false, no churn.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceFailed,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Reason:   status.ReasonDeadlineExceeded,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	if err := stampInstanceFailed(context.Background(), p, v1beta1.EngineComponent, 0); err != nil {
		t.Fatalf("stampInstanceFailed (already-failed): %v", err)
	}
}

func TestStampInstanceFailed_DoesNotResurrectRemovedInstance(t *testing.T) {
	// The (component, idx) pair has been removed from status (the
	// migrate-abandon path's surge cleanup). MutateInstance will
	// re-append an empty slot — stampInstanceFailed MUST bail when
	// Operation is nil rather than stamping Failed on a fresh slot.
	// Otherwise the abandoned surge would resurrect as Phase=Failed.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Remove the existing InstanceStatus to simulate post-abandon state.
	cs := isvc.Status.Components[v1beta1.EngineComponent]
	cs.OMENative.InstanceStatuses = nil
	isvc.Status.Components[v1beta1.EngineComponent] = cs
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	if err := stampInstanceFailed(context.Background(), p, v1beta1.EngineComponent, 99); err != nil {
		t.Fatalf("stampInstanceFailed (missing instance): %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	for _, s := range fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses {
		if s.Index == 99 {
			t.Errorf("removed instance must not be resurrected; got %+v", s)
		}
	}
}

func TestExpireOperationsIfDeadlinePassed_PhaseEscalationIsIdempotent(t *testing.T) {
	// Already-stuck + already-Failed: neither stamp should churn the
	// status on subsequent reconciles. Guards against LastTransitionTime
	// thrash on the Component-level Failed surface.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceFailed,
		RunningRevision: "old-rev",
		TargetRevision:  "new-rev",
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Step:     updateStepDrain,
			Reason:   status.ReasonDeadlineExceeded,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	for i := 0; i < 3; i++ {
		if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed", s.Phase)
	}
	if s.Operation == nil {
		t.Errorf("Operation must remain preserved across repeated passes")
	}
}

// TestExpireOperationsIfDeadlinePassed_SurgeStepEscalatesPhase pins the
// slow-path equivalent of the fast-path status_aggregate
// stuck-pod-failure escalation: a SurgeThenDrain Op{Step=Surge} whose
// Deadline elapsed (default InstanceReadyTimeout 30 min) must escalate
// the Instance to Phase=Failed so the coordination layer's per-Component
// rollup can flip the group's phase to Failed.
//
// The fast-path (status_aggregate.escalateStuckPodFailures) catches
// ImagePullBackOff / ErrImagePull within ~60s; this slow-path catches
// every OTHER wedge mode (drain races, runtime hangs, slow registries
// where the pull succeeds but the container never reports Ready) where
// no kubelet signal is available to fast-fail on. Locks in the
// behavior the X-1 reproduction needs as a backstop independent of pod
// state.
func TestExpireOperationsIfDeadlinePassed_SurgeStepEscalatesPhase(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "old-rev",
		TargetRevision:  "new-rev",
		ActiveOrdinal:   0,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationUpdate,
			Step:     updateStepSurge,
			Deadline: pastDeadline(),
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil {
		t.Fatalf("Operation must be preserved on surge timeout; got nil")
	}
	if s.Operation.Reason != status.ReasonDeadlineExceeded {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, status.ReasonDeadlineExceeded)
	}
	if s.Operation.Step != updateStepSurge {
		t.Errorf("Operation.Step: got %q want Surge (preserved as breadcrumb)", s.Operation.Step)
	}
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: must escalate to Failed (coord layer fails the group); got %q", s.Phase)
	}
}

func TestExpireOperationsIfDeadlinePassed_InPlaceFallbackDoesNotEscalate(t *testing.T) {
	// In-place updates have a recovery path (fall back to recreate).
	// Phase is reset to Ready, NOT escalated to Failed — the rollout
	// continues on the next reconcile.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
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
	plan, _ := core.BuildPlan(p)

	if err := ExpireOperationsIfDeadlinePassed(context.Background(), p, plan); err != nil {
		t.Fatalf("ExpireOperationsIfDeadlinePassed: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready (in-place fallback is recoverable)", s.Phase)
	}
}
