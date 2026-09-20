package ops

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Every create path in the engine funnels through createMissingPods, so a
// classified apiserver rejection must reach the SAME outcome whichever
// operation issued the create. These tests pin that for the sites the
// Create pass does not cover: Restart Phase B, RecreatePod Phase B, the
// per-pod surge, the gang surge, and the migration surge.

// rejectPodCreates wraps c so every Pod create is answered with err.
func rejectPodCreates(t *testing.T, c client.Client, err error) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
}

// siteQuotaError is the ResourceQuota admission refusal the apiserver
// returns when a namespace is out of capacity.
func siteQuotaError() error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "engine-pod", errors.New(
		"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=56, limited: requests.nvidia.com/gpu=64"))
}

// siteInvalidError is the apiserver refusing the pod object itself.
func siteInvalidError() error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "engine-pod", nil)
}

// TestRestartPhaseB_QuotaExceeded_WaitsWithoutFailing: Restart's recreate
// phase hits the same quota wall as a fresh create. The pass must end
// without an error (so the dispatcher's restart interval owns the retry,
// not controller-runtime's escalating backoff) and record the quota as
// the operation's waiting reason rather than failing the Instance.
//
// Cells: instance/Restart/Drain/api.quotaDenied/before
func TestRestartPhaseB_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	// Phase A already completed: the instance is Restarting at the bumped
	// incarnation and its old pod is gone, so Phase B does the create.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Recreate", Reason: "pod lost",
		},
	}
	base := legacyNewFakeClient(t, isvc, ir)
	c := rejectPodCreates(t, base, siteQuotaError())
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	done, err := Restart(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], "pod lost")
	if err != nil {
		t.Fatalf("Restart: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("Restart reported done while blocked on quota")
	}

	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatalf("instance 0: got Failed want the restart still in flight")
	}
	if s.Operation == nil || !workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota waiting token recorded", s.Operation)
	}
	// The restart's own cause must survive the wait: Reason says WHY the
	// operation exists and is part of the terminal-finalize identity
	// tuple, so a transient quota blip may not overwrite it.
	if s.Operation.Reason != "pod lost" {
		t.Errorf("Operation.Reason: got %q want the restart cause %q preserved", s.Operation.Reason, "pod lost")
	}
}

// TestRecreatePhaseB_Throttled_HonorsServerDelay: a 429 during the
// recreate rollout's Phase B is the apiserver pacing us. The pass ends
// clean and deposits the server's suggested delay on the pass pacing, so
// the dispatcher's requeue is floored by it instead of the error path
// escalating a backoff the server never asked for.
//
// Cells: instance/Update/Drain/api.throttled[TooManyRequests]/before
func TestRecreatePhaseB_Throttled_HonorsServerDelay(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("example.com/app:v2")
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, base, isvc, targetSpec)
	// Phase A already completed: Updating at the bumped incarnation with
	// no pods left, so Phase B does the create.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Phase B status: %v", err)
	}
	c := rejectPodCreates(t, base, apierrors.NewTooManyRequests("apiserver is shedding load", 7))
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	input.Pacing = &workload.APIPacing{}
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec)
	if err != nil {
		t.Fatalf("Update: %v (throttling is paced, not failed)", err)
	}
	if done {
		t.Fatal("Update reported done while throttled")
	}
	if got := input.Pacing.Throttle(); got != 7*time.Second {
		t.Fatalf("pass pacing: got %v want the server's suggested 7s", got)
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("instance 0: got Failed want the rollout still in flight")
	}
	if s.Operation != nil && workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Errorf("Operation.Waiting: got %q want unset (throttling writes no status)", s.Operation.Waiting)
	}
}

// recreatePhaseBFixture seeds a recreate whose Phase A is done: the
// Instance is Updating at the bumped incarnation with no pods left, so
// the pass's only work is the Phase B rebuild create. Returns the base
// client (unwrapped, for reading status back), the pass input and the
// target revision.
func recreatePhaseBFixture(t *testing.T) (client.Client, *v1beta1.InferenceService, workload.ReconcileInput, *appsv1.ControllerRevision, *corev1.PodSpec) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("example.com/app:v2")
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, base, isvc, targetSpec)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Phase B status: %v", err)
	}
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = tcr.Name
	return base, isvc, input, tcr, targetSpec
}

// TestRecreatePhaseB_QuotaExceeded_WaitsWithoutFailing: the recreate has
// already drained and deleted the old incarnation, so a quota wall in
// front of the rebuild leaves the row serving nothing. It is still a
// wait, not a failure: the pass ends clean and the quota lands as the
// operation's waiting token, which is what parks the deadline clock
// while the namespace has no room.
//
// Cells: instance/Update/Drain/api.quotaDenied/before
func TestRecreatePhaseB_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	base, isvc, input, tcr, targetSpec := recreatePhaseBFixture(t)
	c := rejectPodCreates(t, base, siteQuotaError())
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec)
	if err != nil {
		t.Fatalf("Update: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("Update reported done while blocked on quota")
	}

	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Fatalf("Phase: got %q want Updating (the recreate is still in flight)", s.Phase)
	}
	if s.Operation == nil || !workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota waiting token recorded", s.Operation)
	}
}

// TestRecreatePhaseB_RejectionDispositions: the rebuild create meets the
// same classified rejections a fresh create does. A 422 is a statement
// about the revision, so the attempt ends with the revision blamed; a
// terminating namespace ends it with the revision blameless, since no
// corrected pod spec would be admitted either.
//
// Cells: instance/Update/Drain/api.invalid/before, instance/Update/Drain/api.namespaceTerminating/before
func TestRecreatePhaseB_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reject     error
		wantReason string
		wantBlocks int
	}{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     siteNamespaceTerminatingError("engine-pod"),
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, isvc, input, tcr, targetSpec := recreatePhaseBFixture(t)
			blocks := &[]string{}
			input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			c := rejectPodCreates(t, base, tc.reject)
			plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

			if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec); err != nil {
				t.Fatalf("Update: %v (a classified rejection is disposed, not returned)", err)
			}

			s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceFailed {
				t.Fatalf("Phase: got %q want Failed", s.Phase)
			}
			if s.Operation != nil {
				t.Errorf("Operation: got %+v want cleared", s.Operation)
			}
			if s.LastFailure == nil || s.LastFailure.Reason != tc.wantReason {
				t.Errorf("LastFailure: got %+v want reason %s", s.LastFailure, tc.wantReason)
			}
			if len(*blocks) != tc.wantBlocks {
				t.Errorf("RetryBlock writes: got %+v want %d", *blocks, tc.wantBlocks)
			}
		})
	}
}

// TestSurgeCreate_QuotaExceeded_WaitsWithoutFailing: the per-pod surge's
// Phase 1 create is quota-blocked. The source stays serving and the
// rollout waits with its clock parked — it must not surface an error.
//
// Cells: instance/Update/Surge/api.quotaDenied/before
func TestSurgeCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := makeCR(t, base, isvc, "llama-70b-engine-rev-abc12345")
	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := base.Create(context.Background(), sourcePod); err != nil {
		t.Fatalf("seed source pod: %v", err)
	}
	c := rejectPodCreates(t, base, siteQuotaError())
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := surgePlan()

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr,
		[]*corev1.Pod{sourcePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("surgeUpdate reported done while blocked on quota")
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatalf("instance 0: got Failed want the surge still in flight")
	}
	if s.Operation == nil || !workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota waiting token recorded", s.Operation)
	}
}

// gangSurgeSourceIndex is the source half of the gang surge fixture pair.
const gangSurgeSourceIndex = int32(0)

// gangSurgeRejectionFixture is a gang surge mid-flight: the source at
// index 0 pinned to the replacement gang's marker at index 2, both on
// the target revision, with a mutation store the pass writes through.
func gangSurgeRejectionFixture(t *testing.T) (workload.ReconcileInput, *terminalMutationStore, int32, string) {
	t.Helper()
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-quota", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-quota-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-quota-engine-oldrev",
		TargetRevision:  revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			TargetRevision: revision,
			SurgeIndex:     &surgeIndex,
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: revision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			surgeIndex:   cloneTerminalStatus(marker),
		},
	}
	input := workload.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key: workload.Key{
			Namespace: namespace,
			OwnerName: isvcName,
			Component: workload.ComponentEngine,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source), cloneTerminalStatus(marker)},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: legacyTargetSpecImage("example.com/app:v2"),
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			return store.apply(context.Background(),
				[]workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	return input, store, surgeIndex, revision
}

// TestGangSurgeCreate_QuotaExceeded_WaitsWithoutFailing: the gang surge
// creates a whole replacement gang at once, so it is the most likely site
// to exhaust a quota. It must wait like every other site rather than
// erroring the pass — AND the wait must reach the SOURCE, whose operation
// and deadline govern the rollout while the pods are created under the
// surge index. The test drives the two readers that consume it (the
// deadline park, the escalation skip) to prove the source is held.
//
// Cells: instance/Update/GangSurgeTarget/api.quotaDenied/before
func TestGangSurgeCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	input, store, surgeIndex, revision := gangSurgeRejectionFixture(t)
	base := legacyNewFakeClient(t)
	deps := legacyTestDeps(rejectPodCreates(t, base, siteQuotaError()))
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while blocked on quota")
	}
	blocked, found := store.statuses[surgeIndex]
	if !found {
		t.Fatalf("surge marker missing after the blocked pass")
	}
	if blocked.Phase == workload.InstancePhaseFailed {
		t.Fatalf("surge instance: got Failed want the surge still in flight")
	}
	if !workload.OperationCapacityBlocked(blocked.Operation) {
		t.Fatalf("surge Operation: got %+v want the quota waiting token recorded", blocked.Operation)
	}

	// The SOURCE carries the deadline the rollout is judged by, and the
	// readers that hold it (the deadline park, the escalation skip) live
	// in the parent workload package, which cannot be imported from here.
	// Their side of this contract is pinned by
	// TestGangSurgeSource_HeldWhileItsSurgeWaitsOnQuota.
	if blocked.Operation.SurgeIndex != nil {
		t.Errorf("surge row Operation.SurgeIndex: got %v want nil (the pin lives on the source)", blocked.Operation.SurgeIndex)
	}
	if src := store.statuses[gangSurgeSourceIndex]; src.Operation == nil || src.Operation.SurgeIndex == nil ||
		*src.Operation.SurgeIndex != surgeIndex {
		t.Errorf("source Operation: got %+v want the surge pin the readers follow", src.Operation)
	}
}

// TestMigrateSurge_InvalidPodSpec_FailsTheMigration: a surge pod the
// apiserver will never accept cannot be waited out, so the migration
// record is closed Failed immediately instead of idling to its deadline.
func TestMigrateSurge_InvalidPodSpec_FailsTheMigration(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	f := newSinglePodMigFixture(t)
	f.clk = clk
	const uuid = "mig-surge-invalid-spec"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(time.Minute)),
	}
	f.c = rejectPodCreates(t, f.c, siteInvalidError())

	// passResult builds its own input, so drive Migrate directly to
	// observe the RetryBlock seam this test is about.
	legacyResetExpectations(t)
	record := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      record.SourceInstance,
		FromNode:      record.FromNode,
		Reason:        record.Reason,
	}
	in := f.input(t)
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}

	done, accepted, err := Migrate(context.Background(), f.deps(), in, f.plan, record.SourceInstance, uuid, req)
	if err != nil {
		t.Fatalf("migrate pass: %v", err)
	}
	if !accepted {
		t.Fatalf("migrate pass: accepted=false want true")
	}
	if !done {
		t.Fatalf("migrate pass: done=false want true (the request is closed, not waiting)")
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record phase: got %s want Failed", rec.Phase)
	}
	if rec.CompletedAt == nil {
		t.Errorf("record CompletedAt: got nil want the close timestamp")
	}
	// The surge pod carries the request's placement overlay, so a 422 may
	// indict the overlay rather than the revision. Closing the request is
	// the whole remedy; holding the revision would wedge an innocent
	// rollout on an operator's bad migration request.
	if len(*blocks) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none for an overlay-bearing surge", *blocks)
	}
}

// siteNamespaceTerminatingError is the apiserver refusing a create
// because the namespace is on its way out.
func siteNamespaceTerminatingError(name string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "` + name + `" is forbidden: unable to create new content in namespace prod because it is being terminated`,
		Details: &metav1.StatusDetails{
			Kind: "pods", Name: name,
			Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause, Message: "namespace prod is being terminated"}},
		},
	}}
}

// restartPhaseBRow is a repair whose Phase A is done: the old pod is
// gone and the bumped incarnation is waiting for its rebuild create.
func restartPhaseBRow() v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Recreate", Reason: "pod lost",
		},
	}
}

// TestRestartPhaseB_RejectionDispositions: the rebuild create meets the
// same classified rejections a fresh create does, and the repair answers
// them the same way — a 422 is permanent and ends the attempt with the
// revision blamed, a terminating namespace ends it with the revision
// blameless, and a 429 is pacing that writes nothing at all.
//
// Cells: instance/Restart/Drain/api.invalid/before, instance/Restart/Drain/api.namespaceTerminating/before, instance/Restart/Drain/api.throttled[TooManyRequests]/before
func TestRestartPhaseB_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reject     error
		wantFailed bool
		wantReason string
		wantBlocks int
	}{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantFailed: true,
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     siteNamespaceTerminatingError("engine-pod"),
			wantFailed: true,
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
		{
			name:   "throttled",
			reject: apierrors.NewTooManyRequests("apiserver is shedding load", 7),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyResetExpectations(t)
			isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
			ir.Status.InstanceStatuses[0] = restartPhaseBRow()
			base := legacyNewFakeClient(t, isvc, ir)
			c := rejectPodCreates(t, base, tc.reject)
			input := legacyTestInput(isvc, base, workload.ComponentEngine)
			// The repair pins no revision of its own, so the revision a
			// 422 blames is the owner's current one.
			input.ObservedState.UpdateRevision = "llama-70b-engine-newhash"
			blocks := &[]string{}
			input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

			if _, err := Restart(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], "pod lost"); err != nil {
				t.Fatalf("Restart: %v (a classified rejection is disposed, not returned)", err)
			}

			s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
			if tc.wantFailed {
				if s.Phase != v1beta1.OMENativeInstanceFailed {
					t.Fatalf("Phase: got %q want Failed", s.Phase)
				}
				if s.LastFailure == nil || s.LastFailure.Reason != tc.wantReason {
					t.Errorf("LastFailure: got %+v want reason %s", s.LastFailure, tc.wantReason)
				}
			} else {
				if s.Phase != v1beta1.OMENativeInstanceRestarting {
					t.Errorf("Phase: got %q want Restarting (throttling is paced, not failed)", s.Phase)
				}
				if s.LastFailure != nil {
					t.Errorf("LastFailure: got %+v want none (throttling writes no status)", s.LastFailure)
				}
				if s.Operation == nil || s.Operation.Reason != "pod lost" {
					t.Errorf("Operation: got %+v want the repair intact", s.Operation)
				}
			}
			if len(*blocks) != tc.wantBlocks {
				t.Errorf("RetryBlock writes: got %+v want %d", *blocks, tc.wantBlocks)
			}
		})
	}
}

// gangNamespaceTerminatingError is the apiserver refusing a gang member
// because the namespace is on its way out.
func gangNamespaceTerminatingError(name string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "` + name + `" is forbidden: unable to create new content in namespace test-ns because it is being terminated`,
		Details: &metav1.StatusDetails{
			Kind: "pods", Name: name,
			Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause, Message: "namespace test-ns is being terminated"}},
		},
	}}
}

// TestGangSurgeCreate_RejectionDispositions: a quota refusal is the one
// gang-member rejection worth waiting out. The permanent ones are not —
// a 422 and a terminating namespace both dispose the surge row on the
// first rejection rather than idling it to the deadline, and they differ
// only in whether a revision is to blame. A 429 is neither: it is pacing,
// and it writes nothing at all.
//
// Cells: instance/Update/GangSurgeTarget/api.invalid/before, instance/Update/GangSurgeTarget/api.namespaceTerminating/before, instance/Update/GangSurgeTarget/api.throttled[TooManyRequests]/before
func TestGangSurgeCreate_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reject     error
		wantFailed bool
		wantReason string
		wantBlocks int
	}{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantFailed: true,
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     gangNamespaceTerminatingError("engine-pod"),
			wantFailed: true,
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
		{
			name:   "throttled",
			reject: apierrors.NewTooManyRequests("apiserver is shedding load", 7),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, store, surgeIndex, revision := gangSurgeRejectionFixture(t)
			blocks := &[]string{}
			input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			base := legacyNewFakeClient(t)
			deps := legacyTestDeps(rejectPodCreates(t, base, tc.reject))
			plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

			if _, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target); err != nil {
				t.Fatalf("gangSurgeUpdate: %v (a classified rejection is disposed, not returned)", err)
			}

			marker, found := store.statuses[surgeIndex]
			if !found {
				t.Fatalf("surge marker missing after the rejected pass")
			}
			if tc.wantFailed {
				if marker.Phase != workload.InstancePhaseFailed {
					t.Fatalf("marker Phase: got %q want Failed (the rejection is permanent)", marker.Phase)
				}
				if marker.LastFailure == nil || marker.LastFailure.Reason != tc.wantReason {
					t.Errorf("marker LastFailure: got %+v want reason %s", marker.LastFailure, tc.wantReason)
				}
				if marker.Operation != nil {
					t.Errorf("marker Operation: got %+v want cleared with the disposal", marker.Operation)
				}
			} else {
				if marker.Phase == workload.InstancePhaseFailed {
					t.Errorf("marker Phase: got Failed want the surge still in flight (throttling is paced)")
				}
				if marker.LastFailure != nil {
					t.Errorf("marker LastFailure: got %+v want none (throttling writes no failure accounting)", marker.LastFailure)
				}
			}
			if len(*blocks) != tc.wantBlocks {
				t.Errorf("RetryBlock writes: got %v want %d (only a bad pod spec blames the revision)", *blocks, tc.wantBlocks)
			}
		})
	}
}
