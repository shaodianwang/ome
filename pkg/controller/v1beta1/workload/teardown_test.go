package workload_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// Teardown-mode dispatcher contract (owner deletion): the planned index
// set is treated as empty so EVERY observed Instance runs the ordinary
// Delete pipeline, and nothing else runs — no Paused gate, no
// Restart / Migrate / Update / Create. The caller owns completion
// detection and finalizer decisions, so the dispatcher only reports
// "deletes still in flight" (DeleteRequeueInterval) or "all observed
// Instances resolved" (zero result).

// TestReconcile_Teardown_DeletesEveryObservedInstance drives teardown
// against three observed Instances (with live pods) while the plan
// covers only index 0. All three indices — INCLUDING the planned one —
// must be Delete-dispatched, no pod may be created, and neither the
// migration detector nor the update pass may run.
func TestReconcile_Teardown_DeletesEveryObservedInstance(t *testing.T) {
	scheme := makeScheme(t)
	in := minimalInput(t)
	objs := []client.Object{
		enginePod("llama-70b", "prod", 0),
		enginePod("llama-70b", "prod", 1),
		enginePod("llama-70b", "prod", 2),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workload.Deps{Client: c, Expectations: workload.NewExpectations()}

	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady},
	}
	phaseByIdx := map[int32]workload.InstancePhase{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		s := workload.InstanceStatus{Index: idx}
		fn(&s)
		phaseByIdx[idx] = s.Phase
		return nil
	}
	// A non-terminal Manual record that would be dispatched on a normal
	// reconcile; teardown must never reach the migration pass.
	in.ObservedState.Migrations = []workload.MigrationRecord{{
		RequestUUID: "teardown-mig", Trigger: workload.MigrationTriggerManual,
		Phase: workload.MigrationPhaseAccepted, SourceInstance: 0,
	}}
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*workload.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}

	plan := minimalPlan() // covers only index 0
	plan.MigrationMode = workload.MigrationModeAuto
	// A non-nil target would arm the Update pass on a normal reconcile;
	// teardown must never reach it.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != workloadops.DeleteRequeueInterval {
		t.Errorf("teardown with pods in flight must requeue at DeleteRequeueInterval, got %+v", result)
	}
	for _, idx := range []int32{0, 1, 2} {
		if phaseByIdx[idx] != workload.InstancePhaseDeleting {
			t.Errorf("instance %d: phase = %q, want Deleting (every observed index is an extra under teardown)", idx, phaseByIdx[idx])
		}
	}
	if migrationMutated {
		t.Errorf("teardown must not run the migration pass")
	}
	// Every pod deleted, none created (Create pass must not run even
	// though the plan asks for index 0).
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("teardown must delete every pod and create none; %d pods remain", len(pods.Items))
	}
}

// TestReconcile_Teardown_DeletesMigratingPair pins the mid-migration
// teardown contract: a Phase=Migrating source AND its
// Operation.Type=Migrate surge — both excluded from the NORMAL
// scale-down pass so Migrate isn't ripped apart — must each get a
// Delete dispatch under Teardown. Excluding them would leave their
// pods with no Delete op, no drain, no force-delete escalation: the
// teardown wedges forever (strict hold) or falls to un-drained GC at
// the deadline.
func TestReconcile_Teardown_DeletesMigratingPair(t *testing.T) {
	scheme := makeScheme(t)
	objs := []client.Object{
		enginePod("llama-70b", "prod", 0), // migration source
		enginePod("llama-70b", "prod", 7), // surge
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workload.Deps{Client: c, Expectations: workload.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseMigrating},
		{Index: 7, Incarnation: 1, Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationMigrate}},
	}
	phaseByIdx := map[int32]workload.InstancePhase{}
	in.MutateInstance = func(_ context.Context, idx int32, fn func(*workload.InstanceStatus) bool) error {
		s := workload.InstanceStatus{Index: idx}
		fn(&s)
		phaseByIdx[idx] = s.Phase
		return nil
	}

	plan := minimalPlan() // covers only index 0
	plan.MigrationMode = workload.MigrationModeAuto

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != workloadops.DeleteRequeueInterval {
		t.Errorf("teardown with pods in flight must requeue at DeleteRequeueInterval, got %+v", result)
	}
	for _, idx := range []int32{0, 7} {
		if phaseByIdx[idx] != workload.InstancePhaseDeleting {
			t.Errorf("instance %d: phase = %q, want Deleting (mid-migration pair must not survive teardown)", idx, phaseByIdx[idx])
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("teardown must delete the source AND surge pods; %d remain", len(pods.Items))
	}
}

// TestReconcile_Teardown_NoInstancesNoPods_CleanReturn pins the empty
// case: nothing observed, nothing live → immediate zero result, no
// callback fires. The caller's completion check owns the rest.
func TestReconcile_Teardown_NoInstancesNoPods_CleanReturn(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c, Expectations: workload.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	mutations, removals := 0, 0
	in.MutateInstance = func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
		mutations++
		return nil
	}
	in.RemoveInstance = func(_ context.Context, _ int32) (bool, error) {
		removals++
		return false, nil
	}

	result, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("empty teardown must return a zero result, got %+v", result)
	}
	if mutations != 0 || removals != 0 {
		t.Errorf("empty teardown must fire no status callbacks; mutations=%d removals=%d", mutations, removals)
	}
}

// TestReconcile_Teardown_PodlessInstances_RemovedAndDone pins the
// converged shape: observed Instances whose pods are already gone
// resolve in one pass — RemoveInstance drops each entry and the
// dispatcher returns a zero result so the caller can run its
// completion check.
func TestReconcile_Teardown_PodlessInstances_RemovedAndDone(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c, Expectations: workload.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseDeleting},
	}
	removed := map[int32]bool{}
	in.RemoveInstance = func(_ context.Context, idx int32) (bool, error) {
		removed[idx] = true
		return true, nil
	}

	result, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("fully-resolved teardown must return a zero result, got %+v", result)
	}
	if !removed[0] || !removed[1] {
		t.Errorf("both podless Instances must be removed; got %v", removed)
	}
}

// TestReconcile_Teardown_IgnoresPaused pins that teardown short-circuits
// ahead of the Paused gate: an operator pause must not hold a deletion's
// Delete dispatch (deletion is the stronger intent).
func TestReconcile_Teardown_IgnoresPaused(t *testing.T) {
	scheme := makeScheme(t)
	pod := enginePod("llama-70b", "prod", 0)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	deps := workload.Deps{Client: c, Expectations: workload.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady},
	}
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != workloadops.DeleteRequeueInterval {
		t.Errorf("paused teardown must still dispatch Delete and requeue, got %+v", result)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("paused teardown must still delete the pod; %d remain", len(pods.Items))
	}
}
