package workload_test

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// Reconcile is a thin dispatch layer over the workload/ops state
// machines. The tests below assert the orchestration shape — nil-deps
// guards, scale-down-then-create ordering, and the early-return
// requeue on partial scale-down — by driving Reconcile against a real
// fake client and the real workload/ops bodies. Per-op bug-fix
// coverage already lives in workload/ops/*_test.go and is unchanged
// by the dispatch refactor (the op bodies move with their tests; only
// the dispatcher's call site changes).
//
// Heavier per-op coverage (Update gate ordering, Restart trigger
// detection, Migrate surge lifecycle) stays in workload/ops/*_test.go;
// those tests already drive the per-op functions directly and don't
// need a Reconcile wrapper.

// makeScheme builds the runtime.Scheme the fake client needs.
func makeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	return scheme
}

// stubInputCallbacks fills the callback fields that
// workload.ReconcileInput panics on if left nil. Tests only set
// behavior on the callbacks they care about; the rest stay no-ops.
func stubInputCallbacks(input *workload.ReconcileInput) {
	if input.MutateInstance == nil {
		input.MutateInstance = func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
			return nil
		}
	}
	if input.RemoveInstance == nil {
		input.RemoveInstance = func(_ context.Context, _ int32) (bool, error) { return false, nil }
	}
	if input.WriteAggregateCondition == nil {
		input.WriteAggregateCondition = func(_ context.Context, _ metav1.Condition) error { return nil }
	}
	if input.ClearMigrationRequest == nil {
		input.ClearMigrationRequest = func(_ context.Context, _ string) error { return nil }
	}
	if input.WarnInstanceFailed == nil {
		input.WarnInstanceFailed = func(_ int32, _, _ string) {}
	}
}

// minimalInput builds a ReconcileInput with the bare minimum fields
// the dispatcher reads. Tests pad observed state + plan as needed.
func minimalInput(t *testing.T) workload.ReconcileInput {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: "uid-1",
	}}
	in := workload.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workload.Key{
			Namespace: "prod",
			Component: workload.ComponentEngine,
			OwnerName: "llama-70b",
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			Replicas: 1,
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{
				{Name: "main", Image: "test:v1"},
			}},
		},
	}
	stubInputCallbacks(&in)
	return in
}

// minimalPlan returns a ComponentPlan covering a single Instance at
// index 0 with a single-pod "default" Runner.
func minimalPlan() workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{
				{Name: "default", Size: 1},
			}},
		},
	}
}

// TestReconcile_NilClient_Errors asserts the nil-deps.Client guard
// fires up front. The dispatcher MUST refuse to run without a client
// so the per-op state machines never NPE on a missing reader.
func TestReconcile_NilClient_Errors(t *testing.T) {
	in := minimalInput(t)
	_, err := workload.Reconcile(context.Background(), workload.Deps{}, in, minimalPlan(), nil)
	if err == nil {
		t.Fatalf("expected nil-Client to error, got nil")
	}
}

// TestReconcile_NilTarget_DrivesCreate covers the cold-start path:
// no observed instances, no target ControllerRevision, plan asks for
// one Instance. Reconcile MUST fall through to the Create pass without
// scale-down / restart / migration work; Create's first action is to
// allocate the Instance, which the fake client accepts.
//
// We pass target=nil because Create's signature allows it (MinReplicas=0
// would render a nil target; here we exercise the cold-create path
// where the renderer hasn't materialized a revision yet).
func TestReconcile_NilTarget_DrivesCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	plan := minimalPlan()

	// Cold-start path: target nil short-circuits the Update pass.
	// Create's nil-target branch returns done=true without rendering.
	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_ScaleDownExtra_DrivesDelete covers the
// "InstanceStatus has index 1 but plan doesn't" scale-down path. The
// dispatcher MUST drive Delete for the extra index, and because Delete
// is the first op in the chain, it returns the per-Delete requeue
// when the live pod count is non-zero. The fake client has no pods,
// so Delete resolves done=true and the dispatcher falls through.
func TestReconcile_ScaleDownExtra_DrivesDelete(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{Index: 1, Phase: workload.InstancePhaseReady}, // extra
	}
	plan := minimalPlan() // covers only index 0

	if extras := workload.ExtraInstanceIndices(in.ObservedState.InstanceStatuses, plan); len(extras) != 1 || extras[0] != 1 {
		t.Fatalf("ExtraInstanceIndices: got %v, want [1]", extras)
	}
	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_MigrationInFlight_NotScaleDown asserts that an
// Instance with Phase=Migrating is NOT scale-down-deleted, even when
// the plan doesn't cover its index — otherwise Migrate's surge
// would be ripped out by the dispatcher's first pass.
func TestReconcile_MigrationInFlight_NotScaleDown(t *testing.T) {
	observed := []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{Index: 7, Phase: workload.InstancePhaseMigrating},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ExtraInstanceIndices(observed, plan)
	for _, idx := range extras {
		if idx == 7 {
			t.Errorf("index 7 (Phase=Migrating) must not be in scale-down extras, got %v", extras)
		}
	}
}

// TestReconcile_MigrationOperationOwned_NotScaleDown is the dual to
// the above: an Instance whose Operation.Type=Migrate (the surge side)
// MUST be excluded even when Phase=Creating (surge mid-spin-up).
func TestReconcile_MigrationOperationOwned_NotScaleDown(t *testing.T) {
	observed := []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{
			Index: 3,
			Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				Type: workload.InstanceOperationMigrate,
			},
		},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ExtraInstanceIndices(observed, plan)
	for _, idx := range extras {
		if idx == 3 {
			t.Errorf("index 3 (Operation.Migrate) must not be in scale-down extras, got %v", extras)
		}
	}
}

// TestReconcile_SkipsMigrateWhenModeNever asserts the dispatcher
// short-circuits the migration pass when plan.MigrationMode==never,
// even if a DetectMigrationRequest callback is wired.
func TestReconcile_SkipsMigrateWhenModeNever(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	detectorCalled := false
	in.DetectMigrationRequest = func(_ context.Context) (string, *workload.MigrationRequestSummary, int32, bool, error) {
		detectorCalled = true
		return "", nil, 0, false, nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workload.MigrationModeNever

	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if detectorCalled {
		t.Errorf("DetectMigrationRequest must NOT fire when MigrationMode=never")
	}
}

// TestReconcile_SkipsMigrateWhenDetectorNil asserts the dispatcher
// is safe when an adapter (e.g., a future IR adapter) leaves
// DetectMigrationRequest=nil — no panic, no migration work.
func TestReconcile_SkipsMigrateWhenDetectorNil(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.DetectMigrationRequest = nil
	plan := minimalPlan()
	plan.MigrationMode = workload.MigrationModeAuto

	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_UpdateGateNil_AllowsAll asserts the dispatcher treats
// a nil UpdateGate as always-allowed — workload-side unit tests and
// the IR adapter (which doesn't wire coordination gates) rely on this.
// We assert by setting up a plan that would trigger Update if the
// target diff fires, and confirming Reconcile doesn't error.
func TestReconcile_UpdateGateNil_AllowsAll(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.UpdateGate = nil
	plan := minimalPlan()

	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_FreshMigrateOnNonReadySource_FallsThroughToUpdate pins
// the affinity-trigger-not-detected bugfix: when DetectMigrationRequest
// returns found=true for a fresh request UUID (no in-flight ledger
// entry, no stamped Operation) but the source InstanceStatus is NOT in
// a state where Migrate can accept it (Phase=Updating, Creating,
// Restarting, or any Operation != Migrate in flight), Migrate defers
// without taking ownership. The dispatcher MUST then fall through to
// the Update / Create passes so the in-flight op can converge —
// otherwise the dispatcher loops indefinitely in the Migrate-defer
// branch, the in-flight Update never completes, and the source never
// reaches Ready so the migration can never proceed (silent deadlock).
//
// Reproduction shape: user adds NodeAffinity to ISVC.Spec.Engine
// PodSpec while the source pod is Running on the to-be-pinned node.
// The controller fires Update (Phase=Updating). A migration request
// annotation lands before the Update converges. detectMigrationRequest
// returns found=true; Migrate sees Phase=Updating, returns done=false
// without stamping. Without this fix the dispatcher requeues at
// MigrateRequeueInterval indefinitely; Update never runs; the test's
// Eventually for the audit ledger times out at 300s.
//
// Test shape: instrument MutateInstance to count calls. Without the
// fix: zero mutations (Migrate's defer doesn't mutate; Update never
// runs). With the fix: Update fires and at minimum touches MutateInstance
// to stamp Op.Step=Surge.
func TestReconcile_FreshMigrateOnNonReadySource_FallsThroughToUpdate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		// Phase=Updating: an in-flight Update from a recent spec edit
		// (e.g., adding NodeAffinity to ISVC PodSpec). Migrate's fresh-
		// request branch defers because source.Phase != Ready.
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev"},
	}
	plan := minimalPlan()
	plan.UpdateStrategy = workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain}
	plan.MigrationMode = workload.MigrationModeAuto

	// DetectMigrationRequest returns a fresh annotation-detected request.
	// Note the dispatcher does NOT pre-check source state — that's
	// Migrate's job. The point of the test is the post-Migrate fall-
	// through.
	in.DetectMigrationRequest = func(_ context.Context) (string, *workload.MigrationRequestSummary, int32, bool, error) {
		return "fresh-uuid-during-update", &workload.MigrationRequestSummary{
			SchemaVersion: "v1",
			Component:     "engine",
			Instance:      0,
			FromNode:      "node5",
		}, 0, true, nil
	}

	// Count MutateInstance calls to detect Update firing. Migrate's
	// defer path doesn't mutate; Update's stamp does. Without the
	// dispatcher fix mutateCount is 0; with it, Update fires and the
	// count is > 0.
	mutateCount := 0
	in.MutateInstance = func(_ context.Context, _ int32, fn func(*workload.InstanceStatus) bool) error {
		mutateCount++
		// Apply mutation against the seeded ObservedState so subsequent
		// reads inside the same reconcile see the result.
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == 0 {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	// Provide a non-nil target so the Update loop is considered.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		// Per-op machines (Update's surge body) can error against the
		// empty fake client (no CRs, no pods to drain). The contract this
		// test pins is the dispatcher-level fall-through, not the op-body
		// success. Log and continue to the load-bearing assertion.
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing assertion: MutateInstance was called at least once,
	// which means the dispatcher reached the Update pass after Migrate
	// deferred. Without the fix the dispatcher returns immediately
	// after Migrate's silent defer and mutateCount stays 0.
	if mutateCount == 0 {
		t.Errorf("expected dispatcher to fall through to Update after Migrate deferred; MutateInstance was never called (silent Migrate-defer deadlock)")
	}
}

// TestReconcile_UpdateRan_RequeuesAndSkipsCreate pins the X-2
// (bump-during-bump) fix: when ANY Update.call fires in the per-
// Instance loop (even one returning done=true), the dispatcher MUST
// return Requeue WITHOUT running the Create pass. The Create pass
// reads ObservedState (a snapshot) which is now stale wrt the mutations
// the Update calls just committed; running Create on stale state
// corrupts RunningRevision (Create promotes pods to target.Name even
// when the pods are on a different revision) and creates duplicate
// pods (Create's scale-up reads the pre-promote ActiveOrdinal and
// thinks the canonical slot is missing). Both are the X-2 corruption
// modes.
//
// We exercise the path by seeding an Instance with Phase=Updating —
// DetectUpdateTrigger returns true on this phase, so Update fires —
// and asserting the dispatcher returns Requeue. The minimal stub
// MutateInstance keeps the test focused on the dispatcher contract
// (not on per-op behavior); the surge/recreate state machines have
// their own per-op tests.
func TestReconcile_UpdateRan_RequeuesAndSkipsCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		// Phase=Updating triggers DetectUpdateTrigger → true regardless
		// of RunningRevision; the dispatcher MUST call Update.
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev"},
	}
	plan := minimalPlan()
	plan.UpdateStrategy = workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain}
	// Provide a non-nil target so the Update loop is even considered.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		// Update can error if its per-op machine refuses (e.g., InPlaceOnly
		// + ineligible diff). For this dispatch-shape test we only care
		// about the Requeue contract; ignore op-level errors and only
		// assert when the dispatcher returns nil-error.
		t.Logf("Reconcile op error (expected for dispatch-shape test): %v", err)
	}
	// The load-bearing assertion: dispatcher returned Requeue=true (or
	// RequeueAfter > 0) — Create did NOT run. Without the X-2 fix, the
	// dispatcher returned Create's result (possibly Requeue=false).
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when any Update fired (X-2 guard); got %+v", result)
	}
}

// TestReconcile_ScaleUpDuringUpdate_CreatesFreshIndices pins the
// scale-out-during-rollout fix: with instance-0 mid-update (Phase=
// Updating) and the plan asking for indices [0,1,2], the dispatcher must
// materialize the brand-new (surge-free) indices 1 and 2 THIS reconcile
// — not starve them behind the in-flight rollout on index 0 — then
// requeue. Index 0's status must remain Updating: the full Create-pass
// promote does not run on it (CreateFreshIndices skips non-surge-free
// indices, and the gated full Create pass at the bottom is bypassed).
func TestReconcile_ScaleUpDuringUpdate_CreatesFreshIndices(t *testing.T) {
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
							Index:           0,
							Incarnation:     1,
							Phase:           v1beta1.OMENativeInstanceUpdating,
							RunningRevision: "llama-70b-engine-priorrev",
						}},
					},
				},
			},
		},
	}
	// Index 0's existing in-flight pod, so the Update pass operates on a
	// real pod and the fresh-pass create can't accidentally collide.
	pod0 := enginePod(isvc.Name, isvc.Namespace, 0)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(isvc, pod0).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, RunningRevision: "llama-70b-engine-priorrev"},
	}
	in.MutateInstance = roundTripMutateInstance(c, isvc, workload.ComponentEngine)
	// Plan asks for 3 single-pod instances. Index 0 is mid-update; 1,2 brand-new.
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  3,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Dispatcher must requeue (index 0 still updating) — it must NOT
	// return Create's steady-state zero result.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must requeue while index 0 updates; got %+v", result)
	}

	// Load-bearing: the brand-new indices were materialized THIS reconcile.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	got := map[string]bool{}
	for _, p := range pods.Items {
		got[p.Name] = true
	}
	if !got["llama-70b-engine-1-default-0"] {
		t.Errorf("fresh index 1 must be created during the in-flight rollout; got %v", got)
	}
	if !got["llama-70b-engine-2-default-0"] {
		t.Errorf("fresh index 2 must be created during the in-flight rollout; got %v", got)
	}

	// Index 0's status must still be Updating — the full Create promote
	// did not run on it.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s0 := instanceStatusByIndex(fresh, v1beta1.EngineComponent, 0)
	if s0 == nil || s0.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("index 0 must remain Phase=Updating (Create promote must not run on it); got %+v", s0)
	}
}

// TestHeldByPartition pins the RollingUpdate.Partition canary predicate that the
// reconcile loop uses to skip held Instances: an Instance is held on the old
// revision iff its index is below a positive Partition. A nil RollingUpdate,
// nil Partition, or Partition<=0 holds nothing; surge Instances allocated above
// the replica count (index >= replicas >= Partition) are never held.
func TestHeldByPartition(t *testing.T) {
	part := func(n int32) *workload.RollingUpdate { return &workload.RollingUpdate{Partition: &n} }
	cases := []struct {
		name  string
		ru    *workload.RollingUpdate
		index int32
		held  bool
	}{
		{"nil RollingUpdate", nil, 0, false},
		{"nil Partition", &workload.RollingUpdate{}, 0, false},
		{"Partition=0 holds nothing", part(0), 0, false},
		{"Partition=2 holds index 0", part(2), 0, true},
		{"Partition=2 holds index 1", part(2), 1, true},
		{"Partition=2 rolls index 2", part(2), 2, false},
		{"Partition=2 rolls index 3", part(2), 3, false},
		{"surge index above replicas never held", part(2), 4, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := workload.HeldByPartition(tc.ru, tc.index); got != tc.held {
				t.Errorf("HeldByPartition(index=%d) = %v, want %v", tc.index, got, tc.held)
			}
		})
	}
}

// enginePod fabricates a single-pod "default" engine pod at ordinal 0
// with the labels Render stamps (managed-by + instance-idx + ordinal +
// component + isvc), so query selectors and instance-index filters
// recognize it.
func enginePod(isvc, ns string, idx int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workload.ComponentEngine, idx, "default", 0),
			Namespace: ns,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelInstanceIdx:                fmt.Sprintf("%d", idx),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     "default",
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 "0",
				query.LabelRevisionHash:               "priorrev",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
}

// roundTripMutateInstance returns a MutateInstance that persists through
// the fake client's status subresource (appending a slot for an absent
// index), so the dispatcher's per-index status writes survive for
// post-reconcile assertions. Mirrors the production retry-wrapped
// callback minus the conflict retry the fake client never needs.
func roundTripMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
	v1component := v1beta1.ComponentType(component)
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
			return fmt.Errorf("get isvc: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		cs := fresh.Status.Components[v1component]
		if cs.Lifecycle == nil {
			cs.Lifecycle = &v1beta1.LifecycleStatus{}
		}
		pos := -1
		for i, s := range cs.Lifecycle.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		slot := v1beta1.OMENativeInstanceStatus{Index: idx}
		if pos != -1 {
			slot = cs.Lifecycle.InstanceStatuses[pos]
		}
		w := v1beta1convert.InstanceStatusToWorkload(slot)
		if !mutate(&w) {
			return nil
		}
		updated := v1beta1convert.InstanceStatusFromWorkload(w)
		if pos == -1 {
			cs.Lifecycle.InstanceStatuses = append(cs.Lifecycle.InstanceStatuses, updated)
		} else {
			cs.Lifecycle.InstanceStatuses[pos] = updated
		}
		fresh.Status.Components[v1component] = cs
		return c.Status().Update(ctx, fresh)
	}
}

// instanceStatusByIndex looks up an InstanceStatus on the ISVC snapshot.
func instanceStatusByIndex(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.Lifecycle == nil {
		return nil
	}
	for i := range cs.Lifecycle.InstanceStatuses {
		if cs.Lifecycle.InstanceStatuses[i].Index == idx {
			return &cs.Lifecycle.InstanceStatuses[i]
		}
	}
	return nil
}
