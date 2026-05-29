package coordination

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

func TestReconcile_NilISVCErrors(t *testing.T) {
	_, err := Reconcile(context.Background(), ReconcileInputs{Client: testClient()})
	if err == nil {
		t.Errorf("nil ISVC: want error")
	}
}

func TestReconcile_NilClientErrors(t *testing.T) {
	_, err := Reconcile(context.Background(), ReconcileInputs{ISVC: testReconcilerISVC()})
	if err == nil {
		t.Errorf("nil Client: want error")
	}
}

func TestReconcile_NoOpForNonOMENativeISVC(t *testing.T) {
	isvc := testReconcilerISVC()
	// No engine/decoder OMENative annotation.
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:   isvc,
		Client: testClient(),
		Reader: testClient(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured != 0 {
		t.Errorf("should not emit Services for non-OMENative ISVC: got %d", r.PerRevisionServicesEnsured)
	}
}

func TestReconcile_EmitsPerRevisionServiceForOMENativeComponent(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.RolloutCoordination = independentEngineGroup()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 1),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:   isvc,
		Client: c,
		Reader: c,
		Now:    time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("should ensure per-revision Services: got %+v", r)
	}
	// Verify the Service exists.
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Errorf("per-revision Service missing: %v", err)
	}
}

func TestReconcile_DataPlaneRunsWithoutCoordinationGroup(t *testing.T) {
	// Updated contract: per-revision Services + Traffic[] are the
	// data plane for OEP-0011 HTTPRoute weighted-backendRef consumers.
	// They emit for every OMENative-mode Component, with or without a
	// rolloutCoordination group declared. Only the per-group state
	// machine (Status.RolloutCoordination, RolloutCoordinationReady
	// condition, group events) stays opt-in via the rolloutCoordination
	// block.
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("no groups: per-revision Services should still emit for the data plane, got %d", r.PerRevisionServicesEnsured)
	}
	if isvc.Status.RolloutCoordination != nil {
		t.Errorf("no groups: should not write RolloutCoordination status, got %+v", isvc.Status.RolloutCoordination)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs.Traffic) == 0 {
		t.Errorf("no groups: Traffic[] should still populate from observed pods, got %+v", cs)
	}
}

func TestReconcile_WritesTrafficStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.RolloutCoordination = independentEngineGroup()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 1),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok {
		t.Fatalf("Components[engine] not populated")
	}
	if len(cs.Traffic) != 1 {
		t.Fatalf("Traffic: got %d entries want 1", len(cs.Traffic))
	}
	if cs.Traffic[0].RevisionName != "llama-engine-rev-hash1" {
		t.Errorf("Traffic[0].RevisionName: got %q want llama-engine-rev-hash1", cs.Traffic[0].RevisionName)
	}
	if cs.Traffic[0].Percent != 100 {
		t.Errorf("Traffic[0].Percent: got %d want 100", cs.Traffic[0].Percent)
	}
}

// TestReconcile_TrafficLatestRevisionIdenticalShapeAcrossComponents pins
// the per-Component contract surfaced by PD-disaggregated A-6 testing:
// for an ISVC running engine + decoder + router (all OMENative, each on
// a single ControllerRevision), every Component's traffic[0] must carry
// the same canonical shape — LatestRevision=true, Percent=100, and the
// per-revision Service name as RevisionName. The cluster repro showed
// engine.traffic[0] missing the LatestRevision flag while
// decoder.traffic[0] + router.traffic[0] had it set correctly — caused
// by the no-op short-circuit in TrafficDiffersMeaningfully treating the
// flag as cosmetic, so an early reconcile that wrote LatestRevision=false
// (UpdateRevision not yet observed from the IR Status) stranded the
// stale value even after the rollout converged.
func TestReconcile_TrafficLatestRevisionIdenticalShapeAcrossComponents(t *testing.T) {
	isvc := testOMENativeISVC()
	// Add decoder + router as OMENative-mode Components.
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	isvc.Spec.Router = &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	// Seed Components[c].OMENative.UpdateRevision so latestRevisionHash
	// recovers the per-Component hash (matches the steady-state shape
	// AggregateIRStatus writes after the IR controller commits Status).
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				UpdateRevision: isvc.Name + "-engine-engineHash",
			},
		},
		v1beta1.DecoderComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				UpdateRevision: isvc.Name + "-decoder-decoderHash",
			},
		},
		v1beta1.RouterComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				UpdateRevision: isvc.Name + "-router-routerHash",
			},
		},
	}
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "engineHash", 0),
		buildPod(isvc, v1beta1.DecoderComponent, "decoderHash", 0),
		buildPod(isvc, v1beta1.RouterComponent, "routerHash", 0),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, comp := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		cs, ok := isvc.Status.Components[comp]
		if !ok {
			t.Errorf("%s: status entry missing", comp)
			continue
		}
		if len(cs.Traffic) != 1 {
			t.Errorf("%s: Traffic len=%d want 1: %+v", comp, len(cs.Traffic), cs.Traffic)
			continue
		}
		if cs.Traffic[0].Percent != 100 {
			t.Errorf("%s: Percent=%d want 100", comp, cs.Traffic[0].Percent)
		}
		if !cs.Traffic[0].LatestRevision {
			t.Errorf("%s: LatestRevision=false want true (regression: PD-disaggregated A-6 cluster bug)", comp)
		}
	}
}

// TestReconcile_TrafficLatestRevisionFlipPersists is the focused
// reconcile-level regression for the no-op short-circuit bug. On the
// first observation the UpdateRevision is empty (matches the
// first-reconcile race where AggregateIRStatus has not yet mirrored
// IR.Status). On the second observation UpdateRevision is populated.
// The writer MUST flip Traffic[0].LatestRevision from false to true on
// the second pass — without the fix, TrafficDiffersMeaningfully treats
// the change as cosmetic and skips the write.
func TestReconcile_TrafficLatestRevisionFlipPersists(t *testing.T) {
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "engineHash", 0),
	}
	c := testClient(pods...)

	// First reconcile: UpdateRevision empty → LatestRevision=false.
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("first reconcile: unexpected error: %v", err)
	}
	cs1, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs1.Traffic) != 1 {
		t.Fatalf("first pass: Traffic missing or wrong len: %+v", cs1.Traffic)
	}
	if cs1.Traffic[0].LatestRevision {
		t.Fatalf("first pass: LatestRevision should be false when UpdateRevision is empty: %+v", cs1.Traffic[0])
	}

	// Mirror AggregateIRStatus committing the IR-observed
	// UpdateRevision back onto the in-memory ISVC.
	cs1.OMENative = &v1beta1.OMENativeComponentStatus{
		UpdateRevision: isvc.Name + "-engine-engineHash",
	}
	isvc.Status.Components[v1beta1.EngineComponent] = cs1

	// Second reconcile: same pod set, but UpdateRevision is now
	// observed — LatestRevision must flip to true on the next write.
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("second reconcile: unexpected error: %v", err)
	}
	cs2, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs2.Traffic) != 1 {
		t.Fatalf("second pass: Traffic missing or wrong len: %+v", cs2.Traffic)
	}
	if !cs2.Traffic[0].LatestRevision {
		t.Errorf("second pass: LatestRevision did not flip to true (regression: A-6 engine traffic[].latestRevision stuck stale)")
	}
}

// independentEngineGroup returns a minimal RolloutCoordinationSpec
// that opts the engine Component into the coordination layer without
// changing rollout semantics (Independent matches today's defaults).
func independentEngineGroup() *v1beta1.RolloutCoordinationSpec {
	return &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyIndependent,
			},
		},
	}
}

func TestReconcile_WritesGroupStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyIndependent},
		},
	}
	pods := []runtime.Object{buildPod(isvc, v1beta1.EngineComponent, "hash1", 0)}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isvc.Status.RolloutCoordination == nil {
		t.Fatalf("RolloutCoordination status nil")
	}
	if len(isvc.Status.RolloutCoordination.Groups) != 1 {
		t.Fatalf("groups: got %d want 1", len(isvc.Status.RolloutCoordination.Groups))
	}
	g := isvc.Status.RolloutCoordination.Groups[0]
	if g.Name != "0" {
		t.Errorf("Name: got %q want 0", g.Name)
	}
	if g.Phase == "" {
		t.Errorf("Phase should be populated")
	}
}

func TestReconcile_InvalidGroupShapeErrors(t *testing.T) {
	isvc := testOMENativeISVC()
	// Sequential without order — Should be caught by webhook in
	// production, but ValidateGroupShape is the runtime safety net.
	isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
			},
		},
	}
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	})
	if err == nil {
		t.Errorf("Sequential without order: want error")
	}
}

func testOMENativeISVC() *v1beta1.InferenceService {
	isvc := testReconcilerISVC()
	isvc.Spec.Engine = &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	return isvc
}

func testReconcilerISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "prod",
			UID:       types.UID("llama-uid"),
		},
	}
}

func buildPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int) runtime.Object {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(isvc.Name, component, idx),
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               revisionHash,
			},
		},
	}
}

func podName(isvc string, component v1beta1.ComponentType, idx int) string {
	// Test pod names — don't need the full Loop A pattern.
	return isvc + "-" + string(component) + "-pod-" + itoa(idx)
}

func itoa(i int) string {
	switch i {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	default:
		return "n"
	}
}

func testClient(objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
}

func TestReconcile_ConsiderCoordinationGroupEventFiresForMultiOMENativeWithoutCoord(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	rec := record.NewFakeRecorder(10)
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := drainEvents(rec)
	var saw bool
	for _, e := range events {
		if contains(e, EventReasonConsiderCoordinationGroup) {
			saw = true
		}
	}
	if !saw {
		t.Errorf("expected %s event for multi-OMENative ISVC without coord block; got %v",
			EventReasonConsiderCoordinationGroup, events)
	}
}

func TestReconcile_ConsiderCoordinationGroupEventNotFiredForSingleComponent(t *testing.T) {
	isvc := testOMENativeISVC() // engine only
	rec := record.NewFakeRecorder(10)
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := drainEvents(rec)
	for _, e := range events {
		if contains(e, EventReasonConsiderCoordinationGroup) {
			t.Errorf("single-Component ISVC must not get the hint event; got %v", events)
		}
	}
}

func TestReconcile_ConsiderCoordinationGroupEventNotFiredWhenGroupDeclared(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
		}},
	}
	rec := record.NewFakeRecorder(10)
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := drainEvents(rec)
	for _, e := range events {
		if contains(e, EventReasonConsiderCoordinationGroup) {
			t.Errorf("ISVC with coord block must not get the hint event; got %v", events)
		}
	}
}

// --- buildComponentObservation per-Instance failure rollup ---

// withInstanceStatus builds an ISVC carrying the given InstanceStatus
// slice on Components.<c>.OMENative. Used by the propagation tests.
func withInstanceStatus(c v1beta1.ComponentType, instances []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test"},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				c: {OMENative: &v1beta1.OMENativeComponentStatus{
					InstanceStatuses: instances,
				}},
			},
		},
	}
}

func TestBuildComponentObservation_NoStatusReturnsZero(t *testing.T) {
	// No status block populated for the Component — observation must
	// report zero values, NOT Failed (otherwise a brand-new ISVC would
	// flip the group to Failed).
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc-test"}}
	obs := buildComponentObservation(isvc, v1beta1.EngineComponent, nil)
	if obs.Failed {
		t.Errorf("missing status must not report Failed; got %+v", obs)
	}
	if obs.FailureMessage != "" {
		t.Errorf("missing status must not carry a FailureMessage; got %q", obs.FailureMessage)
	}
}

func TestBuildComponentObservation_NoFailedInstancesIsNotFailed(t *testing.T) {
	// All Instances Ready/Updating/etc., none Failed — observation
	// reports Failed=false. Pins that the rollup only fires on the
	// explicit Failed phase, not on transient states.
	isvc := withInstanceStatus(v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating},
	})
	obs := buildComponentObservation(isvc, v1beta1.EngineComponent, nil)
	if obs.Failed {
		t.Errorf("no Failed Instances should not flip Failed; got %+v", obs)
	}
}

func TestBuildComponentObservation_AnyFailedInstanceMarksComponentFailed(t *testing.T) {
	// One Instance in Phase=Failed — the rollup must mark the
	// Component Failed so the BlueGreen / Sequential / RollingUpdate
	// state machines transition the group to Failed.
	isvc := withInstanceStatus(v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
	})
	obs := buildComponentObservation(isvc, v1beta1.EngineComponent, nil)
	if !obs.Failed {
		t.Errorf("any Failed Instance must mark Component Failed; got %+v", obs)
	}
	if obs.FailureMessage == "" {
		t.Errorf("Failed Component must carry a FailureMessage; got empty")
	}
	if !contains(obs.FailureMessage, "1") {
		t.Errorf("FailureMessage should name the failed Instance index; got %q", obs.FailureMessage)
	}
}

func TestBuildComponentObservation_MultipleFailedInstancesListedInMessage(t *testing.T) {
	// Two Instances in Phase=Failed — message names both.
	isvc := withInstanceStatus(v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 2, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
	})
	obs := buildComponentObservation(isvc, v1beta1.EngineComponent, nil)
	if !obs.Failed {
		t.Errorf("two Failed Instances must mark Component Failed; got %+v", obs)
	}
	if !contains(obs.FailureMessage, "0") || !contains(obs.FailureMessage, "2") {
		t.Errorf("FailureMessage should name both failed indices (0, 2); got %q", obs.FailureMessage)
	}
}

func TestBuildComponentObservation_FailedPropagatesThroughBlueGreen(t *testing.T) {
	// End-to-end: a Failed Instance flows through buildComponentObservation
	// into the BlueGreen state machine, which returns Phase=Failed for
	// the group. This is the bug the X-1 KIND regression pins.
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test"},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {OMENative: &v1beta1.OMENativeComponentStatus{
					InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
						{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
					},
				}},
				v1beta1.DecoderComponent: {OMENative: &v1beta1.OMENativeComponentStatus{
					InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
						{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
					},
				}},
			},
		},
	}
	obs := buildGroupObservation(isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("BlueGreen with any Failed Component must report Failed; got %q", tr.Phase)
	}
}

func TestCollectFailedInstanceIndices_PreservesOrder(t *testing.T) {
	out := collectFailedInstanceIndices([]v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 2, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 3, Phase: v1beta1.OMENativeInstanceUpdating},
	})
	if len(out) != 2 {
		t.Fatalf("got %d failed indices want 2: %v", len(out), out)
	}
	// Slice order matches the input order (the InstanceStatuses
	// iteration order). This is intentional — the formatter doesn't
	// sort either, so operators see indices as they appear in status.
	if out[0] != 2 || out[1] != 1 {
		t.Errorf("collect order: got %v want [2 1] (slice order, not sorted)", out)
	}
}

func TestCollectFailedInstanceIndices_EmptyOnNoFailures(t *testing.T) {
	out := collectFailedInstanceIndices([]v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
	})
	if len(out) != 0 {
		t.Errorf("no failures should return nil/empty; got %v", out)
	}
}
