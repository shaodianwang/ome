package coordination

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"
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
	isvc.Spec.Rollout = singleEngineGroup()
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
	// data plane for HTTPRoute weighted-backendRef consumers.
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

// steadyEngineStatus seeds the engine Component as fully settled onto a
// single revision with a converged 100% Traffic[0] target.
func steadyEngineStatus(isvc *v1beta1.InferenceService, hash string) {
	rev := isvc.Name + "-engine-" + hash
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				CurrentRevision: rev,
				UpdateRevision:  rev,
			},
			Traffic: []v1beta1.ComponentTrafficTarget{{
				RevisionName:   PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, hash),
				Percent:        100,
				LatestRevision: true,
			}},
		},
	}
}

// TestReconcile_SkipsGCWhenRevisionSetConverged pins the change-driven
// perf contract: when the live revision-hash set already matches the set
// recorded on Status.Components.<c>.Traffic, the heavier GC orphan sweep
// (a namespace Service LIST) is skipped — a prior reconcile already GC'd
// every orphan for that set. Ensure, by contrast, still runs (self-heal,
// see TestReconcile_RecreatesDeletedServiceWhenConverged); its Gets are
// cache-served and cheap. The cached-read switch is orthogonal: pods are
// still observed via Client every pass.
func TestReconcile_SkipsGCWhenRevisionSetConverged(t *testing.T) {
	isvc := testOMENativeISVC()
	steadyEngineStatus(isvc, "hash1")
	// Live pod whose hash matches the recorded Traffic set -> converged.
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := &listCountingClient{Client: testClient(pods...)}
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Ensure still runs every reconcile (self-heal); the converged set has
	// one live hash, so exactly that hash is ensured.
	if r.PerRevisionServicesEnsured != 1 {
		t.Errorf("converged revision set: ensure must still run (self-heal), got %d ensured want 1", r.PerRevisionServicesEnsured)
	}
	// The GC orphan sweep (Service LIST) must be skipped when converged.
	if c.serviceLists != 0 {
		t.Errorf("converged revision set: GC Service LIST must be skipped, got %d LIST calls", c.serviceLists)
	}
}

// TestReconcile_RecreatesDeletedServiceWhenConverged is the regression
// test for the self-heal bug: a per-revision Service deleted out-of-band
// must be recreated on the next reconcile even when the live revision-hash
// set is unchanged (converged). Pre-fix, ensure was gated on convergence,
// so the deleted Service stayed gone and the HTTPRoute routed to a dead
// backend.
func TestReconcile_RecreatesDeletedServiceWhenConverged(t *testing.T) {
	isvc := testOMENativeISVC()
	steadyEngineStatus(isvc, "hash1")
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	ctx := context.Background()

	// First reconcile creates the per-revision Services for hash1.
	if _, err := Reconcile(ctx, ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: time.Now()}); err != nil {
		t.Fatalf("first reconcile: unexpected error: %v", err)
	}
	routingKey := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(ctx, routingKey, &corev1.Service{}); err != nil {
		t.Fatalf("setup: routing Service should exist after first reconcile: %v", err)
	}

	// Delete the routing Service out-of-band (e.g. operator fat-fingered a
	// kubectl delete) while the revision set stays converged.
	if err := c.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: routingKey.Namespace, Name: routingKey.Name}}); err != nil {
		t.Fatalf("delete out-of-band: %v", err)
	}
	if err := c.Get(ctx, routingKey, &corev1.Service{}); err == nil {
		t.Fatalf("setup: routing Service should be gone after delete")
	}

	// Second reconcile: revision set is unchanged (converged), but the
	// deleted Service must be recreated.
	if _, err := Reconcile(ctx, ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: time.Now()}); err != nil {
		t.Fatalf("second reconcile: unexpected error: %v", err)
	}
	if err := c.Get(ctx, routingKey, &corev1.Service{}); err != nil {
		t.Errorf("converged revision set: deleted per-revision Service must be recreated, got: %v", err)
	}
}

// TestReconcile_EnsuresServicesWhenRevisionSetDiverges pins the other
// branch of the change-detector: when the live pod-hash set differs from
// the recorded Traffic set (here no Traffic is recorded yet but a live pod
// exists), the full ensure path runs and creates the per-revision Service.
func TestReconcile_EnsuresServicesWhenRevisionSetDiverges(t *testing.T) {
	isvc := testOMENativeISVC()
	// No recorded Traffic: recorded set is empty, live set is {hash1}; they
	// diverge, so the ensure path must run and create the Service.
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
		t.Errorf("diverged revision set: per-revision Services must be ensured, got %d", r.PerRevisionServicesEnsured)
	}
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Errorf("diverged revision set: per-revision Service should be created, got: %v", err)
	}
}

// TestReconcile_ObservesPodsViaCachedClient confirms the coordination
// layer reads per-revision pods through the cached Client, not the live
// Reader: a nil Reader must not be dereferenced. The full observe / ensure
// path runs entirely off Client.
func TestReconcile_ObservesPodsViaCachedClient(t *testing.T) {
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	// Reader deliberately nil: if coordination still read pods via Reader
	// this would panic. The cached Client must carry the observation.
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: nil, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("cached-read path: pods observed via Client must drive Service ensure, got %d", r.PerRevisionServicesEnsured)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs.Traffic) == 0 {
		t.Errorf("cached-read path: Traffic[] must populate from pods read via Client, got %+v", cs)
	}
}

func TestReconcile_WritesTrafficStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
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
func TestReconcile_DoesNotClobberCanaryTraffic(t *testing.T) {
	isvc := testOMENativeISVC()
	// Canary owns the engine; canary.Dispatch is the authoritative traffic
	// producer for it (it sets the explicit step weight, decoupled from pod
	// count). coordination.Reconcile runs unconditionally in v2 — it must NOT
	// recompute the engine's traffic from pod proportions and clobber that weight.
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 10},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			},
		},
	}
	// Canary set an explicit 10/90 split (the step weight), independent of pods.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Traffic: []v1beta1.ComponentTrafficTarget{
			{RevisionName: "llama-engine-rev-canaryhash", Percent: 10},
			{RevisionName: "llama-engine-rev-stablehash", Percent: 90},
		}},
	}
	// Pods are 50/50 across the two revisions, so coordination's pod-proportional
	// recompute (50/50) differs from the canary's 10/90 and would clobber it.
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "canaryhash", 0),
		buildPod(isvc, v1beta1.EngineComponent, "stablehash", 1),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: time.Now()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	for _, target := range got {
		var want int32
		switch target.RevisionName {
		case "llama-engine-rev-canaryhash":
			want = 10
		case "llama-engine-rev-stablehash":
			want = 90
		default:
			t.Fatalf("coordination clobbered canary traffic with revision %q", target.RevisionName)
		}
		if target.Percent != want {
			t.Errorf("%s: got %d%% want %d%% — coordination clobbered the canary step weight", target.RevisionName, target.Percent, want)
		}
	}
}

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
	// Seed Components[c].Lifecycle.UpdateRevision so latestRevisionHash
	// recovers the per-Component hash (matches the steady-state shape
	// AggregateIRStatus writes after the IR controller commits Status).
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				UpdateRevision: isvc.Name + "-engine-engineHash",
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				UpdateRevision: isvc.Name + "-decoder-decoderHash",
			},
		},
		v1beta1.RouterComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
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
	cs1.Lifecycle = &v1beta1.LifecycleStatus{
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

// singleEngineGroup returns a minimal RolloutSpec that opts the engine
// Component into a rollout group. (v1's Independent policy is gone in v2;
// a lone single-Component blueGreen group is the minimal opt-in that
// keeps the per-revision data plane + group status writing exercised.)
func singleEngineGroup() *v1beta1.RolloutSpec {
	return &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
		},
	}
}

func TestReconcile_WritesGroupStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
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
	// A rollingUpdate group with maxSurge=0 AND maxUnavailable=0 is shape-invalid:
	// no surge headroom and no drain headroom, so the roll can never make
	// progress. The webhook rejects it in production; ValidateGroupShape is the
	// runtime safety net the reconciler runs in case an object bypassed admission.
	// (Single-Component rollingUpdate is NOT invalid in v2 — it's a valid
	// progression; only the deadlock-budget shape is rejected here. v1's
	// "Sequential without order" is likewise no longer expressible: Sequential is
	// a run of single-Component blueGreen groups the controller collapses with a
	// derived Order, so it can never be order-less.)
	zero := intstr.FromInt(0)
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &zero,
					MaxUnavailable: &zero,
				},
			},
		},
	}
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	})
	if err == nil {
		t.Errorf("zero-budget pacing: want error")
	}
}

func TestReconcile_ClearsStaleCoordinationStatusWhenGroupsGone(t *testing.T) {
	isvc := testOMENativeISVC()
	// Pre-seed status as if a prior blueGreen rollout ran and is mid-flight.
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{
			{Name: "0", Policy: v1beta1.CoordinationPolicyBlueGreen, Phase: v1beta1.CoordinationPhaseSurging},
		},
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady), &apis.Condition{
		Type:   apis.ConditionType(v1beta1.RolloutCoordinationReady),
		Status: corev1.ConditionFalse,
		Reason: "InProgress",
	})
	// The ISVC now has NO coordination-style group (switched to canary-only).
	// coordination.Reconcile still runs (unconditional in v2) but ResolveGroups
	// returns empty — it must reconcile the stale status away, not leave it.
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}},
			},
		},
	}
	c := testClient()
	if _, err := Reconcile(context.Background(), ReconcileInputs{ISVC: isvc, Client: c, Reader: c, Now: time.Now()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isvc.Status.RolloutCoordination != nil {
		t.Errorf("stale RolloutCoordination not cleared: %+v", isvc.Status.RolloutCoordination)
	}
	cond := isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady))
	if cond == nil || cond.Status != corev1.ConditionTrue {
		t.Errorf("RolloutCoordinationReady should be resolved to True, got %+v", cond)
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
	// Test pod names — simplified pattern for test fixtures.
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

// listCountingClient wraps a client.Client and counts Service LIST calls so
// a test can assert the GC orphan sweep (the heavier namespace Service LIST)
// is skipped when the revision set is converged, while ensure still runs.
type listCountingClient struct {
	client.Client
	serviceLists int
}

func (c *listCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.ServiceList); ok {
		c.serviceLists++
	}
	return c.Client.List(ctx, list, opts...)
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
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
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
// view on Components.<c>.OMENative. Used by the propagation tests.
func withInstanceStatus(c v1beta1.ComponentType, instances []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test"},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				c: {Lifecycle: &v1beta1.LifecycleStatus{
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
	// the group. This is the bug the bad_image_kind KIND regression pins.
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-test"},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{
					InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
						{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
					},
				}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{
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

// TestBuildRatioState_DefersSnapshotWhenStatusEmpty pins the fix for
// the BlueGreen+RatioBalanced deadlock. The first reconcile of a
// freshly-created ISVC observes every Component with
// status.OMENative.Replicas == 0 (the OMENative status writer hasn't
// run yet). Snapshotting from that empty status anchored Original to
// {Engine: 1, Decoder: 1} via SnapshotOriginal's zero-clamp, deadlocking
// rollouts whose live ratio disagreed with 1:1.
//
// The fix defers the snapshot until every Component has non-zero
// observed Replicas. This test pins that contract: empty status →
// empty Original; populated status → {eng: 4, dec: 2} matches the
// cluster shape.
func TestBuildRatioState_DefersSnapshotWhenStatusEmpty(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}

	// Status not yet populated — DesiredReplicas==0 for both Components.
	// buildComponentObservation returns zero-valued observations when
	// cs.OMENative is missing.
	isvcEmpty := &v1beta1.InferenceService{}
	obsEmpty := buildGroupObservation(isvcEmpty, g, map[v1beta1.ComponentType]map[string]int32{})
	stateEmpty := buildRatioState(isvcEmpty, g, obsEmpty, map[v1beta1.ComponentType]map[string]int32{})

	if len(stateEmpty.Original) != 0 {
		t.Fatalf("empty status MUST defer snapshot to avoid the SnapshotOriginal zero-clamp anchoring Original to {1, 1}; got Original=%v", stateEmpty.Original)
	}

	// Once status carries real Replicas counts, the snapshot should
	// reflect the cluster shape verbatim.
	isvcPopulated := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{Replicas: 2},
				},
			},
		},
	}
	obsPopulated := buildGroupObservation(isvcPopulated, g, map[v1beta1.ComponentType]map[string]int32{})
	statePopulated := buildRatioState(isvcPopulated, g, obsPopulated, map[v1beta1.ComponentType]map[string]int32{})

	if statePopulated.Original[v1beta1.EngineComponent] != 4 {
		t.Errorf("Original engine: got %d want 4 (matches status.OMENative.Replicas)", statePopulated.Original[v1beta1.EngineComponent])
	}
	if statePopulated.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("Original decoder: got %d want 2 (matches status.OMENative.Replicas)", statePopulated.Original[v1beta1.DecoderComponent])
	}
}

// TestBuildRatioState_PartiallyPopulatedStatusDefersSnapshot covers the
// race where one Component has been reconciled (status.OMENative.Replicas
// populated) but the other hasn't yet. The snapshot anchor must reflect
// the WHOLE group's shape, so until every Component reports non-zero
// Replicas, defer. Otherwise the in-flight Component pegs the ratio
// against a still-zero peer and immediately rejects every surge.
func TestBuildRatioState_PartiallyPopulatedStatusDefersSnapshot(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	// Engine populated; decoder not yet.
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4},
				},
			},
		},
	}
	obs := buildGroupObservation(isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if len(state.Original) != 0 {
		t.Fatalf("partially populated status MUST defer snapshot; got Original=%v", state.Original)
	}
}

// TestBuildRatioState_PreservesAlreadyRecordedAnchor pins the
// already-recorded path: when ObservedRatio.Original is already
// persisted from a prior reconcile, buildRatioState reads it as-is
// (verbatim) and does NOT re-snapshot — the anchor must remain stable
// across the rollout even if MinReplicas changes mid-flight.
func TestBuildRatioState_PreservesAlreadyRecordedAnchor(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
		Pacing: v1beta1.CoordinationPacing{
			Type: v1beta1.CoordinationPacingRatioBalanced,
		},
	}
	// Status: scaled DOWN to 2 engine + 1 decoder mid-rollout.
	// ObservedRatio.Original captured at rollout start = 4:2 (the
	// anchor we want to preserve, NOT the current shape).
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{Replicas: 2},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{Replicas: 1},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name: "0",
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}
	obs := buildGroupObservation(isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[v1beta1.EngineComponent] != 4 {
		t.Errorf("anchor preservation: engine Original got %d want 4 (the snapshot from rollout start, NOT the current 2)", state.Original[v1beta1.EngineComponent])
	}
	if state.Original[v1beta1.DecoderComponent] != 2 {
		t.Errorf("anchor preservation: decoder Original got %d want 2 (the snapshot from rollout start, NOT the current 1)", state.Original[v1beta1.DecoderComponent])
	}
}

// TestPodReadyAndServing covers the serving-readiness predicate the canary
// capacity gate relies on: only Running, non-deleting pods reporting
// PodReady=True count as serving.
func TestPodReadyAndServing(t *testing.T) {
	condTrue, condFalse := corev1.ConditionTrue, corev1.ConditionFalse
	mk := func(phase corev1.PodPhase, cond *corev1.ConditionStatus, deleting bool) *corev1.Pod {
		p := &corev1.Pod{}
		p.Status.Phase = phase
		if cond != nil {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: *cond}}
		}
		if deleting {
			p.DeletionTimestamp = &metav1.Time{}
		}
		return p
	}
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"running + ready", mk(corev1.PodRunning, &condTrue, false), true},
		{"running + ready=false", mk(corev1.PodRunning, &condFalse, false), false},
		{"running + no ready condition", mk(corev1.PodRunning, nil, false), false},
		{"pending (ContainerCreating)", mk(corev1.PodPending, nil, false), false},
		{"running + ready but terminating", mk(corev1.PodRunning, &condTrue, true), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podReadyAndServing(tc.pod); got != tc.want {
				t.Errorf("podReadyAndServing(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestObservePerRevisionPods_TotalVsReady is the regression guard for the canary
// premature-cutover bug: total counts every pod carrying a revision hash, but the
// READY map excludes pods that merely exist (ContainerCreating/Pending). The
// canary capacity gate consumes the ready map, so it never shifts traffic / cuts
// over before the new revision is actually serving.
func TestObservePerRevisionPods_TotalVsReady(t *testing.T) {
	isvc := testReconcilerISVC()
	const stable, canary = "stablehash", "canaryhash"
	objs := []runtime.Object{
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 0, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 1, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 2, true),  // serving
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 3, false), // exists, not serving yet
	}
	total, ready, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, []v1beta1.ComponentType{v1beta1.EngineComponent}, true)
	if err != nil {
		t.Fatalf("observePerRevisionPods: %v", err)
	}
	eng := v1beta1.EngineComponent
	if total[eng][canary] != 2 {
		t.Errorf("total[canary] = %d, want 2 (both canary pods exist)", total[eng][canary])
	}
	if ready[eng][canary] != 1 {
		t.Errorf("ready[canary] = %d, want 1 (the not-yet-serving pod must be excluded)", ready[eng][canary])
	}
	if total[eng][stable] != 2 || ready[eng][stable] != 2 {
		t.Errorf("stable total/ready = %d/%d, want 2/2", total[eng][stable], ready[eng][stable])
	}
}

// TestObservePerRevisionPods_IndexParityWithLabelPath asserts the indexed read
// strategy (useIndex=true) buckets the EXACT same pods, by revision hash, as the
// label-selector strategy (useIndex=false) — including a pod with a missing
// revision-hash label, which must be dropped from both maps. The fake client has
// no field index, so useIndex=true exercises ListOMENativePodsByName's index
// fallback; this guards that the index path stays bucket-for-bucket identical to
// the old raw label List.
func TestObservePerRevisionPods_IndexParityWithLabelPath(t *testing.T) {
	isvc := testReconcilerISVC()
	const stable, canary = "stablehash", "canaryhash"
	objs := []runtime.Object{
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 0, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 1, false),
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 2, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, "", 3, true), // missing hash → dropped
	}
	comps := []v1beta1.ComponentType{v1beta1.EngineComponent}

	totalIdx, readyIdx, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, comps, true)
	if err != nil {
		t.Fatalf("observePerRevisionPods(useIndex=true): %v", err)
	}
	totalLbl, readyLbl, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, comps, false)
	if err != nil {
		t.Fatalf("observePerRevisionPods(useIndex=false): %v", err)
	}

	if !reflect.DeepEqual(totalIdx, totalLbl) {
		t.Errorf("total maps differ: index=%v label=%v", totalIdx, totalLbl)
	}
	if !reflect.DeepEqual(readyIdx, readyLbl) {
		t.Errorf("ready maps differ: index=%v label=%v", readyIdx, readyLbl)
	}
	eng := v1beta1.EngineComponent
	// Empty-hash pod must not appear under any bucket.
	if _, ok := totalIdx[eng][""]; ok {
		t.Errorf("empty-hash pod leaked into total bucket: %v", totalIdx[eng])
	}
	if totalIdx[eng][stable] != 2 || totalIdx[eng][canary] != 1 {
		t.Errorf("total buckets = %v, want stable=2 canary=1", totalIdx[eng])
	}
}

// buildReadyPod is buildPod with a serving status set (serving=true → Running +
// PodReady=True) or a not-yet-serving status (serving=false → Pending), for the
// total-vs-ready accounting test.
func buildReadyPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int, serving bool) runtime.Object {
	p := buildPod(isvc, component, revisionHash, idx).(*corev1.Pod)
	if serving {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	} else {
		p.Status.Phase = corev1.PodPending
	}
	return p
}
