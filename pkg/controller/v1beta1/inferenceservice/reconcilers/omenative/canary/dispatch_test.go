package canary

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// ir builds an InferenceReplica whose UpdateRevision names the per-Component
// canary target (so canaryTargetHash resolves the right per-Component hash).
func ir(ns, isvc string, comp v1beta1.ComponentType, hash string) *v1beta1.InferenceReplica {
	r := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: isvc + "-" + string(comp),
	}}
	r.Status.UpdateRevision = isvc + "-" + string(comp) + "-" + hash
	return r
}

func TestSecondaryCapacityReady(t *testing.T) {
	ns := "default"
	n4 := 4
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	ctx := context.Background()

	// Single component (engine only) → no secondaries → always ready. The canary
	// group covers just [engine].
	engPlan := &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}
	eng := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "e1"}, Spec: v1beta1.InferenceServiceSpec{Rollout: engPlan}}
	eng.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	engRead := fake.NewClientBuilder().WithScheme(canaryScheme(t)).Build()
	if !secondaryCapacityReady(ctx, engRead, eng,
		map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: {"eng": 1}},
		map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: {"eng": 1}},
		v1beta1.EngineComponent) {
		t.Fatal("single-component canary has no secondaries → must be ready")
	}

	// PD: router primary + engine secondary; step 0 newCount = 50% of 4 = 2. The
	// canary group couples [router, engine]. Revision hashes are PER-Component:
	// router=rtr, engine=eng — distinct, as in production.
	pdPlan := &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}
	mkPD := func() *v1beta1.InferenceService {
		pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}, Spec: v1beta1.InferenceServiceSpec{Rollout: pdPlan}}
		pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
		pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
		return pd
	}
	// IRs name the per-Component canary targets; engine has a stable revision
	// "engOld" too, so a roll is genuinely in progress for the secondary.
	pdReads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		ir(ns, "pd", v1beta1.RouterComponent, "rtr"),
		ir(ns, "pd", v1beta1.EngineComponent, "eng"),
	).Build()

	below := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 1, "engOld": 3},
	}
	if secondaryCapacityReady(ctx, pdReads, mkPD(), below, below, v1beta1.RouterComponent) {
		t.Fatal("engine secondary below its step newCount → not ready")
	}
	at := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 2, "engOld": 2},
	}
	if !secondaryCapacityReady(ctx, pdReads, mkPD(), at, at, v1beta1.RouterComponent) {
		t.Fatal("engine secondary at its step newCount → ready")
	}
}

// TestSecondaryCapacityReady_PerComponentHashes reproduces the PD-canary stall:
// revision hashes are PER-Component, so gating a secondary on the PRIMARY's hash
// (the old behavior) found 0 secondary canary pods and blocked Pending forever
// even though the secondary's OWN canary capacity was fully up. The gate must
// resolve each secondary's own target hash.
func TestSecondaryCapacityReady_PerComponentHashes(t *testing.T) {
	ns := "default"
	n4 := 4
	ctx := context.Background()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p302"}}
	pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}

	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		ir(ns, "p302", v1beta1.RouterComponent, "53db7c07"),
		ir(ns, "p302", v1beta1.EngineComponent, "73e1335d"),
		ir(ns, "p302", v1beta1.DecoderComponent, "f8f90d6d"),
	).Build()

	// Every Component's OWN canary capacity is up (2 of 4 = step-0 50%), each on
	// its own per-Component hash, with the stable revision still present.
	ready := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"53db7c07": 2, "c6f33d7b": 2},
		v1beta1.EngineComponent:  {"73e1335d": 2, "engOld": 2},
		v1beta1.DecoderComponent: {"f8f90d6d": 2, "decOld": 2},
	}
	if !secondaryCapacityReady(ctx, reads, pd, ready, ready, v1beta1.RouterComponent) {
		t.Fatal("secondaries at their own per-Component canary capacity → must be ready (was stuck Pending on the primary's hash)")
	}

	// A secondary still short on its own canary capacity must still gate.
	short := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"53db7c07": 2, "c6f33d7b": 2},
		v1beta1.EngineComponent:  {"73e1335d": 1, "engOld": 3},
		v1beta1.DecoderComponent: {"f8f90d6d": 2, "decOld": 2},
	}
	if secondaryCapacityReady(ctx, reads, pd, short, short, v1beta1.RouterComponent) {
		t.Fatal("engine secondary below its own step newCount → not ready")
	}
}

// TestSecondaryCapacityReady_UnbumpedSecondary covers the primary-only-bumped PD
// canary (p302's real trigger): only the router template changed, so engine and
// decoder have no canary revision distinct from stable. Those secondaries have
// nothing to stage and must NOT block the rollout.
func TestSecondaryCapacityReady_UnbumpedSecondary(t *testing.T) {
	ns := "default"
	n4 := 4
	ctx := context.Background()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p302"}}
	pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}

	// Only the router was bumped: engine/decoder IRs name their stable revision as
	// the (only) target, and only that one revision has pods.
	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		ir(ns, "p302", v1beta1.RouterComponent, "53db7c07"),
		ir(ns, "p302", v1beta1.EngineComponent, "engStable"),
		ir(ns, "p302", v1beta1.DecoderComponent, "decStable"),
	).Build()

	pods := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"53db7c07": 2, "c6f33d7b": 2},
		v1beta1.EngineComponent:  {"engStable": 4},
		v1beta1.DecoderComponent: {"decStable": 4},
	}
	if !secondaryCapacityReady(ctx, reads, pd, pods, pods, v1beta1.RouterComponent) {
		t.Fatal("unbumped secondaries have no canary revision to stage → must be ready (was stuck Pending forever)")
	}
}

func canaryScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// canaryPod builds a Running + Ready (serving) OMENative pod. The canary capacity
// gate counts only READY pods, so fixtures that represent live capacity must be
// Ready — a pod that merely exists does not satisfy the gate.
func canaryPod(ns, isvc, comp, hash, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           comp,
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               hash,
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

// TestDispatch_ServicesHashAndRollbackWarning exercises the controller-side entry
// point: per-revision Service ensure, IR-sourced canary hash, and the deferred-
// rollback warning event.
func TestDispatch_ServicesHashAndRollbackWarning(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{
			Promotion: promPolicy(v1beta1.PromotionPolicyManual),
			Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
	}}}
	// IR names the canary target (its UpdateRevision) — Dispatch must source the
	// hash from here, not the (stale) ISVC aggregate.
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1-engine"}}
	ir.Status.UpdateRevision = "d1-engine-newhash"
	ir.Status.CurrentRevision = "d1-engine-stable0" // the stable revision rollback reverts to

	objs := []runtime.Object{isvc, ir,
		canaryPod(ns, "d1", "engine", "newhash", "d1-engine-2"),
		canaryPod(ns, "d1", "engine", "newhash", "d1-engine-3"),
		canaryPod(ns, "d1", "engine", "stable0", "d1-engine-0"),
		canaryPod(ns, "d1", "engine", "stable0", "d1-engine-1"),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()

	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc}); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}
	// IR-sourced hash recorded.
	if isvc.Status.Canary == nil || isvc.Status.Canary.CanaryRevisionHash != "newhash" {
		t.Fatalf("canary hash should come from the IR UpdateRevision (newhash), got %+v", isvc.Status.Canary)
	}
	// Per-revision Services created for both live revisions.
	for _, h := range []string{"newhash", "stable0"} {
		svc := &corev1.Service{}
		key := types.NamespacedName{Namespace: ns, Name: coordination.PerRevisionServiceName("d1", v1beta1.EngineComponent, h)}
		if err := c.Get(ctx, key, svc); err != nil {
			t.Fatalf("per-revision Service for %s should exist: %v", h, err)
		}
	}
	// Capacity met (2 new >= 50%% of 4) → Manual pause.
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("expected Paused (capacity met, Manual), got %q", got)
	}

	// Rollback: the executor records the rejected hash + Dispatch points the IR at
	// the stable revision (Pacing.RollbackToRevision = IR CurrentRevision).
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc}); err != nil {
		t.Fatalf("Dispatch (rollback) error: %v", err)
	}
	if isvc.Status.Canary == nil || isvc.Status.Canary.RolledBackRevisionHash != "newhash" {
		t.Fatalf("rollback should record the rejected hash (newhash), got %+v", isvc.Status.Canary)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("expected RollingBack (canary pods still present), got %q", got)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "d1-engine"}, gotIR); err != nil {
		t.Fatal(err)
	}
	if gotIR.Spec.Pacing == nil || gotIR.Spec.Pacing.RollbackToRevision == nil ||
		*gotIR.Spec.Pacing.RollbackToRevision != "d1-engine-stable0" {
		t.Fatalf("IR should be signaled to roll back to the stable revision, got pacing=%+v", gotIR.Spec.Pacing)
	}
}

// TestDispatch_EnsuresPerRevisionServicesForSecondary pins that a multi-Component
// canary ensures per-revision Services for EVERY group Component, not just the
// primary. coordination skips canary-owned Components, so the canary engine is
// their sole producer; ensuring only the primary's left the secondary's
// peer-discovery revision endpoint (OME_<SECONDARY>_REVISION_ENDPOINT) pointing
// at a Service nothing created.
func TestDispatch_EnsuresPerRevisionServicesForSecondary(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd1"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary: &v1beta1.GroupCanary{
			Promotion: promPolicy(v1beta1.PromotionPolicyManual),
			Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
	}}}
	objs := []runtime.Object{isvc,
		canaryPod(ns, "pd1", "engine", "newhash", "pd1-engine-0"),
		canaryPod(ns, "pd1", "decoder", "newhash", "pd1-decoder-0"),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc}); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}
	svc := &corev1.Service{}
	key := types.NamespacedName{Namespace: ns, Name: coordination.PerRevisionServiceName("pd1", v1beta1.DecoderComponent, "newhash")}
	if err := c.Get(ctx, key, svc); err != nil {
		t.Fatalf("per-revision Service for the decoder secondary should exist (else its peer revision endpoint dangles): %v", err)
	}
}

// TestPrimaryComponent pins the router>engine>decoder priority for the externally-
// routed Component that carries the canary's traffic weight. In v2 the primary is
// chosen among the canary GROUP's Components (not the raw Spec.* presence), so the
// fixture sets the group membership directly.
func TestPrimaryComponent(t *testing.T) {
	mk := func(comps ...v1beta1.ComponentType) *v1beta1.InferenceService {
		isvc := &v1beta1.InferenceService{}
		if len(comps) > 0 {
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: comps,
				Canary:     &v1beta1.GroupCanary{},
			}}}
		}
		return isvc
	}
	if got := primaryComponent(mk(v1beta1.EngineComponent)); got != v1beta1.EngineComponent {
		t.Fatalf("engine-only → engine, got %q", got)
	}
	if got := primaryComponent(mk(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent)); got != v1beta1.RouterComponent {
		t.Fatalf("PD with router → router, got %q", got)
	}
	if got := primaryComponent(mk(v1beta1.EngineComponent, v1beta1.DecoderComponent)); got != v1beta1.EngineComponent {
		t.Fatalf("engine+decoder (no router) → engine, got %q", got)
	}
	if got := primaryComponent(mk()); got != "" {
		t.Fatalf("no canary group → empty, got %q", got)
	}
}

func TestOtherRevision(t *testing.T) {
	if got := otherRevision(map[string]int32{"new": 2, "old": 3}, "new"); got != "old" {
		t.Fatalf("want old, got %q", got)
	}
	if got := otherRevision(map[string]int32{"new": 4}, "new"); got != "" {
		t.Fatalf("only canary present → empty, got %q", got)
	}
	if got := otherRevision(map[string]int32{"a": 1, "b": 5, "new": 2}, "new"); got != "b" {
		t.Fatalf("most-populated non-canary → b, got %q", got)
	}
}

func TestComponentReplicas(t *testing.T) {
	n := 4
	isvc := &v1beta1.InferenceService{}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 6}}
	if got := componentReplicas(isvc, v1beta1.EngineComponent); got != 4 {
		t.Fatalf("MinReplicas wins → 4, got %d", got)
	}
	isvc.Spec.Engine.MinReplicas = nil
	if got := componentReplicas(isvc, v1beta1.EngineComponent); got != 6 {
		t.Fatalf("fallback MaxReplicas → 6, got %d", got)
	}
	if got := componentReplicas(isvc, v1beta1.DecoderComponent); got != 0 {
		t.Fatalf("absent component → 0, got %d", got)
	}
}
