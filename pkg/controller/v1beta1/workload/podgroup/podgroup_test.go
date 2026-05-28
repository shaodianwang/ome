package podgroup

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// ---------------------------------------------------------------------
// IsMultiPodInstance — threshold (>1 total pods) classifies correctly.
// ---------------------------------------------------------------------

func TestIsMultiPodInstance(t *testing.T) {
	cases := []struct {
		name    string
		runners []core.RunnerPlan
		want    bool
	}{
		{"single-pod default", []core.RunnerPlan{{Name: "default", Size: 1}}, false},
		{"leader + zero workers", []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 0}}, false},
		{"leader + 1 worker", []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}, true},
		{"leader + 3 workers", []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 3}}, true},
		{"zero runners", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inst := core.InstancePlan{Index: 0, Runners: tc.runners}
			if got := IsMultiPodInstance(inst); got != tc.want {
				t.Errorf("IsMultiPodInstance: got %v want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// BuildPodGroup — pure-compute shape assertions.
// ---------------------------------------------------------------------

func TestBuildPodGroup_NilISVC(t *testing.T) {
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	if _, err := BuildPodGroup(nil, testPodGroupOwnerGVK, plan, inst); err == nil {
		t.Fatal("expected error for nil owner")
	}
}

func TestBuildPodGroup_SinglePodRejected(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "default", Size: 1}}}
	if _, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, plan, inst); err == nil {
		t.Fatal("expected single-pod build to error (caller must guard with IsMultiPodInstance)")
	}
}

// TestBuildPodGroup_Shape pins the canonical multi-pod PodGroup
// (leader + 3 workers, 15-minute timeout). One assertion per surface
// (name, namespace, labels, ownerRefs, spec) keeps diffs readable.
func TestBuildPodGroup_Shape(t *testing.T) {
	isvc := newPodGroupISVC("prod-models", "llama-engine")
	plan := core.ComponentPlan{
		Component:            v1beta1.EngineComponent,
		InstanceReadyTimeout: 15 * time.Minute,
	}
	inst := core.InstancePlan{
		Index: 0,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 3},
		},
	}

	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}

	// Name: matches query.PodGroupName.
	if pg.Name != "llama-engine-engine-0" {
		t.Errorf("Name: got %q want llama-engine-engine-0", pg.Name)
	}
	if pg.Namespace != "prod-models" {
		t.Errorf("Namespace: got %q want prod-models", pg.Namespace)
	}

	// MinMember: 1 leader + 3 workers = 4.
	if pg.Spec.MinMember != 4 {
		t.Errorf("MinMember: got %d want 4", pg.Spec.MinMember)
	}

	// ScheduleTimeoutSeconds: 15m = 900s, clamped down to 600.
	if pg.Spec.ScheduleTimeoutSeconds == nil {
		t.Fatal("ScheduleTimeoutSeconds: nil pointer")
	}
	if *pg.Spec.ScheduleTimeoutSeconds != 600 {
		t.Errorf("ScheduleTimeoutSeconds: got %d want 600 (clamped from 900)", *pg.Spec.ScheduleTimeoutSeconds)
	}

	// Labels: Component-scoped trio + instance-index for fan-out grep.
	wantLabels := map[string]string{
		"ome.io/inferenceservice": "llama-engine",
		"ome.io/component":        "engine",
		query.LabelManagedBy:      query.ManagedByOMENative,
		query.LabelInstanceIdx:    "0",
	}
	if diff := cmp.Diff(wantLabels, pg.Labels); diff != "" {
		t.Errorf("Labels (-want +got):\n%s", diff)
	}

	// OwnerReferences: single Controller-owner pointing at the ISVC.
	if len(pg.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(pg.OwnerReferences))
	}
	ref := pg.OwnerReferences[0]
	if ref.Name != "llama-engine" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-engine", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
	if ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Errorf("OwnerRef.BlockOwnerDeletion: want true, got %v", ref.BlockOwnerDeletion)
	}
}

// TestBuildPodGroup_TimeoutClamp covers the three clamp regions —
// zero (floor), in-range (passthrough), too-big (cap).
func TestBuildPodGroup_TimeoutClamp(t *testing.T) {
	cases := []struct {
		name    string
		in      time.Duration
		wantSec int32
	}{
		{"zero defaults to 60s floor", 0, minScheduleTimeoutSeconds},
		{"30s rounded up to 60s floor", 30 * time.Second, minScheduleTimeoutSeconds},
		{"3 minutes pass through", 3 * time.Minute, 180},
		{"10 minutes ceiling", 10 * time.Minute, maxScheduleTimeoutSeconds},
		{"30 minutes clamped to 10m ceiling", 30 * time.Minute, maxScheduleTimeoutSeconds},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := newPodGroupISVC("prod", "llama")
			plan := core.ComponentPlan{
				Component:            v1beta1.EngineComponent,
				InstanceReadyTimeout: tc.in,
			}
			inst := core.InstancePlan{
				Index: 0,
				Runners: []core.RunnerPlan{
					{Name: "leader", Size: 1},
					{Name: "worker", Size: 1},
				},
			}
			pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, plan, inst)
			if err != nil {
				t.Fatalf("BuildPodGroup: %v", err)
			}
			if pg.Spec.ScheduleTimeoutSeconds == nil {
				t.Fatal("ScheduleTimeoutSeconds: nil")
			}
			if *pg.Spec.ScheduleTimeoutSeconds != tc.wantSec {
				t.Errorf("ScheduleTimeoutSeconds: got %d want %d (in=%v)",
					*pg.Spec.ScheduleTimeoutSeconds, tc.wantSec, tc.in)
			}
		})
	}
}

// TestBuildPodGroup_HighWorkerCount confirms MinMember scales linearly
// with worker count — guards against an off-by-one in the sum.
func TestBuildPodGroup_HighWorkerCount(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{
		Index: 2,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 7}, // tensor-parallel-8 setup
		},
	}
	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, plan, inst)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	if pg.Spec.MinMember != 8 {
		t.Errorf("MinMember: got %d want 8 (1 leader + 7 workers)", pg.Spec.MinMember)
	}
	if pg.Name != "llama-engine-2" {
		t.Errorf("Name: got %q want llama-engine-2 (instance-2)", pg.Name)
	}
	if pg.Labels[query.LabelInstanceIdx] != "2" {
		t.Errorf("LabelInstanceIdx: got %q want 2", pg.Labels[query.LabelInstanceIdx])
	}
}

// ---------------------------------------------------------------------
// EnsurePodGroup — controller-side reconcile via fake client.
// ---------------------------------------------------------------------

func TestEnsurePodGroup_SinglePodNoOp(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "default", Size: 1}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}

	// Assert nothing was created.
	pg := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg)
	if err == nil {
		t.Errorf("expected single-pod Instance to NOT create a PodGroup, found %s/%s", pg.Namespace, pg.Name)
	} else if !apierrors.IsNotFound(err) {
		t.Errorf("unexpected error type: %v", err)
	}
}

func TestEnsurePodGroup_CreatesWhenAbsent(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 3}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}

	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get PodGroup after ensure: %v", err)
	}
	if got.Spec.MinMember != 4 {
		t.Errorf("MinMember: got %d want 4", got.Spec.MinMember)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != "llama" {
		t.Errorf("OwnerReferences: got %#v want one pointing at the ISVC", got.OwnerReferences)
	}
}

// TestEnsurePodGroup_DriftCorrected — pre-existing PodGroup with stale
// MinMember / ScheduleTimeoutSeconds / Labels gets reconciled to the
// desired shape on the next ensure.
func TestEnsurePodGroup_DriftCorrected(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	stale := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine-0",
			Namespace: "prod",
			Labels:    map[string]string{"stale": "true"},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              99, // stale
			ScheduleTimeoutSeconds: int32Ptr(9999),
		},
	}
	c := newPodGroupClient(t, isvc, stale)
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent, InstanceReadyTimeout: 3 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup: %v", err)
	}

	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get PodGroup after ensure: %v", err)
	}
	if got.Spec.MinMember != 2 {
		t.Errorf("MinMember: got %d want 2 (drift not corrected)", got.Spec.MinMember)
	}
	if got.Spec.ScheduleTimeoutSeconds == nil || *got.Spec.ScheduleTimeoutSeconds != 180 {
		t.Errorf("ScheduleTimeoutSeconds: want 180 (3m), got %v", got.Spec.ScheduleTimeoutSeconds)
	}
	if got.Labels["ome.io/component"] != "engine" {
		t.Errorf("Labels: want engine label set, got %#v", got.Labels)
	}
	if _, hasStale := got.Labels["stale"]; hasStale {
		t.Errorf("Labels: stale label not overwritten, got %#v", got.Labels)
	}
}

// TestEnsurePodGroup_Idempotent — two ensures back-to-back leave
// MinMember stable and don't error.
func TestEnsurePodGroup_Idempotent(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	for i := 0; i < 2; i++ {
		if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, plan, inst); err != nil {
			t.Fatalf("EnsurePodGroup pass %d: %v", i, err)
		}
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get PodGroup: %v", err)
	}
	if got.Spec.MinMember != 3 {
		t.Errorf("MinMember after idempotent ensure: got %d want 3", got.Spec.MinMember)
	}
}

// ---------------------------------------------------------------------
// DeletePodGroup — explicit removal path (scale-down / migration).
// ---------------------------------------------------------------------

func TestDeletePodGroup_Removes(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-0", Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	c := newPodGroupClient(t, isvc, existing)

	if err := DeletePodGroup(context.Background(), c, isvc, v1beta1.EngineComponent, 0); err != nil {
		t.Fatalf("DeletePodGroup: %v", err)
	}
	got := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound after delete, got err=%v pg=%+v", err, got)
	}
}

func TestDeletePodGroup_AbsentIsNoOp(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	c := newPodGroupClient(t, isvc)
	if err := DeletePodGroup(context.Background(), c, isvc, v1beta1.EngineComponent, 7); err != nil {
		t.Errorf("DeletePodGroup on absent PodGroup should be no-op, got %v", err)
	}
}

// ---------------------------------------------------------------------
// Helpers — local fixtures (intentionally not shared with the parent
// package; podgroup is leaf-level and shouldn't import test_helpers).
// ---------------------------------------------------------------------

// testPodGroupOwnerGVK is the GroupVersionKind every test passes
// alongside the ISVC fixture so the OwnerReference matches what the
// ISVC adapter populates at the dispatch site. Mirrors the gang
// reconciler's wiring of internalsource.ISVCGVK() without taking on
// that import here.
var testPodGroupOwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

func newPodGroupISVC(ns, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
	}
}

func newPodGroupClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1 to scheme: %v", err)
	}
	if err := schedulingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheduling/v1alpha1 to scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func int32Ptr(n int32) *int32 { return &n }
