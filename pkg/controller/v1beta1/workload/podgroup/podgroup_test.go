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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
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
	plan := core.ComponentPlan{Component: workload.ComponentEngine}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}
	if _, err := BuildPodGroup(nil, testPodGroupOwnerGVK, "", plan, inst); err == nil {
		t.Fatal("expected error for nil owner")
	}
}

func TestBuildPodGroup_SinglePodRejected(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	plan := core.ComponentPlan{Component: workload.ComponentEngine}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "default", Size: 1}}}
	if _, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err == nil {
		t.Fatal("expected single-pod build to error (caller must guard with IsMultiPodInstance)")
	}
}

// TestBuildPodGroup_NameUsesOwnerNameNotOwnerObject pins the IR-path naming
// fix: the PodGroup NAME must follow ownerName (the workload owner name =
// Key.OwnerName, what the pod renderer uses), NOT owner.GetName(). On the
// IR-managed path the owner OBJECT is the InferenceReplica ("p66-engine"),
// but the pods' pod-group label is "p66-engine-0" (ISVC "p66"). Using
// owner.GetName() would emit "p66-engine-engine-0" → the gang scheduler's
// "PodGroup not found". The OwnerReference must still target the owner object
// (the IR) for GC.
func TestBuildPodGroup_NameUsesOwnerNameNotOwnerObject(t *testing.T) {
	irOwner := newPodGroupISVC("sandbox", "p66-engine") // owner object named like an IR: "<isvc>-<component>"
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 15 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}

	pg, err := BuildPodGroup(irOwner, testPodGroupOwnerGVK, "p66", plan, inst) // ownerName = ISVC name (Key.OwnerName)
	if err != nil {
		t.Fatalf("BuildPodGroup: %v", err)
	}
	if pg.Name != "p66-engine-0" {
		t.Errorf("Name: got %q want p66-engine-0 — must follow ownerName, not owner.GetName()=%q (which would give p66-engine-engine-0)", pg.Name, irOwner.GetName())
	}
	if len(pg.OwnerReferences) != 1 || pg.OwnerReferences[0].Name != "p66-engine" {
		t.Errorf("OwnerReference must still target the owner object (IR p66-engine) for GC; got %+v", pg.OwnerReferences)
	}
}

// TestBuildPodGroup_Shape pins the canonical multi-pod PodGroup
// (leader + 3 workers, 15-minute timeout). One assertion per surface
// (name, namespace, labels, ownerRefs, spec) keeps diffs readable.
func TestBuildPodGroup_Shape(t *testing.T) {
	isvc := newPodGroupISVC("prod-models", "llama-engine")
	plan := core.ComponentPlan{
		Component:            workload.ComponentEngine,
		InstanceReadyTimeout: 15 * time.Minute,
	}
	inst := core.InstancePlan{
		Index: 0,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 3},
		},
	}

	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
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

// TestBuildPodGroup_TopologyKeyAnnotation verifies the gang topology-domain
// key is stamped as the ome.io/topology-key annotation the OME gang scheduler
// plugin (OMEGangPack) reads to pin the gang to one free partition. It is set
// when the ComponentPlan declares a TopologyKey and absent (nil map) when it
// doesn't, so non-topology gangs don't churn on an empty-string annotation.
func TestBuildPodGroup_TopologyKeyAnnotation(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	inst := core.InstancePlan{
		Index: 0,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 1},
		},
	}

	t.Run("declared topology key is stamped", func(t *testing.T) {
		plan := core.ComponentPlan{
			Component:            workload.ComponentEngine,
			InstanceReadyTimeout: 5 * time.Minute,
			TopologyKey:          "cloud.google.com/gke-tpu-partition-2x2x2-id",
		}
		pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
		if err != nil {
			t.Fatalf("BuildPodGroup: %v", err)
		}
		if got := pg.Annotations[query.AnnotationTopologyKey]; got != "cloud.google.com/gke-tpu-partition-2x2x2-id" {
			t.Errorf("annotation %s: got %q want cloud.google.com/gke-tpu-partition-2x2x2-id",
				query.AnnotationTopologyKey, got)
		}
	})

	t.Run("no topology key means no annotations", func(t *testing.T) {
		plan := core.ComponentPlan{
			Component:            workload.ComponentEngine,
			InstanceReadyTimeout: 5 * time.Minute,
			// TopologyKey deliberately unset.
		}
		pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
		if err != nil {
			t.Fatalf("BuildPodGroup: %v", err)
		}
		if pg.Annotations != nil {
			t.Errorf("annotations: got %v want nil (no topology key declared)", pg.Annotations)
		}
	})
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
				Component:            workload.ComponentEngine,
				InstanceReadyTimeout: tc.in,
			}
			inst := core.InstancePlan{
				Index: 0,
				Runners: []core.RunnerPlan{
					{Name: "leader", Size: 1},
					{Name: "worker", Size: 1},
				},
			}
			pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
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
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{
		Index: 2,
		Runners: []core.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 7}, // tensor-parallel-8 setup
		},
	}
	pg, err := BuildPodGroup(isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst)
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
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "default", Size: 1}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
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
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 3}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
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
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 3 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}

	if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
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
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	for i := 0; i < 2; i++ {
		if err := EnsurePodGroup(context.Background(), c, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
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

// TestEnsurePodGroup_FastPathReadsBeforeWriteAndSkipsNoDrift pins the
// steady-state fast path: EnsurePodGroup must do its own cache Get up
// front and, on the no-drift re-ensure, return after that Get without
// touching CreateOrUpdate (no Create/Update/Patch).
//
// A ResourceVersion / counting-Update-only assertion is NOT enough to
// guard the fast path: the fallback CreateOrUpdate is itself a no-op on
// a matching object (Get -> DeepEqual match -> OperationResultNone, no
// Update), so it never bumps RV or issues an Update either. The signal
// that the fast path actually runs is the extra pre-check Get on the
// write path: with the fast path, the create-from-absent ensure issues
// TWO Gets (the fast-path probe that returns NotFound, then
// CreateOrUpdate's own Get); without it, only CreateOrUpdate's single
// Get. Reverting gang.go/podgroup.go collapses that to one Get and
// fails this test.
func TestEnsurePodGroup_FastPathReadsBeforeWriteAndSkipsNoDrift(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	cc := newCountingClient(newPodGroupClient(t, isvc))
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	// First ensure: object absent -> must be created. The fast path runs
	// its NotFound probe Get BEFORE handing off to CreateOrUpdate, so we
	// expect 2 Gets and exactly 1 Create. Reverted code does a single
	// Get (CreateOrUpdate's) -> getsCreate == 1, which fails the check
	// below.
	cc.reset()
	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup (create): %v", err)
	}
	if cc.creates != 1 {
		t.Errorf("create ensure: Create count = %d, want 1", cc.creates)
	}
	if cc.gets != 2 {
		t.Errorf("create ensure: Get count = %d, want 2 (fast-path pre-check probe + CreateOrUpdate's Get). "+
			"A count of 1 means the fast-path Get was removed — fast path reverted.", cc.gets)
	}

	// Second ensure: object now matches desired -> the fast path must
	// short-circuit after a single Get with NO write.
	cc.reset()
	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup (no-drift): %v", err)
	}
	if cc.creates != 0 || cc.updates != 0 || cc.patches != 0 {
		t.Errorf("no-drift ensure issued a write: creates=%d updates=%d patches=%d, want all 0",
			cc.creates, cc.updates, cc.patches)
	}
	if cc.gets != 1 {
		t.Errorf("no-drift ensure: Get count = %d, want 1 (fast-path probe only, no CreateOrUpdate)", cc.gets)
	}

	// Sanity: the PodGroup that survives the no-drift pass still carries
	// the reconciled shape.
	got := &schedulingv1alpha1.PodGroup{}
	if err := cc.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, got); err != nil {
		t.Fatalf("get after no-drift ensure: %v", err)
	}
	if got.Spec.MinMember != 3 {
		t.Errorf("MinMember after no-drift ensure: got %d want 3", got.Spec.MinMember)
	}
}

// TestEnsurePodGroup_DriftStillWritesViaCreateOrUpdate is the
// counterpart to the fast-path skip: when the cached object drifts from
// desired, podGroupMatches must return false and EnsurePodGroup must
// fall through to CreateOrUpdate and issue exactly one write. Pins that
// the fast path never swallows a real drift.
func TestEnsurePodGroup_DriftStillWritesViaCreateOrUpdate(t *testing.T) {
	isvc := newPodGroupISVC("prod", "llama")
	stale := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine-0",
			Namespace: "prod",
			Labels:    map[string]string{"stale": "true"},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              99, // drift vs. desired (3)
			ScheduleTimeoutSeconds: int32Ptr(9999),
		},
	}
	cc := newCountingClient(newPodGroupClient(t, isvc, stale))
	plan := core.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: 5 * time.Minute}
	inst := core.InstancePlan{Index: 0, Runners: []core.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 2}}}

	cc.reset()
	if err := EnsurePodGroup(context.Background(), cc, isvc, testPodGroupOwnerGVK, isvc.GetName(), plan, inst); err != nil {
		t.Fatalf("EnsurePodGroup (drift): %v", err)
	}
	if cc.updates+cc.patches+cc.creates == 0 {
		t.Errorf("drift ensure issued no write (creates=%d updates=%d patches=%d): fast path swallowed a real drift",
			cc.creates, cc.updates, cc.patches)
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

	if err := DeletePodGroup(context.Background(), c, isvc, isvc.GetName(), workload.ComponentEngine, 0); err != nil {
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
	if err := DeletePodGroup(context.Background(), c, isvc, isvc.GetName(), workload.ComponentEngine, 7); err != nil {
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

// countingClient wraps a client.Client (via an interceptor) and tallies
// the Get / Create / Update / Patch calls EnsurePodGroup makes, so a
// test can prove the steady-state fast path reads before writing and
// skips CreateOrUpdate entirely on the no-drift path. reset() zeroes the
// counters between phases of a single test.
type countingClient struct {
	client.Client
	gets, creates, updates, patches int
}

func newCountingClient(base client.Client) *countingClient {
	cc := &countingClient{}
	cc.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			cc.gets++
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			cc.creates++
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			cc.updates++
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			cc.patches++
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	return cc
}

func (cc *countingClient) reset() { cc.gets, cc.creates, cc.updates, cc.patches = 0, 0, 0, 0 }

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
