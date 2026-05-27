package omenative

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
)

// ---------------------------------------------------------------------
// ensurePodGroups — orchestration around podgroup.{Ensure,Delete}PodGroup
// plus the GangSchedulingUnavailable condition.
// ---------------------------------------------------------------------

// TestEnsurePodGroups_SinglePodNoPodGroupConditionFalse — single-pod
// Component: no PodGroup, condition=False (not required).
func TestEnsurePodGroups_SinglePodNoPodGroupConditionFalse(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	p.GangSchedulingAvailable = true // CRD present, but single-pod doesn't matter
	plan, _ := core.BuildPlan(p)

	if err := ensurePodGroups(context.Background(), p, plan); err != nil {
		t.Fatalf("ensurePodGroups: %v", err)
	}

	// No PodGroup created.
	pg := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound (single-pod = no PodGroup), got err=%v pg=%+v", err, pg)
	}

	// Condition=False, reason=GangSchedulingAvailable.
	assertCondition(t, c, isvc, v1beta1.EngineComponent, metav1.ConditionFalse, status.ReasonGangSchedulingAvailable)
}

// TestEnsurePodGroups_MultiPodCRDAvailableCreatesGang — multi-pod
// Component WITH CRD available: PodGroup created, condition=False
// (gang available, not degraded).
func TestEnsurePodGroups_MultiPodCRDAvailableCreatesGang(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 2
	p.GangSchedulingAvailable = true
	plan, _ := core.BuildPlan(p)

	if err := ensurePodGroups(context.Background(), p, plan); err != nil {
		t.Fatalf("ensurePodGroups: %v", err)
	}

	// PodGroup created for instance 0 with minMember=3 (1 leader + 2 workers).
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg); err != nil {
		t.Fatalf("expected PodGroup created, got %v", err)
	}
	if pg.Spec.MinMember != 3 {
		t.Errorf("MinMember: got %d want 3", pg.Spec.MinMember)
	}

	// Condition=False (gang available, not degraded).
	assertCondition(t, c, isvc, v1beta1.EngineComponent, metav1.ConditionFalse, status.ReasonGangSchedulingAvailable)
}

// TestEnsurePodGroups_MultiPodCRDMissingSetsConditionTrueNoPodGroup —
// the degradation path the OEP describes.
func TestEnsurePodGroups_MultiPodCRDMissingSetsConditionTrueNoPodGroup(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 2
	p.GangSchedulingAvailable = false // CRD absent — soft-fail
	plan, _ := core.BuildPlan(p)

	if err := ensurePodGroups(context.Background(), p, plan); err != nil {
		t.Fatalf("ensurePodGroups: %v", err)
	}

	// No PodGroup created.
	pg := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound (CRD missing = no PodGroup), got err=%v pg=%+v", err, pg)
	}

	// Condition=True, reason=PodGroupCRDNotInstalled.
	assertCondition(t, c, isvc, v1beta1.EngineComponent, metav1.ConditionTrue, status.ReasonPodGroupCRDNotInstalled)
}

// TestEnsurePodGroups_ScaleDownDeletesExtraPodGroups — when an Instance
// drops out of the plan, its PodGroup is GC'd. Scale-down case.
func TestEnsurePodGroups_ScaleDownDeletesExtraPodGroups(t *testing.T) {
	// ISVC with two engine Instances already in status.
	isvc := minimalISVC("llama", "prod", 1) // plan replicas=1 → index 0 stays, index 1 drops
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady, Incarnation: 1},
					{Index: 1, Phase: v1beta1.OMENativeInstanceReady, Incarnation: 1},
				},
			},
		},
	}
	// Pre-existing PodGroups for both Instances.
	pg0 := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-0", Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	pg1 := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-1", Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	c := newFakeClientWithDiscovery(t, isvc, pg0, pg1)
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 1
	p.GangSchedulingAvailable = true

	plan, _ := core.BuildPlan(p) // replicas=1 → plan covers only index 0

	if err := ensurePodGroups(context.Background(), p, plan); err != nil {
		t.Fatalf("ensurePodGroups: %v", err)
	}

	// Index 0 PodGroup stays (updated).
	gotPg0 := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, gotPg0); err != nil {
		t.Errorf("expected llama-engine-0 to remain, got %v", err)
	}
	// Index 1 PodGroup deleted.
	gotPg1 := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-1"}, gotPg1)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected llama-engine-1 to be deleted, got err=%v pg=%+v", err, gotPg1)
	}
}

// TestEnsurePodGroups_MultiInstanceMultiPodPodGroupsPerInstance —
// 3 multi-pod Instances ⇒ 3 PodGroups, one per Instance.
func TestEnsurePodGroups_MultiInstanceMultiPodPodGroupsPerInstance(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 3)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 1
	p.GangSchedulingAvailable = true
	plan, _ := core.BuildPlan(p)

	if err := ensurePodGroups(context.Background(), p, plan); err != nil {
		t.Fatalf("ensurePodGroups: %v", err)
	}

	for i := int32(0); i < 3; i++ {
		pg := &schedulingv1alpha1.PodGroup{}
		key := client.ObjectKey{Namespace: "prod", Name: instancePodGroupName("llama", v1beta1.EngineComponent, i)}
		if err := c.Get(context.Background(), key, pg); err != nil {
			t.Errorf("expected PodGroup %s, got %v", key, err)
		}
	}
}

// TestEnsurePodGroups_Idempotent — two calls back-to-back, no errors,
// no condition transition.
func TestEnsurePodGroups_Idempotent(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 1
	p.GangSchedulingAvailable = true
	plan, _ := core.BuildPlan(p)

	for i := 0; i < 2; i++ {
		if err := ensurePodGroups(context.Background(), p, plan); err != nil {
			t.Fatalf("ensurePodGroups pass %d: %v", i, err)
		}
	}
	// PodGroup exists once.
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg); err != nil {
		t.Errorf("PodGroup missing after idempotent ensure: %v", err)
	}
}

// TestEnsurePodGroups_NilClientErrors guards against a wiring slip.
func TestEnsurePodGroups_NilClientErrors(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	p := minimalParams(t, isvc, nil)
	p.Client = nil
	if err := ensurePodGroups(context.Background(), p, ComponentPlan{}); err == nil {
		t.Fatal("expected nil-client error")
	}
}

// ---------------------------------------------------------------------
// patchGangSchedulingCondition — drift correction + idempotency on
// the live ISVC status path.
// ---------------------------------------------------------------------

// TestPatchGangSchedulingCondition_StatusUpdateFlipsCondition writes
// True→False (recovery) and asserts the live status reflects it.
func TestPatchGangSchedulingCondition_StatusUpdateFlipsCondition(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	// Pre-seed with True (degraded) so the test exercises the flip.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				Conditions: []metav1.Condition{{
					Type:    status.ConditionGangSchedulingUnavailable,
					Status:  metav1.ConditionTrue,
					Reason:  status.ReasonPodGroupCRDNotInstalled,
					Message: "scheduler-plugins scheduling.x-k8s.io/v1alpha1 PodGroup CRD is not installed; multi-pod Instances may schedule partially",
				}},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	// CRD became available between reconciles → condition flips to False.
	if err := patchGangSchedulingCondition(context.Background(), p, true, true); err != nil {
		t.Fatalf("patchGangSchedulingCondition: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get fresh ISVC: %v", err)
	}
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	got := apimeta.FindStatusCondition(cs.OMENative.Conditions, status.ConditionGangSchedulingUnavailable)
	if got == nil {
		t.Fatal("GangSchedulingUnavailable condition missing")
	}
	if got.Status != metav1.ConditionFalse {
		t.Errorf("Status: got %s want False (CRD now available)", got.Status)
	}
	if got.Reason != status.ReasonGangSchedulingAvailable {
		t.Errorf("Reason: got %q want %q", got.Reason, status.ReasonGangSchedulingAvailable)
	}
}

// TestPatchGangSchedulingCondition_ISVCGoneIsSafe — the helper must
// not error when the ISVC was deleted mid-reconcile (NotFound at re-
// read). Mirrors bumpCollisionCount's tolerance.
func TestPatchGangSchedulingCondition_ISVCGoneIsSafe(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	c := newFakeClientWithDiscovery(t) // no objects = ISVC absent
	p := minimalParams(t, isvc, c)

	// Should not error even though Get returns NotFound under the hood.
	if err := patchGangSchedulingCondition(context.Background(), p, true, true); err != nil {
		t.Errorf("expected nil error when ISVC missing, got %v", err)
	}
}

// ---------------------------------------------------------------------
// extraPodGroupIndices — set-difference semantic.
// ---------------------------------------------------------------------

func TestExtraPodGroupIndices(t *testing.T) {
	isvc := minimalISVC("llama", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
					{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
					{Index: 2, Phase: v1beta1.OMENativeInstanceMigrating}, // protected from delete
					{Index: 3, Phase: v1beta1.OMENativeInstanceCreating,
						Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationMigrate}}, // protected surge
				},
			},
		},
	}
	plan := ComponentPlan{
		Component: v1beta1.EngineComponent,
		Instances: []core.InstancePlan{{Index: 0}}, // plan only covers index 0
	}

	got := extraPodGroupIndices(isvc, v1beta1.EngineComponent, plan)
	want := []int32{1}
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("extraPodGroupIndices: got %v want %v", got, want)
	}
}

// ---------------------------------------------------------------------
// maybeWarnNoGangScheduler — heuristic warning when the rendered pod
// template's schedulerName is empty or "default-scheduler" while the
// controller is creating PodGroup objects (which a stock kube-scheduler
// will silently ignore).
// ---------------------------------------------------------------------

// TestEnsurePodGroups_MaybeNoGangSchedulerWarning_Matrix exercises the
// four scenarios in the spec: empty/default schedulerName + multi-pod
// fires; custom schedulerName + multi-pod is silent; single-pod is
// silent regardless of schedulerName.
func TestEnsurePodGroups_MaybeNoGangSchedulerWarning_Matrix(t *testing.T) {
	tests := []struct {
		name          string
		schedulerName string
		multiPod      bool
		workerSize    int
		wantFired     bool
	}{
		{
			name:          "empty schedulerName multi-pod fires",
			schedulerName: "",
			multiPod:      true,
			workerSize:    2,
			wantFired:     true,
		},
		{
			name:          "default-scheduler multi-pod fires",
			schedulerName: corev1.DefaultSchedulerName,
			multiPod:      true,
			workerSize:    2,
			wantFired:     true,
		},
		{
			name:          "custom schedulerName multi-pod silent",
			schedulerName: "scheduler-plugins-scheduler",
			multiPod:      true,
			workerSize:    2,
			wantFired:     false,
		},
		{
			name:          "single-pod default-scheduler silent",
			schedulerName: corev1.DefaultSchedulerName,
			multiPod:      false,
			workerSize:    0,
			wantFired:     false,
		},
		{
			name:          "single-pod empty silent",
			schedulerName: "",
			multiPod:      false,
			workerSize:    0,
			wantFired:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Each subtest gets a fresh dedup map so prior subtests
			// can't suppress the next one (different ISVCs would also
			// work, but resetting is simpler and tests the helper).
			resetMaybeNoGangSchedulerSeen()

			isvc := minimalISVC("llama-"+strings.ReplaceAll(tc.name, " ", "-"), "prod", 1)
			c := newFakeClientWithDiscovery(t, isvc)
			rec := record.NewFakeRecorder(8)
			p := minimalParams(t, isvc, c)
			p.Recorder = rec
			p.MultiPod = tc.multiPod
			p.WorkerSize = tc.workerSize
			p.GangSchedulingAvailable = true
			p.PodSpec = &corev1.PodSpec{
				Containers:    []corev1.Container{{Name: "main", Image: "test:v1"}},
				SchedulerName: tc.schedulerName,
			}
			if tc.multiPod {
				p.WorkerPodSpec = &corev1.PodSpec{
					Containers:    []corev1.Container{{Name: "main", Image: "test:v1"}},
					SchedulerName: tc.schedulerName,
				}
			}
			plan, _ := core.BuildPlan(p)

			if err := ensurePodGroups(context.Background(), p, plan); err != nil {
				t.Fatalf("ensurePodGroups: %v", err)
			}

			events := drainGangSchedulerEvents(rec)
			fired := false
			for _, e := range events {
				if strings.Contains(e, status.EventReasonMaybeNoGangScheduler) {
					fired = true
					break
				}
			}
			if fired != tc.wantFired {
				t.Errorf("MaybeNoGangScheduler event fired=%v want=%v (events=%v)", fired, tc.wantFired, events)
			}
			if fired {
				// Sanity: the warning must mention the suggested
				// scheduler so the operator has a copy-paste fix.
				var warning string
				for _, e := range events {
					if strings.Contains(e, status.EventReasonMaybeNoGangScheduler) {
						warning = e
						break
					}
				}
				if !strings.Contains(warning, "scheduler-plugins-scheduler") {
					t.Errorf("warning should suggest scheduler-plugins-scheduler, got %q", warning)
				}
				if !strings.HasPrefix(warning, "Warning ") {
					t.Errorf("warning event should be Warning-type, got %q", warning)
				}
			}
		})
	}
}

// TestEnsurePodGroups_MaybeNoGangScheduler_DedupPerProcess verifies
// that two reconciles for the same (ISVC, Component) only fire ONE
// MaybeNoGangScheduler event — the dedup map's job.
func TestEnsurePodGroups_MaybeNoGangScheduler_DedupPerProcess(t *testing.T) {
	resetMaybeNoGangSchedulerSeen()

	isvc := minimalISVC("dedup-llama", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	rec := record.NewFakeRecorder(16)
	p := minimalParams(t, isvc, c)
	p.Recorder = rec
	p.MultiPod = true
	p.WorkerSize = 2
	p.GangSchedulingAvailable = true
	// Empty schedulerName — should warn.
	p.PodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
	}
	p.WorkerPodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
	}
	plan, _ := core.BuildPlan(p)

	// Two reconciles back-to-back.
	for i := 0; i < 2; i++ {
		if err := ensurePodGroups(context.Background(), p, plan); err != nil {
			t.Fatalf("ensurePodGroups pass %d: %v", i, err)
		}
	}

	events := drainGangSchedulerEvents(rec)
	count := 0
	for _, e := range events {
		if strings.Contains(e, status.EventReasonMaybeNoGangScheduler) {
			count++
		}
	}
	if count != 1 {
		t.Errorf("MaybeNoGangScheduler event count: got %d want 1 (events=%v)", count, events)
	}
}

// TestEnsurePodGroups_MaybeNoGangScheduler_DedupSeparateComponents
// verifies the dedup key is (ISVC, Component) — same ISVC but a
// different Component must still get a warning. Catches the
// regression where the key was accidentally collapsed to just the
// ISVC name.
func TestEnsurePodGroups_MaybeNoGangScheduler_DedupSeparateComponents(t *testing.T) {
	resetMaybeNoGangSchedulerSeen()

	isvc := minimalISVC("multi-comp", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	rec := record.NewFakeRecorder(16)
	emptySpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}}

	// Engine reconcile.
	pEng := minimalParams(t, isvc, c)
	pEng.Recorder = rec
	pEng.MultiPod = true
	pEng.WorkerSize = 2
	pEng.GangSchedulingAvailable = true
	pEng.PodSpec = emptySpec
	pEng.WorkerPodSpec = emptySpec
	planEng, _ := core.BuildPlan(pEng)
	if err := ensurePodGroups(context.Background(), pEng, planEng); err != nil {
		t.Fatalf("engine ensurePodGroups: %v", err)
	}

	// Decoder reconcile — directly invoke the helper with the Decoder
	// component (no Decoder spec on the ISVC, so we test the helper
	// directly rather than rebuilding a Decoder plan).
	maybeWarnNoGangScheduler(pEng, v1beta1.DecoderComponent, emptySpec, emptySpec)

	events := drainGangSchedulerEvents(rec)
	count := 0
	for _, e := range events {
		if strings.Contains(e, status.EventReasonMaybeNoGangScheduler) {
			count++
		}
	}
	if count != 2 {
		t.Errorf("MaybeNoGangScheduler event count (engine+decoder): got %d want 2 (events=%v)", count, events)
	}
}

// TestMaybeWarnNoGangScheduler_NilSafety guards the helper against
// the test-fixture pattern where Recorder / ISVC / PodSpec may be nil.
func TestMaybeWarnNoGangScheduler_NilSafety(t *testing.T) {
	resetMaybeNoGangSchedulerSeen()

	// nil ISVC: no-op (early return).
	rec := record.NewFakeRecorder(2)
	p := ReconcileParams{Recorder: rec}
	maybeWarnNoGangScheduler(p, v1beta1.EngineComponent, nil, nil)
	if len(rec.Events) != 0 {
		t.Errorf("nil ISVC should be no-op, got %d events", len(rec.Events))
	}

	// nil PodSpec + nil WorkerPodSpec + non-nil ISVC: schedulerName is
	// "" so the warning DOES fire (caller's responsibility to gate at
	// ensurePodGroups, which already does — single-pod / no-podgroup
	// paths never reach this helper). Verifies the helper itself
	// handles nil specs gracefully.
	rec2 := record.NewFakeRecorder(2)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns"}}
	p2 := ReconcileParams{Recorder: rec2, ISVC: isvc}
	maybeWarnNoGangScheduler(p2, v1beta1.EngineComponent, nil, nil)
	if len(rec2.Events) != 1 {
		t.Errorf("nil specs + valid ISVC should fire once, got %d events", len(rec2.Events))
	}

	// nil Recorder + valid rest: helper must not panic. Dedup map is
	// still updated though, so reset to keep test isolation clean.
	resetMaybeNoGangSchedulerSeen()
	p3 := ReconcileParams{ISVC: isvc}
	maybeWarnNoGangScheduler(p3, v1beta1.EngineComponent, nil, nil) // no panic
}

// TestEffectiveSchedulerName_LeaderWinsThenWorker covers the
// precedence: leader's schedulerName takes over worker's; both empty
// → empty. Pure-helper test.
func TestEffectiveSchedulerName_LeaderWinsThenWorker(t *testing.T) {
	tests := []struct {
		name   string
		leader *corev1.PodSpec
		worker *corev1.PodSpec
		want   string
	}{
		{"both nil", nil, nil, ""},
		{"both empty", &corev1.PodSpec{}, &corev1.PodSpec{}, ""},
		{"leader set", &corev1.PodSpec{SchedulerName: "custom"}, &corev1.PodSpec{}, "custom"},
		{"only worker set", &corev1.PodSpec{}, &corev1.PodSpec{SchedulerName: "worker-sched"}, "worker-sched"},
		{"leader wins", &corev1.PodSpec{SchedulerName: "leader"}, &corev1.PodSpec{SchedulerName: "worker"}, "leader"},
		{"nil leader, worker set", nil, &corev1.PodSpec{SchedulerName: "worker"}, "worker"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveSchedulerName(tc.leader, tc.worker); got != tc.want {
				t.Errorf("effectiveSchedulerName(%+v, %+v) = %q want %q", tc.leader, tc.worker, got, tc.want)
			}
		})
	}
}

// drainGangSchedulerEvents returns all events currently buffered on
// rec. Equivalent to coordination/sequential_event_test.go's
// drainEvents — duplicated here to keep this test file's import
// surface narrow.
func drainGangSchedulerEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

// instancePodGroupName mirrors query.PodGroupName without importing
// the query package twice into this test file.
func instancePodGroupName(isvc string, component v1beta1.ComponentType, idx int32) string {
	return isvc + "-" + string(component) + "-" + itoaSmall(int(idx))
}

func itoaSmall(n int) string {
	if n == 0 {
		return "0"
	}
	digits := "0123456789"
	var b []byte
	for n > 0 {
		b = append([]byte{digits[n%10]}, b...)
		n /= 10
	}
	return string(b)
}

// assertCondition asserts the ISVC's GangSchedulingUnavailable
// condition on the named Component matches (status, reason).
func assertCondition(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, wantStatus metav1.ConditionStatus, wantReason string) {
	t.Helper()
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get ISVC: %v", err)
	}
	cs := fresh.Status.Components[component]
	if cs.OMENative == nil {
		t.Fatalf("component %s OMENative status nil", component)
	}
	cond := apimeta.FindStatusCondition(cs.OMENative.Conditions, status.ConditionGangSchedulingUnavailable)
	if cond == nil {
		t.Fatalf("GangSchedulingUnavailable condition missing on component %s", component)
	}
	if cond.Status != wantStatus {
		t.Errorf("Status: got %s want %s", cond.Status, wantStatus)
	}
	if cond.Reason != wantReason {
		t.Errorf("Reason: got %q want %q", cond.Reason, wantReason)
	}
}

// unused but here so test-time changes to time-based helpers compile.
var _ = time.Now
