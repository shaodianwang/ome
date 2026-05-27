package ops

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

// surgePodAtOrdinal builds a fixture pod for a SurgeThenDrain test. Same
// shape as podAtIncarnation but stamps LabelPodOrdinal explicitly so
// partitionPodsBySurgeOrdinal can place it in the right bucket. Also
// overrides the pod name to match the ordinal (production Render uses
// the ordinal for both the name suffix and the label; tests must match).
func surgePodAtOrdinal(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ordinal int32, ready, serving bool) *corev1.Pod {
	pod := podAtIncarnation(isvc, instanceIdx, incarnation, ready, serving)
	pod.Name = query.PodName(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", ordinal)
	pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "test:v1"}}
	return pod
}

func surgeISVCWithStrategy(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := isvcReadyAtIncarnation(name, ns, incarnation)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategySurgeThenDrain,
		},
	}
	return isvc
}

// TestSurgeUpdate_Phase1StampsStepSurgeAndCreatesSurgePod pins the
// entry point: from a steady-state Instance (1 old pod at ordinal 0),
// surgeUpdate stamps Op{Step=Surge, TargetRev} and creates a new pod
// at ordinal 1. The old pod is untouched (still serving). ActiveOrdinal
// stays at 0 — it advances only after promote.
func TestSurgeUpdate_Phase1StampsStepSurgeAndCreatesSurgePod(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	target := targetSpecImage("llama:v2")
	c := newFakeClientWithDiscovery(t, isvc, oldPod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	done, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: Phase 1 just stamped + created surge pod")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.Operation == nil || s.Operation.Step != updateStepSurge {
		t.Errorf("Operation.Step: want Surge, got %+v", s.Operation)
	}
	if s.TargetRevision != tcr.Name {
		t.Errorf("TargetRevision: got %q want %q", s.TargetRevision, tcr.Name)
	}
	if s.ActiveOrdinal != 0 {
		t.Errorf("ActiveOrdinal should stay 0 until promote, got %d", s.ActiveOrdinal)
	}

	// Surge pod should now exist at ordinal 1.
	surgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	surgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: surgeName}, surgePod); err != nil {
		t.Fatalf("surge pod %s not created: %v", surgeName, err)
	}
	if got := surgePod.Labels[query.LabelPodOrdinal]; got != "1" {
		t.Errorf("surge pod ordinal label: got %q want 1", got)
	}
}

// TestSurgeUpdate_Phase1WaitsForSurgeReady pins the no-downtime
// invariant: surge pod exists but ContainersReady=False → caller does
// NOT touch the old pod's serving gate. The old pod stays in rotation
// until the surge proves ready.
func TestSurgeUpdate_Phase1WaitsForSurgeReady(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	// Surge pod exists but NOT ContainersReady.
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, false, false)
	target := targetSpecImage("llama:v2")
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	// Pre-stamp Step=Surge so we test the "waiting" branch, not the entry.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), p, v1beta1.EngineComponent, 0, tcr.Name, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp: %v", err)
	}

	done, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod, surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: surge not yet Ready")
	}

	// Old pod's serving gate must still be True (no drain started).
	freshOld := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld)
	for _, cond := range freshOld.Status.Conditions {
		if cond.Type == query.ServingConditionType && cond.Status != corev1.ConditionTrue {
			t.Errorf("old pod's serving gate was flipped prematurely: %+v", cond)
		}
	}
}

// TestSurgeUpdate_Phase3PromotesAndAdvancesActiveOrdinal pins the
// terminator: when no old pods remain (Phase A complete + delete
// observed), surgeUpdate returns done=true, sets Phase=Ready,
// RunningRevision=target, clears Operation, AND advances
// ActiveOrdinal to the new slot.
func TestSurgeUpdate_Phase3PromotesAndAdvancesActiveOrdinal(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	// Only the surge pod exists, runtime-ready and serving (post-Phase 2).
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := targetSpecImage("llama:v2")
	c := newFakeClientWithDiscovery(t, isvc, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	// Status reflects mid-surge state at Step=Drain.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), p, v1beta1.EngineComponent, 0, tcr.Name, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), p, v1beta1.EngineComponent, 0); err != nil {
		t.Fatalf("pre-stamp drain: %v", err)
	}

	done, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], tcr, []*corev1.Pod{surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true: no old pods, surge ready, promote")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q", s.RunningRevision, tcr.Name)
	}
	if s.Operation != nil {
		t.Errorf("Operation should be cleared, got %+v", s.Operation)
	}
	if s.TargetRevision != "" {
		t.Errorf("TargetRevision should be cleared, got %q", s.TargetRevision)
	}
	if s.ActiveOrdinal != 1 {
		t.Errorf("ActiveOrdinal should advance to 1 (the new slot), got %d", s.ActiveOrdinal)
	}
}

// TestSurgeUpdate_AlternatesOrdinalAcrossSurges pins the toggle: after
// a previous surge promoted ActiveOrdinal=1, the next surge creates a
// pod at ordinal 0 (not at ordinal 2 — names alternate, bounded set).
func TestSurgeUpdate_AlternatesOrdinalAcrossSurges(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	// Simulate post-previous-surge state: ActiveOrdinal=1, pod at ord=1.
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].ActiveOrdinal = 1
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := targetSpecImage("llama:v3")
	c := newFakeClientWithDiscovery(t, isvc, oldPod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	_, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}

	// New surge pod should now exist at ordinal 0 (since old is at 1).
	newSurgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 0)
	surgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: newSurgeName}, surgePod); err != nil {
		t.Fatalf("next-surge pod %s should exist at ordinal 0: %v", newSurgeName, err)
	}
	if got := surgePod.Labels[query.LabelPodOrdinal]; got != "0" {
		t.Errorf("ordinal label: got %q want 0", got)
	}
}

// TestSurgeUpdate_MultiPodRejected confirms the single-pod guard. Until
// multi-pod surge plumbing lands, surgeUpdate refuses to act on a gang
// runner (Size > 1) rather than producing an inconsistent state.
func TestSurgeUpdate_MultiPodRejected(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	// Synthesize a multi-pod plan (BuildPlan won't produce this normally,
	// but the guard must hold for any caller that constructs one).
	plan.Instances[0].Runners[0].Size = 4
	target := targetSpecImage("llama:v2")
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	_, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], tcr, nil)
	if err == nil {
		t.Fatalf("expected error: multi-pod surge not yet supported")
	}
}

// TestSurgeUpdate_PartitionByLabelHandlesLegacyPods pins backward
// compatibility for partitionPodsBySurgeOrdinal: pods without the
// LabelPodOrdinal label (pre-feature pods on the cluster) fall through
// to ordinal=0 via the PodOrdinalFromLabels default. Mixing one legacy
// pod (treated as ordinal 0) with a surge pod at ordinal 1 must NOT
// classify the legacy pod as a straggler.
func TestSurgeUpdate_PartitionByLabelHandlesLegacyPods(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	// Legacy old pod — has all the standard labels EXCEPT LabelPodOrdinal.
	legacyOld := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	delete(legacyOld.Labels, query.LabelPodOrdinal)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)

	old, surge, stragglers := partitionPodsBySurgeOrdinal([]*corev1.Pod{legacyOld, surgePod}, 0, 1)
	if len(stragglers) != 0 {
		t.Errorf("legacy pod should be treated as ordinal 0, not stragglers; got %d stragglers", len(stragglers))
	}
	if len(old) != 1 {
		t.Errorf("old: got %d want 1 (legacy pod with default ordinal 0)", len(old))
	}
	if len(surge) != 1 {
		t.Errorf("surge: got %d want 1 (surge pod at ordinal 1)", len(surge))
	}
}

// TestPatchInstanceStatusReadyOnRevisionWithOrdinal_Idempotent pins the
// promote helper's idempotency: re-invoking with the same target and
// ordinal is a no-op (no ResourceVersion bump).
func TestPatchInstanceStatusReadyOnRevisionWithOrdinal_Idempotent(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, 0, "rev-abc", 1); err != nil {
		t.Fatalf("first call: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, 0, "rev-abc", 1); err != nil {
		t.Fatalf("second call: %v", err)
	}
	afterRV := readRV(t, c, isvc)
	if beforeRV != afterRV {
		t.Errorf("idempotent call bumped ResourceVersion: %s -> %s", beforeRV, afterRV)
	}
}

func readRV(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) string {
	t.Helper()
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	return fresh.ResourceVersion
}

// TestExpectedPodNamesForInstance_RespectsActiveOrdinal pins the
// primary SurgeThenDrain drain-incomplete fix: when a single-pod
// Instance has a status entry whose ActiveOrdinal has been advanced
// by a prior surge promote (e.g., from 0 → 1), the function must
// emit a single target at THAT ordinal — NOT hard-code ordinal 0.
//
// The bug: hard-coding ordinal 0 means the next steady-state Create
// pass after surge promote sees "ordinal 0 is missing" and creates a
// duplicate pod, leaving the Instance with 2 alive pods forever.
//
// Cases covered:
//   - No status (initial create) → ordinal 0 (default).
//   - status.ActiveOrdinal=0 (post-recreate / steady-state) → ordinal 0.
//   - status.ActiveOrdinal=1 (post-surge promote) → ordinal 1.
func TestExpectedPodNamesForInstance_RespectsActiveOrdinal(t *testing.T) {
	cases := []struct {
		name          string
		activeOrdinal int32
		withStatus    bool
		wantOrdinal   int32
		wantName      string
	}{
		{
			name:        "no status entry defaults to ordinal 0",
			withStatus:  false,
			wantOrdinal: 0,
			wantName:    "llama-70b-engine-0-default-0",
		},
		{
			name:          "ActiveOrdinal=0 emits target at ordinal 0",
			activeOrdinal: 0,
			withStatus:    true,
			wantOrdinal:   0,
			wantName:      "llama-70b-engine-0-default-0",
		},
		{
			name:          "ActiveOrdinal=1 (post-surge promote) emits target at ordinal 1",
			activeOrdinal: 1,
			withStatus:    true,
			wantOrdinal:   1,
			wantName:      "llama-70b-engine-0-default-1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := minimalISVC("llama-70b", "prod", 1)
			if tc.withStatus {
				isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent: {
						OMENative: &v1beta1.OMENativeComponentStatus{
							InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
								{Index: 0, ActiveOrdinal: tc.activeOrdinal, Phase: v1beta1.OMENativeInstanceReady},
							},
						},
					},
				}
			}
			inst := core.InstancePlan{
				Index:       0,
				Incarnation: 1,
				Runners:     []core.RunnerPlan{{Name: "default", Size: 1}},
			}
			got := expectedPodNamesForInstance(isvc, v1beta1.EngineComponent, inst)
			if len(got) != 1 {
				t.Fatalf("len(targets): got %d want 1", len(got))
			}
			if got[0].Ordinal != tc.wantOrdinal {
				t.Errorf("Ordinal: got %d want %d", got[0].Ordinal, tc.wantOrdinal)
			}
			if got[0].Name != tc.wantName {
				t.Errorf("Name: got %q want %q", got[0].Name, tc.wantName)
			}
		})
	}
}

// TestExpectedPodNamesForInstance_MultiPodPreservesAllOrdinals pins
// that the ActiveOrdinal handling is a single-pod-only branch; gang
// Runners (Size > 1) keep emitting every 0..Size-1 ordinal so the
// multi-pod scale-out path that future work will enable doesn't
// regress. ActiveOrdinal applies only to single-pod surge — multi-pod
// surge needs per-gang-member ordinal allocation not yet plumbed.
func TestExpectedPodNamesForInstance_MultiPodPreservesAllOrdinals(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	// Even with ActiveOrdinal set, multi-pod must not skew the enumeration.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, ActiveOrdinal: 1, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	inst := core.InstancePlan{
		Index:       0,
		Incarnation: 1,
		Runners:     []core.RunnerPlan{{Name: "default", Size: 4}},
	}
	got := expectedPodNamesForInstance(isvc, v1beta1.EngineComponent, inst)
	if len(got) != 4 {
		t.Fatalf("len(targets): got %d want 4 (Size=4 must enumerate all ordinals)", len(got))
	}
	wantOrdinals := map[int32]bool{0: true, 1: true, 2: true, 3: true}
	for _, target := range got {
		if !wantOrdinals[target.Ordinal] {
			t.Errorf("unexpected ordinal: %d (want one of 0..3)", target.Ordinal)
		}
		delete(wantOrdinals, target.Ordinal)
	}
	if len(wantOrdinals) > 0 {
		t.Errorf("missing ordinals: %v", wantOrdinals)
	}
}

// TestSurgeUpdate_PostPromoteCreateDoesNotResurrectDrainedOrdinal pins
// the integration-shaped surge-cycle invariant: after surge has
// completed (drain + promote), the next reconcile pass that lands in
// Create must NOT recreate a pod at the now-drained ordinal slot.
//
// This is the user-visible bug: dispatcher's Update pass returns
// done=true on promote, then falls through to Create on the same
// reconcile. With ActiveOrdinal=1 and Runner.Size=1, expectedPodNames
// must say "ordinal 1 is the canonical slot, and that pod exists" —
// not "ordinal 0 is missing, create it".
func TestSurgeUpdate_PostPromoteCreateDoesNotResurrectDrainedOrdinal(t *testing.T) {
	resetExpectations(t)
	// Simulate the post-promote state directly: Phase=Ready,
	// ActiveOrdinal=1, RunningRevision=target, one pod alive at ordinal 1.
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	target := targetSpecImage("llama:v2")
	c := newFakeClientWithDiscovery(t, isvc)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	// Promote the surge: bumps ActiveOrdinal to 1 and stamps RunningRevision.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), minimalParams(t, isvc, c), v1beta1.EngineComponent, 0, tcr.Name, 1); err != nil {
		t.Fatalf("patch promote: %v", err)
	}
	// Surge pod at ordinal 1, runtime-ready and serving — what survives the drain.
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	if err := c.Create(context.Background(), surgePod); err != nil {
		t.Fatalf("seed surge pod: %v", err)
	}

	// Re-read ISVC so the params have the promoted status (ActiveOrdinal=1).
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	p := minimalParams(t, fresh, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// Drive Create as the dispatcher would after Update returns done=true.
	if _, err := Create(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// CRITICAL: there must STILL be only one pod for instance 0 — at ordinal 1.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("Create resurrected a stale ordinal! got %d pods (%v), want 1 (the surge pod at ord=1)", len(pods.Items), names)
	}
	if pods.Items[0].Name != surgePod.Name {
		t.Errorf("unexpected pod survived: got %q want %q", pods.Items[0].Name, surgePod.Name)
	}
}

// Compile-time sanity: ensure the test file uses the metav1 import (the
// linter complains otherwise on imports added defensively).
var _ = metav1.NewTime
