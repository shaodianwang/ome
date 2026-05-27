package ops

import (
	"context"
	"fmt"
	"testing"
	"time"

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

// surgeISVCMultiInstance builds a 2-replica ISVC with SurgeThenDrain
// configured and an InstanceStatus for each (Phase=Ready,
// ActiveOrdinal=0). This is the steady-state from which a multi-instance
// SurgeThenDrain rollout fires.
func surgeISVCMultiInstance(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := minimalISVC(name, ns, 2)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
			Type: v1beta1.OMENativeUpdateStrategySurgeThenDrain,
		},
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
					{Index: 1, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	return isvc
}

// TestSurgeUpdate_MultiInstance_NoOrdResurrectionAfterPromote pins the
// multi-instance follow-up to PR #243. Even when BOTH instances have
// been promoted to ActiveOrdinal=1 (surge cycle complete), the next
// Create pass must not see "ordinal 0 is missing" for either instance
// and resurrect a drained slot.
//
// Reproduces the user-visible bug observed on the dev cluster
// (msl-cw-use2-dev-2): instance 1's ord-0 was re-created 36 seconds
// after promote because the in-memory ISVC snapshot still showed
// ActiveOrdinal=0. The fix (MutateInstance mirror) ensures the
// post-promote ActiveOrdinal=1 is visible to the tail Create pass.
func TestSurgeUpdate_MultiInstance_NoOrdResurrectionAfterPromote(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCMultiInstance("llama-70b", "prod", 1)
	target := targetSpecImage("llama:v2")
	c := newFakeClientWithDiscovery(t, isvc)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)

	// Simulate post-promote state for BOTH instances: ActiveOrdinal=1,
	// surge pod alive at ord=1 for each, old pod at ord=0 gone.
	p := minimalParams(t, isvc, c)
	for _, idx := range []int32{0, 1} {
		if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, idx, tcr.Name, 1); err != nil {
			t.Fatalf("patch promote instance %d: %v", idx, err)
		}
		surgePod := surgePodAtOrdinal(isvc, idx, 1, 1, true, true)
		if err := c.Create(context.Background(), surgePod); err != nil {
			t.Fatalf("seed surge pod for instance %d: %v", idx, err)
		}
	}

	// Verify the in-memory mirror landed: both instances should reflect
	// ActiveOrdinal=1 without any explicit re-read.
	for _, idx := range []int32{0, 1} {
		s := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[idx]
		if s.ActiveOrdinal != 1 {
			t.Fatalf("in-memory mirror missed instance %d: got ActiveOrdinal=%d, want 1", idx, s.ActiveOrdinal)
		}
	}

	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// Drive Create as the dispatcher would after Update returns done=true
	// for both instances. Without the mirror this resurrects ord-0 on
	// the second instance (the bug); with the mirror it sees ord-1 is
	// canonical and skips creation.
	if _, err := Create(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("Create: %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("Create resurrected a stale ordinal! got %d pods (%v), want 2 (one per instance, both at ord=1)", len(pods.Items), names)
	}
	// Both surviving pods should be at ordinal 1 (the post-promote slot).
	for _, pod := range pods.Items {
		if got := pod.Labels[query.LabelPodOrdinal]; got != "1" {
			t.Errorf("pod %s should be at ordinal 1, got %q", pod.Name, got)
		}
	}
}

// TestSurgeUpdate_MultiInstance_InterleavedPromotes pins the timing
// where inst-0 promotes earlier than inst-1 within the SAME reconcile
// (or across reconciles): when the dispatcher's Update loop completes
// the second instance's Phase 3 promote and falls through to Create,
// the tail Create must not resurrect EITHER instance's drained
// ordinal — even though inst-0 was promoted earlier (and thus its
// in-memory state already had to be mirrored once during the loop).
//
// This is the exact failure mode the sample log captured at
// /tmp/policy-samples/v-std.log sample 12, where `pods_e` went from
// `OLD,NEW,OLD` to `NEW,OLD,NEW` after inst-1 promoted.
func TestSurgeUpdate_MultiInstance_InterleavedPromotes(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCMultiInstance("llama-70b", "prod", 1)
	target := targetSpecImage("llama:v2")
	c := newFakeClientWithDiscovery(t, isvc)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, target)
	p := minimalParams(t, isvc, c)

	// Cycle A: inst-0 just completed its Phase 3 promote. inst-1 is
	// still mid-surge (surge pod created but old pod not yet drained).
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, 0, tcr.Name, 1); err != nil {
		t.Fatalf("patch inst-0 promote: %v", err)
	}
	inst0SurgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	if err := c.Create(context.Background(), inst0SurgePod); err != nil {
		t.Fatalf("seed inst-0 surge pod: %v", err)
	}
	// inst-1 mid-surge: BOTH ord-0 (old) and ord-1 (surge) alive.
	inst1OldPod := surgePodAtOrdinal(isvc, 1, 1, 0, true, true)
	inst1SurgePod := surgePodAtOrdinal(isvc, 1, 1, 1, true, true)
	if err := c.Create(context.Background(), inst1OldPod); err != nil {
		t.Fatalf("seed inst-1 old pod: %v", err)
	}
	if err := c.Create(context.Background(), inst1SurgePod); err != nil {
		t.Fatalf("seed inst-1 surge pod: %v", err)
	}

	// Cycle B starts here. The Update pass for inst-1 advances through
	// Phase 3 promote (simulated by directly invoking
	// patchInstanceStatusReadyOnRevisionWithOrdinal — this is the
	// status mutation surgeUpdate's Phase 3 does on the dispatcher's
	// behalf). At THIS point in cycle B the in-memory `isvc` has been
	// mutated TWICE: once for inst-0 in a prior cycle, once for inst-1
	// just now. The dispatcher then falls through to Create at the
	// tail of cycle B.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, 1, tcr.Name, 1); err != nil {
		t.Fatalf("patch inst-1 promote: %v", err)
	}
	// Simulate the post-drain delete: inst-1's old ord-0 pod is gone.
	if err := c.Delete(context.Background(), inst1OldPod); err != nil {
		t.Fatalf("delete inst-1 old pod: %v", err)
	}

	// Pre-check: in-memory ISVC reflects both promotes WITHOUT re-read.
	// This is the contract MutateInstance's mirror provides.
	for _, idx := range []int32{0, 1} {
		s := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[idx]
		if s.ActiveOrdinal != 1 {
			t.Fatalf("in-memory mirror missed instance %d post-promote: got ActiveOrdinal=%d, want 1", idx, s.ActiveOrdinal)
		}
	}

	// Tail Create: must not resurrect inst-0's ord-0 (the older
	// promote) nor inst-1's ord-0 (the just-deleted one).
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if _, err := Create(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("Create: %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("interleaved-promote tail Create resurrected an ordinal! got %d pods (%v), want 2 (one per instance, both at ord=1)", len(pods.Items), names)
	}
	for _, pod := range pods.Items {
		if got := pod.Labels[query.LabelPodOrdinal]; got != "1" {
			t.Errorf("pod %s should be at ordinal 1 post-interleaved-promote, got %q", pod.Name, got)
		}
	}
}

// TestExpectedPodNamesForInstance_StaleActiveOrdinalDoesNotResurrect
// pins the per-Instance contract that backs the multi-instance
// fix: after MutateInstance commits an ActiveOrdinal advancement for
// any instance, expectedPodNamesForInstance reading the same
// in-memory ISVC for ANY OTHER instance must return the correct
// per-instance ordinal — neither inheriting nor mis-attributing the
// advanced ordinal.
//
// Cases covered:
//   - inst-0 freshly promoted (ActiveOrdinal=1), inst-1 still at the
//     default (ActiveOrdinal=0) → inst-0 emits ord=1, inst-1 emits ord=0.
//   - both instances promoted (ActiveOrdinal=1) → both emit ord=1.
//   - both instances at default (ActiveOrdinal=0) → both emit ord=0.
//
// The bug shape it guards against: the prior implementation read a
// stale ActiveOrdinal that didn't reflect the apiserver commit,
// causing the function to emit ord=0 for a just-promoted instance
// and resurrect the drained slot.
func TestExpectedPodNamesForInstance_StaleActiveOrdinalDoesNotResurrect(t *testing.T) {
	cases := []struct {
		name          string
		promoteIdx    []int32 // indices to mutate to ActiveOrdinal=1 via MutateInstance
		wantOrdByIdx  map[int32]int32
		wantNameByIdx map[int32]string
	}{
		{
			name:       "inst-0 promoted, inst-1 at default — per-instance ordinals stay distinct",
			promoteIdx: []int32{0},
			wantOrdByIdx: map[int32]int32{
				0: 1,
				1: 0,
			},
			wantNameByIdx: map[int32]string{
				0: "llama-70b-engine-0-default-1",
				1: "llama-70b-engine-1-default-0",
			},
		},
		{
			name:       "both promoted — both emit ord=1",
			promoteIdx: []int32{0, 1},
			wantOrdByIdx: map[int32]int32{
				0: 1,
				1: 1,
			},
			wantNameByIdx: map[int32]string{
				0: "llama-70b-engine-0-default-1",
				1: "llama-70b-engine-1-default-1",
			},
		},
		{
			name:       "neither promoted — both emit ord=0 (default)",
			promoteIdx: nil,
			wantOrdByIdx: map[int32]int32{
				0: 0,
				1: 0,
			},
			wantNameByIdx: map[int32]string{
				0: "llama-70b-engine-0-default-0",
				1: "llama-70b-engine-1-default-0",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc := surgeISVCMultiInstance("llama-70b", "prod", 1)
			c := newFakeClientWithDiscovery(t, isvc)
			p := minimalParams(t, isvc, c)

			for _, idx := range tc.promoteIdx {
				if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, idx, "rev-x", 1); err != nil {
					t.Fatalf("promote instance %d: %v", idx, err)
				}
			}

			// Build per-Instance plans and exercise expectedPodNamesForInstance
			// against the in-memory ISVC (the same isvc pointer that
			// MutateInstance mirrored to).
			for _, idx := range []int32{0, 1} {
				inst := core.InstancePlan{
					Index:       idx,
					Incarnation: 1,
					Runners:     []core.RunnerPlan{{Name: "default", Size: 1}},
				}
				got := expectedPodNamesForInstance(isvc, v1beta1.EngineComponent, inst)
				if len(got) != 1 {
					t.Fatalf("instance %d: len(targets)=%d want 1", idx, len(got))
				}
				if got[0].Ordinal != tc.wantOrdByIdx[idx] {
					t.Errorf("instance %d: Ordinal=%d want %d", idx, got[0].Ordinal, tc.wantOrdByIdx[idx])
				}
				if got[0].Name != tc.wantNameByIdx[idx] {
					t.Errorf("instance %d: Name=%q want %q", idx, got[0].Name, tc.wantNameByIdx[idx])
				}
			}
		})
	}
}

// TestMutateInstance_MirrorsCommittedStateOntoInMemoryISVC pins the
// fix mechanism: any MutateInstance write must also stamp the
// committed InstanceStatus onto the caller's in-memory `isvc`
// snapshot. Without the mirror, callers that don't re-read between
// MutateInstance calls (e.g. the dispatcher's tail Create pass)
// observe stale state for fields that were just committed.
//
// Verifies for two fields that matter post-fix:
//   - ActiveOrdinal (the SurgeThenDrain post-promote slot) — the
//     field whose staleness triggered the production bug.
//   - Phase + Operation (any other field) — confirms the mirror is
//     general, not ActiveOrdinal-specific.
//
// Also asserts mirror semantics:
//   - First MutateInstance for a missing slot APPENDS to the in-
//     memory slice (matches the apiserver-side behavior).
//   - Subsequent MutateInstance for an existing slot UPDATES it in
//     place (no duplicate).
func TestMutateInstance_MirrorsCommittedStateOntoInMemoryISVC(t *testing.T) {
	resetExpectations(t)
	// Empty status — first MutateInstance must create the slot in the
	// mirror, not crash.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	// Patch Phase=Updating with Op{Step=Surge} via the helper.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), p, v1beta1.EngineComponent, 0, "rev-1", 30*time.Second); err != nil {
		t.Fatalf("first MutateInstance: %v", err)
	}
	// Mirror should now contain the slot.
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || cs.OMENative == nil {
		t.Fatalf("mirror missed OMENative substatus")
	}
	if len(cs.OMENative.InstanceStatuses) != 1 {
		t.Fatalf("InstanceStatuses len=%d want 1 (mirror should APPEND on first create)", len(cs.OMENative.InstanceStatuses))
	}
	if got := cs.OMENative.InstanceStatuses[0].Phase; got != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase mirror: got %q want Updating", got)
	}
	if op := cs.OMENative.InstanceStatuses[0].Operation; op == nil || op.Step != updateStepSurge {
		t.Errorf("Operation mirror: got %+v, want Step=Surge", op)
	}

	// Subsequent MutateInstance must UPDATE in place — no duplicate slot.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), p, v1beta1.EngineComponent, 0, "rev-1", 1); err != nil {
		t.Fatalf("second MutateInstance: %v", err)
	}
	cs = isvc.Status.Components[v1beta1.EngineComponent]
	if len(cs.OMENative.InstanceStatuses) != 1 {
		t.Fatalf("InstanceStatuses len=%d want 1 (mirror should UPDATE in place, not append)", len(cs.OMENative.InstanceStatuses))
	}
	if got := cs.OMENative.InstanceStatuses[0].ActiveOrdinal; got != 1 {
		t.Errorf("ActiveOrdinal mirror: got %d want 1", got)
	}
	if got := cs.OMENative.InstanceStatuses[0].Phase; got != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase mirror after promote: got %q want Ready", got)
	}
	if op := cs.OMENative.InstanceStatuses[0].Operation; op != nil {
		t.Errorf("Operation should be cleared in mirror: got %+v", op)
	}
}

// TestSurgeUpdate_BumpDuringBump_ConvergesToFinalRev pins the X-2
// regression (SRT real-cluster suite): a second spec bump landing while
// a first surge cycle is still mid-flight must NOT silently re-target
// the in-flight surge to the newer revision. The in-flight surge is
// committed to its recorded TargetRevision (rev2). When it promotes,
// the next reconcile observes RunningRevision=rev2 != target.Name=rev3
// and fires a fresh surge cycle that drives the pod to rev3.
//
// Without the fix (the production bug shape): the in-flight surge pod
// (labeled rev2-hash) is promoted with status saying RunningRevision=
// rev3, so detectUpdateTrigger's fast-path sees s.RunningRevision ==
// target.Name and short-circuits. The pod stays on rev2 forever; the
// cluster shows pods on rev1+rev2 with status claiming rev3 — the
// "5 engine pods, none on v3" symptom from the X-2 capture.
func TestSurgeUpdate_BumpDuringBump_ConvergesToFinalRev(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// Steady state: one pod at ord=0 on rev1.
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}

	// Bump 1: rev2 becomes the target. Run surgeUpdate's first pass
	// (Phase 1 entry + create surge pod at ord=1 stamped rev2-hash).
	rev2 := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpecImage("llama:v2"))
	if _, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("first surge cycle: %v", err)
	}

	// Sanity: surge pod for rev2 exists at ord=1, status shows
	// TargetRevision=rev2-name, Step=Surge.
	rev2SurgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2Surge); err != nil {
		t.Fatalf("rev2 surge pod missing after first cycle: %v", err)
	}
	if got := rev2Surge.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(rev2.Name) {
		t.Fatalf("rev2 surge pod hash label: got %q want %q",
			got, query.RevisionHashFromControllerRevisionName(rev2.Name))
	}
	s := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Fatalf("after bump 1: TargetRevision should be rev2 (%q), got %+v", rev2.Name, s.Operation)
	}

	// Bump 2 arrives BEFORE the surge has progressed past Step=Surge.
	// The dispatcher's next call to surgeUpdate is given a fresh target
	// (rev3) and the same in-flight surge pod (rev2-hash). The fix
	// keeps the surge committed to rev2 — the pod is on rev2, the
	// recorded TargetRevision stays rev2, and the eventual promote
	// writes RunningRevision=rev2 (not rev3).
	rev3 := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpecImage("llama:v3"))
	// Make the surge pod ContainersReady so Phase 1 advances to Phase 2.
	rev2Surge.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), rev2Surge); err != nil {
		t.Fatalf("flip rev2 surge to ContainersReady: %v", err)
	}

	// Cycle 2 with rev3 as the parameter target. The fix should keep
	// surgeTargetName pinned to rev2 (from Op.TargetRevision).
	if _, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], rev3,
		[]*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("second surge cycle: %v", err)
	}

	// Recorded TargetRevision must still be rev2 (NOT rev3). This is
	// the load-bearing assertion: without the fix, the helper
	// overwrites TargetRevision to rev3 because the parameter target
	// changed, and the next promote writes RunningRevision=rev3 with
	// the pod still on rev2.
	s = isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Fatalf("after bump 2 mid-surge: TargetRevision must stay rev2 (%q), got %+v",
			rev2.Name, s.Operation)
	}
	if s.TargetRevision != rev2.Name {
		t.Errorf("InstanceStatus.TargetRevision: got %q want %q (pinned to in-flight surge)",
			s.TargetRevision, rev2.Name)
	}

	// CRITICAL: no second surge pod was created for rev3 (would be a
	// second pod at ord=1, which is impossible — single slot). Pod
	// count must remain 2 (old at ord=0, surge at ord=1 on rev2).
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("mid-bump-during-bump pod count: got %d (%v) want 2 (old@0 + surge@1)",
			len(pods.Items), names)
	}

	// Simulate the surge cycle for rev2 completing: surge pod is now
	// serving, old is drained and deleted.
	if err := c.Delete(context.Background(), oldPod); err != nil {
		t.Fatalf("delete old pod (drain complete): %v", err)
	}
	// Re-fetch to get the freshest RV before mutating status — prior
	// surgeUpdate passes may have written serving=False on the pod.
	rev2SurgeFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2SurgeFresh); err != nil {
		t.Fatalf("re-read rev2 surge before flip Serving: %v", err)
	}
	rev2SurgeFresh.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), rev2SurgeFresh); err != nil {
		t.Fatalf("flip rev2 surge to Serving: %v", err)
	}

	// Cycle 3 — Phase 3 promote. Must write RunningRevision=rev2 (NOT
	// rev3). Even though target=rev3, the pinned surge target is rev2.
	if _, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], rev3,
		[]*corev1.Pod{rev2SurgeFresh}); err != nil {
		t.Fatalf("third surge cycle (promote): %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read after promote: %v", err)
	}
	s = fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.RunningRevision != rev2.Name {
		t.Errorf("post-promote RunningRevision: got %q want %q (must match the pod's actual rev)",
			s.RunningRevision, rev2.Name)
	}
	if s.ActiveOrdinal != 1 {
		t.Errorf("post-promote ActiveOrdinal: got %d want 1", s.ActiveOrdinal)
	}
	// Mirror should reflect the promote so the next reconcile's
	// detectUpdateTrigger sees RunningRevision=rev2 != target=rev3 and
	// fires a fresh surge for rev3.
	s = isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.RunningRevision != rev2.Name {
		t.Errorf("in-memory mirror RunningRevision: got %q want %q",
			s.RunningRevision, rev2.Name)
	}
}

// TestSurgeUpdate_SingleBump_DoesNotShiftTarget pins the A-2 invariant:
// for a single bump (no mid-surge bump-2), the recorded
// TargetRevision and Operation.TargetRevision must remain stable
// across every reconcile pass of the surge cycle. The recorded values
// must match the parameter target.Name throughout — no drift, no
// stale-pointer corruption.
//
// Guards against the symptom seen in the A-2 capture: "UpdateRevision
// != CurrentRevision, both post-bump but different, one of UR/CR
// points at a hash with no backing CR". The fix's pinning logic must
// not mis-attribute the parameter target as drift on the very first
// reconcile (where there's no in-flight Op yet); subsequent passes
// must continue to use the same target (which equals what was stamped
// on the first pass).
func TestSurgeUpdate_SingleBump_DoesNotShiftTarget(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}

	// Single bump: rev2 is the only target across all passes.
	rev2 := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpecImage("llama:v2"))

	// Pass 1: Phase 1 entry — creates surge pod.
	if _, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], rev2,
		[]*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	s := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Fatalf("pass 1 TargetRevision: got %+v want rev2 (%q)", s.Operation, rev2.Name)
	}
	if s.TargetRevision != rev2.Name {
		t.Errorf("pass 1 InstanceStatus.TargetRevision: got %q want %q", s.TargetRevision, rev2.Name)
	}

	// Pass 2: surge pod is ContainersReady but not yet serving — Phase
	// 2 transition (Step=Surge → SurgeDrain). Re-read the rev2 surge
	// pod from the client and seed ContainersReady.
	rev2SurgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2Surge); err != nil {
		t.Fatalf("get rev2 surge after pass 1: %v", err)
	}
	rev2Surge.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), rev2Surge); err != nil {
		t.Fatalf("flip rev2 surge ContainersReady: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], rev2,
		[]*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	s = isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Fatalf("pass 2 TargetRevision drift: got %+v want rev2 (%q)", s.Operation, rev2.Name)
	}

	// Pass 3: drain complete, old gone, surge serving — Phase 3
	// promote. RunningRevision must be rev2 (the only target this
	// cycle ever saw).
	if err := c.Delete(context.Background(), oldPod); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	rev2SurgeFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2SurgeFresh); err != nil {
		t.Fatalf("re-read rev2 surge before flip Serving: %v", err)
	}
	rev2SurgeFresh.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), rev2SurgeFresh); err != nil {
		t.Fatalf("flip rev2 surge Serving: %v", err)
	}
	done, err := surgeUpdate(context.Background(), p, plan, plan.Instances[0], rev2,
		[]*corev1.Pod{rev2SurgeFresh})
	if err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	if !done {
		t.Fatalf("pass 3 should return done=true after promote")
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read after promote: %v", err)
	}
	s = fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.RunningRevision != rev2.Name {
		t.Errorf("post-promote RunningRevision: got %q want %q (single bump must land exactly on rev2)",
			s.RunningRevision, rev2.Name)
	}
	// Pod count exactly 1 (the promoted surge pod). No extras.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Errorf("post-promote pod count: got %d (%v) want 1", len(pods.Items), names)
	}
}

// TestPatchInstanceStatusSurgingForUpdate_IdempotentOnSurgeDrainStep
// pins the helper's idempotency for the SurgeDrain phase. Re-invoking
// with the same target on an instance already at Step=SurgeDrain must
// NOT regress the Step back to Surge — that would lose track of which
// half of the surge cycle is in flight and let the dispatcher fire a
// fresh drain after the previous drain had already started.
//
// The bug shape it guards against: surgeUpdate calls
// patchInstanceStatusSurgingForUpdate at the top of every pass. If the
// helper only idempotency-skipped on Step==Surge, the second pass
// (after the previous pass had transitioned to SurgeDrain) would
// overwrite the Operation with a fresh Step=Surge stamp — corrupting
// the state machine.
func TestPatchInstanceStatusSurgingForUpdate_IdempotentOnSurgeDrainStep(t *testing.T) {
	resetExpectations(t)
	isvc := surgeISVCWithStrategy("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	// Stamp Step=Surge first, then transition to Step=SurgeDrain.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), p, v1beta1.EngineComponent, 0, "rev-x", 30*time.Second); err != nil {
		t.Fatalf("stamp Surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), p, v1beta1.EngineComponent, 0); err != nil {
		t.Fatalf("stamp SurgeDrain: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	beforeOpID := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Operation.ID

	// Re-invoke with the same target — must be a no-op.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), p, v1beta1.EngineComponent, 0, "rev-x", 30*time.Second); err != nil {
		t.Fatalf("re-stamp Surge (idempotency check): %v", err)
	}
	afterRV := readRV(t, c, isvc)
	if beforeRV != afterRV {
		t.Errorf("idempotent re-invoke bumped RV: %s -> %s", beforeRV, afterRV)
	}
	s := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil {
		t.Fatalf("Operation cleared by idempotent re-invoke")
	}
	if s.Operation.Step != updateStepSurgeDrain {
		t.Errorf("Step regressed to %q on idempotent re-invoke; want SurgeDrain", s.Operation.Step)
	}
	if s.Operation.ID != beforeOpID {
		t.Errorf("Operation.ID changed across idempotent re-invoke: %q -> %q (would lose timing baselines)",
			beforeOpID, s.Operation.ID)
	}
}

// Compile-time sanity: ensure the test file uses the metav1 import (the
// linter complains otherwise on imports added defensively).
var _ = metav1.NewTime
