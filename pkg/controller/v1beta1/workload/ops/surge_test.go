package ops

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Tests for the per-Instance SurgeThenDrain state machine. Each test in
// this file maps to one of the named bug-fix tests in PR #243, #246,
// #256, #261 — the tests moved from
// pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/ops/surge_test.go
// when the per-Instance state machine itself moved to workload/ops in
// part 2 slice 4f of N. Names are byte-equal to the upstream omenative
// copies so the prior bug-coverage map carries over unchanged.

// surgePodAtOrdinal builds a fixture pod for a SurgeThenDrain test. Same
// shape as legacyPodAtIncarnation but stamps LabelPodOrdinal explicitly
// so partitionPodsBySurgeOrdinal can place it in the right bucket.
func surgePodAtOrdinal(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ordinal int32, ready, serving bool) *corev1.Pod {
	pod := legacyPodAtIncarnation(isvc, instanceIdx, incarnation, ready, serving)
	pod.Name = query.PodName(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", ordinal)
	pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "test:v1"}}
	return pod
}

// surgeISVCMultiInstance builds a 2-replica ISVC with SurgeThenDrain
// configured and an InstanceStatus for each (Phase=Ready,
// ActiveOrdinal=0).
func surgeISVCMultiInstance(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := legacyMinimalISVC(name, ns, 2)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
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

// readRV returns the ResourceVersion stamped on the persisted ISVC.
// Used by idempotency tests to confirm a re-invocation didn't bump RV.
func readRV(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) string {
	t.Helper()
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	return fresh.ResourceVersion
}

// makeCR fabricates a ControllerRevision pre-created in the fake client
// at the given name. The Data payload is left empty; the surge state
// machine only reads target.Name + (in inPlaceUpdate, via
// loadControllerRevisionPayload) target.Data — surge tests don't go
// through in-place so the empty payload is fine.
func makeCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, name string) *appsv1.ControllerRevision {
	t.Helper()
	cr := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: isvc.Namespace,
		},
	}
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create CR %s: %v", name, err)
	}
	return cr
}

// surgePlan builds the single-Instance ComponentPlan a SurgeThenDrain
// test consumes.
func surgePlan() workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}
}

// surgeISVCReady is the steady-state Instance the surge tests start
// from — Phase=Ready at the given incarnation, no pods alive yet (the
// test seeds them).
func surgeISVCReady(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := legacyMinimalISVC(name, ns, 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	return isvc
}

// TestPatchInstanceStatusSurgingForUpdate_IdempotentOnSurgeDrainStep
// pins the surge entry helper's idempotency on the SurgeDrain step (PR
// #246). The first pass through Phase 2 transitions Step from Surge to
// SurgeDrain; on a subsequent pass the entry-point stamp must NOT
// regress that Step back to Surge (which would re-fire the create branch
// and resurrect the just-deleted old pod), and the Operation.ID must
// stay stable so timing baselines don't reset.
func TestPatchInstanceStatusSurgingForUpdate_IdempotentOnSurgeDrainStep(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)

	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", 30*time.Second); err != nil {
		t.Fatalf("stamp Surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), input, 0); err != nil {
		t.Fatalf("stamp SurgeDrain: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	beforeOpID := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Operation.ID

	// Re-invoke with the same target — must be a no-op.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", 30*time.Second); err != nil {
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

// TestMutateInstance_MirrorsCommittedStateOntoInMemoryISVC pins the
// MutateInstance mirror requirement (#246). After a MutateInstance
// closure commits a status change to the apiserver, the closure must
// ALSO mirror the new state onto the in-memory ISVC the test (and
// production callers in the same reconcile) hold a pointer to —
// without the mirror, the next pass within the same reconcile reads
// the stale pre-mutation status.
func TestMutateInstance_MirrorsCommittedStateOntoInMemoryISVC(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)

	// Commit a Phase transition.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("MutateInstance: %v", err)
	}

	// In-memory ISVC pointer should reflect the new ActiveOrdinal=1
	// without a re-Get. The omenative status.MutateInstance wraps the
	// apiserver round-trip and ALSO writes the result back onto the
	// in-memory mirror; the workload-side MutateInstance callback
	// preserves that contract by going through the same omenative
	// status writer.
	s := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.ActiveOrdinal != 1 {
		t.Errorf("ActiveOrdinal mirror: in-memory got %d want 1 (committed)", s.ActiveOrdinal)
	}
	if s.RunningRevision != "rev-abc" {
		t.Errorf("RunningRevision mirror: in-memory got %q want %q", s.RunningRevision, "rev-abc")
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase mirror: in-memory got %q want Ready", s.Phase)
	}
}

// TestExpectedPodNamesForInstance_RespectsActiveOrdinal pins the
// primary SurgeThenDrain drain-incomplete fix (#243): when a single-pod
// Instance has a status entry whose ActiveOrdinal has been advanced by
// a prior surge promote (e.g., from 0 → 1), expectedPodNamesForInstance
// must emit a single target at THAT ordinal — NOT hard-code 0.
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
			isvc := legacyMinimalISVC("llama-70b", "prod", 1)
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
			input := legacyTestInput(isvc, nil, v1beta1.EngineComponent)
			plan := surgePlan()
			inst := workload.InstancePlan{
				Index:       0,
				Incarnation: 1,
				Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
			}
			got := expectedPodNamesForInstance(input, plan, inst)
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

// TestExpectedPodNamesForInstance_StaleActiveOrdinalDoesNotResurrect
// pins the post-promote ord-resurrection invariant (PR #246): after a
// surge promote stamps ActiveOrdinal=1, the next steady-state Create
// pass must NOT see "ordinal 0 is missing" and resurrect a pod there.
// Walking the function with a status carrying ActiveOrdinal=1 must
// emit exactly one target at ordinal 1.
func TestExpectedPodNamesForInstance_StaleActiveOrdinalDoesNotResurrect(t *testing.T) {
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	// Simulate post-promote state: ActiveOrdinal=1.
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].ActiveOrdinal = 1

	input := legacyTestInput(isvc, nil, v1beta1.EngineComponent)
	plan := surgePlan()
	inst := workload.InstancePlan{
		Index:       0,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
	}
	got := expectedPodNamesForInstance(input, plan, inst)
	if len(got) != 1 {
		t.Fatalf("len(targets): got %d want 1 (single-pod must not resurrect ord 0)", len(got))
	}
	if got[0].Ordinal != 1 {
		t.Errorf("Ordinal: got %d want 1 (the post-promote canonical slot)", got[0].Ordinal)
	}
}

// TestExpectedPodNamesForInstance_MultiPodPreservesAllOrdinals pins
// that the ActiveOrdinal handling is a single-pod-only branch; gang
// Runners (Size > 1) keep emitting every 0..Size-1 ordinal.
func TestExpectedPodNamesForInstance_MultiPodPreservesAllOrdinals(t *testing.T) {
	isvc := legacyMinimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, ActiveOrdinal: 1, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	input := legacyTestInput(isvc, nil, v1beta1.EngineComponent)
	plan := surgePlan()
	inst := workload.InstancePlan{
		Index:       0,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 4}},
	}
	got := expectedPodNamesForInstance(input, plan, inst)
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

// TestReclassifyByRevisionHash_KeepsValidSurge pins the recheck's
// no-op contract (PR #261 part 1): a surge pod labeled with the
// in-flight target (or unrelated label) must NOT be moved into the
// drain bucket.
func TestReclassifyByRevisionHash_KeepsValidSurge(t *testing.T) {
	mk := func(rev string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{query.LabelRevisionHash: rev},
			},
		}
	}
	validSurge := mk("rev3")
	unrelated := mk("synthetic")
	missingLabel := &corev1.Pod{}

	// runningRev=rev2, targetHash=rev3 — the normal mid-cycle state.
	surge, drain := reclassifyByRevisionHash(
		[]*corev1.Pod{validSurge, unrelated, missingLabel},
		nil, "rev3", "rev2",
	)
	if len(drain) != 0 {
		t.Errorf("drain bucket should be empty (no stale labels); got %d", len(drain))
	}
	if len(surge) != 3 {
		t.Errorf("surge bucket should keep all three pods; got %d", len(surge))
	}

	// runningRev="" (first-ever surge) — recheck is a no-op even with
	// a fully mismatched label.
	mismatched := mk("anything")
	surge2, drain2 := reclassifyByRevisionHash(
		[]*corev1.Pod{mismatched}, nil, "rev3", "",
	)
	if len(drain2) != 0 || len(surge2) != 1 {
		t.Errorf("empty runningRev should leave buckets untouched; surge=%d drain=%d",
			len(surge2), len(drain2))
	}
}

// TestReclassifyByRevisionHash_DrainsStaleByRunningRev pins the
// recheck's primary purpose (PR #261 part 2): a pod labeled with the
// just-promoted RunningRevision but sitting in the surge bucket MUST
// be moved to drain so Phase 2 deletes it.
func TestReclassifyByRevisionHash_DrainsStaleByRunningRev(t *testing.T) {
	mk := func(rev string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{query.LabelRevisionHash: rev},
			},
		}
	}
	staleRev2 := mk("rev2")
	validRev3 := mk("rev3")

	surge, drain := reclassifyByRevisionHash(
		[]*corev1.Pod{staleRev2, validRev3},
		[]*corev1.Pod{}, "rev3", "rev2",
	)
	if len(drain) != 1 || drain[0] != staleRev2 {
		t.Errorf("drain bucket should contain the rev2-labeled stale pod; got %d entries", len(drain))
	}
	if len(surge) != 1 || surge[0] != validRev3 {
		t.Errorf("surge bucket should keep only the rev3-labeled valid surge; got %d entries", len(surge))
	}
}

// TestSurgeUpdate_PostPromoteCreateDoesNotResurrectDrainedOrdinal
// pins the integration-shaped surge-cycle invariant (PR #243). After
// surge has completed (drain + promote), the next reconcile pass that
// lands in Create must NOT recreate a pod at the now-drained ordinal
// slot.
func TestSurgeUpdate_PostPromoteCreateDoesNotResurrectDrainedOrdinal(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	tcr := makeCR(t, c, isvc, "llama-70b-engine-rev-abc12345")
	// Promote the surge: bumps ActiveOrdinal to 1 and stamps RunningRevision.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("patch promote: %v", err)
	}
	// Surge pod at ordinal 1, runtime-ready and serving.
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	if err := c.Create(context.Background(), surgePod); err != nil {
		t.Fatalf("seed surge pod: %v", err)
	}

	// Re-read ISVC so the input has the promoted status (ActiveOrdinal=1).
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(fresh, c, v1beta1.EngineComponent)
	plan := surgePlan()

	// Drive Create as the dispatcher would after Update returns done=true.
	if _, err := Create(context.Background(), workload.Deps{Client: c}, input2, plan, tcr); err != nil {
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

// TestSurgeUpdate_MultiInstance_NoOrdResurrectionAfterPromote pins
// the multi-instance follow-up to PR #243 (PR #246). Even when BOTH
// instances have been promoted to ActiveOrdinal=1, the next Create
// pass must not see "ordinal 0 is missing" for either instance and
// resurrect a drained slot.
func TestSurgeUpdate_MultiInstance_NoOrdResurrectionAfterPromote(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCMultiInstance("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	tcr := makeCR(t, c, isvc, "llama-70b-engine-rev-abc12345")

	// Simulate post-promote state for BOTH instances: ActiveOrdinal=1.
	for _, idx := range []int32{0, 1} {
		if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, idx, tcr.Name, 1); err != nil {
			t.Fatalf("patch promote instance %d: %v", idx, err)
		}
		surgePod := surgePodAtOrdinal(isvc, idx, 1, 1, true, true)
		if err := c.Create(context.Background(), surgePod); err != nil {
			t.Fatalf("seed surge pod for instance %d: %v", idx, err)
		}
	}

	// Re-read ISVC so input has the post-promote state for both instances.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(fresh, c, v1beta1.EngineComponent)
	plan := workload.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  2,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		InstanceReadyTimeout: 30 * time.Minute,
	}

	if _, err := Create(context.Background(), workload.Deps{Client: c}, input2, plan, tcr); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// CRITICAL: there must STILL be only 2 pods — one per Instance at ordinal 1.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("Create resurrected stale ordinals across instances! got %d pods (%v), want 2", len(pods.Items), names)
	}
}

// TestSurgeUpdate_MultiInstance_InterleavedPromotes pins the
// post-promote mirror behavior under interleaved promotes (PR #246).
// Promoting instance 0 and then promoting instance 1 in the same
// reconcile must NOT lose the ActiveOrdinal bump on either instance.
func TestSurgeUpdate_MultiInstance_InterleavedPromotes(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCMultiInstance("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	tcr := makeCR(t, c, isvc, "llama-70b-engine-rev-abc12345")

	// Promote instance 0.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("promote 0: %v", err)
	}
	// Build a fresh input from the in-memory ISVC (mirror should have
	// updated it). Promote instance 1.
	input2 := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input2, 1, tcr.Name, 1); err != nil {
		t.Fatalf("promote 1: %v", err)
	}

	// Both ActiveOrdinals must be 1 on the persisted status.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	for _, idx := range []int32{0, 1} {
		found := false
		for _, s := range fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses {
			if s.Index == idx {
				found = true
				if s.ActiveOrdinal != 1 {
					t.Errorf("instance %d ActiveOrdinal: got %d want 1", idx, s.ActiveOrdinal)
				}
				if s.RunningRevision != tcr.Name {
					t.Errorf("instance %d RunningRevision: got %q want %q", idx, s.RunningRevision, tcr.Name)
				}
			}
		}
		if !found {
			t.Errorf("instance %d missing from status", idx)
		}
	}
}

// TestSurgeUpdate_BumpDuringBump_ConvergesToFinalRev pins the
// bump-during-bump target-stability invariant (PR #256). A mid-surge
// spec bump to a new rev must NOT corrupt the in-flight surge — the
// surge in progress continues to drive to its pinned target, and the
// next surge cycle starts naturally for the newer rev after the
// in-flight one promotes.
func TestSurgeUpdate_BumpDuringBump_ConvergesToFinalRev(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()

	// Old pod at ord=0.
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")

	// Pass 1 against rev2: stamps Op{Surge, rev2}, creates surge at ord=1.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("pass 1 (rev2 surge create): %v", err)
	}

	// User bumps to rev3 mid-surge.
	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")

	// Re-read so input observes the recorded Op{TargetRevision=rev2}.
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(freshISVC, c, v1beta1.EngineComponent)
	s := freshISVC.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Fatalf("expected recorded Op.TargetRevision=rev2 after pass 1, got %+v", s.Operation)
	}

	// Pass 2 against rev3 (the bump-during-bump). surgeUpdate should
	// pin to the in-flight rev2 — NOT drive to rev3 yet.
	rev2SurgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2Surge); err != nil {
		t.Fatalf("rev2 surge pod missing: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input2, plan, plan.Instances[0], rev3, []*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("pass 2 (mid-surge rev3 bump): %v", err)
	}

	freshISVC2 := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC2); err != nil {
		t.Fatalf("re-read ISVC after pass 2: %v", err)
	}
	s = freshISVC2.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	// Op.TargetRevision MUST still be rev2 — surge pinned.
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Errorf("Op.TargetRevision should stay pinned to rev2 across the mid-surge rev3 bump; got %+v", s.Operation)
	}
}

// TestSurgeUpdate_SingleBump_DoesNotShiftTarget pins the second-half
// invariant of #256: a single bump (no in-flight surge) followed by a
// re-invoke at the SAME target must NOT alter the recorded
// Op.TargetRevision — idempotent on the in-flight surge.
func TestSurgeUpdate_SingleBump_DoesNotShiftTarget(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()

	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")

	// First surge pass.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	beforeRV := freshISVC.ResourceVersion

	// Re-invoke at the same target (rev2). The Op.TargetRevision should
	// remain pinned without status churn.
	input2 := legacyTestInput(freshISVC, c, v1beta1.EngineComponent)
	rev2SurgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2Surge); err != nil {
		t.Fatalf("rev2 surge pod missing: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input2, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("pass 2 (idempotent): %v", err)
	}
	freshISVC2 := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC2); err != nil {
		t.Fatalf("re-read after pass 2: %v", err)
	}
	s := freshISVC2.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Errorf("Op.TargetRevision shifted unexpectedly: got %+v want rev2", s.Operation)
	}
	// Note: the surge pod's serving flip + the SurgeDrain stamp DO bump
	// RV (legitimate state-machine progress), so we don't assert
	// strict RV equality across passes here. The TargetRevision pinning
	// is the load-bearing assertion.
	_ = beforeRV
}

// TestSurgeUpdate_PostV2Promote_V2PodsDrainedWhenV3SurgeFires pins the
// X-2 follow-up regression (PR #261, post-#256): after a v1→v2 surge
// cycle COMPLETES with RunningRevision=v2 and ActiveOrdinal=1, a fresh
// spec bump to v3 must drive the Instance to RunningRevision=v3 with
// the v2 pod fully drained.
func TestSurgeUpdate_PostV2Promote_V2PodsDrainedWhenV3SurgeFires(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()

	// Simulate post-v2-promote state: RunningRevision=rev2, ActiveOrdinal=1.
	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, rev2.Name, 1); err != nil {
		t.Fatalf("seed post-v2-promote: %v", err)
	}
	v2Pod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	v2Pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev2.Name)
	if err := c.Create(context.Background(), v2Pod); err != nil {
		t.Fatalf("seed v2 pod: %v", err)
	}
	// Re-read so input observes the post-promote ActiveOrdinal=1 + RunningRevision=rev2.
	input = legacyTestInput(isvc, c, v1beta1.EngineComponent)

	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")

	// Pass 1: Phase 1 entry for the v3 cycle. Stamps Op{Surge, rev3}
	// and creates the v3 surge pod at ord=0.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{v2Pod}); err != nil {
		t.Fatalf("pass 1 (v3 surge create): %v", err)
	}
	rev3SurgeName := query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 0)
	rev3Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev3SurgeName}, rev3Surge); err != nil {
		t.Fatalf("v3 surge pod missing after pass 1: %v", err)
	}
	if got := rev3Surge.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(rev3.Name) {
		t.Errorf("v3 surge pod hash label: got %q want %q", got, query.RevisionHashFromControllerRevisionName(rev3.Name))
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

	// Pass 2: flip v3 surge pod ContainersReady so Phase 1 advances to Phase 2.
	rev3Surge.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), rev3Surge); err != nil {
		t.Fatalf("flip v3 surge ContainersReady: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{v2Pod, rev3Surge}); err != nil {
		t.Fatalf("pass 2 (drain + delete v2): %v", err)
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

	// The v2 pod should be deleted.
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(v2Pod), &corev1.Pod{}); err == nil {
		t.Errorf("v2 pod should be deleted after Phase 2 drain, but still exists")
	}

	// Pass 3: v3 surge promoted.
	rev3SurgeFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev3SurgeName}, rev3SurgeFresh); err != nil {
		t.Fatalf("re-read v3 surge: %v", err)
	}
	rev3SurgeFresh.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), rev3SurgeFresh); err != nil {
		t.Fatalf("flip v3 surge Serving: %v", err)
	}
	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{rev3SurgeFresh})
	if err != nil {
		t.Fatalf("pass 3 (promote): %v", err)
	}
	if !done {
		t.Fatalf("pass 3 should return done=true after promote")
	}

	// Final assertions.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("post-v3-promote pod count: got %d want 1", len(pods.Items))
	}
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read after promote: %v", err)
	}
	s := freshISVC.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.RunningRevision != rev3.Name {
		t.Errorf("post-promote RunningRevision: got %q want %q", s.RunningRevision, rev3.Name)
	}
	if s.ActiveOrdinal != 0 {
		t.Errorf("post-promote ActiveOrdinal: got %d want 0 (alternated back)", s.ActiveOrdinal)
	}
}

// TestSurgeUpdate_StaleRevPodAtSurgeSlot_Drained pins the X-2 follow-up
// invariant for the fix-shape-A drain path (PR #261, task #54). When a
// pod labeled with the just-promoted RunningRevision survives at the
// surge ordinal, the rev-hash recheck must route it through drain +
// delete; Phase 3 must NOT promote on it.
func TestSurgeUpdate_StaleRevPodAtSurgeSlot_Drained(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, rev2.Name, 1); err != nil {
		t.Fatalf("seed post-v2-promote: %v", err)
	}

	canonical := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	canonical.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev2.Name)
	if err := c.Create(context.Background(), canonical); err != nil {
		t.Fatalf("seed canonical: %v", err)
	}

	// STALE pod at ord=0 — also labeled with rev2.
	stale := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	stale.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev2.Name)
	if err := c.Create(context.Background(), stale); err != nil {
		t.Fatalf("seed stale: %v", err)
	}
	// Re-read so input observes the post-v2-promote state.
	input = legacyTestInput(isvc, c, v1beta1.EngineComponent)

	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")

	// Pass 1: drain + delete the stale + canonical pods.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{canonical, stale}); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

	// Both pods should be deleted by Phase 2's drain.
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(canonical), &corev1.Pod{}); err == nil {
		t.Errorf("canonical pod should be deleted, but still exists")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stale), &corev1.Pod{}); err == nil {
		t.Errorf("stale pod at surge slot should be deleted, but still exists")
	}

	// Status MUST still say Updating with TargetRev=rev3 — no promote on a drained slot.
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	s := freshISVC.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating (no promote on a drained slot)", s.Phase)
	}
	if s.Operation == nil || s.Operation.TargetRevision != rev3.Name {
		t.Fatalf("Op.TargetRevision: got %+v want rev3 (%q)", s.Operation, rev3.Name)
	}
	if s.RunningRevision != rev2.Name {
		t.Errorf("RunningRevision: got %q want rev2 (%q) — must NOT advance to rev3 yet",
			s.RunningRevision, rev2.Name)
	}
}

// TestSurgeUpdate_Phase1StampsStepSurgeAndCreatesSurgePod pins the
// entry point: from a steady-state Instance (1 old pod at ordinal 0),
// surgeUpdate stamps Op{Step=Surge, TargetRev} and creates a new pod
// at ordinal 1. The old pod is untouched (still serving). ActiveOrdinal
// stays at 0 — it advances only after promote.
func TestSurgeUpdate_Phase1StampsStepSurgeAndCreatesSurgePod(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, oldPod)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod})
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
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	// Surge pod exists but NOT ContainersReady.
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, false, false)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, oldPod, surgePod)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	// Pre-stamp Step=Surge so we test the "waiting" branch, not the entry.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, tcr.Name, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp: %v", err)
	}

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod, surgePod})
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
// RunningRevision=target, clears Operation, AND advances ActiveOrdinal
// to the new slot.
func TestSurgeUpdate_Phase3PromotesAndAdvancesActiveOrdinal(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	// Only the surge pod exists, runtime-ready and serving (post-Phase 2).
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, surgePod)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	// Status reflects mid-surge state at Step=Drain.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, tcr.Name, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), input, 0); err != nil {
		t.Fatalf("pre-stamp drain: %v", err)
	}

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{surgePod})
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
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	// Simulate post-previous-surge state: ActiveOrdinal=1, pod at ord=1.
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].ActiveOrdinal = 1
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := legacyTargetSpecImage("llama:v3")
	c := legacyNewFakeClient(t, isvc, oldPod)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	_, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod})
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
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)
	plan := surgePlan()
	// Synthesize a multi-pod plan (BuildPlan won't produce this normally,
	// but the guard must hold for any caller that constructs one).
	plan.Instances[0].Runners[0].Size = 4
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	_, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, nil)
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
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	// Legacy old pod — has all the standard labels EXCEPT LabelPodOrdinal.
	legacyOld := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	delete(legacyOld.Labels, query.LabelPodOrdinal)
	surgePodFresh := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)

	old, surge, stragglers := partitionPodsBySurgeOrdinal([]*corev1.Pod{legacyOld, surgePodFresh}, 0, 1)
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
	legacyResetExpectations(t)
	isvc := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)
	input := legacyTestInput(isvc, c, v1beta1.EngineComponent)

	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("first call: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("second call: %v", err)
	}
	afterRV := readRV(t, c, isvc)
	if beforeRV != afterRV {
		t.Errorf("idempotent call bumped ResourceVersion: %s -> %s", beforeRV, afterRV)
	}
}
