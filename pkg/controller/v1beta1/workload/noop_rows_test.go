package workload_test

// Op-less rows: an index with no status row (Empty), a paused row that
// lost its pods (Pending) and a converged row (Ready) all carry no
// attempt. Most of what reaches them — a pod condition flipping, a
// PodGroup verdict, an operator edit the revision payload filters out —
// therefore has no reader, and the pass must come out of it having
// written nothing. These tests are the proof of that inertness, one
// event shape at a time.

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// TestEscalation_GangFailedReadingOnAnOpLessRowIsInert: the gang
// scheduler's Failed phase is absorbing, so the group is deleted and
// rebuilt by the PodGroup pass — that is where the reset and the
// withheld members live. On the row itself the reading has nothing to
// act on: an op-less row holds no clock to park and no attempt to end,
// whether it has no status yet, is parked at Pending, or is converged.
//
// Cells: instance/Empty/gang.podGroup[PhaseFailed]/before, instance/Pending/gang.podGroup[PhaseFailed]/before, instance/Ready/gang.podGroup[PhaseFailed]/before
func TestEscalation_GangFailedReadingOnAnOpLessRowIsInert(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []workload.InstanceStatus
	}{
		{"empty index", nil},
		{"pending row", []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhasePending, PodCount: 2}}},
		{"ready row", []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseReady, PodCount: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture(tc.rows)
			input.Gangs = gangObservations(0, workload.GangStateFailed, gangFailedMessage)

			if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			if len(rec.store) != len(tc.rows) {
				t.Fatalf("rows: got %d want %d (the reading must not create or drop one)", len(rec.store), len(tc.rows))
			}
			for i := range rec.store {
				if rec.store[i].Phase != tc.rows[i].Phase {
					t.Errorf("Phase: got %q want %q", rec.store[i].Phase, tc.rows[i].Phase)
				}
				if rec.store[i].Operation != nil {
					t.Errorf("Operation: got %+v want none (an op-less row has no clock to park)", rec.store[i].Operation)
				}
				if rec.store[i].LastFailure != nil {
					t.Errorf("LastFailure: got %+v want none (the group resets itself)", rec.store[i].LastFailure)
				}
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none (no revision is blamed)", rec.blocks)
			}
		})
	}
}

// opLessRowFixture is a paused Component holding the given rows, so the
// lifecycle passes are parked and whatever the pass does write is the
// event's own doing.
func opLessRowFixture(t *testing.T, rows []workload.InstanceStatus) (workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *testAtomicMutationStore) {
	t.Helper()
	in := minimalInput(t)
	in.DesiredSpec.Paused = true
	in.ObservedState.UpdateRevision = opLessRowRevision
	in.ObservedState.CurrentRevision = opLessRowRevision
	in.ObservedState.InstanceStatuses = rows
	store := installTestAtomicMutationStore(&in)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: opLessRowRevision}}
	plan := minimalPlan()
	plan.Paused = true
	return in, plan, target, store
}

// opLessRowRevision is the revision every op-less fixture row is both
// running and targeting, so nothing about the revision is in question.
const opLessRowRevision = "llama-70b-engine-noop0001"

// TestPendingRow_PodObservationsAreInert: a Pending row is a
// status-only demotion — it holds no operation and claims no pods, and
// it exists only under pause, which is the reconcile shape the truth
// pass that writes it runs on. A pod of that index turning ready,
// entering or leaving the serving rotation, or disappearing altogether
// therefore reaches nothing: the promote is the Create pass re-deriving
// the whole set once the pause clears, not any one of these
// observations.
//
// Cells: instance/Pending/pod.ready[source]/before, instance/Pending/pod.servingOn[source]/before, instance/Pending/pod.servingOff[source]/before, instance/Pending/pod.deleted[source]/before
func TestPendingRow_PodObservationsAreInert(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange puts the index's pod into the observed shape.
		arrange func(*testing.T, *recoveryHarness)
		want    int
	}{
		{name: "pod ready", arrange: func(*testing.T, *recoveryHarness) {}, want: 1},
		{name: "pod serving on", arrange: func(t *testing.T, h *recoveryHarness) {
			h.flipServing(t, 0, corev1.ConditionTrue)
		}, want: 1},
		{name: "pod serving off", arrange: func(t *testing.T, h *recoveryHarness) {
			h.flipServing(t, 0, corev1.ConditionFalse)
		}, want: 1},
		{name: "pod deleted", arrange: func(_ *testing.T, h *recoveryHarness) { h.losePods(0) }, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			if !h.run(30, func() bool { return h.allReady(1) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never converged")
			}
			h.desired.Paused = true
			h.forcePhase(t, 0, workload.InstancePhasePending)
			tc.arrange(t, h)

			h.step()

			got := h.instance(0)
			if got == nil || got.Phase != workload.InstancePhasePending || got.Operation != nil {
				t.Fatalf("row: got %+v want Pending with no operation", got)
			}
			if n := len(h.podsOf(0)); n != tc.want {
				t.Errorf("pods: got %d want %d (the observation creates and deletes nothing)", n, tc.want)
			}
		})
	}
}

// forcePhase stamps a phase onto an existing row through the harness's
// own status round-trip, standing in for the truth pass that writes it.
func (h *recoveryHarness) forcePhase(t *testing.T, idx int32, phase workload.InstancePhase) {
	t.Helper()
	if err := h.mutateInstance()(h.ctx, idx, func(s *workload.InstanceStatus) bool {
		s.Phase = phase
		return true
	}); err != nil {
		t.Fatalf("force phase %q on instance %d: %v", phase, idx, err)
	}
}

// flipServing sets the controller-owned serving gate on the index's
// single pod, the condition the load-balancer rotation is keyed on.
func (h *recoveryHarness) flipServing(t *testing.T, idx int32, status corev1.ConditionStatus) {
	t.Helper()
	pods := h.podsOf(idx)
	if len(pods) != 1 {
		t.Fatalf("pods of instance %d: got %d want 1", idx, len(pods))
	}
	setPodCondition(pods[0], query.ServingConditionType, status, h.clk.Now())
	if err := h.c.Status().Update(h.ctx, pods[0]); err != nil {
		t.Fatalf("flip the serving gate: %v", err)
	}
}

// TestOpLessRows_ExcludedAnnotationEditStartsNoWork: an edit to an
// annotation the revision payload filters out leaves the hash where it
// was, so the target does not move and no pass has anything new to act
// on. The hash check is half the claim — without it a pass that wrote
// nothing would only mean the edit never reached the renderer; the
// paused fixture is the other half, isolating the edit from the
// lifecycle work an unpaused index would do anyway.
//
// Cells: instance/Empty/spec.annotationOnly/before, instance/Pending/spec.annotationOnly/before, instance/Ready/spec.annotationOnly/before
func TestOpLessRows_ExcludedAnnotationEditStartsNoWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []workload.InstanceStatus
		want workload.InstancePhase
	}{
		{"empty index", nil, ""},
		{"pending row", []workload.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workload.InstancePhasePending}}, workload.InstancePhasePending},
		{"ready row", []workload.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, PodCount: 1, RunningRevision: opLessRowRevision}}, workload.InstancePhaseReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, plan, target, store := opLessRowFixture(t, tc.rows)

			before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
			after := &metav1.ObjectMeta{Annotations: map[string]string{
				"ome.io/base-model-name":                   "llama-7b",
				constants.ReleaseHeldRevisionAnnotationKey: "llama-70b-engine-old00001",
			}}
			hBefore, _, err := revision.Hash(in.DesiredSpec.PodSpec, before, nil, "")
			if err != nil {
				t.Fatalf("hash before the edit: %v", err)
			}
			hAfter, _, err := revision.Hash(in.DesiredSpec.PodSpec, after, nil, "")
			if err != nil {
				t.Fatalf("hash after the edit: %v", err)
			}
			if hBefore != hAfter {
				t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
			}
			in.DesiredSpec.PodTemplateObjectMeta = after

			c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
			if _, err := workload.Reconcile(context.Background(), workload.Deps{Client: c, APIReader: c}, in, plan, target); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if store.writes != 0 {
				t.Errorf("status writes: got %d want 0", store.writes)
			}
			got := store.status(0)
			if tc.want == "" {
				if got != nil {
					t.Errorf("row: got %+v want none (the edit plans nothing for an empty index)", got)
				}
			} else if got == nil || got.Phase != tc.want || got.Operation != nil {
				t.Errorf("row: got %+v want %q with no operation", got, tc.want)
			}
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace(in.Key.Namespace)); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != 0 {
				t.Errorf("pods: got %d want none", len(pods.Items))
			}
		})
	}
}

// TestUnpause_EmptyIndexIsMaterializedLikeAPendingRow: clearing the
// pause lets the Create pass materialize a planned index whose pods are
// missing. An index with no status row at all takes the same path a
// demoted Pending row takes — nothing about the unpause is special to
// having a row.
//
// Cells: instance/Empty/spec.unpause/before
func TestUnpause_EmptyIndexIsMaterializedLikeAPendingRow(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.desired.Paused = true

	h.step()
	if s := h.instance(0); s != nil {
		t.Fatalf("paused pass materialized the index: %+v", s)
	}
	if pods := h.livePods(); len(pods) != 0 {
		t.Fatalf("paused pass created %d pod(s)", len(pods))
	}

	h.desired.Paused = false
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("after unpause")
		t.Fatalf("the index was never materialized after the unpause")
	}
}

// TestReadyRow_ServingGateFlipsStampNoTransition: the serving gate is
// the load-balancer rotation, not the Instance lifecycle. A pod entering
// it is what the Create promote already wrote, and a pod leaving it
// drops the serving count the coordination gate reads — but neither is
// a per-Instance transition, so the converged row keeps its phase and
// stays operation-free through both.
//
// Cells: instance/Ready/pod.servingOn[source]/before, instance/Ready/pod.servingOff[source]/before
func TestReadyRow_ServingGateFlipsStampNoTransition(t *testing.T) {
	for _, serving := range []bool{true, false} {
		name := "serving off"
		if serving {
			name = "serving on"
		}
		t.Run(name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			if !h.run(30, func() bool { return h.allReady(1) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never converged")
			}
			before := h.instance(0)

			status := corev1.ConditionFalse
			if serving {
				status = corev1.ConditionTrue
			}
			h.flipServing(t, 0, status)

			h.step()

			got := h.instance(0)
			if got == nil || got.Phase != workload.InstancePhaseReady || got.Operation != nil {
				t.Fatalf("row: got %+v want Ready with no operation", got)
			}
			if got.Incarnation != before.Incarnation {
				t.Errorf("Incarnation: got %d want %d (no repair is armed)", got.Incarnation, before.Incarnation)
			}
			if got.RunningRevision != before.RunningRevision {
				t.Errorf("RunningRevision: got %q want %q", got.RunningRevision, before.RunningRevision)
			}
		})
	}
}
