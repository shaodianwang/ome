package inferencereplica

// The gang-member-loss rebuild below Ready, driven through the adapter.
//
// Outside Phase=Ready the rebuild trigger compares the live pods of an
// Instance against the pod count the row RECORDS, and that counter is
// written only by the adapter's status publication — never by
// workload.Reconcile. A harness that drives the engine alone can never
// put a complete count on a row whose pods are already gone, so these
// two arrows are covered here, where the published counter is part of
// the persisted status the reconcile reads.

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// gangLossIR is a two-pod Instance (one leader, one worker) under the
// RecreateInstanceOnPodRestart policy, its single row parked at Pending
// with the published pod count reporting the complete gang.
func gangLossIR(name, namespace string) *v1beta1.InferenceReplica {
	ir := baselineIR(name, namespace, 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		}}},
	}
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{
		RestartPolicy: ptr.To(v1beta1.InstanceRestartPolicyRecreateInstance),
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstancePending,
		PodCount:    2,
	}}
	return ir
}

// instanceRow reads the single Instance row back off the stored IR.
func instanceRow(t *testing.T, c client.Client, ir *v1beta1.InferenceReplica) v1beta1.OMENativeInstanceStatus {
	t.Helper()
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if len(fresh.Status.InstanceStatuses) != 1 {
		t.Fatalf("instance statuses: %+v", fresh.Status.InstanceStatuses)
	}
	return fresh.Status.InstanceStatuses[0]
}

// TestRestart_PendingGangMemberTerminal_RebuildsInstance drives the
// rebuild off a terminal member: the row still reports both pods, but
// one of them is Failed and therefore holds no capacity, so the survivor
// is drained and the gang rebuilt at a bumped incarnation.
//
// Arrows: T-pending-terminal-restart
func TestRestart_PendingGangMemberTerminal_RebuildsInstance(t *testing.T) {
	ir := gangLossIR("llama-engine", "prod")
	survivor := podForIR(ir, 0, string(v1beta1.RunnerNameLeader), 0, true, true)
	terminal := podForIR(ir, 0, string(v1beta1.RunnerNameWorker), 0, false, false)
	terminal.Status.Phase = corev1.PodFailed
	r, c := newReconciler(t, ir, survivor, terminal)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	row := instanceRow(t, c, ir)
	if row.Phase != v1beta1.OMENativeInstanceRestarting ||
		row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationRestart || row.Operation.Step != "Drain" {
		t.Fatalf("terminal gang member must open a Restart Drain, got %+v (op %+v)", row, row.Operation)
	}
	if len(eventsContaining(drainEvents(rec), "gang member lost")) == 0 {
		t.Errorf("the RestartTriggered event must name the loss")
	}
}

// TestRestart_PendingGangMemberLost_RebuildsOnceRetryBlockIsDue drives
// the rebuild off the retry clock: the partial gang is rebuilt on the
// pass where the RetryBlock recorded against its revision has come due,
// and is not while the block still denies the revision.
//
// While the block denies, the rebuild is not merely postponed — the
// Create pass fills the missing member instead, because below Ready it
// owns an Instance the rebuild gate declined. That is the behavior the
// held case records.
//
// Arrows: T-pending-gang-restart
func TestRestart_PendingGangMemberLost_RebuildsOnceRetryBlockIsDue(t *testing.T) {
	for _, tc := range []struct {
		name          string
		block         v1beta1.RetryBlockState
		nextRetryAt   *metav1.Time
		wantRestarted bool
	}{
		{name: "due", block: v1beta1.RetryBlockBackoff, nextRetryAt: ptr.To(metav1.NewTime(time.Now().Add(-time.Minute))), wantRestarted: true},
		{name: "held", block: v1beta1.RetryBlockHeld, wantRestarted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := time.Now()
			ir := gangLossIR("llama-engine", "prod")
			survivor := podForIR(ir, 0, string(v1beta1.RunnerNameLeader), 0, true, true)
			worker := podForIR(ir, 0, string(v1beta1.RunnerNameWorker), 0, true, true)
			r, c := newReconciler(t, ir, survivor, worker)
			r.Clock = clocktesting.NewFakeClock(base)
			r.Recorder = record.NewFakeRecorder(32)
			request := ctrl.Request{NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}}

			// One settling pass names the revision the gang runs, which is
			// the revision a rebuild would re-materialize and therefore the
			// one a RetryBlock has to answer for.
			if _, err := r.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("settle Reconcile: %v", err)
			}
			fresh := &v1beta1.InferenceReplica{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
				t.Fatalf("get IR: %v", err)
			}
			blocked := fresh.Status.UpdateRevision
			if blocked == "" {
				t.Fatalf("no update revision on the settled IR: %+v", fresh.Status)
			}
			fresh.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
				Index:           0,
				Incarnation:     1,
				Phase:           v1beta1.OMENativeInstancePending,
				PodCount:        2,
				RunningRevision: blocked,
			}}
			fresh.Status.RetryBlocks = []v1beta1.RetryBlock{{
				TargetRevision:  blocked,
				State:           tc.block,
				AttemptsStarted: 1,
				NextRetryAt:     tc.nextRetryAt,
			}}
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatalf("seed the row: %v", err)
			}
			// The worker is gone: one of the two recorded pods is lost.
			if err := c.Delete(context.Background(), worker); err != nil {
				t.Fatalf("lose the worker: %v", err)
			}
			r.Expectations = workload.NewExpectations()

			if _, err := r.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			row := instanceRow(t, c, ir)
			restarted := row.Phase == v1beta1.OMENativeInstanceRestarting &&
				row.Operation != nil && row.Operation.Type == v1beta1.InstanceOperationRestart && row.Operation.Step == "Drain"
			if restarted != tc.wantRestarted {
				t.Fatalf("restarted=%v want %v; row %+v (op %+v)", restarted, tc.wantRestarted, row, row.Operation)
			}
		})
	}
}
