package ops_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// unknownPhasePod is a pod whose kubelet has stopped reporting: phase
// Unknown, bound to node, neither terminal nor ready.
func unknownPhasePod(pod *corev1.Pod, node string) *corev1.Pod {
	pod.Spec.NodeName = node
	pod.Status.Phase = corev1.PodUnknown
	pod.Status.Conditions = nil
	return pod
}

func unknownNodeUnreachable(name string, since time.Duration) *corev1.Node {
	added := metav1.NewTime(time.Now().Add(-since))
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionUnknown,
			LastTransitionTime: added,
		}}},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
			Key:       corev1.TaintNodeUnreachable,
			Effect:    corev1.TaintEffectNoExecute,
			TimeAdded: &added,
		}}},
	}
}

func unknownNodeReady(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
		}}},
	}
}

// restartingAtIncarnation seeds an Instance mid-restart at incarnation 2
// with a live attempt deadline, the shape Phase B of a restart runs in.
func restartingAtIncarnation(ir *v1beta1.InferenceReplica) {
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "pod lost",
			StartedAt:      metav1.Now(),
			LastProgressAt: metav1.Now(),
			Deadline:       metav1.NewTime(time.Now().Add(time.Hour)),
		},
	}
}

// TestRestart_UnknownPhasePodHoldsWithoutForceDeletePolicy: a pod at the
// bumped incarnation whose phase went Unknown keeps its stable name — a
// returning kubelet may still be running it, so recycling the name could
// double-run the container. With no force-delete policy configured
// nothing can prove the node dead, so the repair reports the wait
// instead of spending its deadline in silence: the Restart operation
// carries the NodeUnknown token, LastFailure names the pod and its node,
// and the pod is left alone.
//
// Cells: instance/Restart/Drain/pod.phaseTerminal[Unknown]/before
func TestRestart_UnknownPhasePodHoldsWithoutForceDeletePolicy(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	quiet := unknownPhasePod(podAtIncarnation(isvc, 0, 2, false, false), "node-a")
	c := newFakeClient(t, isvc, ir, quiet, unknownNodeUnreachable("node-a", time.Hour))
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	done, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("Restart must not complete while a pod of the rebuild is in phase Unknown")
	}
	if !podExists(c, quiet) {
		t.Fatalf("pod %s must not be deleted without a force-delete policy", quiet.Name)
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Waiting != workload.WaitingReasonNodeUnknown {
		t.Fatalf("Operation.Waiting: got %+v want %s", s.Operation, workload.WaitingReasonNodeUnknown)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != workload.WaitingReasonNodeUnknown ||
		s.LastFailure.PodName != quiet.Name || !strings.Contains(s.LastFailure.Message, "node-a") {
		t.Fatalf("LastFailure must name the pod and its node, got %+v", s.LastFailure)
	}
	if n := countRejectionEvents(drainEvents(rec), workload.EventReasonPodForceDeleted); n != 0 {
		t.Fatalf("PodForceDeleted events: got %d want 0 (no policy configured)", n)
	}
}

// TestRestart_UnknownPhasePodClearsHoldOnceGone: the authority that set
// the NodeUnknown token releases it as soon as the pod stops holding a
// target name in that phase, so a repair that resumes does not carry a
// wait that has ended.
//
// Cells: instance/Restart/Drain/pod.phaseTerminal[Unknown]/before
func TestRestart_UnknownPhasePodClearsHoldOnceGone(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	ir.Status.InstanceStatuses[0].Operation.Waiting = workload.WaitingReasonNodeUnknown
	c := newFakeClient(t, isvc, ir)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation != nil && s.Operation.Waiting != "" {
		t.Fatalf("Operation.Waiting must be released once no target name is held in phase Unknown, got %q", s.Operation.Waiting)
	}
}

// TestRestart_UnknownPhasePodForceDeletedOnProvenNodeDeath: with a
// force-delete policy configured and the node unreachable past the
// configured threshold, the same sweep that clears stuck-Terminating
// pods frees the name — grace zero, UID-preconditioned — and the restart
// rebuilds it on a later pass.
//
// Cells: instance/Restart/Drain/pod.phaseTerminal[Unknown]/before
func TestRestart_UnknownPhasePodForceDeletedOnProvenNodeDeath(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	quiet := unknownPhasePod(podAtIncarnation(isvc, 0, 2, false, false), "node-a")
	c := newFakeClient(t, isvc, ir, quiet, unknownNodeUnreachable("node-a", time.Hour))
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if podExists(c, quiet) {
		t.Fatalf("pod %s must be force-deleted once its node is provably gone", quiet.Name)
	}
	if n := countRejectionEvents(drainEvents(rec), workload.EventReasonPodForceDeleted); n != 1 {
		t.Fatalf("PodForceDeleted events: got %d want 1", n)
	}

	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart pass 2: %v", err)
	}
	got := listPods(t, c, "prod")
	if len(got) != 1 || got[0].Status.Phase == corev1.PodUnknown {
		t.Fatalf("expected one freshly recreated pod, got %+v", got)
	}
}

// TestRestart_UnknownPhasePodOnLiveNodeIsNeverDeleted: node-death
// evidence, not the pod's phase, authorizes the delete. A node still
// posting Ready=True means the kubelet is merely slow, so the name is
// left occupied and the wait stays visible.
//
// Cells: instance/Restart/Drain/pod.phaseTerminal[Unknown]/before
func TestRestart_UnknownPhasePodOnLiveNodeIsNeverDeleted(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	quiet := unknownPhasePod(podAtIncarnation(isvc, 0, 2, false, false), "node-a")
	c := newFakeClient(t, isvc, ir, quiet, unknownNodeReady("node-a"))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !podExists(c, quiet) {
		t.Fatalf("pod %s must survive: a Ready node vetoes every node-death branch", quiet.Name)
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Waiting != workload.WaitingReasonNodeUnknown {
		t.Fatalf("Operation.Waiting: got %+v want %s", s.Operation, workload.WaitingReasonNodeUnknown)
	}
}

// TestCreate_UnknownPhasePodRoutesToTheForceDeleteSweep: the create's
// missing-pod diff reads an Unknown pod as present, so without this the
// attempt polls a name no kubelet is reporting on until its deadline.
// The same node-death evidence frees it.
//
// Cells: instance/Create/CreatePods/pod.phaseTerminal[Unknown]/before
func TestCreate_UnknownPhasePodRoutesToTheForceDeleteSweep(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	quiet := unknownPhasePod(podForInstance(isvc, 0, false, false), "node-a")
	c := newFakeClient(t, isvc, quiet, unknownNodeUnreachable("node-a", time.Hour))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	plan := buildPlanSinglePodEngine(1)

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if podExists(c, quiet) {
		t.Fatalf("pod %s must be force-deleted once its node is provably gone", quiet.Name)
	}
}

// TestCreate_UnknownPhasePodKeepsThePassRequeuing: a held Instance is
// dropped from the pass's selection, which on its own would leave the
// pass looking converged and asking for no requeue at all. The wait ends
// on a clock over a node that emits no events, so the pass has to keep
// waking itself or the sweep fires only when something unrelated pokes
// the owner.
//
// Cells: instance/Create/CreatePods/pod.phaseTerminal[Unknown]/before
func TestCreate_UnknownPhasePodKeepsThePassRequeuing(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	quiet := unknownPhasePod(podForInstance(isvc, 0, false, false), "node-a")
	policy := &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	// Unreachable for less than the threshold: the name stays held and
	// the evidence turns actionable only once the remainder elapses.
	young := time.Minute
	c := newFakeClient(t, isvc, quiet, unknownNodeUnreachable("node-a", young))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	plan := buildPlanSinglePodEngine(1)

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = policy
	res, err := ops.Create(context.Background(), deps, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !podExists(c, quiet) {
		t.Fatalf("pod %s must survive: its node has not been unreachable long enough", quiet.Name)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter: got %v want a positive delay; nothing else wakes this pass", res.RequeueAfter)
	}
	if remaining := policy.NodeUnreachableThreshold - young; res.RequeueAfter > remaining {
		t.Errorf("RequeueAfter: got %v want at most the threshold remainder %v", res.RequeueAfter, remaining)
	}
}
