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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// restartPolicyPtr is a tiny pointer helper used by the
// restart-policy-explicit tests below.
func restartPolicyPtr(v v1beta1.OMENativeRestartPolicy) *v1beta1.OMENativeRestartPolicy {
	return &v
}

// isvcReadyAtIncarnation builds an ISVC whose InstanceStatus for engine
// index 0 is Phase=Ready at the given Incarnation — the steady-state from
// which a restart trigger fires.
func isvcReadyAtIncarnation(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := minimalISVC(name, ns, 1)
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

// podAtIncarnation extends podForInstance to stamp a specific incarnation
// label. Restart pods at the bumped incarnation are distinguished from old
// pods by this label.
func podAtIncarnation(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ready, serving bool) *corev1.Pod {
	pod := podForInstance(isvc, instanceIdx, ready, serving)
	pod.Labels[query.LabelInstanceIncarnation] = fmt.Sprintf("%d", incarnation)
	return pod
}

func TestRestart_NilClient(t *testing.T) {
	resetExpectations(t)
	_, err := Restart(context.Background(), core.ReconcileParams{}, core.ComponentPlan{Component: v1beta1.EngineComponent}, core.InstancePlan{Index: 0, Incarnation: 1}, "")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestRestart_FirstPassBumpsIncarnationAndPatchesStatus(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	c := newFakeClientWithDiscovery(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Restart(context.Background(), p, plan, plan.Instances[0], "test trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false on first pass (drain not converged)")
	}

	// Status should now be Phase=Restarting, Incarnation=2.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (bumped from 1)", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: %+v", s.Operation)
	}
	if s.Operation.Reason != "test trigger" {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, "test trigger")
	}
	if s.Operation.Step != "Drain" {
		t.Errorf("Operation.Step: got %q want Drain", s.Operation.Step)
	}
}

func TestRestart_DrainOldPod_FlipsServingFalse(t *testing.T) {
	// Pre-bumped status (Restarting at Inc=2); old pod at Inc=1 still has
	// serving=True. Restart should flip serving=False and requeue while
	// EndpointSlice still publishes the pod as Ready.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true /* still Ready */)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Restart(context.Background(), p, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while drain hasn't converged")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving should be False, got %+v", got.Name, got.Status.Conditions)
	}
}

func TestRestart_DeletesOldPodOnceDrained(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, false /* already drained */)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false /* Ready=false */)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Restart(context.Background(), p, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after issuing delete (next pass creates new pod)")
	}

	// Old pod should be gone.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err == nil {
		t.Errorf("old pod %s should be deleted", pod.Name)
	}
	if core.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0) {
		t.Errorf("ExpectDeletes should record the in-flight delete")
	}
}

func TestRestart_RefusesToDeleteOrphanPod(t *testing.T) {
	// A pod under the OMENative selector but missing
	// ome.io/instance-incarnation is an orphan. Restart must emit
	// FoundOrphan and short-circuit rather than deleting it — the operator
	// re-labels or removes it manually.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	orphan := podAtIncarnation(isvc, 0, 1, true, false)
	orphan.Name = "orphan-pod"
	delete(orphan.Labels, query.LabelInstanceIncarnation)
	c := newFakeClientWithDiscovery(t, isvc, orphan)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Restart(context.Background(), p, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: orphan must block Restart from advancing")
	}

	// Orphan pod must STILL exist.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(orphan), got); err != nil {
		t.Fatalf("orphan pod must still exist after Restart bails: %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Errorf("orphan pod must NOT have been deleted; got DeletionTimestamp=%v", got.DeletionTimestamp)
	}
}

func TestRestart_CreatesNewPodAtBumpedIncarnation(t *testing.T) {
	// Status is Restarting at Inc=2, no pods (old already deleted, new
	// not yet created). Restart should create the new pod at Inc=2.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Restart(context.Background(), p, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after creating new pod (not yet Ready)")
	}

	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 1 {
		t.Fatalf("pods: got %d want 1", len(pods.Items))
	}
	if got := pods.Items[0].Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("new pod %s incarnation label: got %q want 2", pods.Items[0].Name, got)
	}
}

func TestRestart_ConvergesAcrossPasses(t *testing.T) {
	// End-to-end: start with a Ready instance whose pod is Failed.
	// Trigger Restart and run it until done=true; assert the final state
	// has one new-incarnation pod, serving=True, and Phase=Ready.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	failed := podAtIncarnation(isvc, 0, 1, true, true)
	failed.Status.Phase = corev1.PodFailed
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", failed, false /* drained */)
	c := newFakeClientWithDiscovery(t, isvc, failed, slice)

	const maxPasses = 8
	for pass := 0; pass < maxPasses; pass++ {
		// Re-read each pass: core.BuildPlan needs current status to pick up the
		// new incarnation after Restart bumps it.
		fresh := &v1beta1.InferenceService{}
		_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
		pPass := minimalParams(t, fresh, c)
		planPass, _ := core.BuildPlan(pPass)

		// Fast-forward: simulate the fake watch observing every delete the
		// previous pass issued.
		core.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0)

		// Synthesize ContainersReady on any new-incarnation pod the
		// previous pass created — the fake client doesn't run kubelet.
		makeNewPodReady(t, c, "prod", "llama-70b-engine-0-default-0", planPass.Instances[0].Incarnation)

		done, err := Restart(context.Background(), pPass, planPass, planPass.Instances[0], "pod Failed")
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done {
			// Verify end state.
			finalISVC := &v1beta1.InferenceService{}
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), finalISVC)
			s := finalISVC.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
			if s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("final Phase: got %q want Ready", s.Phase)
			}
			if s.Incarnation != 2 {
				t.Errorf("final Incarnation: got %d want 2", s.Incarnation)
			}
			if s.Operation != nil {
				t.Errorf("final Operation: want nil, got %+v", s.Operation)
			}
			return
		}
	}
	t.Fatalf("Restart did not converge after %d passes", maxPasses)
}

// makeNewPodReady is a test helper that synthesizes ContainersReady=True
// on the named pod if it exists and carries the given incarnation label.
// It mimics what kubelet would write once the runtime is up.
func makeNewPodReady(t *testing.T, c client.Client, ns, name string, incarnation int64) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, pod); err != nil {
		return
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != fmt.Sprintf("%d", incarnation) {
		return
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady && cond.Status == corev1.ConditionTrue {
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.ContainersReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("synthesize ContainersReady: %v", err)
	}
}

func TestDetectRestartTrigger_NoStatus_NoTrigger(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, _, err := detectRestartTrigger(context.Background(), p, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("no status entry should not trigger restart")
	}
}

func TestDetectRestartTrigger_PhaseCreating_NoTrigger(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, _, err := detectRestartTrigger(context.Background(), p, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Creating must not be interruptible by Restart")
	}
}

func TestDetectRestartTrigger_PhaseMigratingSuppressed(t *testing.T) {
	// Migrate's source-drain deletes the source pods; without the
	// explicit suppression, detectRestartTrigger would observe "pod
	// count below desired" and try to Restart the source mid-migration.
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, _, err := detectRestartTrigger(context.Background(), p, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Migrating must not be interruptible by Restart")
	}
}

func TestDetectRestartTrigger_OperationMigrateSuppressed(t *testing.T) {
	// A surge that just got promoted to Phase=Ready with Operation=nil
	// is fine — but during its Creating window it carries
	// Operation.Type=Migrate. The guard catches that transient state.
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{
						Index: 1, Incarnation: 1,
						Phase: v1beta1.OMENativeInstanceCreating,
						Operation: &v1beta1.InstanceOperation{
							Type: v1beta1.InstanceOperationMigrate,
							Step: "CreatePods",
						},
					},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	// The plan doesn't include index=1 (sparse); fake one for the call.
	inst := core.InstancePlan{
		Index: 1, Incarnation: 1,
		Runners: []core.RunnerPlan{{Name: "default", Size: 1}},
	}
	trigger, _, err := detectRestartTrigger(context.Background(), p, plan, inst)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Operation.Type=Migrate must suppress Restart")
	}
}

func TestDetectRestartTrigger_PodFailed(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	pod.Status.Phase = corev1.PodFailed
	c := newFakeClientWithDiscovery(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, reason, err := detectRestartTrigger(context.Background(), p, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Failed pod should trigger restart")
	}
	if reason == "" {
		t.Errorf("reason should be set for failed-pod trigger")
	}
}

func TestDetectRestartTrigger_PodMissing(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// No pod present — under Phase=Ready that's a loss event.
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, reason, err := detectRestartTrigger(context.Background(), p, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("missing pod under Phase=Ready should trigger restart")
	}
	if reason == "" {
		t.Errorf("reason should be set for missing-pod trigger")
	}
}

func TestDetectRestartTrigger_PhaseRestartingAlwaysTrue(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceRestarting},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	trigger, _, err := detectRestartTrigger(context.Background(), p, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("in-progress Restart must keep returning true so caller continues driving it")
	}
}

func TestReconcileComponentOps_RestartPolicyNone_FallsThroughToCreate(t *testing.T) {
	// Even with a Failed pod under Phase=Ready, RestartPolicy=None
	// must not invoke Restart — Create handles missing-by-name recreation.
	// Drives ReconcileComponentOps (in this package) rather than the
	// top-level omenative.ReconcileComponent, which would close a cycle.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// RestartPolicy=None is the single-pod default applied by core.BuildPlan;
	// don't override it.
	failed := podAtIncarnation(isvc, 0, 1, true, true)
	failed.Status.Phase = corev1.PodFailed
	c := newFakeClientWithDiscovery(t, isvc, failed)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	_, err = ReconcileComponentOps(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("ReconcileComponentOps: %v", err)
	}

	// Status must NOT have moved to Restarting.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase == v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase moved to Restarting under None policy: %+v", s)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation must not bump under None policy: got %d", s.Incarnation)
	}
}

func TestReconcileComponentOps_RestartTriggerDispatches(t *testing.T) {
	// Build an ISVC with RestartPolicy=RecreateInstanceOnPodRestart and
	// a Failed pod under Phase=Ready. ReconcileComponentOps should hit
	// Restart, bump Incarnation, and requeue.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.OMENative = &v1beta1.OMENativeComponentSpec{
		RestartPolicy: restartPolicyPtr(v1beta1.OMENativeRestartPolicyRecreateInstance),
	}
	failed := podAtIncarnation(isvc, 0, 1, true, true)
	failed.Status.Phase = corev1.PodFailed
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", failed, false)
	c := newFakeClientWithDiscovery(t, isvc, failed, slice)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	result, err := ReconcileComponentOps(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("ReconcileComponentOps: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected requeue while Restart is in flight, got %+v", result)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (bumped)", s.Incarnation)
	}
}

func TestPartitionPodsByIncarnation(t *testing.T) {
	// Label-missing pods land in the unknown bucket rather than getting
	// silently deleted alongside old-incarnation pods.
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	p1 := podAtIncarnation(isvc, 0, 1, false, false)
	p1.Name = "p1"
	p2 := podAtIncarnation(isvc, 0, 2, false, false)
	p2.Name = "p2"
	noLabel := podForInstance(isvc, 0, false, false)
	noLabel.Name = "noLabel"
	delete(noLabel.Labels, query.LabelInstanceIncarnation)

	old, fresh, unknown := query.PartitionPodsByIncarnation([]*corev1.Pod{p1, p2, noLabel}, 2)
	if len(old) != 1 || old[0].Name != "p1" {
		t.Errorf("old: got %v want [p1]", podNamesOf(old))
	}
	if len(fresh) != 1 || fresh[0].Name != "p2" {
		t.Errorf("fresh: got %v want [p2]", podNamesOf(fresh))
	}
	if len(unknown) != 1 || unknown[0].Name != "noLabel" {
		t.Errorf("unknown: got %v want [noLabel]", podNamesOf(unknown))
	}
}

func TestInstanceIncarnationFromLabels(t *testing.T) {
	// Helper sanity: nil pod, missing label, and malformed value all
	// return ok=false; a valid integer returns the parsed value.
	if _, ok := query.InstanceIncarnationFromLabels(nil); ok {
		t.Errorf("nil pod should return ok=false")
	}
	if _, ok := query.InstanceIncarnationFromLabels(&corev1.Pod{}); ok {
		t.Errorf("missing label should return ok=false")
	}
	bad := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIncarnation: "not-a-number"}}}
	if _, ok := query.InstanceIncarnationFromLabels(bad); ok {
		t.Errorf("malformed label should return ok=false")
	}
	ok := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIncarnation: "7"}}}
	got, found := query.InstanceIncarnationFromLabels(ok)
	if !found || got != 7 {
		t.Errorf("got (%d, %v) want (7, true)", got, found)
	}
}

func podNamesOf(pods []*corev1.Pod) []string {
	out := make([]string, len(pods))
	for i, p := range pods {
		out[i] = p.Name
	}
	return out
}
