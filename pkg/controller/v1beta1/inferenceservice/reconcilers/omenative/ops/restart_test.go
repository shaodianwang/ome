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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// HISTORY: this file originally held the full `TestRestart_*` end-to-end
// coverage; those tests moved alongside the per-Instance Restart state
// machine to pkg/controller/v1beta1/workload/ops/restart_test.go in
// slice 4e of the workload-extraction refactor. The remaining tests
// here cover the v1beta1-typed pieces still living in omenative/ops:
// detectRestartTrigger (called by dispatch.go), the dispatcher's
// RestartPolicy=None fall-through and trigger dispatch (driven through
// ReconcileComponentOps), and the read-side helpers in workload/query
// that Restart consumes.

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
