package ops_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Finding ⑥: gang RecreateInstance race.
//
// A pod loss in an already-Ready Instance can be recovered two ways: the
// Restart pass (whole-Instance drain+recreate — the RecreateInstance
// contract) and the Create pass (backfill the missing pod by name). These
// race. When Create wins it stamps Phase=Creating, which latches the
// outcome by making DetectRestartTrigger skip on every later pass — so
// RecreateInstance silently degrades to "recreate just the dead pod" (the
// leader and surviving workers keep their incarnation). Empirically
// non-deterministic: the same 3-pod gang recovered the whole gang in one
// run and only the dead pod in another.
//
// Fix: Create defers partial-Instance recovery to Restart under
// RecreateInstance, so Restart deterministically owns whole-gang recreate.

// gangPod fabricates a leader/worker gang pod (podForInstance only emits
// the single-pod "default" runner).
func gangPod(isvc *v1beta1.InferenceService, idx int32, runner string, ordinal int32, incarnation int64, ready, serving bool) *corev1.Pod {
	labels := testPodLabels(isvc.Name, workload.ComponentEngine, idx, runner, incarnation, ordinal)
	labels[query.LabelRevisionHash] = testRevisionHash
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, runner, ordinal),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
	now := metav1.NewTime(time.Now())
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: now,
		})
	}
	return pod
}

// buildPlanGangEngine builds a multi-pod (leader+worker) engine plan with
// the given restart policy. Instance 0 sits at incarnation 2 — an
// established Instance that has already been Ready.
func buildPlanGangEngine(restart workload.RestartPolicy) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component:            workload.ComponentEngine,
		Replicas:             1,
		RestartPolicy:        restart,
		InstanceReadyTimeout: 30 * time.Minute,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 2,
			Runners:     []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
}

// seedReadyInstance stamps the ISVC status with one already-Ready engine
// Instance at the given index/incarnation.
func seedReadyInstance(isvc *v1beta1.InferenceService, idx int32, incarnation int64) {
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
					Index:       idx,
					Incarnation: incarnation,
					Phase:       v1beta1.OMENativeInstanceReady,
				}},
			},
		},
	}
}

// An already-Ready gang that loses a pod must NOT be self-healed
// pod-by-pod by Create under RecreateInstance — that races Restart's
// whole-gang recreate and degrades the policy. Create must defer.
func TestCreate_RecreateInstance_DefersPartialGangToRestart(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	seedReadyInstance(isvc, 0, 2)
	// Established gang lost its worker: leader present, worker gone.
	leader := gangPod(isvc, 0, "leader", 0, 2, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The missing worker MUST NOT be backfilled — Restart owns whole-gang recreate.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("Create must defer partial-gang recovery to Restart under RecreateInstance: got %d pod(s), want 1 (leader only)", len(pods.Items))
	}
	// Phase MUST stay Ready — stamping Creating here is what latches the race.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s := findInstanceStatusInISVC(fresh, v1beta1.EngineComponent, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("Phase must remain Ready (Create must not stamp Creating): got %+v", s)
	}
}

// Contrast: under a non-RecreateInstance policy, Create still self-heals a
// missing pod — Restart isn't responsible for recovery there.
func TestCreate_NonRecreatePolicy_SelfHealsMissingPod(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	seedReadyInstance(isvc, 0, 2)
	leader := gangPod(isvc, 0, "leader", 0, 2, true, true)
	c := newFakeClient(t, isvc, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyNone)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("non-RecreateInstance policy must self-heal the missing pod: got %d pod(s), want 2", len(pods.Items))
	}
}

// The guard must not block legitimate fresh creation: an Instance that has
// never been Ready (no status) under RecreateInstance is still
// materialized by Create — otherwise initial bring-up deadlocks (Create
// defers, Restart can't fire on a never-Ready Instance).
func TestCreate_RecreateInstance_StillMaterializesFreshInstance(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	// No instance status seeded → fresh, never Ready.
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("RecreateInstance must still materialize a fresh gang Instance: got %d pod(s), want 2 (leader+worker)", len(pods.Items))
	}
}
