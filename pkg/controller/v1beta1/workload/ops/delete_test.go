package ops_test

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
)

// isvcWithStatus builds an ISVC carrying a single InstanceStatus for the
// engine Component at the given index in Phase. Useful for delete tests
// that need pre-existing status entries.
func isvcWithStatus(name, ns string, replicas int, idx int32, phase v1beta1.OMENativeInstancePhase) *v1beta1.InferenceService {
	isvc := minimalISVC(name, ns, replicas)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: idx, Phase: phase},
				},
			},
		},
	}
	return isvc
}

// sliceWithEndpoint constructs an EndpointSlice for serviceName carrying
// exactly one endpoint that targets pod, with the given Ready condition.
func sliceWithEndpoint(namespace, sliceName, serviceName string, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sliceName,
			Namespace: namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(ready),
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			},
		},
	}
}

// testRemoveInstance is the test-side persistence layer for
// ReconcileInput.RemoveInstance. Mirrors the buildRemoveInstance
// closure in omenative/internalsource at the wire level — Get /
// drop / Status().Update — but skips retry.RetryOnConflict (the fake
// client has no optimistic-concurrency failures to retry).
//
// Returns (true, nil) when a real removal happened, (false, nil) when
// the entry was already absent. Also forgets workload.DefaultExpectations
// so a later index reuse doesn't inherit stale counters — matches the
// production helper.
func testRemoveInstance(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
			return false, fmt.Errorf("get isvc: %w", err)
		}
		if fresh.Status.Components == nil {
			return false, nil
		}
		cs, ok := fresh.Status.Components[component]
		if !ok || cs.OMENative == nil {
			return false, nil
		}
		insts := cs.OMENative.InstanceStatuses
		pos := -1
		for i, s := range insts {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return false, nil
		}
		cs.OMENative.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
		fresh.Status.Components[component] = cs
		if err := c.Status().Update(ctx, fresh); err != nil {
			return false, fmt.Errorf("status update: %w", err)
		}
		workload.DefaultExpectations.Forget(isvc.Namespace, isvc.Name, workload.ComponentType(component), idx)
		return true, nil
	}
}

// buildTestInputForDelete is buildTestInput plus the RemoveInstance
// callback Delete needs. Constructed inline so the test file owns the
// projection from (isvc, component) onto ReconcileInput end-to-end.
func buildTestInputForDelete(isvc *v1beta1.InferenceService, c client.Client, component v1beta1.ComponentType) workload.ReconcileInput {
	input := buildTestInput(isvc, c, component)
	input.RemoveInstance = testRemoveInstance(c, isvc, component)
	return input
}

func TestDelete_NilClient(t *testing.T) {
	resetExpectations(t)
	plan := workload.ComponentPlan{Component: v1beta1.EngineComponent}
	if _, err := ops.Delete(context.Background(), workload.Deps{}, workload.ReconcileInput{}, plan, 0); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestDelete_NoPodsNoStatus_ReturnsDone(t *testing.T) {
	resetExpectations(t)
	// Both pod list and InstanceStatus are empty — nothing to do.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc)
	input := buildTestInputForDelete(isvc, c, v1beta1.EngineComponent)
	plan := buildPlanSinglePodEngine(1)

	done, err := ops.Delete(context.Background(), workload.Deps{Client: c}, input, plan, 0)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true when there is nothing to delete")
	}
}

func TestDelete_PodsGone_RemovesInstanceStatus(t *testing.T) {
	resetExpectations(t)
	isvc := isvcWithStatus("llama-70b", "prod", 1, 0, v1beta1.OMENativeInstanceReady)
	c := newFakeClient(t, isvc)
	input := buildTestInputForDelete(isvc, c, v1beta1.EngineComponent)
	plan := buildPlanSinglePodEngine(1)

	done, err := ops.Delete(context.Background(), workload.Deps{Client: c}, input, plan, 0)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true after status clear")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative != nil && len(cs.OMENative.InstanceStatuses) != 0 {
		t.Errorf("InstanceStatuses: got %d want 0", len(cs.OMENative.InstanceStatuses))
	}
}

func TestDelete_FlipsServingFalseAndRequeuesWhileDraining(t *testing.T) {
	resetExpectations(t)
	isvc := isvcWithStatus("llama-70b", "prod", 1, 0, v1beta1.OMENativeInstanceReady)
	pod := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	// EndpointSlice still publishes the pod with Ready=true — drain not converged.
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, true)
	c := newFakeClient(t, isvc, pod, slice)
	input := buildTestInputForDelete(isvc, c, v1beta1.EngineComponent)
	plan := buildPlanSinglePodEngine(1)

	done, err := ops.Delete(context.Background(), workload.Deps{Client: c}, input, plan, 0)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while EndpointSlice still publishes the pod as Ready")
	}

	// Pod still exists, but with serving=False.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving should be False, got conditions=%+v", got.Name, got.Status.Conditions)
	}

	// Status should be Deleting with Operation Delete.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	is := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if is.Phase != v1beta1.OMENativeInstanceDeleting {
		t.Errorf("Phase: got %q want Deleting", is.Phase)
	}
	if is.Operation == nil || is.Operation.Type != v1beta1.InstanceOperationDelete {
		t.Errorf("Operation: got %+v", is.Operation)
	}
}

func TestDelete_DrainedPodIsDeleted(t *testing.T) {
	resetExpectations(t)
	isvc := isvcWithStatus("llama-70b", "prod", 1, 0, v1beta1.OMENativeInstanceReady)
	// Pod already not serving; EndpointSlice publishes it with Ready=false → drained.
	pod := podForInstance(isvc, 0, true, false)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, false)
	c := newFakeClient(t, isvc, pod, slice)
	input := buildTestInputForDelete(isvc, c, v1beta1.EngineComponent)
	plan := buildPlanSinglePodEngine(1)

	done, err := ops.Delete(context.Background(), workload.Deps{Client: c}, input, plan, 0)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after issuing pod delete (status still present until next pass)")
	}

	// Pod should be gone from the fake API server.
	got := &corev1.Pod{}
	err = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err == nil {
		t.Errorf("expected pod %s to be deleted", pod.Name)
	}

	// workload.DefaultExpectations should record one pending delete.
	if workload.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentEngine, 0) {
		t.Errorf("workload.DefaultExpectations should record the pending delete")
	}
}

func TestDelete_ConvergesAcrossPasses(t *testing.T) {
	// End-to-end multi-pass: start with a Ready Instance, run Delete()
	// until done=true. Asserts the state machine converges without
	// external nudges (other than the fake client observing the deletes
	// immediately, which simulates a fast watch).
	resetExpectations(t)
	isvc := isvcWithStatus("llama-70b", "prod", 1, 0, v1beta1.OMENativeInstanceReady)
	pod := podForInstance(isvc, 0, true, false)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, false)
	c := newFakeClient(t, isvc, pod, slice)
	input := buildTestInputForDelete(isvc, c, v1beta1.EngineComponent)
	plan := buildPlanSinglePodEngine(1)

	const maxPasses = 5
	for pass := 0; pass < maxPasses; pass++ {
		done, err := ops.Delete(context.Background(), workload.Deps{Client: c}, input, plan, 0)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done {
			// Final state: no pods, no InstanceStatus for index 0.
			pods := &corev1.PodList{}
			_ = c.List(context.Background(), pods, client.InNamespace("prod"))
			if len(pods.Items) != 0 {
				t.Errorf("pods left: %d", len(pods.Items))
			}
			fresh := &v1beta1.InferenceService{}
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			if cs.OMENative != nil && len(cs.OMENative.InstanceStatuses) != 0 {
				t.Errorf("status entries left: %d", len(cs.OMENative.InstanceStatuses))
			}
			// Allow the cache to "observe" the delete: clear the
			// expectations entry that ExpectDeletes left behind.
			workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
			return
		}
		// Simulate the watch observing the delete after step 4 issued it.
		workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	}
	t.Fatalf("Delete did not converge after %d passes", maxPasses)
}

func TestDelete_PodAlreadyTerminatingIsSkipped(t *testing.T) {
	resetExpectations(t)
	isvc := isvcWithStatus("llama-70b", "prod", 1, 0, v1beta1.OMENativeInstanceDeleting)
	pod := podForInstance(isvc, 0, false, false)
	// Mark as already terminating (deletionTimestamp).
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	// fake client requires a finalizer when DeletionTimestamp is set,
	// otherwise it rejects the object.
	pod.Finalizers = []string{"omenative.test/keep-around"}
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, false)
	c := newFakeClient(t, isvc, pod, slice)
	input := buildTestInputForDelete(isvc, c, v1beta1.EngineComponent)
	plan := buildPlanSinglePodEngine(1)

	done, err := ops.Delete(context.Background(), workload.Deps{Client: c}, input, plan, 0)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: pod is terminating, status entry still present")
	}
	// No new delete should have been issued — expectations stay clean.
	if !workload.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentEngine, 0) {
		t.Errorf("ExpectDeletes recorded a count for an already-terminating pod")
	}
}
