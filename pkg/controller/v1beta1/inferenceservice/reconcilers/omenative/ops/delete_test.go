package ops

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
)

// newFakeClientWithDiscovery builds a controller-runtime fake client with
// the schemes Delete reads (corev1 + v1beta1 + discoveryv1). The status
// subresource is registered so Status().Update mirrors API-server
// behavior.
func newFakeClientWithDiscovery(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

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

func TestDelete_NilClient(t *testing.T) {
	resetExpectations(t)
	if _, err := Delete(context.Background(), core.ReconcileParams{}, core.ComponentPlan{Component: v1beta1.EngineComponent}, 0); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestDelete_NoPodsNoStatus_ReturnsDone(t *testing.T) {
	resetExpectations(t)
	// Both pod list and InstanceStatus are empty — nothing to do.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Delete(context.Background(), p, plan, 0)
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
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Delete(context.Background(), p, plan, 0)
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
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Delete(context.Background(), p, plan, 0)
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
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Delete(context.Background(), p, plan, 0)
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

	// core.Expectations should record one pending delete.
	if core.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0) {
		t.Errorf("core.DefaultExpectations should record the pending delete")
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
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	const maxPasses = 5
	for pass := 0; pass < maxPasses; pass++ {
		done, err := Delete(context.Background(), p, plan, 0)
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
			core.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0)
			return
		}
		// Simulate the watch observing the delete after step 4 issued it.
		core.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0)
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
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Delete(context.Background(), p, plan, 0)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: pod is terminating, status entry still present")
	}
	// No new delete should have been issued — expectations stay clean.
	if !core.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0) {
		t.Errorf("ExpectDeletes recorded a count for an already-terminating pod")
	}
}

func TestExtraInstanceIndices(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0}, {Index: 1}, {Index: 2},
						},
					},
				},
			},
		},
	}
	// Plan covers only index 0; status carries 0, 1, 2 — so 1 and 2 are extras.
	planWithOnlyZero := core.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Instances: []core.InstancePlan{{Index: 0}},
	}
	got := extraInstanceIndices(isvc, v1beta1.EngineComponent, planWithOnlyZero)
	if len(got) != 2 {
		t.Fatalf("len: got %d want 2 (indices 1 and 2)", len(got))
	}
	hit := map[int32]bool{}
	for _, i := range got {
		hit[i] = true
	}
	if !hit[1] || !hit[2] {
		t.Errorf("got %v want {1,2}", got)
	}

	// No extras when plan covers every status index.
	planWithAll := core.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Instances: []core.InstancePlan{{Index: 0}, {Index: 1}, {Index: 2}},
	}
	if extras := extraInstanceIndices(isvc, v1beta1.EngineComponent, planWithAll); len(extras) != 0 {
		t.Errorf("expected zero extras when plan covers all status indices, got %v", extras)
	}

	// Migration-in-flight: source at index 0 (Phase=Migrating), surge
	// at index 3 (Operation=Migrate). Neither should appear in extras
	// even when the plan doesn't cover them.
	migIsvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Phase: v1beta1.OMENativeInstanceMigrating},
							{Index: 3, Phase: v1beta1.OMENativeInstanceCreating, Operation: &v1beta1.InstanceOperation{
								Type: v1beta1.InstanceOperationMigrate,
							}},
						},
					},
				},
			},
		},
	}
	planEmpty := core.ComponentPlan{Component: v1beta1.EngineComponent}
	if extras := extraInstanceIndices(migIsvc, v1beta1.EngineComponent, planEmpty); len(extras) != 0 {
		t.Errorf("migration-in-flight indices must not be extras; got %v", extras)
	}

	// Scoped to component: a Decoder status entry doesn't bleed into Engine extras.
	isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
		OMENative: &v1beta1.OMENativeComponentStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 7}},
		},
	}
	got = extraInstanceIndices(isvc, v1beta1.EngineComponent, core.ComponentPlan{Component: v1beta1.EngineComponent})
	for _, i := range got {
		if i == 7 {
			t.Errorf("decoder index 7 leaked into engine extras: %v", got)
		}
	}
}

func TestReconcileComponentOps_ScaleDownDrivesDelete(t *testing.T) {
	// MinReplicas=1, but status carries InstanceStatus for index 1
	// (left over from a previous replicas=2 reconcile). Dispatch
	// should drive Delete for index 1 before running Create for index 0.
	// Tests ReconcileComponentOps (the post-plan dispatcher) rather
	// than the top-level ReconcileComponent — that lives in omenative
	// and would close a cycle if pulled in here.
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
					{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	result, err := ReconcileComponentOps(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("ReconcileComponentOps: %v", err)
	}
	// Index 1 had no live pods, so Delete returns done=true on first
	// pass and the dispatcher proceeds to Create. Create on an empty
	// cluster produces pods + a requeue.
	if result.RequeueAfter == 0 {
		t.Errorf("expected requeue from Create after scale-down completed, got %+v", result)
	}

	// Status for index 1 should be gone.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Index == 1 {
			t.Errorf("InstanceStatus for index 1 should have been removed, got %+v", s)
		}
	}
}
