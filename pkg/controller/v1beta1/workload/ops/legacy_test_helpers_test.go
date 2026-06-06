package ops

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// White-box test scaffolding (package ops) for the Surge / Update
// state-machine tests. Distinct from the create_test.go fixtures
// (package ops_test) because both packages need similar helpers but
// can't share them across the package boundary.

// testRevisionHashLegacy is the synthetic revision hash stamped on
// test pods.
const testRevisionHashLegacy = "testrev1"

// legacyMinimalISVC builds an ISVC with the minimum metadata Update /
// Surge state-machine tests need.
func legacyMinimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
	mr := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &mr,
				},
			},
		},
	}
}

// legacyTestPodLabels reproduces the label set Render stamps on every
// emitted pod.
func legacyTestPodLabels(isvc string, component workload.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
		query.LabelInstanceIncarnation:        fmt.Sprintf("%d", incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 fmt.Sprintf("%d", ordinal),
	}
}

// legacyResetExpectations re-seats workload.DefaultExpectations so
// back-to-back tests don't observe prior ExpectCreates entries.
func legacyResetExpectations(t *testing.T) {
	t.Helper()
	workload.DefaultExpectations = workload.NewExpectations()
}

// legacyNewFakeClient builds a controller-runtime fake client with
// the schemes the surge / update tests touch.
func legacyNewFakeClient(t *testing.T, initObjs ...client.Object) client.Client {
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

// legacyPodForInstance fabricates a pod matching what Render() would
// produce for the given (ISVC, instance) pair.
func legacyPodForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := legacyTestPodLabels(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 1, 0)
	labels[query.LabelRevisionHash] = testRevisionHashLegacy
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 0),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
	now := metav1.NewTime(time.Now())
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               query.ServingConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	return pod
}

// legacyPodAtIncarnation extends legacyPodForInstance to stamp a
// specific incarnation label.
func legacyPodAtIncarnation(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ready, serving bool) *corev1.Pod {
	pod := legacyPodForInstance(isvc, instanceIdx, ready, serving)
	pod.Labels[query.LabelInstanceIncarnation] = fmt.Sprintf("%d", incarnation)
	return pod
}

// legacyTestInput projects the ISVC + Component onto a
// workload.ReconcileInput. MutateInstance round-trips through the
// fake client's Status().Update so tests can assert on the persisted
// ISVC.Status. The closure also mirrors the just-committed Operation
// back onto the in-memory ISVC.Status snapshot the test holds a
// pointer to, so subsequent passes within the same reconcile observe
// the just-stamped Phase / Operation without re-reading the
// apiserver.
func legacyTestInput(isvc *v1beta1.InferenceService, c client.Client, component workload.ComponentType) workload.ReconcileInput {
	return workload.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workload.Key{
			Namespace:   isvc.Namespace,
			OwnerName:   isvc.Name,
			Component:   workload.ComponentType(component),
			OwnerLabels: isvc.Labels,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: legacyInstanceStatuses(isvc, component),
		},
		MutateInstance: legacyMutateInstance(c, isvc, component),
	}
}

// legacyInstanceStatuses pulls + converts the OMENative
// InstanceStatuses slice into the workload-owned mirror.
func legacyInstanceStatuses(isvc *v1beta1.InferenceService, component workload.ComponentType) []workload.InstanceStatus {
	var out []workload.InstanceStatus
	if cs, ok := isvc.Status.Components[v1beta1.ComponentType(component)]; ok && cs.Lifecycle != nil {
		for _, s := range cs.Lifecycle.InstanceStatuses {
			out = append(out, workload.InstanceStatus{
				Index:           s.Index,
				Incarnation:     s.Incarnation,
				Phase:           workload.InstancePhase(s.Phase),
				RunningRevision: s.RunningRevision,
				TargetRevision:  s.TargetRevision,
				ActiveOrdinal:   s.ActiveOrdinal,
				Operation:       legacyFromV1beta1Op(s.Operation),
			})
		}
	}
	return out
}

// legacyMutateInstance is the test-side persistence layer for
// ReconcileInput.MutateInstance. Skips retry.RetryOnConflict (fake
// client has no optimistic-concurrency failures to retry). Mirrors
// the post-commit Operation onto the in-memory ISVC.Status snapshot
// the test holds a pointer to.
func legacyMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	v1component := v1beta1.ComponentType(component)
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
			return fmt.Errorf("get isvc: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
		}
		cs := fresh.Status.Components[v1component]
		if cs.Lifecycle == nil {
			cs.Lifecycle = &v1beta1.LifecycleStatus{}
		}
		pos := -1
		for i, s := range cs.Lifecycle.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		var w workload.InstanceStatus
		if pos == -1 {
			w = workload.InstanceStatus{Index: idx}
		} else {
			s := cs.Lifecycle.InstanceStatuses[pos]
			w = workload.InstanceStatus{
				Index:           s.Index,
				Incarnation:     s.Incarnation,
				Phase:           workload.InstancePhase(s.Phase),
				RunningRevision: s.RunningRevision,
				TargetRevision:  s.TargetRevision,
				ActiveOrdinal:   s.ActiveOrdinal,
				Operation:       legacyFromV1beta1Op(s.Operation),
			}
		}
		if !mutate(&w) {
			return nil
		}
		updated := v1beta1.OMENativeInstanceStatus{
			Index:           w.Index,
			Incarnation:     w.Incarnation,
			Phase:           v1beta1.OMENativeInstancePhase(w.Phase),
			RunningRevision: w.RunningRevision,
			TargetRevision:  w.TargetRevision,
			ActiveOrdinal:   w.ActiveOrdinal,
			Operation:       legacyToV1beta1Op(w.Operation),
		}
		if pos == -1 {
			cs.Lifecycle.InstanceStatuses = append(cs.Lifecycle.InstanceStatuses, updated)
		} else {
			cs.Lifecycle.InstanceStatuses[pos] = updated
		}
		fresh.Status.Components[v1component] = cs
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// Mirror onto the in-memory ISVC the test holds a pointer to.
		if isvc.Status.Components == nil {
			isvc.Status.Components = fresh.Status.Components
		} else {
			isvc.Status.Components[v1component] = fresh.Status.Components[v1component]
		}
		return nil
	}
}

func legacyFromV1beta1Op(op *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if op == nil {
		return nil
	}
	return &workload.InstanceOperation{
		ID:             op.ID,
		Type:           workload.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		// SurgeIndex round-trips so gang-surge fixtures (Op.Step=Surge with
		// a SurgeIndex pointer) survive the projection. Without it the
		// gangSurgeUpdate "surging" detection sees SurgeIndex==nil and
		// re-allocates a fresh surge index instead of resuming the
		// in-flight one.
		SurgeIndex: op.SurgeIndex,
	}
}

func legacyToV1beta1Op(op *workload.InstanceOperation) *v1beta1.InstanceOperation {
	if op == nil {
		return nil
	}
	return &v1beta1.InstanceOperation{
		ID:             op.ID,
		Type:           v1beta1.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		SurgeIndex:     op.SurgeIndex,
	}
}

// legacyBoolPtr is a tiny pointer helper used by
// MarkNotReadyDuringLifecycle / similar bool-pointer test fixtures.
func legacyBoolPtr(v bool) *bool { return &v }

// legacyTargetSpecImage builds the basic single-container PodSpec
// used as a rollout target throughout the Update / SurgeThenDrain
// tests.
func legacyTargetSpecImage(image string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: image},
		},
	}
}

// legacyRunningPodAtRevision shapes a pod the controller would
// consider running at revName — labeled with the appropriate
// incarnation, image, runtime-ready + serving.
func legacyRunningPodAtRevision(isvc *v1beta1.InferenceService, instIdx int32, incarnation int64, image string) *corev1.Pod {
	pod := legacyPodAtIncarnation(isvc, instIdx, incarnation, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: image}}
	return pod
}

// legacyISVCReadyAtIncarnation builds an ISVC whose InstanceStatus
// for engine index 0 is Phase=Ready at the given Incarnation — the
// steady-state from which an Update trigger fires.
func legacyISVCReadyAtIncarnation(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := legacyMinimalISVC(name, ns, 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	return isvc
}

// legacySliceWithEndpoint builds an EndpointSlice with one endpoint
// targeting pod with the given Ready condition. Used by drain
// fixtures across the Update / Recreate flows.
func legacySliceWithEndpoint(namespace, sliceName, serviceName string, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	readyPtr := ready
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
					Ready: &readyPtr,
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

// legacyEngineRevisionKey returns the per-Component revision.Key the
// production ISVC adapter emits.
func legacyEngineRevisionKey(isvc *v1beta1.InferenceService) revision.Key {
	return revision.Key{
		Namespace: isvc.Namespace,
		Name:      isvc.Name + "-" + string(workload.ComponentEngine),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(workload.ComponentEngine),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// legacyEnsureTargetCR creates the deterministic CR for spec in the
// fake client so Update has something to dispatch against.
func legacyEnsureTargetCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec *corev1.PodSpec) *appsv1.ControllerRevision {
	t.Helper()
	return legacyEnsureTargetCRWithMeta(t, c, isvc, spec, nil)
}

// legacyEnsureTargetCRWithMeta is legacyEnsureTargetCR's PodMeta-aware
// sibling. Captures user-intent annotations via meta so the in-place
// annotation-propagation regression tests can verify those values
// reach the live pod after an in-place rollout. nil meta degrades to
// the no-meta behavior.
func legacyEnsureTargetCRWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec *corev1.PodSpec, meta *metav1.ObjectMeta) *appsv1.ControllerRevision {
	t.Helper()
	cr, _, err := revision.EnsureControllerRevision(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		spec, meta, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("revision.EnsureControllerRevision: %v", err)
	}
	return cr
}

// legacySeedRunningRevision creates a ControllerRevision capturing
// spec as the Instance's recorded running template and writes its
// name onto InstanceStatus[idx].RunningRevision. The Update state
// machine's inPlaceEligible reads the recorded baseline (NOT the live
// pod) when deciding image-only-vs-bigger-diff.
func legacySeedRunningRevision(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType, idx int32, spec *corev1.PodSpec) {
	t.Helper()
	legacySeedRunningRevisionWithMeta(t, c, isvc, component, idx, spec, nil)
}

// legacySeedRunningRevisionWithMeta captures the previous-revision
// PodMeta so the in-place annotation diff has a baseline to subtract
// from.
func legacySeedRunningRevisionWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType, idx int32, spec *corev1.PodSpec, meta *metav1.ObjectMeta) {
	t.Helper()
	v1component := v1beta1.ComponentType(component)
	cr, _, err := revision.EnsureControllerRevision(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		spec, meta, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("legacySeedRunningRevisionWithMeta: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	if fresh.Status.Components == nil {
		fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := fresh.Status.Components[v1component]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	for i := range cs.Lifecycle.InstanceStatuses {
		if cs.Lifecycle.InstanceStatuses[i].Index == idx {
			cs.Lifecycle.InstanceStatuses[i].RunningRevision = cr.Name
			fresh.Status.Components[v1component] = cs
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatalf("set RunningRevision: %v", err)
			}
			if isvc.Status.Components == nil {
				isvc.Status.Components = fresh.Status.Components
			} else {
				isvc.Status.Components[v1component] = fresh.Status.Components[v1component]
			}
			return
		}
	}
	t.Fatalf("no InstanceStatus for idx=%d to attach RunningRevision", idx)
}

// legacyComponentPlan builds the single-pod engine ComponentPlan
// used by the Update / SurgeThenDrain tests. Reproduced inline
// because workload/ops tests can't import omenative/core. strategy
// controls the UpdateStrategy.Type.
func legacyComponentPlan(strategy workload.UpdateStrategyType, inPlaceStrategy *workload.InPlaceUpdateStrategy) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: workload.UpdateStrategy{
			Type:                  strategy,
			InPlaceUpdateStrategy: inPlaceStrategy,
		},
	}
}

// legacyMultiPodComponentPlan is legacyComponentPlan's multi-pod
// sibling — used by the multi-pod SurgeThenDrain fallback tests where
// the chooser must downgrade surge / in-place to recreate.
func legacyMultiPodComponentPlan(strategy workload.UpdateStrategyType) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners: []workload.RunnerPlan{
				{Name: "leader", Size: 1},
				{Name: "worker", Size: 1},
			},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: workload.UpdateStrategy{
			Type: strategy,
		},
	}
}

// legacyTestDeps builds the standard workload.Deps. Recorder is nil —
// event helpers are nil-safe and tests don't assert on the event
// stream.
func legacyTestDeps(c client.Client) workload.Deps {
	return workload.Deps{Client: c}
}

// Keep the revision import live even when partial test builds don't
// reference any helper that uses it.
var _ = revision.Key{}
