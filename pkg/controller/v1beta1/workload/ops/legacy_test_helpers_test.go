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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// Test scaffolding for the Surge / Update bug-fix tests that moved from
// omenative/ops to workload/ops alongside the production state machine
// (part 2 slice 4f of N). The helpers below mirror their omenative
// counterparts (test_helpers_test.go, restart_test.go, update_test.go,
// surge_test.go) at the wire level but speak the workload-owned types
// the new state machine consumes. The test names are kept identical to
// the upstream omenative copies so the prior bug-coverage map carries
// over unchanged.

// testRevisionHashLegacy is the synthetic revision hash stamped on test
// pods in the legacy fixtures. The original omenative/ops package
// stamped a single constant; this package uses two distinct constants
// to avoid name collision with the workload/ops/create_test.go fixture
// shape (which is also testRevisionHash). The two values are unrelated;
// production callers always stamp the real per-revision hash.
const testRevisionHashLegacy = "testrev1"

// legacyMinimalISVC builds an ISVC with the minimum metadata Update /
// Surge state-machine tests need. Distinct from minimalISVC in
// create_test.go to keep the helpers package-internal and not collide
// with the external-package test fixtures.
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
// emitted pod. Lifted from the omenative test_helpers_test.go
// byte-for-byte so the moved bug-fix tests continue to fabricate
// pods that match production's selector.
func legacyTestPodLabels(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
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
// back-to-back tests don't observe prior ExpectCreates entries. Mirrors
// the omenative resetExpectations helper at the workload layer; the
// legacy tests called the omenative version, which already reset both
// caches.
func legacyResetExpectations(t *testing.T) {
	t.Helper()
	workload.DefaultExpectations = workload.NewExpectations()
}

// legacyNewFakeClient builds a controller-runtime fake client with the
// schemes the surge / update tests touch (corev1 + v1beta1 +
// discoveryv1 + appsv1). Status subresource is registered so
// Status().Update mirrors API-server behavior. Lifted from the
// omenative test_helpers_test.go.
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
// produce for the given (ISVC, instance) pair, with knobs for
// runtime-ready / serving state. Lifted from omenative test_helpers_test.go.
func legacyPodForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := legacyTestPodLabels(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 1, 0)
	labels[query.LabelRevisionHash] = testRevisionHashLegacy
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 0),
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
// workload.ReconcileInput. Mirrors omenative/internalsource.BuildReconcileInput
// at the test layer; we reproduce the shape inline to avoid a forward
// import from workload/ops onto the omenative adapter.
//
// MutateInstance round-trips through the fake client's Status().Update
// so tests can assert on the persisted ISVC.Status.Components[c].OMENative
// shape. The MutateInstance closure also mirrors the just-committed
// Operation back onto the in-memory ISVC.Status snapshot the test
// holds a pointer to — matches the production mirror (#246) that lets
// detectUpdateTrigger / chooseUpdateMode read the post-commit Operation
// step on the next pass within the same reconcile.
func legacyTestInput(isvc *v1beta1.InferenceService, c client.Client, component v1beta1.ComponentType) workload.ReconcileInput {
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
// InstanceStatuses slice into the workload-owned mirror. Mirrors
// internalsource.InstanceStatusSliceToWorkload at the test layer.
func legacyInstanceStatuses(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) []workload.InstanceStatus {
	var out []workload.InstanceStatus
	if cs, ok := isvc.Status.Components[component]; ok && cs.OMENative != nil {
		for _, s := range cs.OMENative.InstanceStatuses {
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
// ReconcileInput.MutateInstance. Mirrors omenative/status.MutateInstance
// at the wire level — Get / mutate / Status().Update — but skips
// retry.RetryOnConflict (the fake client has no optimistic-concurrency
// failures to retry). Also mirrors the post-commit Operation onto the
// in-memory ISVC.Status snapshot the test holds a pointer to (#246
// requirement: subsequent passes within the same reconcile observe the
// just-stamped Phase / Operation without re-reading the apiserver).
func legacyMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
			return fmt.Errorf("get isvc: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
		}
		cs := fresh.Status.Components[component]
		if cs.OMENative == nil {
			cs.OMENative = &v1beta1.OMENativeComponentStatus{}
		}
		pos := -1
		for i, s := range cs.OMENative.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		var w workload.InstanceStatus
		if pos == -1 {
			w = workload.InstanceStatus{Index: idx}
		} else {
			s := cs.OMENative.InstanceStatuses[pos]
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
			cs.OMENative.InstanceStatuses = append(cs.OMENative.InstanceStatuses, updated)
		} else {
			cs.OMENative.InstanceStatuses[pos] = updated
		}
		fresh.Status.Components[component] = cs
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// Mirror onto the in-memory ISVC the test holds a pointer to.
		if isvc.Status.Components == nil {
			isvc.Status.Components = fresh.Status.Components
		} else {
			isvc.Status.Components[component] = fresh.Status.Components[component]
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
	}
}

// (Other legacy helpers — legacyEnsureTargetCR, legacySeedRunningRevision,
// legacyPlanSinglePodEngine — were lifted from the omenative tests but
// the moved-in-this-slice tests use makeCR / surgePlan / surgePlanForInstance
// in surge_test.go directly. They land back in this helper file if a
// future test that lives here ends up needing them.)

// legacyBoolPtr is a tiny pointer helper used by
// MarkNotReadyDuringLifecycle / similar bool-pointer test fixtures.
func legacyBoolPtr(v bool) *bool { return &v }

// legacyTargetSpecImage builds the basic single-container PodSpec used
// as a rollout target throughout the moved Update / SurgeThenDrain
// tests. Mirrors omenative/ops/test_helpers_test.go.targetSpecImage.
func legacyTargetSpecImage(image string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: image},
		},
	}
}

// legacyRunningPodAtRevision shapes a pod the controller would consider
// "running at revName" — labeled with the appropriate incarnation, has
// the given image, runtime-ready + serving. Mirrors
// omenative/ops/test_helpers_test.go.runningPodAtRevision.
func legacyRunningPodAtRevision(isvc *v1beta1.InferenceService, instIdx int32, incarnation int64, image string) *corev1.Pod {
	pod := legacyPodAtIncarnation(isvc, instIdx, incarnation, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: image}}
	return pod
}

// legacyISVCReadyAtIncarnation builds an ISVC whose InstanceStatus for
// engine index 0 is Phase=Ready at the given Incarnation — the
// steady-state from which an Update trigger fires. Mirrors
// omenative/ops/restart_test.go.isvcReadyAtIncarnation.
func legacyISVCReadyAtIncarnation(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := legacyMinimalISVC(name, ns, 1)
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

// legacySliceWithEndpoint builds an EndpointSlice with one endpoint
// targeting pod with the given Ready condition. Mirrors
// omenative/ops/test_helpers_test.go.sliceWithEndpoint. Used by drain
// fixtures across the Update / Recreate flows that gate the drain wait
// on real apiserver state.
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

// legacyEngineRevisionKey is the per-Component revision.Key that
// EnsureControllerRevision needs. Centralized here so the ensureCR /
// seedRunningRevision helpers below produce the same key the
// production ISVC adapter (core.isvcRevisionKey) emits.
func legacyEngineRevisionKey(isvc *v1beta1.InferenceService) revision.Key {
	return revision.Key{
		Namespace: isvc.Namespace,
		Name:      isvc.Name + "-" + string(v1beta1.EngineComponent),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// legacyEnsureTargetCR computes the deterministic CR name for spec and
// creates it in the fake client so Update has something to dispatch
// against. Mirrors omenative/ops/update_test.go.ensureTargetCR but goes
// through workload/revision directly — the omenative wrapper just
// composes the revision.Key we now build inline.
func legacyEnsureTargetCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec *corev1.PodSpec) *appsv1.ControllerRevision {
	t.Helper()
	return legacyEnsureTargetCRWithMeta(t, c, isvc, spec, nil)
}

// legacyEnsureTargetCRWithMeta is legacyEnsureTargetCR's PodMeta-aware
// sibling. Captures user-intent annotations the operator wrote on
// spec.{engine,decoder,router} via the meta parameter so the in-place
// annotation-propagation regression tests can verify those values reach
// the live pod after an in-place rollout. nil meta degrades to the
// no-meta behavior. Mirrors omenative/ops/update_test.go.ensureTargetCRWithMeta.
func legacyEnsureTargetCRWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec *corev1.PodSpec, meta *metav1.ObjectMeta) *appsv1.ControllerRevision {
	t.Helper()
	cr, _, err := revision.EnsureControllerRevision(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		spec, meta, nil,
	)
	if err != nil {
		t.Fatalf("revision.EnsureControllerRevision: %v", err)
	}
	return cr
}

// legacySeedRunningRevision creates a ControllerRevision capturing spec
// as the Instance's recorded running template and writes its name onto
// InstanceStatus[idx].RunningRevision. The Update state machine's
// inPlaceEligible reads the recorded baseline (NOT the live pod) when
// deciding image-only-vs-bigger-diff. Mirrors
// omenative/ops/update_test.go.seedRunningRevision.
func legacySeedRunningRevision(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, spec *corev1.PodSpec) {
	t.Helper()
	legacySeedRunningRevisionWithMeta(t, c, isvc, component, idx, spec, nil)
}

// legacySeedRunningRevisionWithMeta is legacySeedRunningRevision's
// PodMeta-aware sibling — captures the previous-revision PodMeta so the
// in-place annotation diff has a baseline to subtract from. Mirrors
// omenative/ops/update_test.go.seedRunningRevisionWithMeta.
func legacySeedRunningRevisionWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, spec *corev1.PodSpec, meta *metav1.ObjectMeta) {
	t.Helper()
	cr, _, err := revision.EnsureControllerRevision(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		spec, meta, nil,
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
	cs := fresh.Status.Components[component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	for i := range cs.OMENative.InstanceStatuses {
		if cs.OMENative.InstanceStatuses[i].Index == idx {
			cs.OMENative.InstanceStatuses[i].RunningRevision = cr.Name
			fresh.Status.Components[component] = cs
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatalf("set RunningRevision: %v", err)
			}
			if isvc.Status.Components == nil {
				isvc.Status.Components = fresh.Status.Components
			} else {
				isvc.Status.Components[component] = fresh.Status.Components[component]
			}
			return
		}
	}
	t.Fatalf("no InstanceStatus for idx=%d to attach RunningRevision", idx)
}

// legacyComponentPlan builds the single-pod engine ComponentPlan used
// by the Update / SurgeThenDrain tests. Mirrors what
// core.BuildPlan(minimalParams(...)) produced in the omenative
// fixtures; reproduced inline because workload/ops tests can't reach
// for omenative/core. strategy controls the UpdateStrategy.Type.
func legacyComponentPlan(strategy v1beta1.UpdateStrategyType, inPlaceStrategy *v1beta1.InPlaceUpdateStrategy) workload.ComponentPlan {
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
			Type:                  strategy,
			InPlaceUpdateStrategy: inPlaceStrategy,
		},
	}
}

// legacyMultiPodComponentPlan is legacyComponentPlan's multi-pod
// sibling — used by the multi-pod SurgeThenDrain fallback tests where
// the chooser must downgrade surge / in-place to recreate.
func legacyMultiPodComponentPlan(strategy v1beta1.UpdateStrategyType) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: v1beta1.EngineComponent,
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
		UpdateStrategy: v1beta1.UpdateStrategy{
			Type: strategy,
		},
	}
}

// legacyTestDeps builds the standard workload.Deps a Update / Surge
// test consumes. Recorder is intentionally nil — event helpers are
// nil-safe and tests don't assert against the event stream (the
// omenative-side fixtures didn't either).
func legacyTestDeps(c client.Client) workload.Deps {
	return workload.Deps{Client: c}
}

// (legacyEngineRevisionKey / revision import live above.)
//
// Suppress the "imported and not used" warning on the revision package
// during piecemeal builds: the helpers above all reference it, but the
// linker emits a friendlier diagnostic if at least one var-level
// reference exists.
var _ = revision.Key{}
