package omenative

import (
	"context"
	"testing"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// Test fixtures shared by reconciler_test.go and status_test.go.
// Duplicated from ops/*_test.go (intentional): both top-level and
// ops/ tests need the same shapes, and Go test packages can't share
// unexported helpers across directory boundaries. A shared testutil/
// subpackage would force every helper to be exported with capital
// names — not worth the churn for ~150 lines of fixtures.

// resetExpectations clears core.DefaultExpectations so per-test create
// attempts aren't blocked by entries from a previous test.
func resetExpectations(t *testing.T) {
	t.Helper()
	core.DefaultExpectations = core.NewExpectations()
}

// minimalISVC builds an ISVC with the minimum metadata the reconciler needs.
func minimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
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

// minimalParams builds a single-pod / engine ReconcileParams.
// ObjectMeta is populated with the per-Component name + namespace the
// dispatch site in components/engine.go would supply at runtime — the
// top-level service.ServiceReconciler used by ensureHeadlessService
// reads componentMeta.Name to derive the stable Service name.
func minimalParams(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) ReconcileParams {
	t.Helper()
	return ReconcileParams{
		ISVC: isvc,
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.StableServiceName(isvc.Name, internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent)),
			Namespace: isvc.Namespace,
		},
		PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}
}

// newFakeClientWithDiscovery builds a controller-runtime fake client
// pre-populated with the schemes the omenative reconciler exercises:
// corev1, appsv1, discoveryv1 (for EndpointSlice), v1beta1 (with
// status subresource wired), and scheduler-plugins
// scheduling.x-k8s.io/v1alpha1 (for PodGroup).
func newFakeClientWithDiscovery(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	// ReconcileComponent now emits a PodMonitor alongside the headless +
	// stable Services (see podmonitor.go); the fake client needs the
	// monitoring scheme registered so CreateOrUpdate can serialize it.
	if err := monitoringv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add monitoringv1: %v", err)
	}
	// scheduler-plugins PodGroup scheme — registered so tests that flip
	// p.GangSchedulingAvailable=true can exercise the EnsurePodGroup
	// path. Tests that leave GangSchedulingAvailable=false simply skip
	// the PodGroup ops, so registration is harmless either way.
	if err := schedulingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add schedulingv1alpha1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

// helperPodLabels reproduces the label set ops.Render stamps on every
// pod. Used by helper pod constructors below so the labels stay aligned
// even though the ops/ podLabels helper isn't reachable from this
// package.
func helperPodLabels(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string, incarnation int64) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                intToLabel(int64(instanceIdx)),
		query.LabelInstanceIncarnation:        intToLabel(incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

func intToLabel(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// podForInstance fabricates a pod matching what ops.Render would produce
// for the given (ISVC, instance) pair, with knobs for runtime-ready /
// serving state. Equivalent to ops/create_test.go's podForInstance.
func podForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent), instanceIdx, "default", 0),
			Namespace: isvc.Namespace,
			Labels:    helperPodLabels(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 1),
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

// podAtIncarnation extends podForInstance to stamp a specific incarnation
// label.
func podAtIncarnation(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ready, serving bool) *corev1.Pod {
	pod := podForInstance(isvc, instanceIdx, ready, serving)
	pod.Labels[query.LabelInstanceIncarnation] = intToLabel(incarnation)
	return pod
}

// isvcReadyAtIncarnation builds an ISVC whose engine[0] is Phase=Ready
// at the given Incarnation.
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

// sliceWithEndpoint constructs an EndpointSlice carrying exactly one
// endpoint targeting pod with the given Ready condition.
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

// targetSpecImage returns a single-container PodSpec at the given image.
func targetSpecImage(image string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: image}},
	}
}

// ensureTargetCR creates a deterministic ControllerRevision for spec so
// tests that need a target CR have one to dispatch against.
func ensureTargetCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, spec *corev1.PodSpec) *appsv1.ControllerRevision {
	t.Helper()
	cr, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, component, spec, nil, nil)
	if err != nil {
		t.Fatalf("revision.EnsureControllerRevision: %v", err)
	}
	return cr
}
