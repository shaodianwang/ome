package omenative

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

func TestManagedByOMENativePredicate(t *testing.T) {
	p := ManagedByOMENativePredicate()

	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"nil labels", nil, false},
		{"missing label", map[string]string{"other": "value"}, false},
		{"wrong value", map[string]string{query.LabelManagedBy: "Deployment"}, false},
		{"match", map[string]string{query.LabelManagedBy: query.ManagedByOMENative}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels}}
			if got := p.Create(event.TypedCreateEvent[client.Object]{Object: pod}); got != tc.want {
				t.Errorf("Create predicate got=%v want=%v", got, tc.want)
			}
			if got := p.Delete(event.TypedDeleteEvent[client.Object]{Object: pod}); got != tc.want {
				t.Errorf("Delete predicate got=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestPodEventHandler_ObservedCreatesAndEnqueuesOwner(t *testing.T) {
	exp := NewExpectations()
	ns := "prod"
	isvc := "llama-70b"
	component := v1beta1.EngineComponent
	wComponent := workload.ComponentType(component)
	idx := int32(0)

	// Pretend the controller just issued one create for this Instance.
	exp.ExpectCreates(ns, isvc, wComponent, idx, 1)
	if exp.Satisfied(ns, isvc, wComponent, idx) {
		t.Fatalf("expectations should NOT be satisfied immediately after ExpectCreates")
	}

	pod := newOMENativePod(ns, isvc, component, idx, true)
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())

	h := NewPodEventHandler(exp)
	h.Create(context.Background(), event.TypedCreateEvent[client.Object]{Object: pod}, q)

	if !exp.Satisfied(ns, isvc, wComponent, idx) {
		t.Errorf("expectations should be satisfied after observed create")
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 reconcile.Request enqueued, got %d", q.Len())
	}
	req, _ := q.Get()
	want := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: isvc}}
	if req != want {
		t.Errorf("enqueued request mismatch: got=%v want=%v", req, want)
	}
}

func TestPodEventHandler_ObservedDelete(t *testing.T) {
	exp := NewExpectations()
	ns := "prod"
	isvc := "llama-70b"
	component := v1beta1.EngineComponent
	wComponent := workload.ComponentType(component)
	idx := int32(2)
	exp.ExpectDeletes(ns, isvc, wComponent, idx, 1)

	pod := newOMENativePod(ns, isvc, component, idx, true)
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())

	h := NewPodEventHandler(exp)
	h.Delete(context.Background(), event.TypedDeleteEvent[client.Object]{Object: pod}, q)

	if !exp.Satisfied(ns, isvc, wComponent, idx) {
		t.Errorf("expectations should be satisfied after observed delete")
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 reconcile.Request enqueued, got %d", q.Len())
	}
}

func TestPodEventHandler_NoOwnerNoEnqueue(t *testing.T) {
	// Pod has OMENative labels but no controller-ref to ISVC — enqueue
	// is best-effort and silently skips; expectations still flip.
	exp := NewExpectations()
	wEngine := workload.ComponentType(v1beta1.EngineComponent)
	exp.ExpectCreates("prod", "llama-70b", wEngine, 0, 1)
	pod := newOMENativePod("prod", "llama-70b", v1beta1.EngineComponent, 0, false)
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())

	NewPodEventHandler(exp).Create(context.Background(), event.TypedCreateEvent[client.Object]{Object: pod}, q)

	if !exp.Satisfied("prod", "llama-70b", wEngine, 0) {
		t.Errorf("expectations should be satisfied even without owner-ref")
	}
	if q.Len() != 0 {
		t.Errorf("expected no enqueue when owner-ref is missing, got %d", q.Len())
	}
}

func TestPodEventHandler_NilExpectationsFallsBackToDefault(t *testing.T) {
	// Test that passing nil Expectations doesn't panic — handler
	// falls back to core.DefaultExpectations.
	pod := newOMENativePod("prod", "llama-70b", v1beta1.EngineComponent, 0, true)
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())

	defer core.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentType(v1beta1.EngineComponent), 0)

	h := NewPodEventHandler(nil)
	h.Create(context.Background(), event.TypedCreateEvent[client.Object]{Object: pod}, q)

	if q.Len() != 1 {
		t.Errorf("expected enqueue via core.DefaultExpectations fallback, got %d", q.Len())
	}
}

func TestExtractOMENativeKey(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		ok   bool
	}{
		{"nil pod", nil, false},
		{"nil labels", &corev1.Pod{}, false},
		{
			"missing managed-by",
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "x",
				constants.OMEComponentLabel:           "engine",
				query.LabelInstanceIdx:                "0",
			}}},
			false,
		},
		{
			"missing isvc",
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				query.LabelManagedBy:        query.ManagedByOMENative,
				constants.OMEComponentLabel: "engine",
				query.LabelInstanceIdx:      "0",
			}}},
			false,
		},
		{
			"missing component",
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				query.LabelManagedBy:                  query.ManagedByOMENative,
				constants.InferenceServicePodLabelKey: "x",
				query.LabelInstanceIdx:                "0",
			}}},
			false,
		},
		{
			"bad instance index",
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				query.LabelManagedBy:                  query.ManagedByOMENative,
				constants.InferenceServicePodLabelKey: "x",
				constants.OMEComponentLabel:           "engine",
				query.LabelInstanceIdx:                "not-a-number",
			}}},
			false,
		},
		{
			"ok",
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				query.LabelManagedBy:                  query.ManagedByOMENative,
				constants.InferenceServicePodLabelKey: "x",
				constants.OMEComponentLabel:           "engine",
				query.LabelInstanceIdx:                "3",
			}}},
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, got := extractOMENativeKey(tc.pod)
			if got != tc.ok {
				t.Errorf("ok mismatch: got=%v want=%v", got, tc.ok)
			}
		})
	}
}

func TestEndpointSliceToISVC(t *testing.T) {
	cases := []struct {
		name        string
		serviceName string
		ns          string
		wantISVC    string
		wantOK      bool
	}{
		{"empty service-name", "", "prod", "", false},
		{"non-headless", "foo-bar", "prod", "", false},
		{"foreign suffix", "foo-bar-headless", "prod", "", false},
		{"engine headless", "llama-70b-engine-headless", "prod", "llama-70b", true},
		{"decoder headless", "llama-70b-decoder-headless", "prod", "llama-70b", true},
		{"router headless", "llama-70b-router-headless", "prod", "llama-70b", true},
		{"missing isvc name", "engine-headless", "prod", "", false},
		{"isvc name with dashes", "my-fancy-llama-engine-headless", "prod", "my-fancy-llama", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slice := &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: tc.ns,
					Labels:    map[string]string{discoveryv1.LabelServiceName: tc.serviceName},
				},
			}
			got := EndpointSliceToISVC(context.Background(), slice)
			if !tc.wantOK {
				if len(got) != 0 {
					t.Errorf("expected no requests, got %v", got)
				}
				return
			}
			want := []reconcile.Request{{
				NamespacedName: types.NamespacedName{Namespace: tc.ns, Name: tc.wantISVC},
			}}
			if len(got) != 1 || got[0] != want[0] {
				t.Errorf("request mismatch: got=%v want=%v", got, want)
			}
		})
	}
}

func TestEndpointSliceToISVC_NonSliceObject(t *testing.T) {
	got := EndpointSliceToISVC(context.Background(), &corev1.Pod{})
	if len(got) != 0 {
		t.Errorf("expected no requests for non-EndpointSlice object, got %v", got)
	}
}

// newOMENativePod constructs a Pod with the OMENative label set and,
// optionally, a controller-owner ref to the parent ISVC. Used by the
// handler tests to feed the expectation+enqueue path.
func newOMENativePod(ns, isvc string, component v1beta1.ComponentType, idx int32, withOwner bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, internalsource.ComponentTypeToWorkload(component), idx, "default", 0),
			Namespace: ns,
			Labels: map[string]string{
				query.LabelManagedBy:                  query.ManagedByOMENative,
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(component),
				query.LabelInstanceIdx:                "0",
			},
		},
	}
	if idx != 0 {
		pod.Labels[query.LabelInstanceIdx] = intToA(idx)
	}
	if withOwner {
		isController := true
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       isvcKind,
			Name:       isvc,
			Controller: &isController,
		}}
	}
	return pod
}

// intToA avoids pulling strconv into the test helpers; the label
// values used here are tiny and deterministic.
func intToA(n int32) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
