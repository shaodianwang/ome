package omenative

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newDrainTestClient builds a fake controller-runtime client with the
// scheme drain.go reads from (corev1 + discoveryv1).
func newDrainTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func testPod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
		},
	}
}

// sliceForService builds an EndpointSlice labeled for serviceName with one
// endpoint per (podName, ready) tuple supplied. Each endpoint's TargetRef
// is set to {Kind: Pod, Namespace: namespace, Name: podName}.
func sliceForService(namespace, sliceName, serviceName string, endpoints ...endpointSpec) *discoveryv1.EndpointSlice {
	es := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sliceName,
			Namespace: namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	for _, e := range endpoints {
		ep := discoveryv1.Endpoint{
			Addresses: []string{e.address},
			Conditions: discoveryv1.EndpointConditions{
				Ready: e.ready,
			},
		}
		if e.podName != "" {
			ep.TargetRef = &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: namespace,
				Name:      e.podName,
			}
		}
		es.Endpoints = append(es.Endpoints, ep)
	}
	return es
}

type endpointSpec struct {
	podName string
	address string
	ready   *bool
}

func TestIsPodDrained_NilPodRejected(t *testing.T) {
	c := newDrainTestClient(t)
	if _, err := IsPodDrained(context.Background(), c, "ns", "svc", nil); err == nil {
		t.Fatal("expected error for nil pod")
	}
}

func TestIsPodDrained_EmptyServiceNameRejected(t *testing.T) {
	c := newDrainTestClient(t)
	pod := testPod("ns", "p1")
	if _, err := IsPodDrained(context.Background(), c, "ns", "", pod); err == nil {
		t.Fatal("expected error for empty service name")
	}
}

func TestIsPodDrained_NoSlicesAtAll_ReturnsTrue(t *testing.T) {
	// No EndpointSlices AND no Service in the cluster — drained by
	// definition (kube-proxy has nothing to route to). PR B's
	// fail-loud branch only fires when the Service exists; absence
	// of both is the trivial-drain case.
	c := newDrainTestClient(t)
	pod := testPod("ns", "p1")
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true with no slices and no service")
	}
}

func TestIsPodDrained_ServiceExistsNoSlices_ReturnsNotDrained(t *testing.T) {
	// PR B (DRAIN-2 fix): the previous behavior fail-opened when the
	// slice list was empty, but with the Service present this is a
	// transient state — informer cold-start, no matching pods yet,
	// kube-controller-manager lag. Conservative: not drained.
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns"},
	}
	pod := testPod("ns", "p1")
	c := newDrainTestClient(t, svc)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when Service exists but no slices")
	}
}

func TestIsPodDrained_SlicesForDifferentService_ReturnsTrue(t *testing.T) {
	// A slice exists, but it's labeled for a different Service. Drain is
	// scoped to the named Service, so this is still drained.
	pod := testPod("ns", "p1")
	other := sliceForService("ns", "other-1", "other-svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, other)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when slice targets a different service")
	}
}

func TestIsPodDrained_PodAbsentFromSlices_ReturnsTrue(t *testing.T) {
	// Slice exists for svc but lists a different pod. p1 is not in
	// rotation through svc.
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p-other", address: "10.0.0.2", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when pod is absent from slice")
	}
}

func TestIsPodDrained_PodPresentAndReady_ReturnsFalse(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when pod is in slice with Ready=true")
	}
}

func TestIsPodDrained_PodPresentButNotReady_ReturnsTrue(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(false),
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when pod endpoint has Ready=false")
	}
}

func TestIsPodDrained_PodPresentReadyNil_TreatedAsReady(t *testing.T) {
	// discovery/v1 says Conditions.Ready SHOULD be set; if nil, presume
	// ready (matches kube-proxy). So drained=false.
	pod := testPod("ns", "p1")
	slice := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: nil,
	})
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when Ready is nil (presumed ready)")
	}
}

func TestIsPodDrained_AcrossMultipleSlices_FindsRoutableEndpoint(t *testing.T) {
	// Slice A has p1 with Ready=false; Slice B has p1 with Ready=true.
	// Any routable endpoint means not drained.
	pod := testPod("ns", "p1")
	a := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(false),
	})
	b := sliceForService("ns", "svc-2", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, a, b)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when any slice still publishes pod as Ready")
	}
}

func TestIsPodDrained_AcrossMultipleSlices_AllNotReady(t *testing.T) {
	pod := testPod("ns", "p1")
	a := sliceForService("ns", "svc-1", "svc", endpointSpec{
		podName: "p1", address: "10.0.0.1", ready: ptr.To(false),
	})
	b := sliceForService("ns", "svc-2", "svc", endpointSpec{
		podName: "p-other", address: "10.0.0.2", ready: ptr.To(true),
	})
	c := newDrainTestClient(t, a, b)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true: every endpoint targeting p1 is NotReady, other endpoints don't count")
	}
}

func TestIsPodDrained_TargetRefNamespaceMismatchIgnored(t *testing.T) {
	// EndpointSlice targets a pod named p1 in a different namespace —
	// must not match our pod.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: &corev1.ObjectReference{
					Kind: "Pod", Namespace: "other-ns", Name: "p1",
				},
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when TargetRef.Namespace differs from pod namespace")
	}
}

func TestIsPodDrained_TargetRefNilIgnored(t *testing.T) {
	// An endpoint with no TargetRef (e.g., custom publisher) can't be
	// matched to a pod and is ignored.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: nil,
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when no endpoint has TargetRef matching the pod")
	}
}

func TestIsPodDrained_TargetRefKindEmpty_TreatedAsPod(t *testing.T) {
	// A publisher that omits TargetRef.Kind but supplies Namespace+Name
	// should still match. Empty Kind is accepted; only an explicitly
	// non-"Pod" kind is rejected.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: &corev1.ObjectReference{
					Kind: "", Namespace: "ns", Name: "p1",
				},
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drained {
		t.Fatalf("expected drained=false when TargetRef.Kind is empty but Name+Namespace match a ready endpoint")
	}
}

func TestIsPodDrained_TargetRefKindNotPod_Rejected(t *testing.T) {
	// A TargetRef pointing at a non-Pod resource (e.g., Node) must not
	// match the pod, even when the name happens to coincide.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-1",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
				TargetRef: &corev1.ObjectReference{
					Kind: "Node", Namespace: "ns", Name: "p1",
				},
			},
		},
	}
	c := newDrainTestClient(t, slice)
	drained, err := IsPodDrained(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !drained {
		t.Fatalf("expected drained=true when TargetRef.Kind is not 'Pod' (must not match)")
	}
}

// --- DRAIN-1: endpointAvailable tri-state ---

func TestEndpointAvailable_ReadyTrueTerminatingFalseIsAvailable(t *testing.T) {
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ptr.To(true),
			Terminating: ptr.To(false),
		},
	}
	if !endpointAvailable(ep) {
		t.Errorf("Ready=true, Terminating=false should be available")
	}
}

func TestEndpointAvailable_ReadyTrueTerminatingNilIsAvailable(t *testing.T) {
	// nil Terminating is the steady-state shape (no termination underway).
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
	}
	if !endpointAvailable(ep) {
		t.Errorf("Ready=true, Terminating=nil should be available")
	}
}

func TestEndpointAvailable_ReadyTrueTerminatingTrueIsNotAvailable(t *testing.T) {
	// DRAIN-1 core case: kube-proxy keeps a terminating-but-still-Ready
	// endpoint in rotation for in-flight requests; it shouldn't count
	// as Available for status or surge-rotation checks.
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ptr.To(true),
			Terminating: ptr.To(true),
		},
	}
	if endpointAvailable(ep) {
		t.Errorf("Ready=true, Terminating=true must NOT be available")
	}
}

func TestEndpointAvailable_ReadyFalseIsNotAvailable(t *testing.T) {
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)},
	}
	if endpointAvailable(ep) {
		t.Errorf("Ready=false should never be available")
	}
}

// --- DRAIN-1: IsPodInRotation excludes terminating endpoints ---

func TestIsPodInRotation_TerminatingEndpointReportedNotInRotation(t *testing.T) {
	// Even with Ready=true, a terminating endpoint must not be
	// considered "in rotation" — Migrate would otherwise swap onto a
	// pod that's about to disappear.
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(true),
				Terminating: ptr.To(true),
			},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "ns", Name: "p1"},
		}},
	}
	c := newDrainTestClient(t, slice)
	in, err := IsPodInRotation(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if in {
		t.Errorf("terminating endpoint must not be reported as in-rotation")
	}
}

func TestIsPodInRotation_ReadyTrueNotTerminatingReportedInRotation(t *testing.T) {
	pod := testPod("ns", "p1")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s",
			Namespace: "ns",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(true),
				Terminating: ptr.To(false),
			},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "ns", Name: "p1"},
		}},
	}
	c := newDrainTestClient(t, slice)
	in, err := IsPodInRotation(context.Background(), c, "ns", "svc", pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !in {
		t.Errorf("non-terminating Ready endpoint must be reported as in-rotation")
	}
}
