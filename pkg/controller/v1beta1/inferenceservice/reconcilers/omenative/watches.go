package omenative

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// isvcKind is the Kind string carried on owner references OMENative
// stamps on every emitted Pod / ControllerRevision / ConfigMap. Used
// by the Pod event handler to filter parent-owner enqueueing.
const isvcKind = "InferenceService"

// headlessServiceSuffix is the trailing component of the per-Component
// headless Service name (`<isvc>-<component>-headless`). The
// EndpointSlice mapper uses it to recognise an OMENative-owned slice
// and reverse-lookup the parent ISVC.
const headlessServiceSuffix = "-headless"

// ManagedByOMENativePredicate keeps Pod (and other) events flowing
// through the controller's work queue only when the object carries
// `ome.io/managed-by=OMENative`. Without this filter, every Pod
// event in every namespace would wake up the Pod handler and cost
// a label scan.
func ManagedByOMENativePredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[labelManagedBy] == managedByOMENative
	})
}

// NewPodEventHandler returns an EventHandler that updates the
// Expectations cache on Pod adds and deletes before enqueueing the
// parent InferenceService. The expectations update has to happen
// BEFORE enqueue — otherwise the next reconcile reads a stale
// expectations entry and stays in cooldown, even though the watch
// just confirmed the writes.
//
// exp may be nil; the handler then falls back to DefaultExpectations
// so manager wiring before the refactor stays valid.
func NewPodEventHandler(exp *Expectations) handler.EventHandler {
	if exp == nil {
		exp = DefaultExpectations
	}
	return &podEventHandler{expectations: exp}
}

type podEventHandler struct {
	expectations *Expectations
}

func (h *podEventHandler) Create(ctx context.Context, evt event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := evt.Object.(*corev1.Pod)
	if !ok {
		return
	}
	h.observe(pod, true)
	h.enqueueOwner(pod, q)
}

func (h *podEventHandler) Update(ctx context.Context, evt event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Update events are rare for OMENative pods (we patch images +
	// conditions, not bulk fields) and don't move the expectations
	// counter — counters only flip on Add (create observed) and
	// Delete (delete observed). Still enqueue so per-pod transitions
	// like ContainersReady=True drive the parent reconcile forward.
	if pod, ok := evt.ObjectNew.(*corev1.Pod); ok {
		h.enqueueOwner(pod, q)
	}
}

func (h *podEventHandler) Delete(ctx context.Context, evt event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := evt.Object.(*corev1.Pod)
	if !ok {
		return
	}
	h.observe(pod, false)
	h.enqueueOwner(pod, q)
}

func (h *podEventHandler) Generic(ctx context.Context, evt event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	if pod, ok := evt.Object.(*corev1.Pod); ok {
		h.enqueueOwner(pod, q)
	}
}

// observe drives ObservedCreate / ObservedDelete on the expectations
// cache. Pods that don't carry the OMENative label set (predicate
// already filters most of these out) or that lack a parseable
// instance index are skipped — counters can't be released against
// an unknown entry.
func (h *podEventHandler) observe(pod *corev1.Pod, isAdd bool) {
	isvcName, component, idx, ok := extractOMENativeKey(pod)
	if !ok {
		return
	}
	if isAdd {
		h.expectations.ObservedCreate(pod.Namespace, isvcName, component, idx)
		return
	}
	h.expectations.ObservedDelete(pod.Namespace, isvcName, component, idx)
}

// enqueueOwner pushes a reconcile request for the InferenceService
// controller-ref on pod. Mirrors what handler.EnqueueRequestForOwner
// does but without depending on the scheme machinery — keeps the
// dependency surface small and the handler easy to unit-test.
func (h *podEventHandler) enqueueOwner(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if ref.Kind != isvcKind {
			continue
		}
		q.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      ref.Name,
			},
		})
		return
	}
}

// extractOMENativeKey pulls the (isvc, component, instanceIdx) tuple
// from pod labels. Returns ok=false when any required label is
// missing or unparsable — callers treat the pod as not-OMENative-
// managed and skip it.
func extractOMENativeKey(pod *corev1.Pod) (isvc string, component v1beta1.ComponentType, idx int32, ok bool) {
	if pod == nil || pod.Labels == nil {
		return "", "", 0, false
	}
	if pod.Labels[labelManagedBy] != managedByOMENative {
		return "", "", 0, false
	}
	isvc = pod.Labels[constants.InferenceServicePodLabelKey]
	if isvc == "" {
		return "", "", 0, false
	}
	compRaw := pod.Labels[constants.OMEComponentLabel]
	if compRaw == "" {
		return "", "", 0, false
	}
	parsedIdx, ok := instanceIdxFromLabels(pod)
	if !ok {
		return "", "", 0, false
	}
	return isvc, v1beta1.ComponentType(compRaw), parsedIdx, true
}

// EndpointSliceToISVC maps an EndpointSlice for an OMENative
// headless Service back to its parent InferenceService reconcile
// key. Returns an empty slice for any event that doesn't target an
// OMENative-managed Service — the EndpointSlice watch is
// unfiltered, so the mapper does all the filtering.
//
// EndpointSlice carries the `kubernetes.io/service-name` label
// pointing at its parent Service. OMENative headless Services are
// named `<isvc>-<component>-headless`, so a name parse is enough
// to identify them and recover the ISVC name.
func EndpointSliceToISVC(ctx context.Context, obj client.Object) []reconcile.Request {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		return nil
	}
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	isvcName, _, ok := isvcFromHeadlessServiceName(serviceName)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: slice.Namespace,
			Name:      isvcName,
		},
	}}
}

// isvcFromHeadlessServiceName parses `<isvc>-<component>-headless`
// into (isvc, component, ok). The component segment is matched
// against the v1beta1 component-type set so an arbitrary
// `-foo-headless` Service in the same namespace doesn't masquerade
// as OMENative-owned.
func isvcFromHeadlessServiceName(name string) (string, v1beta1.ComponentType, bool) {
	if name == "" || !strings.HasSuffix(name, headlessServiceSuffix) {
		return "", "", false
	}
	trimmed := strings.TrimSuffix(name, headlessServiceSuffix)
	for _, component := range []v1beta1.ComponentType{
		v1beta1.RouterComponent,
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
	} {
		suffix := "-" + string(component)
		if strings.HasSuffix(trimmed, suffix) {
			isvc := strings.TrimSuffix(trimmed, suffix)
			if isvc == "" {
				return "", "", false
			}
			return isvc, component, true
		}
	}
	return "", "", false
}
