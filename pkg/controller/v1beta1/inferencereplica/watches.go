package inferencereplica

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// irKind is the Kind string on the controller owner reference the
// projector stamps on every Pod the IR-managed path creates. The pod
// event handler uses it to enqueue the owning InferenceReplica.
const irKind = "InferenceReplica"

// managedByOMENativePredicate keeps the pod watch to OMENative-managed
// pods only (`ome.io/managed-by=OMENative`), so non-OMENative pod churn
// in shared namespaces doesn't wake the handler.
//
// On Update it ADDITIONALLY drops events that don't touch any field the
// IR reconcile reacts to. At ~6000 pods/cluster the kubelet emits a
// continuous stream of status heartbeats (timestamps, observedGeneration,
// resource metrics) whose only effect is bumping ResourceVersion;
// forwarding those produced a self-sustaining reconcile flood after the
// outage. ResourceVersionChangedPredicate can't help — RV always changes
// — so podReconcileRelevantChanged does a real field comparison.
// Create / Delete still always enqueue (they move the expectations
// counter and must reach the handler).
func managedByOMENativePredicate() predicate.Predicate {
	managed := func(obj client.Object) bool {
		return obj.GetLabels()[query.LabelManagedBy] == query.ManagedByOMENative
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return managed(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return managed(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return managed(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Honor the managed-by gate on the new object first; if the
			// label was just stripped (old managed, new not) we still want
			// the transition to reach the handler so it can react.
			if !managed(e.ObjectNew) && !managed(e.ObjectOld) {
				return false
			}
			oldPod, ok1 := e.ObjectOld.(*corev1.Pod)
			newPod, ok2 := e.ObjectNew.(*corev1.Pod)
			if !ok1 || !ok2 {
				// Not a Pod pair (shouldn't happen on a Pod watch) — fail
				// open and enqueue rather than silently swallow.
				return true
			}
			return podReconcileRelevantChanged(oldPod, newPod)
		},
	}
}

// podReconcileRelevantChanged reports whether an old→new Pod transition
// touched any field the IR reconcile / workload dispatcher actually reads:
//
//   - Status.Phase — Instance phase aggregation.
//   - ContainersReady condition — CountReadyPods / AllPodsRuntimeReady.
//   - ome.io/serving condition — CountServingPods / RatioBalanced gate.
//   - ContainerStatuses / InitContainerStatuses — terminal-failure
//     detection (CrashLoopBackOff / ImagePullBackOff escalation).
//   - DeletionTimestamp — drain / recreate ordering.
//   - Spec.SchedulingGates (via workload.PodAdmissionGated) — drives the
//     edge-triggered InstanceReadyTimeout park/restart on Kueue gate-exit
//     (a gate-removal update changes only SchedulingGates, Phase stays
//     Pending, so without this it would be dropped).
//   - Spec.NodeName — feeds CountScheduledPods / UniqueNodes →
//     Status.ScheduledPodCount / NodesOccupied (a scheduler bind sets
//     NodeName with Phase still Pending).
//   - OwnerReferences — enqueueOwner target.
//   - The OMENative routing labels (managed-by, isvc, component,
//     instance-index, revision-hash) the handler keys the expectations
//     cache and per-revision convergence on.
//
// Heartbeat-only updates (timestamps, hostIP/podIP refresh, resource
// metrics, observedGeneration) leave all of these untouched and are
// dropped. A miss only ever delays a reconcile until the next genuine
// change or the periodic resync, never drops state permanently.
func podReconcileRelevantChanged(oldPod, newPod *corev1.Pod) bool {
	if oldPod == nil || newPod == nil {
		return true
	}
	if oldPod.Status.Phase != newPod.Status.Phase {
		return true
	}
	if (oldPod.DeletionTimestamp == nil) != (newPod.DeletionTimestamp == nil) {
		return true
	}
	if workload.PodAdmissionGated(oldPod) != workload.PodAdmissionGated(newPod) {
		return true
	}
	if oldPod.Spec.NodeName != newPod.Spec.NodeName {
		return true
	}
	if podreadiness.IsContainersReady(oldPod) != podreadiness.IsContainersReady(newPod) {
		return true
	}
	if podreadiness.IsServing(oldPod) != podreadiness.IsServing(newPod) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldPod.Status.ContainerStatuses, newPod.Status.ContainerStatuses) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldPod.Status.InitContainerStatuses, newPod.Status.InitContainerStatuses) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldPod.OwnerReferences, newPod.OwnerReferences) {
		return true
	}
	return routingLabelsChanged(oldPod, newPod)
}

// routingLabelsChanged compares only the OMENative labels the handler
// routes / keys on. Comparing the full label map would re-flood on
// orthogonal label churn (e.g. a node-selector relabel) the reconcile
// doesn't care about.
func routingLabelsChanged(oldPod, newPod *corev1.Pod) bool {
	for _, k := range routingLabelKeys {
		if oldPod.Labels[k] != newPod.Labels[k] {
			return true
		}
	}
	return false
}

// routingLabelKeys are the labels podReconcileRelevantChanged diffs — the
// tuple workloadKeyFromPod / enqueueOwner / per-revision convergence read.
var routingLabelKeys = []string{
	query.LabelManagedBy,
	constants.InferenceServicePodLabelKey,
	constants.OMEComponentLabel,
	query.LabelInstanceIdx,
	query.LabelRevisionHash,
}

// newPodEventHandler returns an EventHandler that updates the
// Expectations cache on pod adds/deletes BEFORE enqueueing the owning
// InferenceReplica. The order matters: if the enqueue fired first the
// next reconcile could read a stale expectation entry and stay parked
// behind the create/delete gate until the 2-minute TTL.
//
// This mirrors omenative.NewPodEventHandler on the legacy ISVC path.
// The IR controller needs its own copy because it dispatches the same
// workload pipeline (ops.surgeUpdate / recreateUpdate / Migrate) whose
// destructive steps gate on ExpectationsCache().Satisfied(); without an
// observer feeding the SAME cache the dispatcher reads, those gates only
// release on the TTL and every surge/recreate stalls (Instance pinned at
// Phase=Updating, readyReplicas=0).
func newPodEventHandler(exp *workload.Expectations) handler.EventHandler {
	return &podEventHandler{expectations: exp}
}

type podEventHandler struct {
	expectations *workload.Expectations
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
	// Update events don't move the expectations counter (creates/deletes
	// do). Still enqueue so per-pod transitions like ContainersReady=True
	// and serving-gate flips drive the owning IR reconcile forward.
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
// cache, keyed identically to the dispatcher's ExpectCreates /
// ExpectDeletes: (namespace, parent-ISVC name, component, instance).
// The parent-ISVC name is the pod's ome.io/inferenceservice label, which
// equals workload.ReconcileInput.Key.OwnerName on the IR path
// (inferencereplica/convert.go sets OwnerName = ParentRef.Name).
func (h *podEventHandler) observe(pod *corev1.Pod, isAdd bool) {
	isvc, component, idx, ok := workloadKeyFromPod(pod)
	if !ok {
		return
	}
	if isAdd {
		h.expectations.ObservedCreate(pod.Namespace, isvc, component, idx)
		return
	}
	h.expectations.ObservedDelete(pod.Namespace, isvc, component, idx)
}

// enqueueOwner pushes a reconcile request for the InferenceReplica
// controller-ref on the pod. Pods the IR-managed path creates carry the
// IR as their controller owner; pods owned by something else (e.g. the
// legacy ISVC-direct path) are skipped — observe already ran and is a
// harmless no-op against this controller's cache.
func (h *podEventHandler) enqueueOwner(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if ref.Kind != irKind {
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

// workloadKeyFromPod pulls (parent-ISVC name, component, instanceIdx)
// from pod labels — the tuple the workload Expectations cache keys on.
// Returns ok=false when any required label is missing/unparsable.
func workloadKeyFromPod(pod *corev1.Pod) (string, workload.ComponentType, int32, bool) {
	if pod == nil || pod.Labels == nil {
		return "", "", 0, false
	}
	if pod.Labels[query.LabelManagedBy] != query.ManagedByOMENative {
		return "", "", 0, false
	}
	isvc := pod.Labels[constants.InferenceServicePodLabelKey]
	if isvc == "" {
		return "", "", 0, false
	}
	comp := pod.Labels[constants.OMEComponentLabel]
	if comp == "" {
		return "", "", 0, false
	}
	idx, ok := query.InstanceIdxFromLabels(pod)
	if !ok {
		return "", "", 0, false
	}
	return isvc, workload.ComponentType(comp), idx, true
}
