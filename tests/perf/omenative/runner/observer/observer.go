// Package observer watches the harness's InferenceReplicas and pods and
// reports their readiness transitions to the recorder. It is the
// measurement core of the OMENative harness.
//
// Two watches, two signals:
//
//   - InferenceReplica: an OMENative ISVC is "Ready" exactly when its IR
//     reports ReadyReplicas == Replicas (every Instance's pods Ready). The
//     IR observer records that transition keyed by the IR's parent ISVC
//     name, which the recorder folds into per-class time-to-Ready. This is
//     the convergence signal the run waits on — directly on the IR status
//     the IR reconciler writes, not on a Deployment proxy.
//
//   - Pod: records each harness pod's creation time and the instant it
//     first reached PodReady, so the recorder can report pod
//     create-to-Ready latency independently of the per-ISVC roll-up. This
//     isolates pod-lifecycle latency (bind + kubelet-sim + serving-gate
//     flip) from the control-plane reconcile latency.
//
// Both record on the event-handler path (the predicate), not inside
// Reconcile, so a timestamp is as close as possible to when the apiserver
// delivered the change — and so no full reconcile is ever queued for
// objects the harness only needs to watch.
package observer

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	perflabels "github.com/ome-projects/ome/tests/perf/omenative/runner/labels"
)

// ReadySink receives the readiness measurements the observer produces.
// Implemented by *recorder.Recorder.
type ReadySink interface {
	RecordISVCReady(key types.NamespacedName, class string, at time.Time)
	RecordPodCreated(key types.NamespacedName, at time.Time)
	RecordPodReady(key types.NamespacedName, at time.Time)
}

// ClassResolver maps a parent-ISVC name to the size class the generator
// assigned it. The generator implements it from the name->class registry
// it builds as it creates objects, so class attribution does not depend on
// label propagation through the render pipeline.
type ClassResolver interface {
	ClassForISVC(name string) (string, bool)
}

// Observer records IR-readiness + pod-lifecycle transitions. It is wired
// as two controllers (one For InferenceReplica, one For Pod); both
// Reconcile methods are no-ops — all work happens in the predicates.
type Observer struct {
	sink     ReadySink
	resolver ClassResolver
}

// New constructs an Observer feeding sink, resolving class via resolver.
func New(sink ReadySink, resolver ClassResolver) *Observer {
	return &Observer{sink: sink, resolver: resolver}
}

var _ reconcile.Reconciler = (*Observer)(nil)

// Reconcile is intentionally a no-op: the observer only watches. Doing
// nothing here keeps the observer's work queues empty so they add no
// reconcile pressure to the run being measured.
func (o *Observer) Reconcile(context.Context, reconcile.Request) (reconcile.Result, error) {
	return ctrl.Result{}, nil
}

// --- InferenceReplica readiness ------------------------------------------

// irPredicate records IR convergence (ReadyReplicas == Replicas) on the
// event-handler path and always returns false so no reconcile is queued.
type irPredicate struct{ o *Observer }

var _ predicate.Predicate = irPredicate{}

func (p irPredicate) Create(ev event.CreateEvent) bool { p.o.recordIR(ev.Object); return false }
func (p irPredicate) Update(ev event.UpdateEvent) bool { p.o.recordIR(ev.ObjectNew); return false }
func (p irPredicate) Delete(event.DeleteEvent) bool    { return false }
func (p irPredicate) Generic(event.GenericEvent) bool  { return false }

// recordIR reports ISVC-readiness when a harness IR first shows every
// Instance Ready. The recorder dedupes a second Ready stamp for the same
// key, so re-reporting on later status churn is harmless.
func (o *Observer) recordIR(obj client.Object) {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return
	}
	desired := int32(0)
	if ir.Spec.Replicas != nil {
		desired = *ir.Spec.Replicas
	}
	// Converged: every desired Instance is Ready. desired==0 can't be
	// "ready" — guard against a transient pre-projection IR.
	if desired == 0 || ir.Status.ReadyReplicas < desired {
		return
	}
	isvcName := ir.Spec.ParentRef.Name
	if isvcName == "" {
		return
	}
	class := o.classOf(ir, isvcName)
	o.sink.RecordISVCReady(
		types.NamespacedName{Namespace: ir.Namespace, Name: isvcName},
		class, time.Now())
}

// classOf resolves the size class for an IR: the name->class registry is
// authoritative; the ClassLabel (which propagates ISVC->component->IR via
// the render pipeline) is a fallback so the observer still attributes a
// class if the registry ever misses an entry.
func (o *Observer) classOf(ir *v1beta1.InferenceReplica, isvcName string) string {
	if o.resolver != nil {
		if c, ok := o.resolver.ClassForISVC(isvcName); ok {
			return c
		}
	}
	return ir.Labels[perflabels.ClassLabel]
}

// --- Pod lifecycle --------------------------------------------------------

// podPredicate records pod create + ready on the event-handler path and
// always returns false so no reconcile is queued.
type podPredicate struct{ o *Observer }

var _ predicate.Predicate = podPredicate{}

func (p podPredicate) Create(ev event.CreateEvent) bool {
	p.o.recordPodCreated(ev.Object)
	p.o.recordPodReady(ev.Object) // already-Ready at first list (observer-late start)
	return false
}
func (p podPredicate) Update(ev event.UpdateEvent) bool {
	p.o.recordPodReady(ev.ObjectNew)
	return false
}
func (p podPredicate) Delete(event.DeleteEvent) bool   { return false }
func (p podPredicate) Generic(event.GenericEvent) bool { return false }

func (o *Observer) recordPodCreated(obj client.Object) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	// Prefer the apiserver-stamped CreationTimestamp so the measured
	// create-to-Ready window matches what an external observer would see,
	// independent of when this watch happened to deliver the create event.
	at := pod.CreationTimestamp.Time
	if at.IsZero() {
		at = time.Now()
	}
	o.sink.RecordPodCreated(client.ObjectKeyFromObject(pod), at)
}

func (o *Observer) recordPodReady(obj client.Object) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	if !podReady(pod) {
		return
	}
	o.sink.RecordPodReady(client.ObjectKeyFromObject(pod), time.Now())
}

// podReady reports whether a pod is "in rotation" by the SAME definition
// the IR status aggregator uses for its serving-pod count
// (status.CountServingPods): ContainersReady (kubelet) AND the
// controller's ome.io/serving readiness gate True.
//
// It deliberately does NOT check the kubelet-computed corev1.PodReady
// condition: envtest has no kubelet status-manager to AND the readiness
// gates into PodReady, so that condition is never synthesized here even
// though every gate is satisfied. Matching the aggregator's predicate keeps
// the observer's "pod ready" instant aligned with when the IR actually
// counts the pod toward ReadyReplicas.
func podReady(pod *corev1.Pod) bool {
	return podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod)
}

// --- Wiring ---------------------------------------------------------------

// SetupWithManager registers two watch-only controllers: one over
// harness-owned InferenceReplicas, one over harness-owned pods. Both are
// scoped by the harness label so they never touch objects outside the run.
func (o *Observer) SetupWithManager(mgr ctrl.Manager) error {
	harnessOnly, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchLabels: map[string]string{perflabels.HarnessLabel: perflabels.HarnessLabelValue},
	})
	if err != nil {
		return err
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("perf-observer-ir").
		For(&v1beta1.InferenceReplica{}, builder.WithPredicates(predicate.And(harnessOnly, irPredicate{o}))).
		Complete(o); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("perf-observer-pod").
		For(&corev1.Pod{}, builder.WithPredicates(predicate.And(harnessOnly, podPredicate{o}))).
		Complete(o)
}
