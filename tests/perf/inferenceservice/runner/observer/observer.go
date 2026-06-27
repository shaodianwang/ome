// Package observer watches harness-owned InferenceServices and reports
// the instant each one first becomes Ready to the recorder. It is the
// measurement counterpart to the executor: the executor makes
// Deployments Ready, the InferenceService reconciler rolls that up into
// the InferenceService's Ready condition, and the observer timestamps
// that transition.
//
// It records on the event-handler path (the predicate), not inside
// Reconcile, so the timestamp is as close as possible to when the
// apiserver delivered the status change — and so a full reconcile is
// never even queued for objects the harness only needs to watch.
package observer

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	perflabels "github.com/ome-projects/ome/tests/perf/inferenceservice/runner/labels"
)

// ReadySink receives the first-Ready timestamp of each InferenceService.
// Implemented by *recorder.Recorder.
type ReadySink interface {
	RecordReady(key types.NamespacedName, class string, at time.Time)
}

// Observer records InferenceService Ready transitions. It implements
// both reconcile.Reconciler (a no-op — all work happens in the
// predicate) and predicate.Predicate.
type Observer struct {
	sink ReadySink
}

// New constructs an Observer feeding sink.
func New(sink ReadySink) *Observer {
	return &Observer{sink: sink}
}

var (
	_ reconcile.Reconciler = (*Observer)(nil)
	_ predicate.Predicate  = (*Observer)(nil)
)

// Reconcile is intentionally a no-op: the observer only watches. Doing
// nothing here keeps the observer's work queue empty so it adds no
// reconcile pressure to the run being measured.
func (o *Observer) Reconcile(context.Context, reconcile.Request) (reconcile.Result, error) {
	return ctrl.Result{}, nil
}

// Create records readiness for InferenceServices that are already Ready
// at the time the observer's cache first lists them (possible if the
// observer starts after some objects converged). Returns false so no
// reconcile is queued.
func (o *Observer) Create(ev event.CreateEvent) bool {
	o.record(ev.Object)
	return false
}

// Update records readiness on the status transition to Ready. Returns
// false so no reconcile is queued.
func (o *Observer) Update(ev event.UpdateEvent) bool {
	o.record(ev.ObjectNew)
	return false
}

func (o *Observer) Delete(event.DeleteEvent) bool   { return false }
func (o *Observer) Generic(event.GenericEvent) bool { return false }

// record reports first-Ready for a harness InferenceService. Idempotency
// (recording each object's readiness at most once) is the recorder's
// job — it ignores a second Ready stamp for the same key.
func (o *Observer) record(obj client.Object) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return
	}
	if !isvc.Status.IsReady() {
		return
	}
	o.sink.RecordReady(
		types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name},
		isvc.Labels[perflabels.ClassLabel],
		time.Now())
}

// SetupWithManager registers the observer, scoped to harness-owned
// InferenceServices via a label predicate ANDed with the observer's own
// record-only predicate.
func (o *Observer) SetupWithManager(mgr ctrl.Manager) error {
	harnessOnly, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchLabels: map[string]string{perflabels.HarnessLabel: perflabels.HarnessLabelValue},
	})
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(predicate.And(harnessOnly, o))).
		Complete(o)
}
