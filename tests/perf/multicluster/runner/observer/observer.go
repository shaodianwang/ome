// Package observer watches the control-plane source InferenceServices and
// reports their placement lifecycle transitions to the recorder. It is the
// control-plane-side measurement counterpart to the member-side admitter:
// the admitter makes a member's derived admitted, the placer rolls that up
// into the source's status.placement, and the observer timestamps those
// transitions.
//
// Two transitions are timestamped:
//   - first fan-out: the source's placement leaves Pending (the placer is
//     Admitting, or has already Placed). This is the
//     control-plane-visible proxy for "the derived exists on the candidates".
//   - placed: the source's placement first reports Placed.
//
// It records on the event-handler path (the predicate), not inside Reconcile,
// so the timestamp is as close as possible to when the apiserver delivered the
// status change, and so a full reconcile is never queued for objects the
// harness only needs to watch.
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
	perflabels "github.com/ome-projects/ome/tests/perf/multicluster/runner/labels"
)

// Sink receives the source-ISVC placement-lifecycle timestamps. Implemented by
// the recorder.
type Sink interface {
	RecordFanOut(key types.NamespacedName, candidates int, at time.Time)
	RecordPlaced(key types.NamespacedName, at time.Time)
}

// Observer records source-ISVC placement transitions. It implements both
// reconcile.Reconciler (a no-op — all work happens in the predicate) and
// predicate.Predicate.
type Observer struct {
	sink Sink
	// candidates is the number of candidate clusters every source fans out to
	// in this run. In the harness all members advertise the accelerator label
	// the sources require, so every member is a candidate for every source; the
	// runner supplies the count (the member count) rather than the observer
	// snapshotting an in-progress fan-out status, which can undercount when the
	// placer skips Admitting and goes straight to Placed under fast admission.
	candidates int
}

// New constructs an Observer feeding sink, tagging fan-out events with the
// per-source candidate count.
func New(sink Sink, candidates int) *Observer {
	return &Observer{sink: sink, candidates: candidates}
}

var (
	_ reconcile.Reconciler = (*Observer)(nil)
	_ predicate.Predicate  = (*Observer)(nil)
)

// Reconcile is intentionally a no-op: the observer only watches. Doing nothing
// keeps its work queue empty so it adds no reconcile pressure to the placer
// being measured.
func (o *Observer) Reconcile(context.Context, reconcile.Request) (reconcile.Result, error) {
	return ctrl.Result{}, nil
}

// Create records lifecycle for sources already past Pending when the observer's
// cache first lists them. Returns false so no reconcile is queued.
func (o *Observer) Create(ev event.CreateEvent) bool {
	o.record(ev.Object)
	return false
}

// Update records lifecycle on each status transition. Returns false so no
// reconcile is queued.
func (o *Observer) Update(ev event.UpdateEvent) bool {
	o.record(ev.ObjectNew)
	return false
}

func (o *Observer) Delete(event.DeleteEvent) bool   { return false }
func (o *Observer) Generic(event.GenericEvent) bool { return false }

// record reports the fan-out and placed transitions for a source ISVC.
// Idempotency (recording each transition at most once) is the recorder's job —
// it ignores a second stamp for the same key.
func (o *Observer) record(obj client.Object) {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok || isvc.Status.Placement == nil {
		return
	}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
	phase := isvc.Status.Placement.Phase
	now := time.Now()

	// Any phase past Pending means the placer has fanned out (Admitting) or has
	// an admitted home (Placed). Both imply the derived exists on the candidates,
	// so either marks first fan-out. Accept legacy Racing during rolling upgrades.
	switch phase {
	case v1beta1.PlacementPhaseAdmitting,
		v1beta1.PlacementPhaseRacing, //nolint:staticcheck // Accept legacy status during rolling upgrades.
		v1beta1.PlacementPhasePlaced, v1beta1.PlacementPhaseFailed:
		o.sink.RecordFanOut(key, o.candidates, now)
	}
	if phase == v1beta1.PlacementPhasePlaced {
		o.sink.RecordPlaced(key, now)
	}
}

// SetupWithManager registers the observer on the control-plane manager, scoped
// to harness-owned source InferenceServices via a label predicate ANDed with
// the observer's own record-only predicate.
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
