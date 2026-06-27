// Package admitter is the member-side stand-in for Kueue + the worker
// reconciler. The control-plane placer fans a source InferenceService out
// onto every candidate member; on each member the placer then watches the
// derived ISVC for AllComponentsAdmitted (an admitted instance on every
// declared component) to declare that member the race winner. envtest runs no
// OME reconciler on the members, so this admitter plays that role: it watches
// the member's derived ISVCs and, after a configurable per-member admit delay,
// flips every declared component's status to an admitted instance.
//
// The per-member delay is what makes the race deterministic: a member with a
// short delay admits first and wins; a member with a long delay loses and has
// its derived swept. The admitter reports the admit instant to the recorder so
// the harness can measure status-convergence latency (admit -> source status
// reflects it) — the headline funnel-vs-poll metric.
package admitter

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	perflabels "github.com/ome-projects/ome/tests/perf/multicluster/runner/labels"
)

// AdmitSink receives the instant a member admitted a derived ISVC. Implemented
// by the recorder; the key is the derived's namespace/name, which equals the
// source ISVC key (placeOn preserves identity), so the recorder can attribute
// the admit to the source under measurement. Only the FIRST admit per key
// matters (the winner's), and the recorder dedups, so the admitter may report
// every admit it performs.
type AdmitSink interface {
	RecordAdmitted(key types.NamespacedName, at time.Time)
}

// Reconciler marks derived ISVCs admitted after a per-member delay. One runs on
// each member cluster's manager.
type Reconciler struct {
	client client.Client

	// admitDelay is this member's simulated admission latency. Config-driven
	// (no in-code default); the runner sets it per member from the scenario so
	// a deterministic winner emerges.
	admitDelay time.Duration

	// concurrency is the admitter's reconciler worker count, so it keeps up
	// with a fan-out burst across many derived ISVCs. Config-driven.
	concurrency int

	// firstSeen records, per derived, when this member first observed it. The
	// admit delay is measured from that instant so the convergence latency the
	// harness records reflects the simulated admission time, not when an
	// unrelated event happened to fire.
	firstSeen *seenClock

	// sink receives the admit timestamp (the winner's admit is the start of the
	// status flow-back the funnel/poll races to converge). Optional.
	sink AdmitSink
}

// New constructs a member-side admitter. clockNow is injected so the timing
// math is testable; pass time.Now in the runner.
func New(c client.Client, admitDelay time.Duration, concurrency int, clockNow func() time.Time, sink AdmitSink) *Reconciler {
	return &Reconciler{
		client:      c,
		admitDelay:  admitDelay,
		concurrency: concurrency,
		firstSeen:   newSeenClock(clockNow),
		sink:        sink,
	}
}

var _ reconcile.Reconciler = (*Reconciler)(nil)

// Reconcile drives one derived ISVC toward admitted. It is a no-op once the
// derived already reports an admitted instance on every declared component.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	derived := &v1beta1.InferenceService{}
	if err := r.client.Get(ctx, req.NamespacedName, derived); err != nil {
		// Gone (the placer swept this loser) — forget our timer entry so the map
		// can't grow without bound across a run.
		r.firstSeen.forget(req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if allComponentsAdmitted(derived) {
		return ctrl.Result{}, nil
	}

	elapsed := r.firstSeen.since(req.NamespacedName.String())
	if elapsed < r.admitDelay {
		// Not yet "admitted": requeue for the remaining time so the status flip
		// lands close to the simulated admission instant.
		return ctrl.Result{RequeueAfter: r.admitDelay - elapsed}, nil
	}

	markAdmitted(derived)
	if err := r.client.Status().Update(ctx, derived); err != nil {
		// Conflict / transient: a requeue retries with a fresh object.
		return ctrl.Result{Requeue: true}, client.IgnoreNotFound(err)
	}
	if r.sink != nil {
		r.sink.RecordAdmitted(req.NamespacedName, time.Now())
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the admitter on a member's manager, scoped to
// harness-derived ISVCs via a label predicate. The harness label rides from the
// source through DeriveISVC onto the derived copy, so this watches exactly the
// objects this run fanned out — never a user's ISVC on the member.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	harnessOnly, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchLabels: map[string]string{perflabels.HarnessLabel: perflabels.HarnessLabelValue},
	})
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(harnessOnly)).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.concurrency}).
		Complete(r)
}

// declaredComponents returns the component types present on the derived spec.
// Mirrors the placer's own declaredComponents so the admitted status the
// admitter writes satisfies AllComponentsAdmitted exactly (a PD service with an
// engine + decoder needs both admitted before it can win).
func declaredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	if isvc.Spec.Engine != nil {
		out = append(out, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		out = append(out, v1beta1.DecoderComponent)
	}
	if isvc.Spec.Router != nil {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}

// markAdmitted sets an admitted, ready instance on every declared component —
// the status shape the placer's AllComponentsAdmitted reads to pick a winner.
func markAdmitted(derived *v1beta1.InferenceService) {
	comps := derived.Status.Components
	if comps == nil {
		comps = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	for _, comp := range declaredComponents(derived) {
		comps[comp] = v1beta1.ComponentStatusSpec{
			Lifecycle: &v1beta1.LifecycleStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady, Admitted: true},
				},
			},
		}
	}
	derived.Status.Components = comps
}

// allComponentsAdmitted reports whether every declared component already has an
// admitted instance (the steady state the admitter leaves a winner in). Kept
// local so the admitter has no dependency on the placement package internals.
func allComponentsAdmitted(isvc *v1beta1.InferenceService) bool {
	declared := declaredComponents(isvc)
	if len(declared) == 0 {
		return false
	}
	for _, comp := range declared {
		cs, ok := isvc.Status.Components[comp]
		if !ok || !hasAdmittedInstance(cs) {
			return false
		}
	}
	return true
}

func hasAdmittedInstance(cs v1beta1.ComponentStatusSpec) bool {
	if cs.Lifecycle == nil {
		return false
	}
	for _, inst := range cs.Lifecycle.InstanceStatuses {
		if inst.Admitted {
			return true
		}
	}
	return false
}
