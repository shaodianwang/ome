// Package admitter is the member-side stand-in for Kueue + the worker
// reconciler. The control-plane placer fans a source InferenceService out
// onto every candidate member; on each member the placer then watches the
// derived ISVC for AllComponentsAdmitted (an admitted instance on every
// declared component) to declare that member the race winner. envtest runs no
// OME reconciler on the members, so this admitter plays that role: it watches
// the member's derived ISVCs and, after a configurable per-member admit delay,
// stamps an admitted instance onto every declared component's InferenceReplica
// (the authoritative per-instance status the placer reads).
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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
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

// Reconcile drives one derived ISVC toward admitted. It is a no-op once every
// declared component's InferenceReplica already reports an admitted instance.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	derived := &v1beta1.InferenceService{}
	if err := r.client.Get(ctx, req.NamespacedName, derived); err != nil {
		// Gone (the placer swept this loser) — forget our timer entry so the map
		// can't grow without bound across a run.
		r.firstSeen.forget(req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	done, err := r.allComponentsAdmitted(ctx, derived)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if done {
		return ctrl.Result{}, nil
	}

	elapsed := r.firstSeen.since(req.NamespacedName.String())
	if elapsed < r.admitDelay {
		// Not yet "admitted": requeue for the remaining time so the status flip
		// lands close to the simulated admission instant.
		return ctrl.Result{RequeueAfter: r.admitDelay - elapsed}, nil
	}

	if err := r.markAdmitted(ctx, derived); err != nil {
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

// componentIRName is the per-component InferenceReplica name the placer reads
// (and this admitter writes): "<isvcName>-<component>". Mirrors the production
// projector's naming so the placer's IR Get resolves to the object we stamp.
func componentIRName(isvcName string, comp v1beta1.ComponentType) string {
	return isvcName + "-" + string(comp)
}

// markAdmitted stamps an admitted, ready instance onto every declared
// component's InferenceReplica — the authoritative per-instance status the
// placer's AllComponentsAdmitted reads from the IR, which alone carries the
// per-instance rows, to pick a winner. Each IR is created if absent, then its
// status subresource is written.
func (r *Reconciler) markAdmitted(ctx context.Context, derived *v1beta1.InferenceService) error {
	for _, comp := range declaredComponents(derived) {
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: derived.Namespace,
				Name:      componentIRName(derived.Name, comp),
			},
			// Minimal valid spec so the Create passes CRD schema validation on
			// bare members (no manager pre-creates the IR). Ignored when the IR
			// already exists (Create returns AlreadyExists).
			Spec: v1beta1.InferenceReplicaSpec{
				ParentRef: &v1beta1.ParentReference{Name: derived.Name},
				Component: comp,
				Runners: []v1beta1.Runner{{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "sim:latest"}}},
					},
				}},
			},
		}
		if err := r.client.Create(ctx, ir); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		// Re-read for the current resourceVersion before the status write (the
		// IR may already exist from a prior reconcile or the worker path).
		if err := r.client.Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: ir.Name}, ir); err != nil {
			return err
		}
		if err := util.WriteDenseInstanceStatuses(ctx, r.client, ir, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, Admitted: true},
		}); err != nil {
			return err
		}
	}
	return nil
}

// allComponentsAdmitted reports whether every declared component's
// InferenceReplica already has an admitted instance (the steady state the
// admitter leaves a winner in). Kept local so the admitter has no dependency on
// the placement package internals.
func (r *Reconciler) allComponentsAdmitted(ctx context.Context, isvc *v1beta1.InferenceService) (bool, error) {
	declared := declaredComponents(isvc)
	if len(declared) == 0 {
		return false, nil
	}
	for _, comp := range declared {
		rows, err := r.componentInstanceStatuses(ctx, isvc.Namespace, isvc.Name, comp)
		if err != nil {
			return false, err
		}
		if !hasAdmittedInstance(rows) {
			return false, nil
		}
	}
	return true, nil
}

// componentInstanceStatuses reads one component's decoded InferenceReplica
// rows, returning (nil, nil) when the IR does not exist yet (treated as "no
// admitted instance").
func (r *Reconciler) componentInstanceStatuses(ctx context.Context, namespace, isvcName string, comp v1beta1.ComponentType) ([]v1beta1.OMENativeInstanceStatus, error) {
	key := types.NamespacedName{Namespace: namespace, Name: componentIRName(isvcName, comp)}
	rows, err := util.GetInstanceStatuses(ctx, r.client, key)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return rows, err
}

func hasAdmittedInstance(rows []v1beta1.OMENativeInstanceStatus) bool {
	for _, inst := range rows {
		if inst.Admitted {
			return true
		}
	}
	return false
}
