// Package executor is the harness's fake "runtime executor". It stands in for
// the kubelet + Deployment controller that envtest does not run: it watches the
// Deployments the InferenceService reconciler produces and, after a per-class
// simulated delay (carried on a label), flips their status to available so the
// InferenceService's Ready condition can converge — isolating control-plane
// reconcile latency from container-start time.
//
// The executor only ever writes the Deployment *status* subresource
// (ReadyReplicas / AvailableReplicas / the Available condition), never spec, so
// it can't fight the reconciler over desired state.
package executor

import (
	"context"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	perflabels "github.com/ome-projects/ome/tests/perf/inferenceservice/runner/labels"
)

// Reconciler flips harness-owned Deployments to Ready after a simulated
// per-class delay.
type Reconciler struct {
	client client.Client

	// firstSeen records, per Deployment, when the executor first observed
	// it. The simulated ready delay is measured from this instant so the
	// time-to-Ready a class exhibits reflects (reconcile latency to
	// produce the Deployment) + (the class's configured ready delay).
	firstSeen *seenClock

	// concurrency is the executor's reconciler worker count. Config-driven
	// (no in-code default); the runner sets it from a flag so the executor
	// can keep up with the InferenceService reconciler at scale.
	concurrency int
}

// New constructs an executor reconciler. The clock is injected so the
// reconcile-time math is testable; pass clockNow=time.Now in production.
func New(c client.Client, concurrency int, clockNow func() time.Time) *Reconciler {
	return &Reconciler{
		client:      c,
		firstSeen:   newSeenClock(clockNow),
		concurrency: concurrency,
	}
}

var _ reconcile.Reconciler = (*Reconciler)(nil)

// Reconcile drives one harness Deployment toward Ready. It is a no-op
// once the Deployment already reports the desired replicas Available.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	dep := &appsv1.Deployment{}
	if err := r.client.Get(ctx, req.NamespacedName, dep); err != nil {
		// Gone (deleted between event and Get) — forget our timer entry so
		// the map can't grow without bound across a long run.
		r.firstSeen.forget(req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	desired := int32(1)
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}

	// Already fully available — nothing to do. This is the steady state
	// the executor leaves every Deployment in.
	if deploymentAvailable(dep, desired) {
		return ctrl.Result{}, nil
	}

	delay := readyDelay(dep)
	elapsed := r.firstSeen.since(req.NamespacedName.String())
	if elapsed < delay {
		// Not yet "warmed up": requeue for the remaining time so the
		// status flip lands close to the simulated readiness instant
		// rather than on the next unrelated event.
		return ctrl.Result{RequeueAfter: delay - elapsed}, nil
	}

	r.markAvailable(dep, desired)
	if err := r.client.Status().Update(ctx, dep); err != nil {
		// Conflict / transient: a requeue retries with a fresh object.
		return ctrl.Result{Requeue: true}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

// markAvailable stamps the Deployment status fields the InferenceService
// status reconciler reads to declare a Component Ready: every desired
// replica Ready + Available, the rollout fully observed, and the
// Available condition True. Mirrors what the real Deployment controller
// converges a healthy rollout to.
func (r *Reconciler) markAvailable(dep *appsv1.Deployment, desired int32) {
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = desired
	dep.Status.ReadyReplicas = desired
	dep.Status.AvailableReplicas = desired
	dep.Status.UpdatedReplicas = desired
	dep.Status.UnavailableReplicas = 0

	setCondition(dep, appsv1.DeploymentCondition{
		Type:               appsv1.DeploymentAvailable,
		Status:             corev1.ConditionTrue,
		Reason:             "MinimumReplicasAvailable",
		Message:            "Deployment has minimum availability (simulated by perf executor)",
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	})
	setCondition(dep, appsv1.DeploymentCondition{
		Type:               appsv1.DeploymentProgressing,
		Status:             corev1.ConditionTrue,
		Reason:             "NewReplicaSetAvailable",
		Message:            "ReplicaSet has successfully progressed (simulated by perf executor)",
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	})
}

// SetupWithManager registers the executor on the manager, scoped to
// harness-owned Deployments via a label predicate so it never touches
// Deployments outside the run.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	harnessOnly, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchLabels: map[string]string{perflabels.HarnessLabel: perflabels.HarnessLabelValue},
	})
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}, builder.WithPredicates(harnessOnly)).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.concurrency}).
		Complete(r)
}

// deploymentAvailable reports whether dep already shows all desired
// replicas ready+available with the Available condition True.
func deploymentAvailable(dep *appsv1.Deployment, desired int32) bool {
	if dep.Status.ReadyReplicas != desired || dep.Status.AvailableReplicas != desired {
		return false
	}
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// readyDelay reads the per-Deployment simulated ready delay. The
// generator stamps it (as a millisecond decimal string) onto the
// InferenceService and the controller propagates pod-template labels onto
// the Deployment's own labels via the component meta, so the value is
// available directly on the Deployment. A missing/garbage value means
// "ready immediately" — the harness's config validation guarantees the
// label is present, so this only triggers for hand-created objects.
func readyDelay(dep *appsv1.Deployment) time.Duration {
	v := dep.Labels[perflabels.ReadyAfterMsLabel]
	if v == "" {
		return 0
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms < 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// setCondition upserts a Deployment status condition by type.
func setCondition(dep *appsv1.Deployment, cond appsv1.DeploymentCondition) {
	for i := range dep.Status.Conditions {
		if dep.Status.Conditions[i].Type == cond.Type {
			dep.Status.Conditions[i] = cond
			return
		}
	}
	dep.Status.Conditions = append(dep.Status.Conditions, cond)
}
