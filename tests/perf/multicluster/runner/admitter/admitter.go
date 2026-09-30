// Package admitter simulates delayed member admission for placement benchmarks.
// It acknowledges the derived execution policy on owned InferenceReplicas;
// physical Pod readiness and rollout transitions require the integration suites.
package admitter

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/ptr"

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
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
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

// Reconcile admits each configured replica and acknowledges its execution policy.
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

// admissionIR derives the single-runner fixture from explicit member authority.
func admissionIR(derived *v1beta1.InferenceService, comp v1beta1.ComponentType) (*v1beta1.InferenceReplica, error) {
	policy, err := protocol.FromDerived(derived)
	if err != nil {
		return nil, err
	}
	if policy == nil || derived.UID == "" {
		return nil, fmt.Errorf("admission requires an identified derived service and execution policy")
	}
	if policy.PauseSurge || policy.Demand != nil {
		return nil, fmt.Errorf("admission benchmark supports unpaused full-policy homes only")
	}
	var replicas int32
	for _, floor := range policy.ReplicaFloors {
		if floor.Component == comp {
			replicas = floor.Replicas
		}
	}
	var runner *v1beta1.RunnerSpec
	switch comp {
	case v1beta1.EngineComponent:
		if derived.Spec.Engine != nil {
			runner = derived.Spec.Engine.Runner
		}
	case v1beta1.DecoderComponent:
		if derived.Spec.Decoder != nil {
			runner = derived.Spec.Decoder.Runner
		}
	case v1beta1.RouterComponent:
		if derived.Spec.Router != nil {
			runner = derived.Spec.Router.Runner
		}
	}
	if replicas <= 0 || runner == nil || runner.Image == "" {
		return nil, fmt.Errorf("admission requires an explicit floor and runner for %s", comp)
	}
	labels := maps.Clone(derived.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[constants.InferenceServicePodLabelKey] = derived.Name
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: derived.Namespace, Name: componentIRName(derived.Name, comp),
			Labels:          labels,
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(derived.Generation, 10)},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(derived, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}},
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: derived.Name}, Component: comp, Replicas: ptr.To(replicas), PlacementExecution: policy,
			Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{runner.Container}}}}}},
	}, nil
}

// markAdmitted acknowledges each exact execution policy after the member delay.
func (r *Reconciler) markAdmitted(ctx context.Context, derived *v1beta1.InferenceService) error {
	for _, comp := range declaredComponents(derived) {
		desired, err := admissionIR(derived, comp)
		if err != nil {
			return err
		}
		ir := &v1beta1.InferenceReplica{}
		err = r.client.Get(ctx, client.ObjectKeyFromObject(desired), ir)
		if apierrors.IsNotFound(err) {
			ir = desired
			if err := r.client.Create(ctx, ir); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			if !metav1.IsControlledBy(ir, derived) {
				return fmt.Errorf("admission component is not owned by the derived service")
			}
			ir.Spec, ir.Annotations = desired.Spec, desired.Annotations
			if err := r.client.Update(ctx, ir); err != nil {
				return err
			}
		}
		if err := r.client.Get(ctx, client.ObjectKeyFromObject(ir), ir); err != nil {
			return err
		}
		rows := make([]v1beta1.OMENativeInstanceStatus, int(*ir.Spec.Replicas))
		for i := range rows {
			rows[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstancePending, Admitted: true}
		}
		ir.Status.ObservedGeneration = ir.Generation
		ir.Status.PlacementObservedGeneration = ir.Generation
		rows, err = util.MaterializeSinglePodInstances(ctx, r.client, ir, rows)
		if err != nil {
			return err
		}
		if err := util.WriteDenseInstanceStatuses(ctx, r.client, ir, rows); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) allComponentsAdmitted(ctx context.Context, derived *v1beta1.InferenceService) (bool, error) {
	declared := declaredComponents(derived)
	if len(declared) == 0 {
		return false, nil
	}
	for _, comp := range declared {
		desired, err := admissionIR(derived, comp)
		if err != nil {
			return false, err
		}
		ir := &v1beta1.InferenceReplica{}
		if err := r.client.Get(ctx, client.ObjectKeyFromObject(desired), ir); apierrors.IsNotFound(err) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		if !metav1.IsControlledBy(ir, derived) || !equality.Semantic.DeepEqual(ir.Spec.PlacementExecution, desired.Spec.PlacementExecution) || !equality.Semantic.DeepEqual(ir.Spec.Replicas, desired.Spec.Replicas) || ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != desired.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] || ir.Status.PlacementObservedGeneration != ir.Generation {
			return false, nil
		}
		rows, err := util.DecodedInstanceStatuses(ir)
		if err != nil {
			return false, err
		}
		if len(rows) != int(*desired.Spec.Replicas) {
			return false, nil
		}
		for _, row := range rows {
			if !row.Admitted {
				return false, nil
			}
		}
	}
	return true, nil
}
