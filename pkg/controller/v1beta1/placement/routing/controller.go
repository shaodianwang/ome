package routing

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps/status,verbs=get;update;patch

// Reconciler watches InferenceServices on the control-plane cluster and
// generates the capacity-aware TrafficMap a gateway consumes: one namespaced
// TrafficMap per ISVC, named after it and owner-ref'd to it so it is
// garbage-collected with the ISVC. It recomputes the routing table whenever
// placement or per-home capacity changes.
//
// The map exists for as long as the ISVC does, even when nothing is routable:
// an empty table plus a Routable=False reason. Absence would be
// unattributable — feature off, not placed, no addressable home and a wedged
// controller all look the same — and deleting on degradation would drop the
// capacity provenance at the moment it is most wanted. Only the feature toggle
// reaps. It writes spec and the Routable condition; the publisher owns the
// rest of status.
type Reconciler struct {
	client.Client
	Log logr.Logger
	// Config gates generation. When disabled the controller reaps what it owns.
	Config Config
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, req.NamespacedName, isvc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Feature off: reap any TrafficMap we previously generated so toggling the
	// feature is reversible without stranding objects.
	if !r.Config.IsEnabled() {
		return ctrl.Result{}, r.reap(ctx, isvc)
	}

	// Deleting: the TrafficMap's owner reference garbage-collects it with the
	// ISVC, so there is nothing to do.
	if !isvc.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Unroutable is a state of the map, not a reason to remove it: build the
	// table (possibly empty) and let the condition carry the explanation.
	spec, status, reason := buildSpec(isvc)
	if err := r.apply(ctx, isvc, spec); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.setRoutable(ctx, isvc, status, reason)
}

// apply creates or updates the ISVC's TrafficMap to carry the desired spec,
// owner-ref'd to the ISVC. It touches only spec; the status subresource (owned
// by the publisher) is left untouched.
//
// It retries on conflict rather than surfacing the rejection: owning the
// TrafficMap re-enqueues this reconciler on its own spec writes, and the
// Routable status write bumps the same object's resourceVersion, so a cached
// read can lag a concurrent pass. CreateOrUpdate re-reads on each attempt, so a
// retry converges on the live version instead of returning a 409 that would log
// as a spurious reconcile error.
func (r *Reconciler) apply(ctx context.Context, isvc *v1beta1.InferenceService, spec v1beta1.TrafficMapSpec) error {
	var op controllerutil.OperationResult
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name, Namespace: isvc.Namespace}}
		var err error
		op, err = controllerutil.CreateOrUpdate(ctx, r.Client, tm, func() error {
			if err := controllerutil.SetControllerReference(isvc, tm, r.Scheme()); err != nil {
				return err
			}
			tm.Spec = spec
			return nil
		})
		return err
	}); err != nil {
		return err
	}
	if op != controllerutil.OperationResultNone {
		r.Log.Info("trafficmap generated", "isvc", client.ObjectKeyFromObject(isvc).String(),
			"op", op, "mode", spec.Mode, "homes", len(spec.Entries))
	}
	return nil
}

// setRoutable writes the routing controller's Routable condition onto the
// ISVC's TrafficMap, explaining an empty or degraded table.
//
// It re-reads and retries on conflict rather than patching blind: the publisher
// owns Programmed on the same subresource, so a lost update here would silently
// drop whichever condition wrote second. SetStatusCondition keys on type, so the
// two writers do not collide, and it leaves LastTransitionTime alone when the
// status has not actually changed — no write amplification on a steady map.
func (r *Reconciler) setRoutable(ctx context.Context, isvc *v1beta1.InferenceService,
	status metav1.ConditionStatus, reason string) error {
	key := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tm := &v1beta1.TrafficMap{}
		if err := r.Get(ctx, key, tm); err != nil {
			// Gone, or not ours to annotate: nothing to report on.
			return client.IgnoreNotFound(err)
		}
		if !metav1.IsControlledBy(tm, isvc) {
			return nil
		}
		cond := metav1.Condition{
			Type:               v1beta1.TrafficMapRoutable,
			Status:             status,
			Reason:             reason,
			Message:            routableMessage(reason, len(tm.Spec.Entries)),
			ObservedGeneration: tm.Generation,
		}
		if !apimeta.SetStatusCondition(&tm.Status.Conditions, cond) {
			return nil // unchanged; skip the write
		}
		return r.Status().Update(ctx, tm)
	})
}

// routableMessage renders the operator-facing explanation for a Routable reason.
// The entry count is included because "empty" and "degraded" are different
// problems and the reason alone does not distinguish how many homes exist.
func routableMessage(reason string, entries int) string {
	switch reason {
	case v1beta1.TrafficMapReasonNotPlaced:
		return "InferenceService is not Placed; no homes to route to yet"
	case v1beta1.TrafficMapReasonNoAddressableHome:
		return "placed, but no admitted home reports an addressable endpoint"
	case v1beta1.TrafficMapReasonAllHomesUnready:
		return fmt.Sprintf("%d home(s) addressable but none ready; traffic spread equally rather than dropped", entries)
	default:
		return fmt.Sprintf("%d home(s) serving", entries)
	}
}

// reap deletes the ISVC's TrafficMap if we own it. It is a no-op when none
// exists or when the object is not controlled by this ISVC (never delete
// something we did not generate).
func (r *Reconciler) reap(ctx context.Context, isvc *v1beta1.InferenceService) error {
	tm := &v1beta1.TrafficMap{}
	key := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}
	if err := r.Get(ctx, key, tm); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(tm, isvc) || !tm.DeletionTimestamp.IsZero() {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, tm))
}

// buildSpec projects an ISVC's placement into the desired TrafficMap spec, plus
// the Routable verdict that explains it. An unroutable ISVC yields an empty
// table rather than no table, so a consumer can tell "known and unroutable"
// from "unknown".
//
// AllHomesUnready is reported with status True: every home is addressable, and
// the weight fallback spreads traffic across them rather than black-holing it,
// so the map IS routable — just degraded. It is a pure function of the ISVC so
// the watch predicate can use it to react only to changes that alter the
// generated table.
func buildSpec(isvc *v1beta1.InferenceService) (v1beta1.TrafficMapSpec, metav1.ConditionStatus, string) {
	empty := v1beta1.TrafficMapSpec{
		Service:                isvc.Name,
		Mode:                   placementMode(isvc),
		ObservedISVCGeneration: isvc.Generation,
	}
	pl := isvc.Status.Placement
	if pl == nil || pl.Phase != v1beta1.PlacementPhasePlaced {
		return empty, metav1.ConditionFalse, v1beta1.TrafficMapReasonNotPlaced
	}
	cands := servingCandidates(pl)
	if len(cands) == 0 {
		return empty, metav1.ConditionFalse, v1beta1.TrafficMapReasonNoAddressableHome
	}

	// Index-aligned: homes feed the weight computation, entries carry the result
	// plus provenance, both in the same cluster-sorted order.
	homes := make([]Home, len(cands))
	for i, c := range cands {
		// A nil factor is the identity 1.0; a per-cluster override scales the home's
		// share for heterogeneous hardware.
		homes[i] = Home{
			Cluster:   c.Cluster,
			Allocated: c.AdmittedReplicas,
			Ready:     c.ReadyReplicas,
			Factor:    capacityFactor(isvc, c.Cluster),
		}
	}
	weights := Weights(homes)

	entries := make([]v1beta1.TrafficMapEntry, len(cands))
	for i, c := range cands {
		entries[i] = v1beta1.TrafficMapEntry{
			Cluster:  c.Cluster,
			Endpoint: c.Endpoint.DeepCopy(),
			Weight:   weights[i],
			Healthy:  c.ReadyReplicas > 0,
			Capacity: &v1beta1.TrafficMapCapacity{
				Allocated: c.AdmittedReplicas,
				Ready:     c.ReadyReplicas,
				Factor:    homes[i].Factor,
			},
		}
	}
	reason := v1beta1.TrafficMapReasonRoutable
	if !slices.ContainsFunc(entries, func(e v1beta1.TrafficMapEntry) bool { return e.Healthy }) {
		reason = v1beta1.TrafficMapReasonAllHomesUnready
	}
	return v1beta1.TrafficMapSpec{
		Service:                isvc.Name,
		Mode:                   placementMode(isvc),
		Entries:                entries,
		ObservedISVCGeneration: isvc.Generation,
	}, metav1.ConditionTrue, reason
}

// servingCandidates returns the admitted candidates that report an addressable
// endpoint — the serving homes — sorted by cluster for a deterministic table.
func servingCandidates(pl *v1beta1.PlacementStatus) []*v1beta1.CandidatePlacement {
	var cs []*v1beta1.CandidatePlacement
	for i := range pl.Candidates {
		c := &pl.Candidates[i]
		if c.Phase == v1beta1.CandidatePhaseAdmitted && c.Endpoint != nil && c.Endpoint.Host != "" {
			cs = append(cs, c)
		}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].Cluster < cs[j].Cluster })
	return cs
}

// capacityFactor returns the per-replica relative serving capacity the ISVC
// declares for a workload cluster, or nil when none is set. Nil is the identity
// factor 1.0 — the weight computation treats a nil or non-positive factor as 1.
func capacityFactor(isvc *v1beta1.InferenceService, cluster string) *resource.Quantity {
	if isvc.Spec.Placement == nil || isvc.Spec.Placement.CapacityFactors == nil {
		return nil
	}
	if q, ok := isvc.Spec.Placement.CapacityFactors[cluster]; ok {
		qc := q.DeepCopy()
		return &qc
	}
	return nil
}

// placementMode returns the ISVC's declared placement mode, or empty when none
// is set (so a consumer reads routing intent without reading the ISVC).
func placementMode(isvc *v1beta1.InferenceService) v1beta1.PlacementMode {
	if isvc.Spec.Placement == nil {
		return ""
	}
	return isvc.Spec.Placement.Mode
}

// SetupWithManager wires the controller: reconcile ISVCs, reacting only to
// events that can change the generated table, and own the TrafficMaps it writes
// so a drifted or deleted map is corrected.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(RoutingControllerName).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(routingTableChange)).
		Owns(&v1beta1.TrafficMap{}).
		Complete(r)
}

// routingTableChange admits ISVC events that can change the generated TrafficMap:
// any create/delete, a deletion-timestamp transition, and updates where the
// projected spec, or the Routable status/reason we would report, differs. Routine ISVC churn that does
// not alter the table is dropped.
var routingTableChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldISVC, ok1 := e.ObjectOld.(*v1beta1.InferenceService)
		newISVC, ok2 := e.ObjectNew.(*v1beta1.InferenceService)
		if !ok1 || !ok2 {
			return true // unexpected type: fail safe
		}
		if oldISVC.DeletionTimestamp.IsZero() != newISVC.DeletionTimestamp.IsZero() {
			return true
		}
		// The condition is part of what we write, so a change in status or reason
		// has to wake us even when the table itself is byte-identical — an ISVC
		// going NotPlaced -> NoAddressableHome keeps an empty table but must
		// re-explain itself.
		oldSpec, oldStatus, oldReason := buildSpec(oldISVC)
		newSpec, newStatus, newReason := buildSpec(newISVC)
		if oldStatus != newStatus || oldReason != newReason {
			return true
		}
		return !equality.Semantic.DeepEqual(oldSpec, newSpec)
	},
}
