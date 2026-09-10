package routing

import (
	"context"
	"sort"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Reconciler watches InferenceServices on the control-plane cluster and, for a
// Placed ISVC, generates the capacity-aware TrafficMap a gateway consumes: one
// namespaced TrafficMap per ISVC, named after it and owner-ref'd to it so it is
// garbage-collected with the ISVC. It recomputes the routing table whenever
// placement or per-home capacity changes, and reaps the TrafficMap when the
// ISVC is no longer placed or the feature is disabled. It writes only spec; the
// downstream publisher owns status.
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

	spec, ok := buildSpec(isvc)
	if !ok {
		// Not placed, or no serving home is addressable yet — remove any stale
		// TrafficMap rather than leave a routing table pointing nowhere.
		return ctrl.Result{}, r.reap(ctx, isvc)
	}
	return ctrl.Result{}, r.apply(ctx, isvc, spec)
}

// apply creates or updates the ISVC's TrafficMap to carry the desired spec,
// owner-ref'd to the ISVC. It touches only spec; the status subresource (owned
// by the publisher) is left untouched.
func (r *Reconciler) apply(ctx context.Context, isvc *v1beta1.InferenceService, spec v1beta1.TrafficMapSpec) error {
	tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name, Namespace: isvc.Namespace}}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, tm, func() error {
		if err := controllerutil.SetControllerReference(isvc, tm, r.Scheme()); err != nil {
			return err
		}
		tm.Spec = spec
		return nil
	})
	if err != nil {
		return err
	}
	if op != controllerutil.OperationResultNone {
		r.Log.Info("trafficmap generated", "isvc", client.ObjectKeyFromObject(isvc).String(),
			"op", op, "mode", spec.Mode, "homes", len(spec.Entries))
	}
	return nil
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

// buildSpec projects an ISVC's placement into the desired TrafficMap spec. ok is
// false when the ISVC is not in a routable state: not Placed, or no admitted
// candidate reports an addressable endpoint. It is a pure function of the ISVC
// so the watch predicate can use it to react only to changes that alter the
// generated table.
func buildSpec(isvc *v1beta1.InferenceService) (v1beta1.TrafficMapSpec, bool) {
	pl := isvc.Status.Placement
	if pl == nil || pl.Phase != v1beta1.PlacementPhasePlaced {
		return v1beta1.TrafficMapSpec{}, false
	}
	cands := servingCandidates(pl)
	if len(cands) == 0 {
		return v1beta1.TrafficMapSpec{}, false
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
	return v1beta1.TrafficMapSpec{
		Service:                isvc.Name,
		Mode:                   placementMode(isvc),
		Entries:                entries,
		ObservedISVCGeneration: isvc.Generation,
	}, true
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
// projected spec (or its routable-ness) differs. Routine ISVC churn that does
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
		oldSpec, oldOK := buildSpec(oldISVC)
		newSpec, newOK := buildSpec(newISVC)
		if oldOK != newOK {
			return true
		}
		return !equality.Semantic.DeepEqual(oldSpec, newSpec)
	},
}
