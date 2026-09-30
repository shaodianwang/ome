package inferencereplica

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// peerReplicasOf maps an InferenceReplica event to the serving peers of the
// same parent InferenceService. A Component's pass holds while a peer's
// projection or status lags its own (resolvePeerRevisions) and pairs the
// pods it renders on the peer's roll target, so the peer's catch-up and
// retargets must wake it: no other watch reaches it from the peer, and its
// own rate-limited requeue can be far off after a burst of spec churn. A
// replica with no parent, or whose parent declares no rollout groups, has
// no peers to wake.
func (r *Reconciler) peerReplicasOf(ctx context.Context, obj client.Object) []reconcile.Request {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return nil
	}
	parent := r.resolveParentFrom(ctx, r.Client, ir)
	if !coordination.PeerEnvDeclared(parent) {
		return nil
	}
	peers := coordination.ServingPeers(parent, ir.Spec.Component)
	reqs := make([]reconcile.Request, 0, len(peers))
	for _, peer := range peers {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: ir.Namespace,
			Name:      irprojector.InferenceReplicaName(parent.Name, peer),
		}})
	}
	return reqs
}

// peerRevisionPredicate admits the InferenceReplica transitions a sibling's
// peer-revision resolution reads; counter churn on a busy peer wakes no
// sibling. Creates and deletes pass: a peer appearing or vanishing changes
// what its siblings pair with.
func peerRevisionPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldIR, ok1 := e.ObjectOld.(*v1beta1.InferenceReplica)
			newIR, ok2 := e.ObjectNew.(*v1beta1.InferenceReplica)
			if !ok1 || !ok2 {
				return true
			}
			return peerRevisionInputsChanged(oldIR, newIR)
		},
	}
}

// peerRevisionInputsChanged reports whether an old→new InferenceReplica
// transition moved one of the fields a sibling's peer-revision resolution
// reads: the parent generation its projection reflects, its own generation
// (the spec target and the rollback pin travel with it), and the status
// that reports the roll it observed (ObservedGeneration, UpdateRevision,
// CurrentRevision).
func peerRevisionInputsChanged(oldIR, newIR *v1beta1.InferenceReplica) bool {
	if oldIR == nil || newIR == nil {
		return true
	}
	if oldIR.Generation != newIR.Generation {
		return true
	}
	key := constants.InferenceReplicaParentGenerationAnnotationKey
	if oldIR.Annotations[key] != newIR.Annotations[key] {
		return true
	}
	return oldIR.Status.ObservedGeneration != newIR.Status.ObservedGeneration ||
		oldIR.Status.UpdateRevision != newIR.Status.UpdateRevision ||
		oldIR.Status.CurrentRevision != newIR.Status.CurrentRevision
}
