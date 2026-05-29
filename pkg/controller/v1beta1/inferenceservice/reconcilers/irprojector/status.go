package irprojector

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// AggregateIRStatus reads the live InferenceReplica for each
// IR-managed Component on the given ISVC and writes IR.Status into
// ISVC.Status.Components[<component>].OMENative via Status().Update.
//
// The downstream status contract (kubectl, dashboards, OEP-0007
// status field shapes) stays identical regardless of which path
// writes it — the IR controller computes the same per-Component
// aggregate counters the omenative direct path does; the projector
// just relays them.
//
// Called from the ISVC controller's reconcile loop AFTER the
// per-Component dispatcher (irprojector.EnsureInferenceReplica for
// IR-managed Components; omenative.ReconcileComponent for the
// direct path) finishes. The dispatch path writes IR.Spec; the IR
// controller reconciles + writes IR.Status; THIS function reads
// IR.Status back onto the parent ISVC.
//
// Writes directly via Status().Update rather than mutating the
// passed-in ISVC for the outer updateStatus to flush, because the
// outer flush's retry.RetryOnConflict path re-Gets latest + merges
// preserved.OMENative from latest — clobbering what we just wrote
// would defeat the purpose. The omenative path mirrors this:
// AggregateAndWriteStatus writes directly via Status().Update and
// the outer updateStatus's mergeOMENativeComponentStatus then
// preserves it across retries.
//
// Mutates the in-memory ISVC after the write so subsequent code in
// the same reconcile pass observes the post-write state (matches
// the mirror pattern in mutate.go / status_aggregate.go).
//
// Sequence per Component:
//
//  1. Skip Components not opted into IR-managed — the omenative
//     direct path already wrote ISVC.Status.Components[c].OMENative
//     in its own AggregateAndWriteStatus pass.
//
//  2. Fetch the IR by name. NotFound is non-fatal — the IR may not
//     yet exist on the first reconcile after opt-in, OR the IR was
//     deleted between the projector's CreateOrUpdate and our read.
//     In either case, leaving the ISVC status entry empty is the
//     right behavior (the next reconcile re-creates / re-reads).
//
//  3. Status().Update under retry.RetryOnConflict to merge the
//     IR-derived OMENative subtree onto the live ISVC. Re-read on
//     conflict so concurrent writers (omenative direct path on a
//     different Component) don't lose to us.
//
// Errors are wrapped with the offending IR namespace/name for grep-
// ability in operator logs. A real read/write error (apiserver
// outage, permission denied) is returned to the caller so the
// reconcile retries; transient NotFound on the IR Get is swallowed.
func AggregateIRStatus(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService) error {
	if c == nil {
		return fmt.Errorf("AggregateIRStatus: nil client")
	}
	if isvc == nil {
		return fmt.Errorf("AggregateIRStatus: nil ISVC")
	}

	for _, comp := range allDeclaredComponents(isvc) {
		if !IsIRManagedComponent(isvc, comp) {
			continue
		}
		if err := aggregateOneComponent(ctx, c, isvc, comp); err != nil {
			return err
		}
	}
	return nil
}

// aggregateOneComponent fetches one IR, computes the desired
// OMENativeComponentStatus, and writes it to the live ISVC via
// Status().Update under retry.RetryOnConflict. Mirrors the pattern
// omenative.AggregateAndWriteStatus uses.
//
// On apierrors.IsNotFound for the IR Get: returns nil so the outer
// loop keeps going. The next reconcile picks up the IR's fresh
// status once the cache observes it.
//
// On apierrors.IsNotFound for the ISVC Update: returns nil so a
// race with ISVC deletion drops cleanly.
func aggregateOneComponent(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) error {
	name := InferenceReplicaName(isvc.Name, component)
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: name}
	ir := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			// First-reconcile race: the projector's CreateOrUpdate
			// landed but the cache hasn't observed it yet, OR the
			// IR was deleted between writes. Leave the status entry
			// empty and let the next reconcile pick it up.
			return nil
		}
		return fmt.Errorf("AggregateIRStatus: get IR %s/%s for component=%s: %w",
			isvc.Namespace, name, component, err)
	}

	desired := irStatusToComponentStatus(ir)
	isvcKey := client.ObjectKeyFromObject(isvc)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, isvcKey, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		cs := fresh.Status.Components[component]
		cs.OMENative = desired.DeepCopy()
		fresh.Status.Components[component] = cs
		if err := c.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("update ISVC status: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("AggregateIRStatus: persist component=%s status: %w", component, err)
	}

	// Mirror onto the caller's in-memory ISVC so downstream code in
	// the same reconcile pass observes the post-write state. Without
	// the mirror, the outer reconciler's status-equality short-circuit
	// (inferenceServiceStatusEqual) may incorrectly skip the next
	// updateStatus call because its baseline doesn't reflect what we
	// just committed.
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[component]
	cs.OMENative = desired.DeepCopy()
	isvc.Status.Components[component] = cs
	return nil
}

// irStatusToComponentStatus projects the IR.Status fields onto a
// fresh OMENativeComponentStatus. 1:1 field mapping — the two types
// were intentionally shaped identically so the dual-write window
// (when both paths could be active in different clusters) can
// byte-compare them via the shadow-mode diff harness.
//
// Returns a non-nil pointer even when every field is zero; the
// presence of the pointer is itself a signal that the IR-managed
// path is driving the Component, and downstream consumers
// (status_aggregate.go in the omenative package + the controller's
// preserveOMENativeComponentStatus helper) treat the nil/non-nil
// boundary as load-bearing.
func irStatusToComponentStatus(ir *v1beta1.InferenceReplica) *v1beta1.OMENativeComponentStatus {
	out := &v1beta1.OMENativeComponentStatus{
		ObservedGeneration:   ir.Status.ObservedGeneration,
		Replicas:             ir.Status.Replicas,
		ReadyReplicas:        ir.Status.ReadyReplicas,
		ServingReplicas:      ir.Status.ServingReplicas,
		AvailableReplicas:    ir.Status.AvailableReplicas,
		UpdatedReplicas:      ir.Status.UpdatedReplicas,
		UpdatedReadyReplicas: ir.Status.UpdatedReadyReplicas,
		CurrentRevision:      ir.Status.CurrentRevision,
		UpdateRevision:       ir.Status.UpdateRevision,
		LabelSelector:        ir.Status.LabelSelector,
	}
	if ir.Status.CollisionCount != nil {
		cc := *ir.Status.CollisionCount
		out.CollisionCount = &cc
	}
	// Copy the per-Instance slice into a fresh backing array so
	// downstream mutators on the ISVC side don't accidentally alias
	// the IR's slice.
	if len(ir.Status.InstanceStatuses) > 0 {
		out.InstanceStatuses = make([]v1beta1.OMENativeInstanceStatus, len(ir.Status.InstanceStatuses))
		for i, s := range ir.Status.InstanceStatuses {
			out.InstanceStatuses[i] = *s.DeepCopy()
		}
	}
	// Conditions follow the same deep-copy contract.
	if len(ir.Status.Conditions) > 0 {
		out.Conditions = make([]metav1.Condition, len(ir.Status.Conditions))
		copy(out.Conditions, ir.Status.Conditions)
	}
	return out
}

// allDeclaredComponents returns the ComponentTypes that have a
// non-nil spec on the ISVC. Used by AggregateIRStatus to decide
// which IRs to look up. Components whose specs are nil are not
// reconciled by anything — including IR-managed paths — so we skip
// them.
func allDeclaredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
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
