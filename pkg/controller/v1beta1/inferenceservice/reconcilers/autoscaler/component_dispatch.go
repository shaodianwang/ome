package autoscaler

import (
	"context"
	"fmt"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// defaultDispatchMinReplicas is the floor applied when the component spec
// does not declare MinReplicas > 0. Matches the per-class generators'
// internal defaults (hpa.calculateMinReplicas, keda.defaultMinReplicas).
const defaultDispatchMinReplicas int32 = 1

// IRDispatchInput is the per-Component projection of everything
// DispatchAutoscaler needs that's not already on the IR object. The
// ISVC-side component dispatchers (engine.go, decoder.go, router.go)
// build this from their local state and the committed IR returned by
// irprojector.EnsureInferenceReplica.
//
// Alpha API. The shape may change without notice.
type IRDispatchInput struct {
	// Client is the controller-runtime client used to apply the HPA
	// or ScaledObject.
	Client client.Client

	// Scheme is the runtime scheme, threaded for parity with the
	// legacy reconciler signatures. DispatchAutoscaler stamps owner-
	// refs directly via the metav1 path so this is reserved for
	// future ctx-aware refactors.
	Scheme *runtime.Scheme

	// IR is the committed InferenceReplica returned by
	// irprojector.EnsureInferenceReplica. Its UID is captured into
	// the OwnerReference so GC cascades the HPA / SO when
	// the IR is deleted. Required — DispatchForIRComponent returns
	// an error if IR is nil or IR.UID is empty (guards against the
	// caller dispatching before the IR is committed).
	IR *v1beta1.InferenceReplica

	// ResolvedAutoscaler is the resolver output (the output of
	// ResolveComponentAutoscaler). nil is treated as Class=none.
	ResolvedAutoscaler *v1beta1.ComponentAutoscaler

	// ComponentExt is the per-Component extension spec; min / max
	// replicas come from here. The dispatch does NOT consult any other
	// field — the Autoscaler block is carried on ResolvedAutoscaler above.
	ComponentExt *v1beta1.ComponentExtensionSpec
}

// DispatchForIRComponent is the convenience wrapper the ISVC-side
// component dispatchers use to translate the per-Component
// IRDispatchInput into a full DispatchParams call. Centralized here
// (rather than copy-pasted into engine.go / decoder.go / router.go)
// so the owner-ref shape, ScaleTargetRef GVK, and min/max bounds
// stay in sync across components.
//
// The HPA + ScaledObject are stamped with:
//
//   - Owner: the live IR (Controller=true, BlockOwnerDeletion implied
//     by controllerutil semantics). Deleting the IR cascades to both
//     via GC.
//   - ScaleTargetRef: ome.io/v1beta1/InferenceReplica/<ir-name>. The
//     IR's /scale subresource is the canonical scale target for
//     IR-managed Components.
//   - Min/Max replicas: input.ComponentExt.MinReplicas (default 1) and
//     input.ComponentExt.MaxReplicas (clamped up to MinReplicas in the
//     generator).
//
// Errors are wrapped with the IR namespace/name so the operator log
// reveals which Component failed dispatch.
//
// Alpha API. The signature may change without notice.
func DispatchForIRComponent(ctx context.Context, input IRDispatchInput) error {
	if input.IR == nil {
		return fmt.Errorf("DispatchForIRComponent: nil IR (refusing to stamp orphaned autoscaler)")
	}
	if input.IR.UID == "" {
		return fmt.Errorf("DispatchForIRComponent: IR %s/%s has empty UID (refusing to stamp orphaned autoscaler)", input.IR.Namespace, input.IR.Name)
	}

	minR, maxR := minMaxReplicas(input.ComponentExt)

	owner := metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       input.IR.Name,
		UID:        input.IR.UID,
		Controller: ptr.To(true),
	}

	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       input.IR.Name,
	}

	return DispatchAutoscaler(ctx, DispatchParams{
		Client:         input.Client,
		Scheme:         input.Scheme,
		Owner:          owner,
		Namespace:      input.IR.Namespace,
		Name:           input.IR.Name,
		ScaleTargetRef: scaleTargetRef,
		Autoscaler:     input.ResolvedAutoscaler,
		MinReplicas:    minR,
		MaxReplicas:    maxR,
	})
}

// minMaxReplicas projects ComponentExtensionSpec bounds into the int32
// pair the generators expect. MinReplicas defaults to 1 when nil or
// non-positive (matches hpa.calculateMinReplicas + keda.defaultMinReplicas).
// MaxReplicas defaults to 0 when zero — the HPA / KEDA generators clamp
// MaxReplicas up to MinReplicas when MaxReplicas < MinReplicas, so a
// zero MaxReplicas degenerates to a single-replica HPA/SO that just
// reports current load without changing the desired count.
func minMaxReplicas(c *v1beta1.ComponentExtensionSpec) (int32, int32) {
	minR := defaultDispatchMinReplicas
	if c != nil && c.MinReplicas != nil && *c.MinReplicas > 0 {
		minR = int32(*c.MinReplicas)
	}
	var maxR int32
	if c != nil {
		maxR = int32(c.MaxReplicas)
	}
	if maxR < minR {
		maxR = minR
	}
	return minR, maxR
}
