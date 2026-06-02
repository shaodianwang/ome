package autoscaler

import (
	"context"
	"fmt"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/hpa"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/keda"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// DispatchParams is the input bag DispatchAutoscaler reads to emit the
// per-Component HPA or ScaledObject. The OMENative IR-managed dispatch
// fills it from the resolved ComponentAutoscaler (the output of
// ResolveComponentAutoscaler), the projected IR identity (for owner-ref
// + scale-target), and the per-Component min/max replicas.
//
// Alpha API. The shape may change without notice.
type DispatchParams struct {
	// Client is the controller-runtime client used to Get / Create /
	// Update / Delete the HPA + ScaledObject.
	Client client.Client

	// Scheme is the controller-runtime scheme threaded for parity with
	// the legacy reconciler signatures. DispatchAutoscaler uses Owner
	// directly for owner-ref stamping rather than going through
	// controllerutil.SetControllerReference.
	Scheme *runtime.Scheme

	// Owner is the OwnerReference stamped on every emitted HPA /
	// ScaledObject. For the IR-managed path it points at the
	// InferenceReplica so foreground GC cascades the autoscaler when
	// the IR is deleted. Required — DispatchAutoscaler returns an
	// error if Owner.UID is empty (defends against a caller wiring
	// an owner-ref before the IR is committed).
	Owner metav1.OwnerReference

	// Namespace + Name identify the (HPA, ScaledObject) pair the
	// dispatch reconciles. Both objects share the same Name for cross-
	// class delete to be idempotent (switching keda → hpa with the
	// same Name deletes the SO via the same key DispatchAutoscaler
	// would have used to look it up).
	Namespace string
	Name      string

	// ScaleTargetRef is forwarded verbatim to the generated HPA /
	// ScaledObject. For the IR-managed path callers supply
	// `{ome.io/v1beta1, InferenceReplica, <ir-name>}` so the HPA /
	// ScaledObject targets the IR's /scale subresource.
	ScaleTargetRef autoscalingv2.CrossVersionObjectReference

	// Autoscaler is the resolved ComponentAutoscaler block — the output
	// of autoscaler.ResolveComponentAutoscaler. nil is treated as
	// Class=none (every OME-managed HPA / SO for this (Namespace, Name)
	// is deleted). NOTE: none + external are status-field twins and
	// share the same reconciliation path.
	Autoscaler *v1beta1.ComponentAutoscaler

	// MinReplicas + MaxReplicas drive the corresponding bounds on the
	// generated HPA / ScaledObject. MaxReplicas < MinReplicas is
	// clamped up by the generator (matches the legacy semantics).
	MinReplicas int32
	MaxReplicas int32
}

// DispatchAutoscaler reconciles the per-(IR, Component) autoscaler from
// the resolved ComponentAutoscaler block. Always-on per reconcile — the
// caller invokes DispatchAutoscaler unconditionally on every Component
// reconcile. Deep-equal-on-Get inside the per-class reconciler keeps the
// cost bounded (no-op when nothing diffs).
//
// Three-way dispatch on Autoscaler.Class (NOTE: none + external are
// status-field twins, both fall through to the "delete both" branch — the
// status writer distinguishes them via ManagedBy):
//
//   - hpa  → ensure the per-IR HPA exists; delete any pre-existing
//     OME-managed ScaledObject sharing the same (Namespace, Name).
//   - keda → ensure the per-IR ScaledObject exists; delete any pre-
//     existing OME-managed HPA sharing the same (Namespace, Name).
//   - none | external | nil → delete both OME-managed HPA + SO; no
//     OME-side autoscaler runs.
//
// Cross-class delete uses a Get-then-Delete-if-found pattern so the
// switch keda → hpa (and vice versa) is idempotent: re-running the
// dispatch after a no-op state-change reads NotFound and skips the
// Delete. Owner-ref + controller flag are stamped from p.Owner so a
// deletion of the parent (IR) cascades the autoscaler via foreground
// GC.
//
// Errors are wrapped with the offending object kind + key for grep-
// ability in operator logs. A failure on the cross-class delete short-
// circuits (the create / update for the new class is skipped) so the
// next reconcile pass retries the same sequence from the same state.
//
// Alpha API. The signature may change without notice.
func DispatchAutoscaler(ctx context.Context, p DispatchParams) error {
	if p.Client == nil {
		return fmt.Errorf("DispatchAutoscaler: nil client")
	}
	if p.Owner.UID == "" {
		return fmt.Errorf("DispatchAutoscaler: empty Owner.UID (refusing to stamp orphaned autoscaler for %s/%s)", p.Namespace, p.Name)
	}
	if p.Namespace == "" || p.Name == "" {
		return fmt.Errorf("DispatchAutoscaler: empty namespace or name (namespace=%q, name=%q)", p.Namespace, p.Name)
	}

	class := autoscalerClass(p.Autoscaler)

	switch class {
	case v1beta1.AutoscalerHPA:
		// Drop any pre-existing OME-managed ScaledObject for the same
		// IR — class switch keda → hpa cleans up the old SO before
		// the new HPA lands.
		if err := deleteScaledObjectIfExists(ctx, p.Client, p.Namespace, p.Name); err != nil {
			return fmt.Errorf("delete stale ScaledObject for HPA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		return ensureHPA(ctx, p)

	case v1beta1.AutoscalerKEDA:
		// Drop any pre-existing OME-managed HPA for the same IR —
		// class switch hpa → keda cleans up the old HPA before the
		// new SO lands.
		if err := deleteHPAIfExists(ctx, p.Client, p.Namespace, p.Name); err != nil {
			return fmt.Errorf("delete stale HPA for KEDA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		return ensureScaledObject(ctx, p)

	default:
		// AutoscalerNone, AutoscalerExternal, and nil all converge here.
		// Delete any OME-managed HPA + SO; the status writer is responsible
		// for the surface-level distinction between "none" (no autoscaler at
		// all) and "external" (operator-managed autoscaler).
		if err := deleteHPAIfExists(ctx, p.Client, p.Namespace, p.Name); err != nil {
			return fmt.Errorf("delete OME-managed HPA on class=%s (ns=%s, name=%s): %w", class, p.Namespace, p.Name, err)
		}
		if err := deleteScaledObjectIfExists(ctx, p.Client, p.Namespace, p.Name); err != nil {
			return fmt.Errorf("delete OME-managed ScaledObject on class=%s (ns=%s, name=%s): %w", class, p.Namespace, p.Name, err)
		}
		return nil
	}
}

// autoscalerClass extracts the resolved Class, treating nil as none. NOTE:
// external + none fold into the same dispatch branch (both => delete both);
// we keep them as distinct AutoscalerClass values so the status writer wired
// off the same resolved block can still emit the correct ManagedBy.
func autoscalerClass(a *v1beta1.ComponentAutoscaler) v1beta1.AutoscalerClass {
	if a == nil {
		return v1beta1.AutoscalerNone
	}
	return a.Class
}

// ensureHPA stamps the IR owner-ref on the generator output and runs
// the HPA reconciler. The HPAReconciler.Reconcile() loop is idempotent
// (Get-then-diff) so re-running on a steady state is a cheap no-op.
func ensureHPA(ctx context.Context, p DispatchParams) error {
	componentMeta := buildComponentMeta(p)

	var hpaSpec *v1beta1.HPAAutoscaler
	if p.Autoscaler != nil {
		hpaSpec = p.Autoscaler.HPA
	}

	r := hpa.NewHPAReconcilerForTarget(
		p.Client,
		p.Scheme,
		componentMeta,
		p.ScaleTargetRef,
		hpaSpec,
		p.MinReplicas,
		p.MaxReplicas,
	)
	if err := r.Reconcile(); err != nil {
		return fmt.Errorf("HPA reconcile (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
	}
	_ = ctx // reconciler currently uses context.TODO internally; threaded here for future ctx-aware refactor
	return nil
}

// ensureScaledObject stamps the IR owner-ref on the generator output
// and runs the KEDA reconciler. The KEDAReconciler.Reconcile() loop is
// idempotent (Get-then-diff).
func ensureScaledObject(ctx context.Context, p DispatchParams) error {
	componentMeta := buildComponentMeta(p)

	var kedaSpec *v1beta1.KedaAutoscaler
	if p.Autoscaler != nil {
		kedaSpec = p.Autoscaler.Keda
	}

	r := keda.NewKEDAReconcilerForTarget(
		p.Client,
		p.Scheme,
		componentMeta,
		kedaScaleTarget(p.ScaleTargetRef),
		kedaSpec,
		p.MinReplicas,
		p.MaxReplicas,
	)
	if err := r.Reconcile(); err != nil {
		return fmt.Errorf("ScaledObject reconcile (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
	}
	_ = ctx
	return nil
}

// buildComponentMeta assembles the ObjectMeta the HPA / KEDA generators
// consume. Owner-ref MUST be set here (the generators write it onto the
// generated object directly — they don't call SetControllerReference).
// Annotations is initialized empty so the legacy AutoscalerClass
// annotation gate in shouldCreateHPA / shouldCreateScaledObject sees
// `""` and falls through to the create branch.
func buildComponentMeta(p DispatchParams) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            p.Name,
		Namespace:       p.Namespace,
		OwnerReferences: []metav1.OwnerReference{p.Owner},
	}
}

// kedaScaleTarget converts the autoscalingv2 scaleTargetRef shape (which
// the dispatch params expose to keep both branches symmetric) into the
// kedav1.ScaleTarget the KEDA generator expects. APIVersion / Kind /
// Name flow through 1:1.
func kedaScaleTarget(ref autoscalingv2.CrossVersionObjectReference) kedav1.ScaleTarget {
	return kedav1.ScaleTarget{
		APIVersion: ref.APIVersion,
		Kind:       ref.Kind,
		Name:       ref.Name,
	}
}

// deleteHPAIfExists is the idempotent Get + Delete-if-found primitive
// the cross-class switch + "delete both" branches share. NotFound is
// swallowed — the operator switching class twice in quick succession
// must not error out on the second pass.
func deleteHPAIfExists(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteScaledObjectIfExists is the SO equivalent. Names through
// utils.GetScaledObjectName so the key matches what the KEDA generator
// uses (prefix "scaledobject-" + truncation).
func deleteScaledObjectIfExists(ctx context.Context, c client.Client, namespace, name string) error {
	soName := utils.GetScaledObjectName(name)
	obj := &kedav1.ScaledObject{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: soName}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		// KEDA scheme is registered unconditionally at manager startup.
		// IsNotRegistered is dead code at steady state, but we keep the
		// defensive guard so a rolling-uninstall-during-reconcile race
		// doesn't panic.
		if runtime.IsNotRegisteredError(err) {
			return nil
		}
		return err
	}
	if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
