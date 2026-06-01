// Package inferencereplica implements the controller for the
// InferenceReplica CRD.
//
// One InferenceReplica per (InferenceService, Component) tuple. The
// IR controller owns the per-Instance pipeline (Create / Update /
// Restart / Migrate / Delete) by handing a workload.ReconcileInput to
// workload.Reconcile each pass. It does NOT own ISVC-shape
// supporting resources (headless / per-revision Services, PodMonitor)
// or coordination — those stay on the ISVC controller in this PR.
//
// Status writer: this controller is the sole writer of
// InferenceReplica.status. The ISVC controller is the sole writer of
// InferenceReplica.spec (see the validating webhook in
// pkg/webhook/admission/inferencereplica/).
//
// The controller is enabled by default; the
// --enable-inferencereplica-controller manager flag lets operators
// flip it off if a regression lands.
package inferencereplica

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;update;patch;delete

// Reconciler is the per-IR lifecycle reconciler. Replaces the no-op
// skeleton from PR 1 (#245) with the real workload-driven pipeline.
type Reconciler struct {
	client.Client
	Log      logr.Logger
	Recorder record.EventRecorder

	// APIReader is the live (uncached) API reader. Used for the
	// correctness-critical reads in revision bookkeeping where stale
	// cache contents could cause spurious collisions. Optional; when
	// nil the cached Client is used.
	APIReader client.Reader

	// Expectations is the create/delete bookkeeping cache the
	// workload pipeline uses to avoid re-issuing batches before the
	// controller-runtime watch has confirmed prior writes. Optional;
	// when nil the workload-level DefaultExpectations singleton is
	// used.
	Expectations *workload.Expectations
}

// Reconcile loads the InferenceReplica, projects (Spec, Status) onto a
// workload.ReconcileInput, computes the target ControllerRevision,
// delegates the per-Instance dispatch loop to workload.Reconcile, and
// aggregates the per-Component counters onto IR.Status.
//
// Loop (mirrors the omenative.ReconcileComponent shape):
//
//  1. Get IR. NotFound → no-op (owner-ref cascade GC handles children).
//  2. DeletionTimestamp set → no-op (foreground GC is owned by the
//     apiserver via blockOwnerDeletion on each child).
//  3. Resolve parent ISVC so user-facing events stay on the ISVC
//     stream. Parent fetch failure is NON-fatal — fall back to the
//     IR itself as the event target.
//  4. Build the workload.ReconcileInput + Deps + ComponentPlan.
//  5. Ensure the target ControllerRevision for the rendered template.
//     The collision-retry shape matches the ISVC adapter: on a
//     same-name-different-Data collision, bump CollisionCount and
//     retry the EnsureControllerRevision call with the new salt.
//  6. Defer the status aggregator so it runs even when the workload
//     dispatcher returns early.
//  7. Call workload.Reconcile.
//
// Returns whatever (ctrl.Result, error) the workload dispatcher hands
// back, unchanged.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	log := r.Log.WithValues("inferencereplica", req.NamespacedName)

	ir := &v1beta1.InferenceReplica{}
	if err := r.Get(ctx, req.NamespacedName, ir); err != nil {
		if apierrors.IsNotFound(err) {
			// IR deleted between enqueue and reconcile. Owner-ref
			// cascade GC handles the children (pods, ControllerRevisions).
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get InferenceReplica")
		return ctrl.Result{}, err
	}

	// Bind structured logging context for the remainder of this
	// reconcile so every downstream callback inherits the same
	// (component, parent, generation) bindings without needing to
	// re-stamp them per-line.
	log = log.WithValues(
		"component", ir.Spec.Component,
		"parent", ir.Spec.ParentRef.Name,
		"generation", ir.Generation)
	ctx = ctrl.LoggerInto(ctx, log)
	log.V(1).Info("Reconcile entry",
		"currentRevision", ir.Status.CurrentRevision,
		"updateRevision", ir.Status.UpdateRevision,
		"replicas", ir.Status.Replicas,
		"readyReplicas", ir.Status.ReadyReplicas)

	// Snapshot revision pointers + failure-state BEFORE workload.Reconcile
	// so the deferred status-write can log on real transitions
	// (CurrentRevision promotion, first Phase=Failed escalation).
	priorCurrentRevision := ir.Status.CurrentRevision
	priorAnyFailed := hasFailedInstance(ir.Status.InstanceStatuses)

	if ir.DeletionTimestamp != nil {
		// Foreground cascade-delete is owned by the apiserver via
		// blockOwnerDeletion on each child owner ref. The IR controller
		// has no finalizer — its work is observation-only at delete time.
		log.V(1).Info("DeletionTimestamp set; no-op")
		return ctrl.Result{}, nil
	}

	// Resolve the parent ISVC for the event stream. Best-effort: an
	// IR can outlive its parent for a brief window during foreground
	// GC; surface NotFound / Forbidden as "no parent" rather than
	// failing the reconcile.
	parent := r.resolveParent(ctx, ir)

	input := r.buildReconcileInput(ir, parent)
	plan, perr := workload.BuildPlan(v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), input.DesiredSpec, input.ObservedState)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: build plan (ir=%s/%s): %w",
			ir.Namespace, ir.Name, perr)
	}

	deps := workload.Deps{
		Client:       r.Client,
		APIReader:    r.APIReader,
		Recorder:     r.Recorder,
		Expectations: r.Expectations,
		// RenderHook stays nil for the IR adapter — peer-env injection
		// lives on the parent ISVC controller for v1 (per design doc).
	}

	// Compute the target ControllerRevision before kicking off the
	// dispatcher so the Update pass + status aggregator both see the
	// same name. The collision-retry shape matches the ISVC adapter:
	// on a hash collision bump CollisionCount and retry. When PodSpec
	// is nil — webhook-rejected today but defensive — skip revision
	// entirely; the workload Update pass short-circuits on nil target.
	var target *appsv1.ControllerRevision
	if input.DesiredSpec.PodSpec != nil {
		t, terr := r.ensureRevisionWithCollisionRetry(ctx, ir, input)
		if terr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: ensure revision (ir=%s/%s): %w",
				ir.Namespace, ir.Name, terr)
		}
		target = t
	}

	// Defer status aggregation so it runs regardless of whether the
	// workload dispatcher returned a requeue or an error. Status-write
	// errors don't clobber a real dispatcher error — they only surface
	// when everything else succeeded. We log the suppressed error at
	// V(1) so a dispatcher+status double-failure isn't fully invisible.
	defer func() {
		if serr := r.aggregateAndWriteStatus(ctx, ir, plan, target); serr != nil {
			if err == nil {
				err = fmt.Errorf("InferenceReplica reconciler: write status: %w", serr)
				return
			}
			log.V(1).Error(serr, "Status write also failed; primary error preserved")
		}
		// State-transition logs: only fire when a real transition
		// crossed in this reconcile (not on steady-state passes). The
		// in-memory mirror in aggregateAndWriteStatus means ir.Status
		// now reflects the post-write counters.
		if priorCurrentRevision != ir.Status.CurrentRevision && ir.Status.CurrentRevision != "" {
			log.Info("Rollout complete; CurrentRevision promoted",
				"previousCurrentRevision", priorCurrentRevision,
				"currentRevision", ir.Status.CurrentRevision)
		}
		if !priorAnyFailed && hasFailedInstance(ir.Status.InstanceStatuses) {
			log.Info("Instance escalation observed; at least one Instance reached Phase=Failed")
		}
	}()

	result, err = workload.Reconcile(ctx, deps, input, plan, target)

	// Reconcile the per-Component headless Service. The Service gives every
	// pod a stable FQDN for peer discovery during gang init (multi-node) and
	// for any downstream proxy that wants per-pod addresses. Idempotent +
	// drift-correcting; safe to call every reconcile.
	//
	// Called AFTER workload.Reconcile so the Service exists by the time pods
	// flip Ready and downstream consumers start resolving them. Earlier
	// placement would risk creating a Service before the pods exist —
	// harmless but produces a brief "no endpoints" window.
	//
	// Service errors don't clobber a real workload.Reconcile error; only
	// surface them when the dispatcher succeeded. Same pattern the deferred
	// status-write uses above; log the suppressed error at V(1) so the
	// double-failure case is grep-able.
	if serr := workload.ReconcileHeadlessService(ctx, r.Client, buildHeadlessServiceSpec(ir)); serr != nil {
		if err == nil {
			err = fmt.Errorf("InferenceReplica reconciler: reconcile headless service: %w", serr)
		} else {
			log.V(1).Error(serr, "Headless Service reconcile also failed; primary error preserved")
		}
	}
	return result, err
}

// resolveParent fetches the parent ISVC named on the IR's ParentRef so
// the workload.ReconcileInput.EventTarget can stay anchored to the
// user-visible parent resource. NotFound / Forbidden are best-effort:
// returns nil and the caller falls back to the IR itself. Other errors
// are logged but also non-fatal — events landing on the IR instead of
// the parent is a soft degradation, not a reason to fail the whole
// reconcile.
func (r *Reconciler) resolveParent(ctx context.Context, ir *v1beta1.InferenceReplica) *v1beta1.InferenceService {
	if ir.Spec.ParentRef.Name == "" {
		return nil
	}
	parent := &v1beta1.InferenceService{}
	key := types.NamespacedName{Namespace: ir.Namespace, Name: ir.Spec.ParentRef.Name}
	if err := r.Get(ctx, key, parent); err != nil {
		if !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
			r.Log.V(1).Info("Failed to fetch parent ISVC for IR event stream; falling back to IR target",
				"ir", client.ObjectKeyFromObject(ir), "parent", key, "err", err)
		}
		return nil
	}
	return parent
}

// ensureRevisionWithCollisionRetry computes the target
// ControllerRevision for the IR's current PodSpec, honoring the
// collision contract from workload/revision: on a same-name-different-
// Data (or foreign-ownership) collision, bump
// IR.Status.CollisionCount, persist, and retry the EnsureControllerRevision
// with the new salt. The retry yields a different hash and lands.
//
// Mirrors the ISVC-side ensureRevisionWithCollisionRetry byte-for-byte
// except for: (a) the v1beta1.InferenceReplica handle, (b) the
// owner-ref shape (IR is owner), (c) collision-counter persistence
// goes to IR.Status.CollisionCount instead of the per-Component status.
func (r *Reconciler) ensureRevisionWithCollisionRetry(ctx context.Context, ir *v1beta1.InferenceReplica, input workload.ReconcileInput) (*appsv1.ControllerRevision, error) {
	revKey := revision.Key{
		Namespace: ir.Namespace,
		Name:      ir.Spec.ParentRef.Name + "-" + string(ir.Spec.Component),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
			constants.OMEComponentLabel:           string(ir.Spec.Component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	// scopeUID = parent ISVC's UID so the IR-managed path partitions the
	// revision history by the same identity the direct OMENative path uses
	// (isvc.UID, via core.EnsureControllerRevisionForISVC). Using ir.UID
	// here would diverge the two paths' CR names — breaking shadow-mode
	// equivalence and stranding pods across a path cutover.
	//
	// Source: the IR's controller OwnerReference. The projector
	// (pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector)
	// stamps the parent ISVC as the IR's controller-owner on every
	// Create/Update, so this is the authoritative parent-UID. Refusing
	// to proceed on a missing controller-owner is correct: an empty
	// scopeUID would silently produce a CR hash that doesn't match the
	// direct OMENative path and break shadow-mode equivalence.
	var scopeUID types.UID
	if ctrlRef := metav1.GetControllerOf(ir); ctrlRef != nil {
		scopeUID = ctrlRef.UID
	}
	if scopeUID == "" {
		return nil, fmt.Errorf("InferenceReplica %s/%s missing controller OwnerReference; cannot derive scopeUID for revision",
			ir.Namespace, ir.Name)
	}

	target, collision, err := revision.EnsureControllerRevisionWithWorker(
		ctx, r.Client, reader,
		ir, irGVK, revKey,
		input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec,
		input.DesiredSpec.PodTemplateObjectMeta,
		ir.Status.CollisionCount,
		scopeUID,
	)
	if err != nil {
		return nil, err
	}
	if !collision {
		return target, nil
	}

	bumped, berr := r.bumpCollisionCount(ctx, ir)
	if berr != nil {
		return nil, fmt.Errorf("bump CollisionCount after revision collision: %w", berr)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(ir, corev1.EventTypeWarning, "RevisionCollision",
			"InferenceReplica %s/%s ControllerRevision hash collision; CollisionCount bumped to %d",
			ir.Namespace, ir.Name, derefInt32(bumped))
	}

	target, collision, err = revision.EnsureControllerRevisionWithWorker(
		ctx, r.Client, reader,
		ir, irGVK, revKey,
		input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec,
		input.DesiredSpec.PodTemplateObjectMeta,
		bumped,
		scopeUID,
	)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("revision still collides after CollisionCount bump (count=%d)", *bumped)
	}
	return target, nil
}

// bumpCollisionCount increments IR.Status.CollisionCount (initializing
// to 1 when nil) and persists the status under retry.RetryOnConflict
// so concurrent reconciles can't lose the bump. Each retry re-reads
// the current count and increments from there — collision counting is
// monotonic, so incrementing whatever the apiserver currently shows is
// the right semantic.
//
// Mirrors the ISVC-side bumpCollisionCount; NotFound on Get/Update is
// treated as "owner gone, nothing to bump" and the caller proceeds.
func (r *Reconciler) bumpCollisionCount(ctx context.Context, ir *v1beta1.InferenceReplica) (*int32, error) {
	key := client.ObjectKeyFromObject(ir)
	var next int32
	gone := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.Client.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				gone = true
				return nil
			}
			return fmt.Errorf("re-read IR for CollisionCount bump: %w", err)
		}
		next = int32(1)
		if fresh.Status.CollisionCount != nil {
			next = *fresh.Status.CollisionCount + 1
		}
		fresh.Status.CollisionCount = &next
		if err := r.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				gone = true
				return nil
			}
			return fmt.Errorf("persist CollisionCount: %w", err)
		}
		return nil
	})
	if gone {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Mirror onto the cached IR so the immediate retry sees the bumped
	// value. Same mirror shape the ISVC adapter uses.
	ir.Status.CollisionCount = &next
	return &next, nil
}

// derefInt32 returns 0 for a nil pointer, *p otherwise. Used for event
// messages where "<nil>" would be confusing.
func derefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}
