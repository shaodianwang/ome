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
	omenativecore "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	omenativeops "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadgang "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/gang"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	"github.com/ome-projects/ome/pkg/utils"
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

// Reconciler is the per-IR lifecycle reconciler. Replaced the no-op
// skeleton with the real workload-driven pipeline.
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

	// GangSchedulingAvailable is the cached cluster-discovery boolean —
	// true when the scheduler-plugins PodGroup CRD is installed. Set once
	// in SetupWithManager (mirrors the ISVC controller's flag) and threaded
	// into DesiredSpec so EnsurePodGroups runs. Without it, IR-managed
	// multi-node renders the gang reference on pods but never creates the
	// PodGroup object, leaving the gang stuck Pending ("PodGroup not found").
	GangSchedulingAvailable bool
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

	// Spec target ControllerRevision (from the rendered desired). Computed up
	// front and reported as UpdateRevision so a new spec target stays VISIBLE to
	// the ISVC even during a rollback — when the actual roll targets the stable
	// revision instead (below), this keeps the canary's re-arm-on-new-revision
	// working. PodSpec nil (webhook-rejected today, but defensive) → no revision.
	var specTarget *appsv1.ControllerRevision
	if input.DesiredSpec.PodSpec != nil {
		t, terr := r.ensureRevisionWithCollisionRetry(ctx, ir, input)
		if terr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: ensure revision (ir=%s/%s): %w",
				ir.Namespace, ir.Name, terr)
		}
		specTarget = t
	}

	// Canary rollback: when the ISVC controller set Pacing.RollbackToRevision,
	// roll every Instance back to that (stable) ControllerRevision by rendering
	// the desired template from the revision's stored payload and using the
	// revision as the ROLL target — while UpdateRevision keeps reporting the spec
	// target (above). The forward-roll machinery then drains the canary pods onto
	// stable. CR gone (GC'd) → fall through to the normal desired spec.
	rollTarget := specTarget
	if ir.Spec.Pacing != nil && ir.Spec.Pacing.RollbackToRevision != nil && *ir.Spec.Pacing.RollbackToRevision != "" {
		cr := &appsv1.ControllerRevision{}
		switch err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: *ir.Spec.Pacing.RollbackToRevision}, cr); {
		case err == nil:
			payload, perr := revision.PayloadFromControllerRevision(cr)
			if perr != nil {
				return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: rollback revision payload (ir=%s/%s): %w", ir.Namespace, ir.Name, perr)
			}
			if payload != nil && payload.PodSpec != nil {
				input.DesiredSpec.PodSpec = payload.PodSpec
				input.DesiredSpec.WorkerPodSpec = payload.WorkerPodSpec
				input.DesiredSpec.PodTemplateObjectMeta = payload.PodMeta
				rollTarget = cr
				log.Info("Canary rollback: rolling Instances back to stable revision", "revision", cr.Name)
			}
		case apierrors.IsNotFound(err):
			// stable CR GC'd — nothing to roll back to; proceed normally.
		default:
			return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: get rollback revision %s: %w", *ir.Spec.Pacing.RollbackToRevision, err)
		}
	}

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
		// Wire OEP-0007.1 peer-env injection on the IR-managed (live) path.
		// ISVCRenderHook overlays OME_<PEER>_ENDPOINT / _REVISION_ENDPOINT
		// onto each rendered pod so PD components (engine <-> decoder) can
		// address each other by stable DNS. It returns nil when the parent
		// ISVC has no rollout groups, so single-component / non-rollout
		// boxes are unaffected. parent may be nil when the
		// parent ISVC is unresolved (foreground-GC window) — the hook handles
		// nil by returning nil.
		RenderHook: omenativecore.ISVCRenderHook(parent),
		// Ensure a gang surge's PodGroup inline, just before its pods —
		// closes the window where the surge index hasn't yet landed in the
		// plan the top-level EnsurePodGroups keys off (a gang scheduler
		// would otherwise reject the surge pods with "PodGroup not found").
		EnsureGangPodGroup: workloadgang.EnsureSurgePodGroup(r.Client),
	}

	// PodGroups must exist BEFORE the first pod of any multi-pod Instance is
	// created, or the gang scheduler rejects the pods with "PodGroup not
	// found" and they stay Pending forever. The direct OMENative path does
	// this; the IR-managed path (the default) must too. No-op for single-pod
	// Components and when the PodGroup CRD is absent — EnsurePodGroups gates
	// on DesiredSpec.GangSchedulingAvailable, stamps GangSchedulingUnavailable,
	// and returns. Runs before the revision/dispatch so the gang is announced
	// ahead of pod creation.
	if gerr := workloadgang.EnsurePodGroups(ctx, deps, input, plan); gerr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: ensure pod groups (component=%s): %w", ir.Spec.Component, gerr)
	}

	// Defer status aggregation so it runs regardless of whether the
	// workload dispatcher returned a requeue or an error. Status-write
	// errors don't clobber a real dispatcher error — they only surface
	// when everything else succeeded. We log the suppressed error at
	// V(1) so a dispatcher+status double-failure isn't fully invisible.
	// Reports specTarget (the spec's desired) as UpdateRevision; in a rollback
	// the roll itself targets rollTarget (stable) but the spec target stays the
	// reported one so a new revision re-arms the canary.
	defer func() {
		if serr := r.aggregateAndWriteStatus(ctx, ir, parent, plan, specTarget); serr != nil {
			if err == nil {
				err = fmt.Errorf("InferenceReplica reconciler: write status: %w", serr)
				return
			}
			log.V(1).Error(serr, "Status write also failed; primary error preserved")
		}
		// ControllerRevision retention sweep. Runs AFTER the status write
		// so CollectLiveRevisionNames reads the just-committed
		// CurrentRevision / UpdateRevision (mirrored onto ir.Status above)
		// and never sweeps a revision the controller just promoted.
		// Best-effort: sweepRevisions logs + events on failure but never
		// fails the reconcile — without this the IR path accumulated
		// ControllerRevisions unbounded (the gap this fixes; retention was
		// only ever wired on the now-retired OMENative direct path).
		r.sweepRevisions(ctx, ir)
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

	// Self-trigger auto-migration for Instances stuck past
	// MigrationPolicy.ProgressDeadlineSeconds (node failure / wedged
	// scheduler / stuck image pull). Mirrors the direct OMENative
	// dispatcher: it writes synthetic AutoRecover ledger entries that
	// DetectMigrationRequest (wired in buildReconcileInput) picks up
	// inside workload.Reconcile below. Reads the parent ISVC's ledger +
	// aggregated InstanceStatuses; no-op when the parent is unresolved or
	// ProgressDeadlineSeconds is unset. Runs BEFORE the dispatch so the
	// request is visible this pass.
	if parent != nil {
		if merr := omenativeops.DetectStuckInstancesForISVC(ctx, parent,
			&v1beta1.ComponentExtensionSpec{Lifecycle: ir.Spec.Lifecycle},
			r.APIReader, r.Client, r.Recorder, ir.Spec.Component); merr != nil {
			return ctrl.Result{}, fmt.Errorf(
				"InferenceReplica reconciler: detect stuck instances for auto-migration (component=%s): %w",
				ir.Spec.Component, merr)
		}
	}

	result, err = workload.Reconcile(ctx, deps, input, plan, rollTarget)

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

// irRevisionKey is the per-(parent ISVC, Component) revision.Key the IR
// path keys ControllerRevisions on. Defined in exactly one place so the
// revision-ensure step (ensureRevisionWithCollisionRetry) and the
// retention sweep (sweepRevisions, retention.go) can never drift apart —
// a mismatched Key would either ensure CRs under one label set and sweep
// under another (retention never fires) or, worse, sweep a foreign
// workload's CRs. Lives here (not retention.go) so reconciler.go keeps
// owning the constants/query imports it already uses.
//
// IR-scoped via the parent ISVC name + Component (NOT the IR's own name
// or UID) so the labels match what the retired direct path stamped and
// shadow-mode equivalence holds.
func irRevisionKey(ir *v1beta1.InferenceReplica) revision.Key {
	return revision.Key{
		Namespace: ir.Namespace,
		Name:      ir.Spec.ParentRef.Name + "-" + string(ir.Spec.Component),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
			constants.OMEComponentLabel:           string(ir.Spec.Component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
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
	revKey := irRevisionKey(ir)

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
			ir.Namespace, ir.Name, utils.DerefInt32(bumped))
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
