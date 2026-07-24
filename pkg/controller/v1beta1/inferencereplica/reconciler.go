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
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	omenativecore "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	workloadgang "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/gang"
	workloadpodgroup "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podgroup"
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
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;update;patch;delete

// Reconciler is the per-IR lifecycle reconciler. Replaced the no-op
// skeleton with the real workload-driven pipeline.
type Reconciler struct {
	client.Client
	Log      logr.Logger
	Recorder record.EventRecorder

	// Clientset reads the inferenceservice-config ConfigMap for the
	// operator lifecycle config (same-target update retry policy).
	// Optional: when nil the policy resolves to nil and retry-blocking
	// fails safe (Held on the first same-target failure).
	Clientset kubernetes.Interface

	// ConfigCache memoizes the inferenceservice-config ConfigMap for a
	// short, flag-driven TTL so per-reconcile config loads share one
	// apiserver GET — the same controllerconfig.ConfigCache the ISVC
	// controller uses (its own instance; the cache type is the shared
	// implementation). Initialized in SetupWithManager.
	ConfigCache *controllerconfig.ConfigCache
	// ConfigCacheTTL is the TTL applied to ConfigCache. Flag-driven (no
	// in-code behavioral default — supplied by the manager's
	// --config-cache-ttl flag / chart value); a non-positive value
	// disables caching (always reads the apiserver).
	ConfigCacheTTL time.Duration

	// APIReader is the live (uncached) API reader. Used for the
	// correctness-critical reads in revision bookkeeping and immutable topology
	// safety where stale cache contents could cause a collision or split a live
	// gang across domains. Optional; when nil the cached Client is used.
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

	// MaxConcurrentReconciles caps parallel IR reconciles (distinct objects
	// only — controller-runtime serializes per object key, so independent IRs
	// reconcile in parallel safely). Sourced from a flag/chart value; no in-code
	// default. Zero (unset) preserves controller-runtime's single-worker default.
	MaxConcurrentReconciles int

	// revisionHashMu guards revisionHashCache. The same IR key is never
	// reconciled concurrently by controller-runtime, but the cache is
	// shared across keys, so the map itself needs a lock if concurrency is
	// ever raised above the default of one.
	revisionHashMu sync.Mutex
	// revisionHashCache memoizes the revision (hash, raw) per IR so the
	// full-PodSpec json.Marshal + FNV in revision.HashWithWorker is skipped
	// when the rendered pod template is provably unchanged. The template is
	// a pure projection of the IR spec, so it only changes on a generation
	// bump or a collision-count bump (both observable without re-marshaling);
	// the scope UID partitions per-parent identity. Keyed by the IR's
	// NamespacedName (so the NotFound branch can evict without the UID) with
	// the IR UID folded into the entry so a delete-and-recreate of the same
	// name misses the stale entry.
	revisionHashCache map[types.NamespacedName]revisionHashEntry

	// Clock supplies wall-clock time to the workload lifecycle layer.
	// SetupWithManager defaults it to the real clock; tests may inject a fake.
	Clock clock.Clock
}

// revisionHashEntry is one memoized revision hash plus the inputs that
// invalidate it. raw is the canonical serialized payload stored in
// ControllerRevision.Data.Raw; it is treated as immutable once cached.
type revisionHashEntry struct {
	uid            types.UID
	generation     int64
	collisionCount int32
	scopeUID       types.UID
	hash           string
	raw            []byte
}

// Reconcile loads the InferenceReplica, projects (Spec, Status) onto a
// workload.ReconcileInput, computes the target ControllerRevision,
// delegates the per-Instance dispatch loop to workload.Reconcile, and
// aggregates the per-Component counters onto IR.Status.
//
// Loop (mirrors the omenative.ReconcileComponent shape):
//
//  1. Get IR. NotFound → no-op (background owner-ref GC handles
//     children).
//  2. DeletionTimestamp set → finalizer-driven teardown (teardown.go):
//     dispatch the ordinary Delete pipeline against every observed
//     Instance until no component pod remains, then lift the teardown
//     finalizer. Without the finalizer, cleanup is plain BACKGROUND
//     kube-GC via the children's owner references (nothing sets a
//     foreground propagation policy).
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
			// Drop the memoized revision hash so the cache doesn't retain
			// entries for IRs that no longer exist.
			r.forgetRevisionHash(req.NamespacedName)
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

	if ir.DeletionTimestamp != nil {
		// Finalizer-driven teardown: reconcile the deletion as "desired
		// instances = zero" so pods drain and escalate through the same
		// Delete pipeline as scale-down; any remainder after the
		// finalizer lifts is BACKGROUND kube-GC via the children's owner
		// references (nothing sets a foreground propagation policy).
		return r.reconcileTeardown(ctx, log, ir)
	}

	// Ensure the teardown finalizer before any other work so a later
	// delete routes through the reconciled teardown path. The IR
	// validating webhook admits finalizer-only updates, so this write
	// clears admission even without the controller-write annotation.
	if !controllerutil.ContainsFinalizer(ir, TeardownFinalizer) {
		controllerutil.AddFinalizer(ir, TeardownFinalizer)
		if err := r.Update(ctx, ir); err != nil {
			if apierrors.IsNotFound(err) {
				// Deleted between Get and Update; nothing to protect.
				return ctrl.Result{}, nil
			}
			if apierrors.IsConflict(err) {
				// Lost a write race; requeue and re-add on the fresh object.
				return ctrl.Result{Requeue: true}, nil
			}
			// Deletion race: a stale cache showed no DeletionTimestamp but
			// the apiserver copy is already Terminating, so the add is
			// rejected ("no new finalizers can be added if the object is
			// being deleted"). Re-read live rather than matching the error
			// shape: Terminating means this is the self-healing race, not a
			// real failure — requeue onto the teardown path without
			// surfacing an error (and its backoff noise).
			fresh := &v1beta1.InferenceReplica{}
			switch gerr := r.reader().Get(ctx, req.NamespacedName, fresh); {
			case gerr == nil && fresh.DeletionTimestamp != nil:
				log.V(1).Info("Finalizer add rejected: IR already Terminating; requeueing onto the teardown path")
				return ctrl.Result{Requeue: true}, nil
			case gerr != nil && apierrors.IsNotFound(gerr):
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
	}

	// Snapshot revision pointers + failure-state BEFORE workload.Reconcile
	// so the deferred status-write can log on real transitions
	// (CurrentRevision promotion, first Phase=Failed escalation).
	priorCurrentRevision := ir.Status.CurrentRevision
	priorAnyFailed := hasFailedInstance(ir.Status.InstanceStatuses)

	// Resolve the parent ISVC for the event stream. Best-effort: an
	// IR can outlive its parent for a brief window during foreground
	// GC; surface NotFound / Forbidden as "no parent" rather than
	// failing the reconcile.
	parent := r.resolveParent(ctx, ir)

	// Resolve the same-target update retry policy ONCE per reconcile from
	// the operator lifecycle config. nil (absent/invalid config) fails
	// safe: the workload layer Holds on the first same-target failure.
	retryPolicy := r.resolveUpdateRetryPolicy(log)

	// Resolve the stuck-pod grace period from the operator lifecycle config.
	// Zero (absent/invalid config) fails safe: the escalator skips fast
	// escalation this pass and only the InstanceReadyTimeout backstop fires.
	stuckPodGrace := r.resolveStuckPodGrace(log)

	// Resolve the auto-migrate relocation budget from the operator lifecycle
	// config. Zero (absent/invalid config) fails safe: the deadline
	// disposition's relocation branch is disabled and non-workload-caused
	// expiries dispose terminal.
	autoMigrateBudget := r.resolveAutoMigrateBudget(log)

	// Resolve the stuck-Terminating force-delete policy from the operator
	// lifecycle config. nil (absent/invalid config) fails safe: the
	// escalation is disabled and wedged teardowns are left alone.
	forceDeletePolicy := r.resolveForceDeletePolicy(log)

	input := r.buildReconcileInput(ctx, ir, parent, retryPolicy, forceDeletePolicy)

	// Relocation-directive memory: prune AutoRecover ledger records for
	// instances observed Ready (the success-prune mirror of the
	// RetryBlock prune), then project the per-instance node-exclusion
	// lists the renderer materializes as NotIn overlays. Fail-open on
	// ledger errors — a missing overlay only risks one rebuild landing
	// back on the suspect node for a pass.
	input.ObservedState.ExcludedNodesByInstance = r.reconcileRelocationDirectives(ctx, log, ir, parent, autoMigrateBudget)

	// Park every in-flight deadline before revision creation, rollback payload
	// reads, or plan construction can fail. The deferred aggregate path below
	// remains authoritative for normal gated-pod bookkeeping and for rearming
	// parked deadlines after unpause; this early edge only makes the pause
	// circuit breaker resilient to failures that occur before that defer exists.
	if input.DesiredSpec.Paused {
		// A paused pass only parks deadlines; the timeout is used solely when
		// rearming during the normal aggregate path after unpause.
		if derr := r.reconcileHeldDeadlines(ctx, ir, parent, nil, true, 0); derr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: park paused operation deadlines: %w", derr)
		}
	}

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
	var rollbackPayload *revision.DataPayload
	var rollbackRevision *appsv1.ControllerRevision
	if ir.Spec.Pacing != nil && ir.Spec.Pacing.RollbackToRevision != nil && *ir.Spec.Pacing.RollbackToRevision != "" {
		cr := &appsv1.ControllerRevision{}
		switch err := r.reader().Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: *ir.Spec.Pacing.RollbackToRevision}, cr); {
		case err == nil:
			payload, perr := revision.PayloadFromControllerRevision(cr)
			if perr != nil {
				return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: rollback revision payload (ir=%s/%s): %w", ir.Namespace, ir.Name, perr)
			}
			if payload != nil && payload.PodSpec != nil {
				if perr := r.applyRollbackPayload(ctx, ir, &input.DesiredSpec, payload, cr); perr != nil {
					return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: apply rollback revision %s: %w", cr.Name, perr)
				}
				rollTarget = cr
				rollbackPayload = payload
				rollbackRevision = cr
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

	// Install status aggregation before PodGroup reconciliation. Besides
	// counters, the aggregator parks InstanceReadyTimeout deadlines while the
	// workload is paused. A live-read or PodGroup error must not bypass that
	// circuit-breaker bookkeeping.
	defer func() {
		if serr := r.aggregateAndWriteStatus(ctx, ir, parent, plan, specTarget, stuckPodGrace, retryPolicy, autoMigrateBudget); serr != nil {
			if err == nil {
				if apierrors.IsConflict(serr) {
					result, err = ctrl.Result{Requeue: true}, nil
					return
				}
				result = ctrl.Result{}
				err = fmt.Errorf("InferenceReplica reconciler: write status: %w", serr)
				return
			}
			log.V(1).Error(serr, "Status write also failed; primary error preserved")
		}
		r.sweepRevisions(ctx, ir)
		if priorCurrentRevision != ir.Status.CurrentRevision && ir.Status.CurrentRevision != "" {
			log.Info("Rollout complete; CurrentRevision promoted",
				"previousCurrentRevision", priorCurrentRevision,
				"currentRevision", ir.Status.CurrentRevision)
		}
		if !priorAnyFailed && hasFailedInstance(ir.Status.InstanceStatuses) {
			log.Info("Instance escalation observed; at least one Instance reached Phase=Failed")
		}
	}()

	if rollbackPayload != nil {
		log.Info("Canary rollback: rolling Instances back to stable revision", "revision", rollbackRevision.Name)
	}

	deps := workload.Deps{
		Client:       r.Client,
		APIReader:    r.APIReader,
		Recorder:     r.Recorder,
		Expectations: r.Expectations,
		Clock:        r.Clock,
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
	}
	deps.EnsureGangPodGroup = workloadgang.EnsureSurgePodGroup(deps)

	// PodGroups must exist BEFORE the first pod of any multi-pod Instance is
	// created, or the gang scheduler rejects the pods with "PodGroup not
	// found" and they stay Pending forever. The direct OMENative path does
	// this; the IR-managed path (the default) must too. No-op for single-pod
	// Components and when the PodGroup CRD is absent — EnsurePodGroups gates
	// on DesiredSpec.GangSchedulingAvailable, stamps GangSchedulingUnavailable,
	// and returns. Runs before the revision/dispatch so the gang is announced
	// ahead of pod creation.
	effectiveTopology, gerr := workloadgang.EnsurePodGroupsWithTopology(ctx, deps, input, plan)
	if gerr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: ensure pod groups (component=%s): %w", ir.Spec.Component, gerr)
	}
	plan.InstanceTopologyKeys = effectiveTopology

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
			// Error-driven requeue: clear any non-zero dispatcher result
			// (controller-runtime ignores the result when err != nil).
			result = ctrl.Result{}
			err = fmt.Errorf("InferenceReplica reconciler: reconcile headless service: %w", serr)
		} else {
			log.V(1).Error(serr, "Headless Service reconcile also failed; primary error preserved")
		}
	}
	return result, err
}

// resolveUpdateRetryPolicy loads lifecycle.updateRetry from the operator
// ConfigMap (through the shared short-TTL ConfigCache). Absent, invalid,
// or unloadable config resolves to nil — the workload layer fails safe
// (the first same-target failure Holds) — with one V(1) log naming the
// cause; there is never a silent in-code fallback policy.
func (r *Reconciler) resolveUpdateRetryPolicy(log logr.Logger) *workload.RetryPolicy {
	if r.Clientset == nil {
		log.V(1).Info("update retry policy unresolved: no clientset wired; failing safe (Held on first same-target failure)")
		return nil
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("update retry policy unresolved: lifecycle config load failed; failing safe (Held on first same-target failure)", "error", err.Error())
		return nil
	}
	if cfg == nil || cfg.UpdateRetry == nil {
		log.V(1).Info("update retry policy unconfigured (no lifecycle.updateRetry in inferenceservice-config); failing safe (Held on first same-target failure)")
		return nil
	}
	policy, err := cfg.UpdateRetry.ToPolicy()
	if err != nil {
		log.V(1).Info("update retry policy invalid; failing safe (Held on first same-target failure)", "error", err.Error())
		return nil
	}
	return policy
}

// resolveStuckPodGrace loads lifecycle.stuckPodGracePeriod from the
// operator ConfigMap. Parse failure or absent config resolves to 0 —
// the escalator skips fast escalation this pass (the
// InstanceReadyTimeout backstop still fires) — with one V(1) log.
func (r *Reconciler) resolveStuckPodGrace(log logr.Logger) time.Duration {
	if r.Clientset == nil {
		log.V(1).Info("stuck-pod grace unresolved: no clientset wired; skipping fast escalation this pass")
		return 0
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("stuck-pod grace unresolved: lifecycle config load failed; skipping fast escalation this pass", "error", err.Error())
		return 0
	}
	if cfg == nil {
		log.V(1).Info("stuck-pod grace unconfigured (no lifecycle block in inferenceservice-config); skipping fast escalation this pass")
		return 0
	}
	grace, err := cfg.ToGracePeriod()
	if err != nil {
		log.V(1).Info("stuck-pod grace invalid; skipping fast escalation this pass", "error", err.Error())
		return 0
	}
	if grace == 0 {
		log.V(1).Info("stuck-pod grace unconfigured (no lifecycle.stuckPodGracePeriod); skipping fast escalation this pass")
		return 0
	}
	return grace
}

// resolveAutoMigrateBudget loads lifecycle.autoMigrate.maxAttempts from
// the operator ConfigMap. Absent, invalid, or unloadable config resolves
// to 0 — the deadline disposition's relocation branch is disabled (no
// migration without an operator budget) — with one V(1) log naming the
// cause; there is never a silent in-code fallback budget.
func (r *Reconciler) resolveAutoMigrateBudget(log logr.Logger) int32 {
	if r.Clientset == nil {
		log.V(1).Info("auto-migrate budget unresolved: no clientset wired; relocation branch disabled this pass")
		return 0
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("auto-migrate budget unresolved: lifecycle config load failed; relocation branch disabled this pass", "error", err.Error())
		return 0
	}
	if cfg == nil || cfg.AutoMigrate == nil {
		log.V(1).Info("auto-migrate budget unconfigured (no lifecycle.autoMigrate in inferenceservice-config); relocation branch disabled this pass")
		return 0
	}
	if err := cfg.AutoMigrate.Validate(); err != nil {
		log.V(1).Info("auto-migrate budget invalid; relocation branch disabled this pass", "error", err.Error())
		return 0
	}
	return cfg.AutoMigrate.MaxAttempts
}

// resolveForceDeletePolicy loads lifecycle.forceDelete from the operator
// ConfigMap (through the shared short-TTL ConfigCache). Absent, invalid,
// or unloadable config resolves to nil — the stuck-Terminating
// force-delete escalation is disabled this pass — with one V(1) log
// naming the cause; there is never a silent in-code fallback policy.
func (r *Reconciler) resolveForceDeletePolicy(log logr.Logger) *workload.ForceDeletePolicy {
	if r.Clientset == nil {
		log.V(1).Info("force-delete policy unresolved: no clientset wired; escalation disabled this pass")
		return nil
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("force-delete policy unresolved: lifecycle config load failed; escalation disabled this pass", "error", err.Error())
		return nil
	}
	if cfg == nil || cfg.ForceDelete == nil {
		log.V(1).Info("force-delete policy unconfigured (no lifecycle.forceDelete in inferenceservice-config); escalation disabled")
		return nil
	}
	policy, err := cfg.ForceDelete.ToPolicy()
	if err != nil {
		log.V(1).Info("force-delete policy invalid; escalation disabled this pass", "error", err.Error())
		return nil
	}
	return policy
}

// resolveTeardownDeadline loads lifecycle.teardown.deadline from the
// operator ConfigMap. Returns (deadline, invalidReason): absent or
// unloadable config yields (nil, "") — strict hold, genuinely
// unconfigured — while config that is PRESENT but invalid yields
// (nil, <parse error>) so the strict-hold diagnostics can say why the
// deadline didn't apply instead of claiming none was configured. The
// invalid case logs at Info (not V(1)), once per resolve: an operator
// who wrote a bad duration must see it without raising verbosity.
func (r *Reconciler) resolveTeardownDeadline(log logr.Logger) (*time.Duration, string) {
	if r.Clientset == nil {
		log.V(1).Info("teardown deadline unresolved: no clientset wired; no deadline this pass")
		return nil, ""
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("teardown deadline unresolved: lifecycle config load failed; no deadline this pass", "error", err.Error())
		return nil, ""
	}
	if cfg == nil || cfg.Teardown == nil {
		log.V(1).Info("teardown deadline unconfigured (no lifecycle.teardown in inferenceservice-config); no deadline")
		return nil, ""
	}
	deadline, err := cfg.Teardown.ToDeadline()
	if err != nil {
		log.Info("teardown deadline configured but invalid; strict hold applies", "error", err.Error())
		return nil, err.Error()
	}
	return deadline, ""
}

// reconcileRelocationDirectives owns the per-reconcile relocation-
// directive bookkeeping against the audit ledger (parent-ISVC-owned
// when resolvable, else IR-owned — same rule as buildReconcileInput's
// LedgerOwner):
//
//  1. Success-prune: AutoRecover records for any instance observed
//     Phase=Ready with no in-flight Operation are removed — the
//     instance proved its placement, so its exclusion memory and
//     relocation budget reset (mirror of the RetryBlock promote-prune;
//     done here, adapter-side, because the workload promote stampers
//     have no client handle, and gated on the ledger actually holding
//     entries so the steady-state hot path pays one cached GET at most).
//  2. Projection: build the Instance-index → excluded-nodes map from
//     the remaining AutoRecover entries, each list bounded to the most
//     recent autoMigrateBudget entries.
//
// All errors fail open with a V(1) log: the overlay is a placement
// steer, not a correctness gate, and the next pass re-derives it.
func (r *Reconciler) reconcileRelocationDirectives(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, autoMigrateBudget int32) map[int32][]string {
	if r.Client == nil || ir == nil {
		return nil
	}
	owner := client.Object(ir)
	gvk := irGVK
	if parent != nil {
		owner = parent
		gvk = isvcGVK
	}
	ledger, err := audit.LoadLedgerForOwner(ctx, r.Client, owner)
	if err != nil {
		log.V(1).Info("relocation directives unavailable: ledger load failed; rendering without node exclusions this pass", "error", err.Error())
		return nil
	}
	if len(ledger.Entries) == 0 {
		return nil
	}
	component := string(ir.Spec.Component)

	pruned := false
	for i := range ir.Status.InstanceStatuses {
		s := &ir.Status.InstanceStatuses[i]
		if s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil {
			continue
		}
		if audit.RemoveAutoRecoverEntries(ledger, component, s.Index) {
			pruned = true
		}
	}
	if pruned {
		if err := audit.PersistLedgerForOwner(ctx, r.Client, owner, gvk, ledger); err != nil {
			// Projection below still uses the pruned in-memory view; the
			// persisted prune retries next pass.
			log.V(1).Info("relocation-directive prune persist failed; will retry next pass", "error", err.Error())
		}
	}

	if autoMigrateBudget <= 0 {
		return nil
	}
	var out map[int32][]string
	for i := range ir.Status.InstanceStatuses {
		idx := ir.Status.InstanceStatuses[i].Index
		nodes := audit.RecentAutoRecoverFromNodes(ledger, component, idx, autoMigrateBudget)
		if len(nodes) == 0 {
			continue
		}
		if out == nil {
			out = map[int32][]string{}
		}
		out[idx] = nodes
	}
	return out
}

// reader returns the authoritative live reader when wired, else the
// cached client. Single home for the consistency-class fallback.
func (r *Reconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// applyRollbackPayload restores revision-owned render inputs. Explicitly
// recorded topology is authoritative. A legacy multi-pod revision has no
// topology field even when its live workers were rendered with OME-generated
// affinity, so a non-empty current topology requires recovery from live pods;
// ambiguity fails closed rather than rendering a split gang. When current
// topology is empty, absence remains a non-blocking topology-free rollback.
func (r *Reconciler) applyRollbackPayload(ctx context.Context, ir *v1beta1.InferenceReplica, desired *workload.WorkloadDesiredSpec, payload *revision.DataPayload, target *appsv1.ControllerRevision) error {
	if desired == nil || payload == nil {
		return nil
	}
	currentTopology := desired.TopologyKey
	desired.PodSpec = payload.PodSpec
	desired.WorkerPodSpec = payload.WorkerPodSpec
	desired.PodTemplateObjectMeta = payload.PodMeta
	desired.TopologyKey = ""
	if payload.TopologyKey != nil {
		desired.TopologyKey = *payload.TopologyKey
		return nil
	}
	if payload.WorkerPodSpec == nil {
		return nil
	}
	if ir == nil || target == nil {
		if currentTopology == "" {
			return nil
		}
		return fmt.Errorf("legacy multi-pod rollback with configured topology requires IR and target revision")
	}
	targetHash := query.RevisionHashFromControllerRevisionName(target.Name)
	if targetHash == "" {
		return fmt.Errorf("legacy rollback revision %q has no recognizable revision hash", target.Name)
	}
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	pods, err := query.ListOMENativePodsByName(ctx, r.reader(), ir.Namespace, ir.Spec.ParentRef.Name, component, false)
	if err != nil {
		return fmt.Errorf("list live pods for legacy rollback topology recovery: %w", err)
	}
	targetPods := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod != nil && pod.Labels[query.LabelRevisionHash] == targetHash {
			targetPods = append(targetPods, pod)
		}
	}
	recovered, ok, recoveryErr := workloadpodgroup.GeneratedTopologyKeyFromPods(
		ir.Spec.ParentRef.Name, component, targetPods)
	if recoveryErr != nil {
		return fmt.Errorf("legacy rollback revision %q has conflicting OME-generated topology: %w", target.Name, recoveryErr)
	}
	if ok {
		desired.TopologyKey = recovered
		return nil
	}
	if currentTopology == "" {
		return nil
	}
	return fmt.Errorf("legacy rollback revision %q has no unambiguous OME-generated topology among %d live target-revision pods", target.Name, len(targetPods))
}

// resolveParent fetches the parent ISVC named on the IR's ParentRef so
// the workload.ReconcileInput.EventTarget can stay anchored to the
// user-visible parent resource. NotFound / Forbidden are best-effort:
// returns nil and the caller falls back to the IR itself. Other errors
// are logged but also non-fatal — events landing on the IR instead of
// the parent is a soft degradation, not a reason to fail the whole
// reconcile.
func (r *Reconciler) resolveParent(ctx context.Context, ir *v1beta1.InferenceReplica) *v1beta1.InferenceService {
	return r.resolveParentFrom(ctx, r.Client, ir)
}

// resolveParentFrom is resolveParent against an explicit reader. The
// teardown path passes the live reader: a stale cached NotFound on a
// still-live parent would re-target the dangling-ledger close at the
// empty IR-owned ledger and leave the parent-owned Started entry to
// resume as a phantom migration.
func (r *Reconciler) resolveParentFrom(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica) *v1beta1.InferenceService {
	if ir.Spec.ParentRef.Name == "" {
		return nil
	}
	parent := &v1beta1.InferenceService{}
	key := types.NamespacedName{Namespace: ir.Namespace, Name: ir.Spec.ParentRef.Name}
	if err := reads.Get(ctx, key, parent); err != nil {
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

	reader := r.reader()

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

	hash, raw, err := r.revisionHash(ir, input, ir.Status.CollisionCount, scopeUID)
	if err != nil {
		return nil, err
	}
	target, collision, err := revision.EnsureControllerRevisionFromHash(
		ctx, r.Client, reader,
		ir, irGVK, revKey, hash, raw,
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

	hash, raw, err = r.revisionHash(ir, input, bumped, scopeUID)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	target, collision, err = revision.EnsureControllerRevisionFromHash(
		ctx, r.Client, reader,
		ir, irGVK, revKey, hash, raw,
	)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("revision still collides after CollisionCount bump (count=%d)", *bumped)
	}
	return target, nil
}

// revisionHash returns the revision (hash, raw) for the IR's rendered pod
// template, reusing the memoized result when neither the generation, the
// collision count, nor the scope UID has changed since the last compute.
//
// The rendered template (input.DesiredSpec.{PodSpec,WorkerPodSpec,
// PodTemplateObjectMeta}) is a pure projection of the IR spec, so it only
// changes on a spec edit (generation bump). collisionCount and scopeUID
// are the remaining hash inputs folded by
// revision.HashWithWorkerAndTopology, so they
// complete the cache key. On a miss it recomputes via the same
// canonical hash call used by the revision creation path,
// so the (hash, raw) is byte-identical to the unmemoized path.
//
// raw is returned by reference (immutable once cached). EnsureController-
// RevisionFromHash only reads it (Get short-circuit, bytes.Equal, and
// Data.Raw assignment on Create), never mutates it.
func (r *Reconciler) revisionHash(ir *v1beta1.InferenceReplica, input workload.ReconcileInput, collisionCount *int32, scopeUID types.UID) (string, []byte, error) {
	cc := utils.DerefInt32(collisionCount)
	cacheKey := client.ObjectKeyFromObject(ir)

	r.revisionHashMu.Lock()
	defer r.revisionHashMu.Unlock()
	if entry, ok := r.revisionHashCache[cacheKey]; ok &&
		entry.uid == ir.UID &&
		entry.generation == ir.Generation &&
		entry.collisionCount == cc &&
		entry.scopeUID == scopeUID {
		return entry.hash, entry.raw, nil
	}

	hash, raw, err := revision.HashWithWorkerAndTopology(
		input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec,
		input.DesiredSpec.PodTemplateObjectMeta,
		input.DesiredSpec.TopologyKey,
		collisionCount, scopeUID,
	)
	if err != nil {
		return "", nil, err
	}
	if r.revisionHashCache == nil {
		r.revisionHashCache = make(map[types.NamespacedName]revisionHashEntry)
	}
	r.revisionHashCache[cacheKey] = revisionHashEntry{
		uid:            ir.UID,
		generation:     ir.Generation,
		collisionCount: cc,
		scopeUID:       scopeUID,
		hash:           hash,
		raw:            raw,
	}
	return hash, raw, nil
}

// forgetRevisionHash evicts the memoized revision hash for a deleted IR so
// the cache doesn't retain entries for IRs that no longer exist.
func (r *Reconciler) forgetRevisionHash(key types.NamespacedName) {
	r.revisionHashMu.Lock()
	defer r.revisionHashMu.Unlock()
	delete(r.revisionHashCache, key)
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
