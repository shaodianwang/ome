// Package omenative implements the OMENative deployment-mode backend for
// InferenceService — direct pod management with native Kubernetes primitives
// instead of LeaderWorkerSet. It plugs into the existing
// InferenceServiceReconciler as a new dispatch case in
// components/{engine,decoder,router}.go; it is not a parallel controller.
package omenative

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadgang "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/gang"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// Carrier types defined in core/ are re-exported here so the rest of
// the omenative package (and its callers) only import the umbrella
// package. The aliases are package-API declarations, not wrappers —
// they share the underlying type with core/ at compile time.
type (
	ReconcileParams  = core.ReconcileParams
	ComponentPlan    = core.ComponentPlan
	InstancePlan     = core.InstancePlan
	RunnerPlan       = core.RunnerPlan
	MigrationOverlay = core.MigrationOverlay
	Expectations     = core.Expectations
)

// BuildPlan / NewExpectations are the constructors paired with the
// carrier types above. Re-exported as `var` so callers see a single
// symbol in the omenative package. The DefaultExpectations *singleton*
// is intentionally not re-exported — a `var` re-export would copy the
// pointer at init time, and any caller that reassigns the local copy
// (e.g. test fixtures) would silently desync from the core singleton
// used by core/params.go's ExpectationsCache fallback. Callers that
// need the singleton import core directly.
var (
	BuildPlan       = core.BuildPlan
	NewExpectations = core.NewExpectations
)

// derefInt32 is a small helper for event messages where we want to
// print the value of an *int32 without an "<nil>" surprise.
func derefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// ReconcileComponent is the Loop A entry point for OMENative-managed
// Components. The InferenceService controller calls it from the
// `case constants.OMENative` branch of each Component's
// reconcileDeployment switch.
//
// Loop:
//  1. Build the desired ComponentPlan from ReconcileParams.
//  2. Scale down: drive Delete for any InstanceStatus index that the
//     plan no longer asks for. Scale-down completes before scale-up
//     to avoid running excess pods alongside in-flight drains.
//  3. Per-Instance restart: detect pod loss / pod-Failed triggers and
//     dispatch Restart when the Component's restart policy allows it.
//     Held to one pass per reconcile to keep the state machine simple.
//  4. Per-Instance update: compute the target ControllerRevision from
//     the rendered PodSpec and drive Update on any Instance whose pods
//     don't yet match. Mode (in-place vs recreate) is resolved per
//     Instance from the UpdateStrategy and the observed diff.
//  5. Scale up: drive Create toward every desired Instance. The
//     operation is multi-pass — pods are created, ome.io/serving is
//     flipped once runtime-ready, and Phase=Ready is recorded.
//
// At the tail of every reconcile pass the function runs
// AggregateAndWriteStatus to refresh replica counters,
// per-Instance counters, revision pointers, and CurrentRevision
// promotion. That write runs regardless of whether the op chain
// returned a requeue or an error.
func ReconcileComponent(ctx context.Context, p ReconcileParams) (result ctrl.Result, err error) {
	if p.Client == nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: nil client (component=%s)", p.Component)
	}
	plan, perr := BuildPlan(p)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: build plan (component=%s): %w", p.Component, perr)
	}

	// Reconcile the per-Component headless Service after the plan
	// validates the Component is supported but before any per-Instance
	// op runs. Every drain check downstream (drain.IsPodDrained /
	// drain.IsPodInRotation) reads the EndpointSlices owned by this Service;
	// without it, drain would fail-loud rather than silently complete.
	//
	// The per-Component stable Service + PodMonitor used to be reconciled
	// here too. Both moved out of the OMENative dispatch — they live on
	// the per-Component Reconcile() in
	// components/{engine,decoder,router}.go's
	// reconcileOMENativeSubresources method so they fire for BOTH OMENative
	// sub-paths (direct and IR-managed). The omenative package now manages
	// only pods + the per-Component headless Service (whose naming is tied
	// to the per-Component pod template renderer); every other top-level
	// sub-resource is owned by the ISVC controller.
	if err := ensureHeadlessService(ctx, p); err != nil {
		return ctrl.Result{}, err
	}

	// PodGroups must exist BEFORE the first pod in any multi-pod Instance
	// is created — otherwise the scheduler-plugins coscheduler may place
	// pods individually before the gang is announced. Also stamps the
	// GangSchedulingUnavailable Component condition: True when multi-pod
	// Instances are requested but the CRD is not installed, False
	// otherwise. No-op for single-pod Components.
	//
	// Bridges into workload/gang via internalsource.BuildReconcileInput;
	// the legacy v1beta1-typed ensurePodGroups helper that previously
	// lived in omenative/gang.go is gone after the workload extraction
	// (part 2 slice 4g of N).
	gangDeps := workload.Deps{
		Client:       p.Client,
		APIReader:    p.APIReader,
		Recorder:     p.Recorder,
		Expectations: p.ExpectationsCache(),
		RenderHook:   core.ISVCRenderHook(p.ISVC),
	}
	gangInput := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:                    p.ISVC,
		Component:               p.Component,
		ObjectMeta:              p.ObjectMeta,
		PodSpec:                 p.PodSpec,
		WorkerPodSpec:           p.WorkerPodSpec,
		Client:                  p.Client,
		Expectations:            p.ExpectationsCache(),
		GangSchedulingAvailable: p.GangSchedulingAvailable,
	})
	if err := workloadgang.EnsurePodGroups(ctx, gangDeps, gangInput, plan); err != nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: ensure pod groups (component=%s): %w", plan.Component, err)
	}

	// target is computed early so the tail status write can stamp
	// UpdateRevision / CurrentRevision even if the op chain returned
	// without reaching the update branch.
	var target *appsv1.ControllerRevision
	if p.PodSpec != nil {
		t, rerr := ensureRevisionWithCollisionRetry(ctx, p, v1beta1convert.ComponentTypeFromWorkload(plan.Component))
		if rerr != nil {
			return ctrl.Result{}, fmt.Errorf("OMENative reconciler: ensure revision (component=%s): %w", plan.Component, rerr)
		}
		target = t
	}

	defer func() {
		if serr := AggregateAndWriteStatus(ctx, p, plan, target); serr != nil {
			// Don't clobber a real op error with a status-write error,
			// but surface it if everything else succeeded.
			if err == nil {
				err = fmt.Errorf("OMENative reconciler: write status: %w", serr)
			}
		}
	}()

	result, err = ops.ReconcileComponentOps(ctx, p, plan, target)
	return
}

// ensureRevisionWithCollisionRetry computes the target ControllerRevision
// for the Component's current PodSpec, honoring the collision contract
// from revision.go: if EnsureControllerRevision reports a hash collision
// (same revision name, different Data), bump
// LifecycleStatus.CollisionCount, persist the status, and retry
// the revision creation with the new salt. The retry yields a different
// hash and lands.
//
// Without this retry, ReconcileComponent would silently drive pods
// toward the conflicting CR's Data, which is not the spec the user
// wrote — exactly the bug the collision return value is designed to
// prevent.
func ensureRevisionWithCollisionRetry(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType) (*appsv1.ControllerRevision, error) {
	var collisionCount *int32
	if cs, ok := p.ISVC.Status.Components[component]; ok && cs.Lifecycle != nil {
		collisionCount = cs.Lifecycle.CollisionCount
	}
	// Multi-pod Components fold the worker template into the revision
	// hash so a worker-only image / env bump triggers a new revision
	// and a rollout. Single-pod Components pass nil, which produces the
	// same hash + bytes as the pre-multi-node Hash form, so existing
	// single-pod revisions stay valid.
	workerSpec := p.WorkerPodSpec
	if !p.MultiPod {
		workerSpec = nil
	}
	target, collision, err := core.EnsureControllerRevisionForISVCWithWorker(ctx, p.Client, p.Reader(), p.ISVC, component, p.PodSpec, workerSpec, &p.ObjectMeta, collisionCount)
	if err != nil {
		return nil, err
	}
	if !collision {
		return target, nil
	}
	// Bump CollisionCount on the Component status, then retry.
	bumped, err := bumpCollisionCount(ctx, p, component)
	if err != nil {
		return nil, fmt.Errorf("bump CollisionCount after revision collision: %w", err)
	}
	status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonRevisionCollision,
		"OMENative component=%s ControllerRevision hash collision; CollisionCount bumped to %d",
		component, derefInt32(bumped))
	target, collision, err = core.EnsureControllerRevisionForISVCWithWorker(ctx, p.Client, p.Reader(), p.ISVC, component, p.PodSpec, workerSpec, &p.ObjectMeta, bumped)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("revision still collides after CollisionCount bump (count=%d)", *bumped)
	}
	return target, nil
}

// bumpCollisionCount increments LifecycleStatus.CollisionCount
// (initializing to 1 when nil) and persists the status. Returns the
// new pointer for the retry caller.
//
// Wrapped in retry.RetryOnConflict so concurrent component reconciles
// can't lose the bump. Each retry re-reads the current count and
// increments from there — collision counting is monotonic, so
// incrementing whatever the apiserver currently shows is the right
// semantic.
func bumpCollisionCount(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType) (*int32, error) {
	var next int32
	isvcGone := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// ISVC deleted during the reconcile — there's no
				// CollisionCount left to bump. Drop the work cleanly so
				// controller-runtime doesn't retry the whole reconcile.
				isvcGone = true
				return nil
			}
			return fmt.Errorf("re-read ISVC for CollisionCount bump: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		cs := fresh.Status.Components[component]
		if cs.Lifecycle == nil {
			cs.Lifecycle = &v1beta1.LifecycleStatus{}
		}
		next = int32(1)
		if cs.Lifecycle.CollisionCount != nil {
			next = *cs.Lifecycle.CollisionCount + 1
		}
		cs.Lifecycle.CollisionCount = &next
		fresh.Status.Components[component] = cs
		if err := p.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				isvcGone = true
				return nil
			}
			return fmt.Errorf("persist CollisionCount: %w", err)
		}
		return nil
	})
	if isvcGone {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Mirror the final landed value onto the cached ISVC the caller
	// is reading. Done outside the retry because it only needs to
	// reflect the value that successfully committed.
	if p.ISVC.Status.Components == nil {
		p.ISVC.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	mirror := p.ISVC.Status.Components[component]
	if mirror.Lifecycle == nil {
		mirror.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	mirror.Lifecycle.CollisionCount = &next
	p.ISVC.Status.Components[component] = mirror
	return &next, nil
}
