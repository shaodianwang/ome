// teardown.go owns the finalizer-driven IR teardown path. An IR with a
// DeletionTimestamp reconciles as "desired instances = zero": the
// ordinary Delete pipeline tears every Instance down (drain → graceful
// delete → stuck-Terminating force-delete escalation → audit) and the
// teardown finalizer lifts only when no component pod remains — so
// teardown carries the same guarantees as scale-down instead of
// un-instrumented background GC.
//
// Pods keep their IR owner references throughout: if the controller
// dies mid-teardown, background GC still collects everything the moment
// the finalizer is removed or force-purged. GC racing a Delete op is
// benign — fewer live pods is progress by the completion definition.
package inferencereplica

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// TeardownFinalizer gates IR deletion on the reconciled teardown path:
// added on every reconcile while DeletionTimestamp is nil, removed once
// no component pod remains (or the configured lifecycle.teardown
// deadline passes). An already-Terminating IR without it — pre-upgrade,
// or hand-stripped — is left entirely to background GC.
const TeardownFinalizer = "ome.io/ir-teardown"

// Teardown event reasons, emitted on the IR itself (the parent ISVC is
// typically already deleted by the time teardown runs).
const (
	// ReasonTeardownBlocked warns that pods survive with no teardown
	// deadline configured (strict hold). Emitted every teardown pass
	// with a stable message shape — the EventRecorder aggregates
	// identical (reason, message) pairs into one Event with a bumped
	// count, so the steady state is one Event, not spam.
	ReasonTeardownBlocked = "TeardownBlocked"
	// ReasonTeardownDeadlineExceeded warns that the configured
	// lifecycle.teardown.deadline elapsed with pods surviving; the
	// finalizer is released and background GC owns the remainder —
	// exactly the pre-finalizer behavior.
	ReasonTeardownDeadlineExceeded = "TeardownDeadlineExceeded"
)

// teardownEscapeHint documents the operator's universal unwedge in
// every teardown Warning: stripping the finalizer by hand always works.
const teardownEscapeHint = "manual escape: kubectl patch inferencereplica %s -n %s --type=merge -p '{\"metadata\":{\"finalizers\":null}}'"

// reconcileTeardown drives one Terminating IR toward gone:
//
//  1. Finalizer absent → no-op; background GC owns cleanup. This IS
//     the upgrade story for IRs already Terminating before the
//     finalizer existed (and the hand-stripped escape hatch).
//  2. Dispatch workload.Reconcile in Teardown mode: every observed
//     Instance runs the ordinary Delete pipeline, including the
//     force-delete escalation when lifecycle.forceDelete is configured
//     — the designed unwedger for pods stuck on dead nodes.
//  3. Completion = no live component pods (live read; a pod whose
//     InstanceStatus entry was lost must still block). Empty → delete
//     the IR-owned headless Service → remove the finalizer.
//  4. Survivors past the configured lifecycle.teardown.deadline →
//     Warning + release the finalizer to background GC (degrade to
//     exactly the pre-finalizer behavior). No deadline configured →
//     strict hold with a per-pass aggregated Warning.
//
// The parent ISVC is typically ALREADY deleted here (background GC
// deletes the ISVC first): the input builder tolerates parent=nil (the
// audit ledger re-parents to the IR, events fall back to the IR) and
// the normal path's deferred status aggregation is deliberately not
// run — see the comment at the dispatch site.
func (r *Reconciler) reconcileTeardown(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(ir, TeardownFinalizer) {
		log.V(1).Info("Terminating without teardown finalizer; background GC owns cleanup")
		return ctrl.Result{}, nil
	}

	// Best-effort parent resolve: NotFound → nil, everything below
	// tolerates it. Live reader, not the cache: a stale cached NotFound
	// on a live parent would aim the dangling-ledger close at the empty
	// IR-owned ledger and leave the parent-owned entry dangling. Resolve
	// lifecycle config as usual — the force-delete escalation must keep
	// working during teardown.
	parent := r.resolveParentFrom(ctx, r.reader(), ir)
	forceDeletePolicy := r.resolveForceDeletePolicy(log)
	deadline, deadlineInvalidReason := r.resolveTeardownDeadline(log)

	// Desired-spec content is irrelevant under Teardown (the planned
	// index set is treated as empty); the input/plan only need to be
	// well-formed so Delete can read Key/Component/InstanceReadyTimeout.
	// The same-target update retry policy is nil: the Update pass never
	// runs in teardown, and neither does the escalation pass (zero grace
	// / zero relocation budget — wedge escalation belongs to the Delete
	// pipeline's lifecycle.forceDelete).
	input := r.buildReconcileInput(ctx, ir, parent, nil, forceDeletePolicy, 0, 0)
	input.Teardown = true

	plan, perr := workload.BuildPlan(v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), input.DesiredSpec, input.ObservedState)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: build plan (ir=%s/%s): %w", ir.Namespace, ir.Name, perr)
	}

	// The normal path's deferred aggregateAndWriteStatus is NOT
	// installed here: its counter/condition writes die with the IR. The
	// workload escalation pass does not run under Teardown either — the
	// stuck-pod / InstanceReadyTimeout backstops assume a converging
	// rollout, and firing them against Phase=Deleting bookkeeping would
	// fight the Delete pipeline that already owns wedge escalation (via
	// lifecycle.forceDelete). Instance statuses still drain through
	// RemoveInstance as each Delete completes.
	deps := workload.Deps{
		Client:       r.Client,
		APIReader:    r.APIReader,
		Recorder:     r.Recorder,
		Expectations: r.Expectations,
		Clock:        r.Clock,
	}
	result, err := workload.Reconcile(ctx, deps, input, plan, nil)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Completion check: LIVE list of every component pod. Live because a
	// stale cache must not release the finalizer while the apiserver
	// still holds pods; component-wide (not per observed Instance)
	// because a statusless orphan pod must still block completion.
	pods, lerr := query.LiveListPodsForComponent(ctx, r.reader(), ir.Namespace, ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	if lerr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: live-list component pods: %w", lerr)
	}
	if len(pods) == 0 {
		if derr := r.deleteHeadlessService(ctx, ir); derr != nil {
			return ctrl.Result{}, derr
		}
		r.closeDanglingLedgerEntries(ctx, log, ir, parent)
		if ferr := r.removeTeardownFinalizer(ctx, ir); ferr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: remove finalizer: %w", ferr)
		}
		log.Info("Teardown complete; finalizer removed")
		return ctrl.Result{}, nil
	}

	survivors := sortedPodNames(pods)
	if deadline != nil && input.Now().After(ir.DeletionTimestamp.Add(*deadline)) {
		r.warnTeardown(ir, ReasonTeardownDeadlineExceeded, fmt.Sprintf(
			"teardown deadline %s exceeded with %d pod(s) still present (%s); releasing finalizer %s to background GC; "+teardownEscapeHint,
			deadline, len(survivors), strings.Join(survivors, ", "), TeardownFinalizer, ir.Name, ir.Namespace))
		r.closeDanglingLedgerEntries(ctx, log, ir, parent)
		if ferr := r.removeTeardownFinalizer(ctx, ir); ferr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: remove finalizer past deadline: %w", ferr)
		}
		return ctrl.Result{}, nil
	}
	if deadline == nil {
		// Strict hold. The message is stable per (survivor set, config
		// state) — sorted names, no timestamps — so the recorder
		// aggregates repeats into one Event with a count. Two variants:
		// deadline genuinely unconfigured vs configured-but-invalid; an
		// operator who wrote a bad duration must not be told nothing is
		// configured.
		detail := "no lifecycle.teardown.deadline configured"
		if deadlineInvalidReason != "" {
			detail = "lifecycle.teardown.deadline configured but invalid: " + deadlineInvalidReason
		}
		r.warnTeardown(ir, ReasonTeardownBlocked, fmt.Sprintf(
			"teardown blocked: %d pod(s) still present (%s); finalizer %s holds until they are gone (%s); "+teardownEscapeHint,
			len(survivors), strings.Join(survivors, ", "), TeardownFinalizer, detail, ir.Name, ir.Namespace))
	}
	if result.IsZero() {
		// Pods survive with no Delete in flight (e.g. a statusless
		// orphan no Instance owns): keep polling at the Delete cadence
		// so external cleanup or config changes are picked up.
		result = ctrl.Result{RequeueAfter: workloadops.DeleteRequeueInterval}
	}
	return result, nil
}

// closeDanglingLedgerEntries marks every in-flight (Phase=Started)
// migration ledger entry for this component terminal
// (Phase=Failed, Outcome=owner-torn-down) before the finalizer lifts.
// A migration whose instances are deleted mid-flight by teardown can
// never complete, and when the ledger owner is the parent ISVC and the
// parent outlives the IR (operator deletes just the IR; the projector
// recreates a fresh one), a Started entry left behind would resume as
// a phantom migration on the fresh IR and keep consuming the in-flight
// capacity cap. Closed rows are records, not work orders.
//
// The ledger ConfigMap is shared across every component of the parent
// ISVC, and persist is a whole-blob overwrite — so the close runs under
// retry.RetryOnConflict with each attempt re-loading the ledger through
// the live reader and re-applying only this component's close. A peer
// component's concurrent write (e.g. a fresh Started entry from its
// Migrate machinery while the parent lives on) conflicts our Update
// instead of being clobbered, and the retry rebases on top of it.
//
// Best-effort by design (never-worse rule): ledger load/persist
// failures — including exhausted conflict retries — log at V(1) and
// return; audit bookkeeping must never block finalizer removal. Owner
// resolution mirrors reconcileRelocationDirectives' rule: parent ISVC
// when resolvable, else the IR (buildReconcileInput sets LedgerOwner
// only when the parent resolves, with no IR fallback).
func (r *Reconciler) closeDanglingLedgerEntries(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService) {
	owner := client.Object(ir)
	gvk := irGVK
	if parent != nil {
		owner = parent
		gvk = isvcGVK
	}
	component := string(ir.Spec.Component)
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ledger, err := audit.LoadLedgerForOwner(ctx, r.reader(), owner)
		if err != nil {
			return err
		}
		closed := false
		for i := range ledger.Entries {
			e := &ledger.Entries[i]
			if e.Phase != audit.PhaseStarted || e.Component != component {
				continue
			}
			*e = audit.NewTerminalEntry(*e, audit.PhaseFailed, audit.OutcomeOwnerTornDown)
			closed = true
		}
		if !closed {
			return nil
		}
		return audit.PersistLedgerForOwner(ctx, r.Client, owner, gvk, ledger)
	})
	if err != nil {
		log.V(1).Info("teardown: dangling-ledger close failed; proceeding with finalizer removal", "error", err.Error())
	}
}

// deleteHeadlessService removes the IR-owned per-Component headless
// Service — the one object the ensure path (buildHeadlessServiceSpec +
// workload.ReconcileHeadlessService) maintains. Delete-by-name is the
// live check; NotFound is success. Per-revision routed Services are
// ISVC-controller-owned and not touched here.
func (r *Reconciler) deleteHeadlessService(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	spec := buildHeadlessServiceSpec(ir)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: spec.Namespace}}
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("InferenceReplica teardown: delete headless service %s/%s: %w", spec.Namespace, spec.Name, err)
	}
	return nil
}

// removeTeardownFinalizer lifts the teardown finalizer against a fresh
// read under RetryOnConflict. NotFound at any step means the IR is
// already gone — success.
func (r *Reconciler) removeTeardownFinalizer(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	key := client.ObjectKeyFromObject(ir)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controllerutil.ContainsFinalizer(fresh, TeardownFinalizer) {
			return nil
		}
		controllerutil.RemoveFinalizer(fresh, TeardownFinalizer)
		if err := r.Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		return nil
	})
}

// warnTeardown emits a teardown Warning on the IR itself — the parent
// ISVC is typically already deleted — mirrored to the controller log so
// log-aggregated views see it without the events stream.
func (r *Reconciler) warnTeardown(ir *v1beta1.InferenceReplica, reason, message string) {
	r.Log.Info("IR teardown warning",
		"inferencereplica", client.ObjectKeyFromObject(ir),
		"reason", reason,
		"message", message)
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(ir, corev1.EventTypeWarning, reason, message)
}

// sortedPodNames returns the pods' names sorted, so survivor lists in
// event messages are byte-stable for recorder aggregation.
func sortedPodNames(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names
}
