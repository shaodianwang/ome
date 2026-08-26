// Package acceleratorquota reconciles the AcceleratorQuota tree's observed
// state. It assembles every node on the cluster, re-runs the invariant checks
// the admission webhook runs, and writes each node's position and condition.
//
// It materializes nothing. Admission is fast feedback but not a serialization
// point — two children admitted concurrently can each pass containment and
// together bust their parent, and a parent can be deleted out from under a
// child — so the tree has to be re-checked by something that sees it whole,
// after the fact. That re-check is this controller's whole job for now; the
// Kueue materializer and the management-plane projector build on the same
// assembled tree.
package acceleratorquota

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/quota/backend"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

// Mode selects which half of the quota plane a manager runs. It is a manager
// flag rather than config because it decides which controllers exist.
type Mode string

const (
	// ModeWorkload renders the local tree into Kueue on a serving cluster. It is
	// the only mode that writes Kueue objects, and only on its own cluster.
	ModeWorkload Mode = "workload"

	// ModeManagement holds the authored fleet tree and projects per-cluster
	// shares onto members. It writes no Kueue object anywhere.
	ModeManagement Mode = "management"
)

// Deliberately no +kubebuilder:rbac markers here, unlike every sibling in this
// directory. controller-gen scans ./pkg/controller/... into the ome-manager
// ClusterRole, but this controller runs in ome-quota-manager, a separate binary
// with its own ServiceAccount. A marker added here would silently hand
// ome-manager the quota plane's permissions — today AcceleratorQuota, later
// cluster-wide write on Kueue's ClusterQueue and Cohort — and that write grant
// belongs only to the process that renders Kueue, not to the one also running
// the InferenceService reconcilers and the fail-closed pod mutating webhook.
//
// The grants live in charts/ome-quota-manager/templates/rbac.yaml, and
// charts/ome-resources/tests/render_test.sh fails if they ever appear in the
// ome-manager role.

// Reconciler keeps every AcceleratorQuota's status in step with the tree they
// collectively describe.
//
// Reconciliation is whole-tree, not per-object: one node's edit can change
// another node's verdict — re-parenting moves a subtree's paths, and a budget
// change can bust a parent that was fine a moment ago — so every pass rebuilds
// the tree and reconciles every node's status. A fleet is a handful of nodes,
// so this costs one LIST.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// APIReader is an uncached reader. The tree verdict must not be computed
	// from a stale cache: a node admitted a moment ago but not yet in the
	// informer cache would be invisible, and this controller would then clear a
	// Degraded condition that is still true.
	APIReader client.Reader

	// Options carry the operator-configured root name and depth bound. The
	// package invents neither.
	Options tree.Options

	// ResyncInterval re-runs the checks on an otherwise-idle plane, so a
	// violation only a clock can clear is noticed without an edit. Zero leaves
	// reconciles purely event-driven.
	ResyncInterval time.Duration

	// Capacity configures deriving this cluster's own accelerator capacity onto
	// the reserved root. Unset leaves the controller reporting tree position
	// only, which is what a management-mode manager wants: it holds the
	// authored fleet tree and has no local silicon to measure.
	Capacity CapacityOptions

	// Materialize configures rendering the tree into an enforcement backend.
	// Unset leaves the tree observed but unenforced, which is both the
	// management-mode shape and the way a workload cluster runs before an
	// operator opts into writing Kueue objects.
	Materialize MaterializeOptions
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("acceleratorquota", req.Name)

	var list v1beta1.AcceleratorQuotaList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing AcceleratorQuotas: %w", err)
	}

	// A node being deleted is not part of the tree it is leaving. Excluding it
	// keeps this pass from re-rendering the objects the finalizer is about to
	// reap, which would read as a failed reap.
	live, deleting := partitionByDeletion(list.Items)

	built, violations, err := tree.Build(live, r.Options)
	if err != nil {
		// A configuration failure, not a property of the nodes. Surfacing it as
		// an error requeues with backoff and logs, rather than silently leaving
		// every node's status untouched as if there were nothing to do.
		return ctrl.Result{}, fmt.Errorf("assembling the quota tree: %w", err)
	}

	// Capacity is cluster state, so the pure tree checks cannot see it. The
	// verdict joins them here rather than in a path of its own, so a budget the
	// fleet cannot afford freezes and reports exactly like any other violated
	// invariant. The numbers come from the root's last observed capacity: this
	// pass has not derived new ones yet, and a value one pass old is the right
	// input for a check whose whole purpose is to ignore momentary dips.
	if r.Capacity.Enabled() && built.Root != nil {
		violations = append(violations,
			capacityViolations(built.Nodes(), built.Root.Quota.Status.Capacity)...)
	}

	frozen := built.Frozen(violations)
	var errs []error

	if r.Materialize.Enabled() {
		// Claim every live node before writing anything, so a delete that
		// arrives mid-pass still finds a finalizer to hold it.
		for i := range live {
			if err := r.ensureFinalizer(ctx, &live[i]); err != nil {
				errs = append(errs, fmt.Errorf("node %s: %w", live[i].Name, err))
			}
		}
		// Reap before applying. A departing node and a live one can name the
		// same object transiently, and reaping second would delete what this
		// pass just wrote.
		keep := sets.New[string]()
		for _, node := range built.Nodes() {
			keep.Insert(node.Name())
		}
		for i := range deleting {
			if err := r.finalize(ctx, &deleting[i], keep); err != nil {
				errs = append(errs, fmt.Errorf("node %s: %w", deleting[i].Name, err))
			}
		}
	}

	outcomes := map[string]backend.NodeOutcome{}
	if r.Materialize.Enabled() && len(errs) == 0 {
		outcomes, err = r.materialize(ctx, built, frozen)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// Observational, so a backend that cannot be read costs the fleet its usage
	// figures for this pass and nothing else. Failing here would stop the
	// conditions and tree positions below from being written too, which is the
	// half an operator actually acts on.
	reading, err := r.rollUsage(ctx, built)
	if err != nil {
		log.V(1).Info("consumption not read this pass", "reason", err.Error())
	}

	for _, node := range built.Nodes() {
		if err := r.reconcileStatus(ctx, node, frozen, outcomes[node.Name()], reading); err != nil {
			errs = append(errs, fmt.Errorf("node %s: %w", node.Name(), err))
		}
	}
	if len(errs) > 0 {
		// One node's conflicted status write must not skip the rest, so failures
		// are collected and the whole pass retried.
		return ctrl.Result{}, fmt.Errorf("writing quota status: %w", errors.Join(errs...))
	}

	if len(violations) > 0 {
		log.V(1).Info("quota tree has violations", "count", len(violations), "nodes", violations.Nodes())
	}

	// Capacity describes the cluster as a whole, so it needs the node that means
	// "the whole cluster". Create it rather than wait: a cluster reports what it
	// has whether or not anyone has authored a budget yet.
	if r.Capacity.Enabled() {
		if built.Root == nil {
			created, err := r.ensureRoot(ctx)
			if err != nil {
				if !webhookUnavailable(err) {
					return ctrl.Result{}, err
				}
				// The startup pass makes the same create and already treats this
				// as a wait; surfacing it here as well would answer a condition
				// that clears on its own with a stack trace per attempt.
				log.V(1).Info("waiting to create the reserved root",
					"root", r.Options.RootName, "reason", err.Error())
				return ctrl.Result{RequeueAfter: rootBootstrapRetry}, nil
			}
			if !created {
				// The name is taken by a node that is not the tree root: it
				// carries a parentRef, or sits in a cycle. Creating cannot fix
				// that and nor can retrying, so without saying so capacity
				// derivation would sit idle forever with no error, no event and
				// no condition to explain it.
				log.Info("capacity derivation is idle: the reserved root name is held by a node that is not the tree root",
					"root", r.Options.RootName)
			}
			// Keep the resync. The create's own watch event normally brings us
			// straight back, but in the idle case above there is no such event
			// and dropping the interval would disable the only thing that would
			// notice the tree being repaired.
			return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
		}
		if err := r.reconcileCapacity(ctx, built.Root.Quota); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
}

// reconcileStatus writes one node's observed position and condition. It patches
// only when something changed, so an idle fleet does not rewrite status on every
// resync tick and spin the watch.
func (r *Reconciler) reconcileStatus(ctx context.Context, node *tree.Node, frozen map[string]tree.Violation,
	outcome backend.NodeOutcome, reading usageReading) error {
	current := node.Quota
	updated := current.DeepCopy()

	updated.Status.ObservedGeneration = current.Generation
	updated.Status.Path = node.Path
	updated.Status.Parent = ""
	if node.Parent != nil {
		updated.Status.Parent = node.Parent.Name()
	}
	// Only rewritten when consumption was actually read. A pass that could not
	// reach the backend leaves the previous figures standing rather than
	// blanking them: "unknown" and "nothing held" are different answers, and
	// only one of them is safe to publish on a failed read.
	if totals, ok := reading.forNode(node.Name()); ok {
		updated.Status.Budgets = budgetStatus(node, totals)
	}

	violation, isFrozen := frozen[node.Name()]
	setConditions(&updated.Status, violation, isFrozen)
	if r.Materialize.Enabled() {
		setMaterializationStatus(&updated.Status, current.Generation, outcome, isFrozen, metav1.Now())
	}

	if equalStatus(current.Status, updated.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFrom(current)); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// The node was deleted or rewritten mid-pass; the next reconcile
			// sees the newer state. Neither is worth failing the pass over.
			return nil
		}
		return err
	}
	return nil
}

// setConditions stamps Ready and Degraded as a mutually exclusive pair, so a
// reader never has to reconcile two conditions that disagree.
//
// A node frozen by an ancestor reports the ancestor's cause rather than a
// generic message: the operator has to fix the ancestor, and saying so is the
// difference between a two-minute fix and a hunt.
func setConditions(status *v1beta1.AcceleratorQuotaStatus, violation tree.Violation, isFrozen bool) {
	now := metav1.Now()
	ready := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "node is a valid member of the quota tree",
		LastTransitionTime: now,
		ObservedGeneration: status.ObservedGeneration,
	}
	degraded := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaDegraded,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "no invariant violated",
		LastTransitionTime: now,
		ObservedGeneration: status.ObservedGeneration,
	}

	if isFrozen {
		ready.Status = metav1.ConditionFalse
		ready.Reason = violation.Reason
		ready.Message = violation.Message
		degraded.Status = metav1.ConditionTrue
		degraded.Reason = violation.Reason
		degraded.Message = violation.Message
	}

	setCondition(status, ready)
	setCondition(status, degraded)
}

// setCondition is the one place conditions are written, so every condition this
// controller owns gets the same treatment.
func setCondition(status *v1beta1.AcceleratorQuotaStatus, condition metav1.Condition) {
	apimeta.SetStatusCondition(&status.Conditions, condition)
}

// equalStatus compares the fields this controller owns. Condition timestamps are
// excluded deliberately: SetStatusCondition only moves LastTransitionTime when
// the status actually flips, so comparing the rest is what makes a no-op pass a
// genuine no-op.
func equalStatus(a, b v1beta1.AcceleratorQuotaStatus) bool {
	if a.ObservedGeneration != b.ObservedGeneration || a.Path != b.Path || a.Parent != b.Parent {
		return false
	}
	if !equalMaterialization(a.Materialization, b.Materialization) {
		return false
	}
	if !equalBudgets(a.Budgets, b.Budgets) {
		return false
	}
	for _, condType := range []string{
		v1beta1.AcceleratorQuotaReady,
		v1beta1.AcceleratorQuotaDegraded,
		v1beta1.AcceleratorQuotaMaterialized,
	} {
		ca := apimeta.FindStatusCondition(a.Conditions, condType)
		cb := apimeta.FindStatusCondition(b.Conditions, condType)
		if ca == nil || cb == nil {
			if ca != cb {
				return false
			}
			continue
		}
		if ca.Status != cb.Status || ca.Reason != cb.Reason || ca.Message != cb.Message ||
			ca.ObservedGeneration != cb.ObservedGeneration {
			return false
		}
	}
	return true
}

// SetupWithManager wires the reconciler to watch AcceleratorQuotas.
//
// Every event enqueues the same key, because the unit of work is the tree, not
// the object: editing one node can change another's verdict, and a per-object
// queue would leave the second node reporting a stale condition until it
// happened to be touched. Collapsing to one key also means a burst of edits
// coalesces into a single rebuild.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.APIReader == nil {
		return fmt.Errorf("acceleratorquota: APIReader is required (the tree verdict must not be read from a cache)")
	}
	if r.Options.RootName == "" {
		return fmt.Errorf("acceleratorquota: %w", tree.ErrRootNameUnset)
	}
	b := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.AcceleratorQuota{}).
		Named("acceleratorquota")

	if r.Capacity.Enabled() {
		// Reconciles are event-driven, and a cluster can start with no
		// AcceleratorQuota and no Nodes — nothing to fire an event, so nothing
		// would ever create the root and capacity would never be reported. One
		// pass at startup removes that dependency on the cluster happening to
		// contain something.
		//
		// A plain RunnableFunc is leader-elected, so a standby replica does not
		// race the leader; AlreadyExists is benign in any case. The pass retries
		// internally and never returns an error, because the write goes through
		// this component's own fail-closed webhook and the manager it would take
		// down is the one that brings that webhook up.
		if err := mgr.Add(manager.RunnableFunc(r.bootstrapRoot)); err != nil {
			return fmt.Errorf("acceleratorquota: scheduling the root check: %w", err)
		}

		// Every node maps to the same request: reconciliation is whole-tree, so
		// which node changed does not narrow the work.
		//
		// The predicate is not an optimisation to revisit later. Node status is
		// among the highest-volume writes in a cluster and almost none of it
		// moves a number this controller reads, so without it a large fleet
		// would rebuild the tree continuously.
		b = b.WatchesRawSource(source.Kind(
			mgr.GetCache(),
			&corev1.Node{},
			handler.TypedEnqueueRequestsFromMapFunc(func(context.Context, *corev1.Node) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: r.Options.RootName}}}
			}),
			predicate.TypedFuncs[*corev1.Node]{
				UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
					return nodeCapacityChanged(e.ObjectOld, e.ObjectNew, r.Capacity.Resources)
				},
			},
		))
	}

	// Consumption moves without anything in the tree changing, and nothing else
	// here would notice: the reconcile is woken by AcceleratorQuota edits and by
	// node capacity, neither of which a workload admission touches. Left to the
	// resync tick alone, a queue's figures are up to a full interval stale — long
	// enough that a fleet with work running reads as an idle one.
	//
	// A backend that cannot say what to watch simply does not get watched, and
	// its figures refresh on the resync tick as before.
	//
	// Every event maps to one request, as the node watch does, so a burst of
	// admissions collapses into a single queued pass rather than one per
	// workload.
	if watcher, ok := r.Materialize.Backend.(backend.UsageWatcher); ok {
		obj, usageChanged := watcher.WatchUsage()
		b = b.Watches(obj,
			handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: r.Options.RootName}}}
			}),
			builder.WithPredicates(usageChanged),
		)
	}
	return b.Complete(r)
}
