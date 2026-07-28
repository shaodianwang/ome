package inferencereplica

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/obsmetrics"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/utils"
)

// InferenceReplicaConditionReady is the top-level condition type written
// onto IR.Status.Conditions. Operators waiting on the IR directly
// (`kubectl wait --for=condition=Ready irep/<name>`), the irprojector
// that mirrors IR conditions onto the parent ISVC, and future autoscaler
// scalers that read the IR all key off this condition.
const InferenceReplicaConditionReady = "Ready"

// Reason strings for the InferenceReplicaConditionReady condition. Kept
// short and machine-readable so
// `kubectl get irep -o jsonpath='...conditions[?(@.type=="Ready")].reason'`
// is grep-friendly. Mirrors the omenative status-conditions vocabulary
// for cross-controller consistency.
const (
	// ReasonAllInstancesReady is stamped when every Instance is Ready AND
	// CurrentRevision == UpdateRevision (rollout fully converged).
	ReasonAllInstancesReady = "AllInstancesReady"
	// ReasonRolloutInProgress is stamped when CurrentRevision !=
	// UpdateRevision — explicit "churning, check back" signal. Maps to
	// Status=Unknown so `kubectl wait --for=condition=Ready` doesn't
	// short-circuit while a rollout is mid-flight.
	ReasonRolloutInProgress = "RolloutInProgress"
	// ReasonStaged is stamped when the IR has converged to a static
	// rollingUpdate.partition — (replicas-partition) Instances on the target
	// revision and `partition` held on the prior one, all Ready. Ready=True:
	// a partitioned rollout is intentionally complete-and-holding, not
	// "in progress". Distinct from AllInstancesReady (fully rolled). (#838)
	ReasonStaged = "Staged"
	// ReasonInstanceFailed is stamped when at least one Instance has
	// terminally failed (stuck-pod escalation, InstanceReadyTimeout).
	// Maps to Status=False so operators paging on Ready=False catch the
	// failure immediately.
	ReasonInstanceFailed = "InstanceFailed"
	// ReasonReplicaCountMismatch is stamped when the serving Instance count is
	// below the Component's availability floor (its MaxUnavailable budget) with
	// no in-flight rollout — genuinely stuck below the minimum without revision
	// churn to blame. Maps to Status=False.
	ReasonReplicaCountMismatch = "ReplicaCountMismatch"
	// ReasonMinimumAvailable is stamped when ReadyReplicas < Replicas but the
	// serving Instance count is still at or above the availability floor —
	// Ready=True (serving within budget), distinct from AllInstancesReady so
	// "fully converged" stays distinguishable from "serving, still rolling".
	ReasonMinimumAvailable = "MinimumAvailable"
	// ReasonNoReplicas is stamped when Replicas == 0 (no desired
	// Instances). Distinct from ReplicaCountMismatch so operators can
	// tell "scaled-to-zero" from "something stuck".
	ReasonNoReplicas = "NoReplicas"
)

// aggregateAndWriteStatus computes the per-IR aggregate counters
// (Replicas / ReadyReplicas / ServingReplicas / AvailableReplicas /
// UpdatedReplicas / UpdatedReadyReplicas), stamps ObservedGeneration +
// LabelSelector, stamps UpdateRevision / CurrentRevision off the
// target ControllerRevision, and (on full convergence) promotes
// UpdateRevision → CurrentRevision.
//
// Per-Instance pod counters (PodCount / ReadyPodCount /
// ServingPodCount / AvailablePodCount / ScheduledPodCount /
// NodesOccupied) are recomputed here from the live pod set, mirroring
// the omenative direct path's AggregateAndWriteStatus. The workload
// per-op writers (Create's patchInstanceStatusReady, Update's
// promotion, etc.) own the lifecycle fields (Phase, Operation,
// Incarnation, RunningRevision, TargetRevision); this aggregator owns
// the observability fields (per-pod counters + Component-level
// rollups). Without populating the per-Instance pod counters, the
// downstream workload.CountReadyInstances / CountUpdatedInstances
// would read zero ReadyPodCount everywhere — the round-2 recreate-
// path bug where ReadyReplicas stuck at 0 even after the new pod
// flipped containers-ready.
//
// Reuses workload.ComputeComponentCounters / workload.RolloutComplete
// directly — same shape, same semantics as the ISVC adapter, so the
// dual-write window can byte-compare the two status surfaces.
//
// target may be nil when DesiredSpec.PodSpec is nil (no rendered
// template — webhook-rejected today; defensive nil-guard kept). When
// target is nil revision pointers are left untouched; counters still
// run.
func (r *Reconciler) aggregateAndWriteStatus(ctx context.Context, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, plan workload.ComponentPlan, target *appsv1.ControllerRevision, stuckPodGrace time.Duration, retryPolicy *workload.RetryPolicy, autoMigrateBudget int32) error {
	if r.Client == nil {
		return fmt.Errorf("aggregateAndWriteStatus: nil client")
	}
	key := client.ObjectKeyFromObject(ir)
	desiredByIdx := workload.DesiredPodCountByInstance(plan)
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)

	// List pods + EndpointSlices via the CACHED client (r.Client), outside
	// the retry closure: both reads are idempotent (no apiserver-side
	// mutation), so on a status conflict we just re-stamp the same counters
	// onto the re-read IR. Mirrors the omenative direct path which reads pods
	// + availability once at the top of AggregateAndWriteStatus.
	//
	// This aggregator is observability-only COUNTING — the manager cache is
	// scoped to OME pods, has the OMENative Pod field index registered, and
	// watches EndpointSlices, so cache-served counts are both correct and
	// cheap. Going live (r.APIReader) on every reconcile was the #1 hot-path
	// cost: an uncached LIST per IR reconcile purely for counters, plus the
	// doomed MatchingFields probe the live reader can't serve. r.APIReader
	// stays reserved for the destructive/correctness-critical paths
	// (drain/delete/recreate confirm) that genuinely need a live read.
	pods, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.Spec.ParentRef.Name, component, true)
	if err != nil {
		return fmt.Errorf("aggregateAndWriteStatus: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, component)
	availableByPod, err := workload.AvailablePodSet(ctx, r.Client, ir.Namespace, serviceName)
	if err != nil {
		return fmt.Errorf("aggregateAndWriteStatus: compute availability: %w", err)
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.Client.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// IR deleted under us; nothing to aggregate into.
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: re-read IR: %w", err)
		}

		// Snapshot the live status before recomputation so a no-op
		// reconcile (computed == live) skips the Status().Update below.
		// Every status write re-triggers this controller AND the
		// irprojector rollup onto the parent ISVC, so at steady state the
		// unconditional write amplified idle reconciles into a write
		// storm. apimeta.SetStatusCondition preserves LastTransitionTime
		// when the condition value is unchanged, so an unchanged status
		// DeepEquals its prior snapshot.
		before := fresh.Status.DeepCopy()

		// Per-Instance pod counters from the live pod buckets. Same
		// shape the omenative direct path uses (status_aggregate.go
		// lines 83-93). Without this step the downstream
		// workload.CountReadyInstances reads zero ReadyPodCount on every
		// Instance — the recreate-path bug where the new pod is alive
		// and ContainersReady but ReadyReplicas stuck at 0 because the
		// IR aggregator never observed the pod's live state.
		populateInstancePodCounters(fresh.Status.InstanceStatuses, byIndex, availableByPod)

		insts := v1beta1convert.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses)

		// Component-level counters from per-Instance state (now with
		// fresh per-pod counters populated above).
		var targetName string
		if target != nil {
			targetName = target.Name
		}
		counters := workload.ComputeComponentCounters(insts, desiredByIdx, targetName)
		fresh.Status.Replicas = counters.Replicas
		fresh.Status.ServingReplicas = counters.ServingReplicas
		// ReadyReplicas + AvailableReplicas use the workload counters
		// directly now that per-Instance pod counters are populated
		// from live pods above. The surge-tolerant classifier
		// (InstanceMeetsThreshold) means an Instance with the canonical
		// pod ContainersReady is counted even mid-surge or mid-drain,
		// matching the omenative direct path byte-for-byte. The prior
		// Phase-only fallback (countReadyInstancesByPhase) under-
		// counted during recreate/SurgeThenDrain transitions: Phase
		// stays Updating until the workload pipeline stamps Ready, but
		// the live pod can be serving traffic in the interim — and the
		// IR's Ready rollup must reflect serving truth, not the
		// lifecycle bookkeeping phase.
		//
		// AvailableReplicas mirrors ReadyReplicas at v1 — no
		// minReadySeconds gating field exists on InferenceReplicaSpec
		// or LifecycleSpec today. When one is added, AvailableReplicas
		// would lag ReadyReplicas by that grace period.
		fresh.Status.ReadyReplicas = counters.ReadyReplicas
		fresh.Status.AvailableReplicas = counters.AvailableReplicas
		if target != nil {
			fresh.Status.UpdatedReplicas = counters.UpdatedReplicas
			fresh.Status.UpdatedReadyReplicas = counters.UpdatedReadyReplicas
			fresh.Status.UpdateRevision = target.Name
			if workload.RolloutComplete(insts, target.Name) {
				fresh.Status.CurrentRevision = target.Name
			}
		}

		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.LabelSelector = irLabelSelectorString(fresh.Spec.ParentRef.Name, fresh.Spec.Component)

		// Top-level Ready condition. Two-axis rule:
		//   Status=True    when ReadyReplicas == Replicas AND
		//                  CurrentRevision == UpdateRevision (fully
		//                  converged, no rollout in flight).
		//   Status=False   when any Instance has Phase=Failed OR the
		//                  serving Instance count is below the availability
		//                  floor (MaxUnavailable budget) with no in-flight
		//                  rollout (genuinely stuck below the minimum).
		//   Status=Unknown when CurrentRevision != UpdateRevision —
		//                  explicit "churning, check back" so
		//                  `kubectl wait --for=condition=Ready` doesn't
		//                  short-circuit mid-rollout.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeReadyCondition(&fresh.Status, fresh.Spec.Pacing))

		// Skip the write when the recomputed status matches what's
		// already live: a no-op reconcile must perform ZERO writes so it
		// triggers nothing downstream. The in-memory mirror below still
		// runs so callers in this pass observe the fresh counters.
		if equality.Semantic.DeepEqual(*before, fresh.Status) {
			mirrorBack(ir, fresh)
			return nil
		}

		if err := r.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: update IR status: %w", err)
		}

		// Mirror the just-committed Component-level fields, per-Instance
		// counters, and Conditions onto the caller's in-memory IR snapshot
		// so downstream callers in the same reconcile pass observe the
		// post-write state.
		mirrorBack(ir, fresh)
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultSuccess)
		return nil
	}); err != nil {
		// Exhausted RetryOnConflict still surfaces a Conflict error
		// (wrapped via %w above); bucket it apart from other write
		// failures so the 409 churn hotspot is visible per controller.
		if apierrors.IsConflict(err) {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultConflict)
		} else {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultError)
		}
		return err
	}

	// Fast-failure escalation: any Instance currently in a transient
	// phase (Creating / Updating / Restarting / Migrating) whose pods
	// have been stuck in a non-recoverable kubelet waiting state
	// (ImagePullBackOff / ErrImagePull / InvalidImageName /
	// CreateContainerError / CrashLoopBackOff / RunContainerError) past
	// the stuck-pod grace window gets escalated to Phase=Failed
	// immediately, without waiting for the (default 30 min)
	// per-Instance Operation deadline.
	//
	// Mirrors omenative/status_aggregate.go::escalateStuckPodFailures
	// byte-for-byte at the contract level (same workload-side helper,
	// same EscalationParams shape, same per-Instance Phase=Failed write
	// via input.MutateInstance). Without this hook the IR-managed path
	// observes the same bad-image symptom the omenative direct path
	// fixed: the surge pod sits in ImagePullBackOff for 30+ minutes
	// while the Instance stays at Phase=Updating, and the parent ISVC's
	// coordination state machine never sees Component.Failed → group
	// Phase stays at Surging indefinitely. Closes the bad_image_kind
	// reproduction reported against the dev cluster after IR-managed
	// became the default OMENative dispatch path.
	// Template specs feed the disposition's required-affinity guard: a
	// relocation exclusion the template can never satisfy (required In
	// pin on the suspect node) disposes terminal instead of recording.
	templates := desiredFromIR(ir)
	dd := workload.DispositionDeps{
		AutoMigrateMaxAttempts: autoMigrateBudget,
		MigrationMode:          plan.MigrationMode,
		PodSpec:                templates.PodSpec,
		WorkerPodSpec:          templates.WorkerPodSpec,
		// Metrics parity with the legacy detector: count each recorded
		// relocation directive on the auto-migration counter, keyed by
		// the user-facing parent ISVC.
		OnRelocationDirective: func(component string) {
			coordination.RecordAutoMigrationTriggered(ir.Namespace, ir.Spec.ParentRef.Name, component, audit.ReasonAutoRecover)
		},
	}
	if !plan.Paused {
		if err := r.escalateStuckPodFailures(ctx, ir, parent, byIndex, stuckPodGrace, desiredByIdx, dd, retryPolicy); err != nil {
			return err
		}
	}

	// Broad backstop: any Instance still in a transient phase whose
	// in-flight Operation.Deadline (StartedAt + InstanceReadyTimeout,
	// default 30 min) has elapsed gets escalated to Phase=Failed. This is
	// the consumer of the InstanceReadyTimeout field the per-op writers
	// stamp onto Operation.Deadline — without it the timeout is inert and
	// a gang that never reaches Ready for a reason the fast-path escalator
	// doesn't catch (perpetually Pending, run-then-crash loop) sits at its
	// transient phase forever. The two backstops converge on Phase=Failed
	// and don't fight: whichever fires first wins, the other no-ops on the
	// already-Failed Instance.
	//
	// First, park the InstanceReadyTimeout clock for any Instance whose
	// pods are held by a scheduling gate (queued for admission, e.g. by
	// Kueue): a gated workload is queued, not stuck, so the gated wait must
	// not count against the deadline. Runs BEFORE the expiry backstop so the
	// backstop reads the parked (zeroed) deadline and skips it; once the pods
	// clear the gate the clock restarts from admission.
	if err := r.reconcileHeldDeadlines(ctx, ir, parent, byIndex, plan.Paused, plan.InstanceReadyTimeout); err != nil {
		return err
	}
	if plan.Paused {
		return nil
	}
	return r.expireOperationsIfDeadlinePassed(ctx, ir, parent, byIndex, desiredByIdx, dd, retryPolicy)
}

// mirrorBack copies the just-computed Component-level fields and
// per-Instance pod counters from `fresh` onto the caller's in-memory IR
// snapshot so downstream callers in the same reconcile pass observe the
// post-compute state — regardless of whether a Status().Update was
// actually issued. Matches the mirror pattern in mirrorInstanceStatus /
// mirrorComponentRevisionFields on the ISVC side. The lifecycle fields
// (Phase, Operation, Incarnation, RunningRevision, TargetRevision) stay
// owned by the workload per-op writers and are left untouched.
func mirrorBack(ir, fresh *v1beta1.InferenceReplica) {
	ir.Status.Replicas = fresh.Status.Replicas
	ir.Status.ReadyReplicas = fresh.Status.ReadyReplicas
	ir.Status.ServingReplicas = fresh.Status.ServingReplicas
	ir.Status.AvailableReplicas = fresh.Status.AvailableReplicas
	ir.Status.UpdatedReplicas = fresh.Status.UpdatedReplicas
	ir.Status.UpdatedReadyReplicas = fresh.Status.UpdatedReadyReplicas
	ir.Status.UpdateRevision = fresh.Status.UpdateRevision
	ir.Status.CurrentRevision = fresh.Status.CurrentRevision
	ir.Status.ObservedGeneration = fresh.Status.ObservedGeneration
	ir.Status.LabelSelector = fresh.Status.LabelSelector
	// Mirror the per-Instance pod counters too so downstream callers
	// reading ir.Status.InstanceStatuses observe the fresh per-pod
	// state, not the stale snapshot from when the reconcile started.
	// Per-pod counter freshness flows through the irprojector rollup
	// onto ISVC.status.components[X].omenative.instanceStatuses, so
	// `kubectl describe isvc` shows the same per-Instance numbers
	// the IR controller just observed.
	mirrorInstanceCounters(&ir.Status, fresh.Status.InstanceStatuses)
	// Conditions: deep-copy the just-computed slice so callers reading
	// IR.Status.Conditions in the same reconcile pass observe the
	// post-compute set without sharing a backing array.
	if len(fresh.Status.Conditions) > 0 {
		ir.Status.Conditions = make([]metav1.Condition, len(fresh.Status.Conditions))
		copy(ir.Status.Conditions, fresh.Status.Conditions)
	} else {
		ir.Status.Conditions = nil
	}
}

// escalateStuckPodFailures is the IR-side adapter that bridges the
// IR aggregator body to the workload-side EscalateStuckPodFailures.
// Builds a minimal workload.ReconcileInput just for the escalation
// step (MutateInstance + WarnInstanceFailed) and delegates the policy
// logic to the workload package.
//
// Mirrors omenative/status_aggregate.go::escalateStuckPodFailures:
// reads `byIndex` (per-Instance live pod buckets the caller already
// computed for counters) so no re-list happens, and walks the
// caller's in-memory IR snapshot AFTER the retry loop has committed
// so the InstanceStatus.Phase observed here is the post-counters
// version.
//
// EventTarget defers to the parent ISVC when available so user-facing
// events stay coherent under `kubectl describe isvc`, matching the
// EventTarget rule in buildReconcileInput.
func (r *Reconciler) escalateStuckPodFailures(ctx context.Context, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, byIndex map[int32][]*corev1.Pod, stuckPodGrace time.Duration, desiredByIdx map[int32]int32, dd workload.DispositionDeps, retryPolicy *workload.RetryPolicy) error {
	if ir == nil {
		return nil
	}
	return workload.EscalateStuckPodFailures(ctx, r.escalationDeps(), r.buildEscalationInput(ir, parent, retryPolicy), workload.EscalationParams{
		Instances:           v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
		PodsByIndex:         byIndex,
		CurrentRevisionHash: query.RevisionFromName(ir.Status.CurrentRevision).Hash(),
		RevisionHashLabel:   query.LabelRevisionHash,
		StuckPodGrace:       stuckPodGrace,
		DesiredPodsByIndex:  desiredByIdx,
		Disposition:         dd,
	})
}

// expireOperationsIfDeadlinePassed is the IR-side adapter that bridges
// the IR aggregator tail to the workload-side
// ExpireOperationsIfDeadlinePassed deadline backstop. Shares the
// escalation input + params shape with the stuck-pod escalator (the
// deadline disposition needs the same pod buckets, desired counts, and
// RetryBlock/ledger capabilities) and walks the caller's post-counters
// IR snapshot.
//
// This is the consumer of the per-Instance Operation.Deadline the
// workload per-op writers stamp from InstanceReadyTimeout; runs right
// after escalateStuckPodFailures so the fast-path escalation has first
// crack and this only fails Instances the fast path didn't already catch.
func (r *Reconciler) expireOperationsIfDeadlinePassed(ctx context.Context, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, byIndex map[int32][]*corev1.Pod, desiredByIdx map[int32]int32, dd workload.DispositionDeps, retryPolicy *workload.RetryPolicy) error {
	if ir == nil {
		return nil
	}
	return workload.ExpireOperationsIfDeadlinePassed(ctx, r.escalationDeps(), r.buildEscalationInput(ir, parent, retryPolicy), workload.EscalationParams{
		Instances:          v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
		PodsByIndex:        byIndex,
		DesiredPodsByIndex: desiredByIdx,
		Disposition:        dd,
	})
}

// escalationDeps is the manager-scoped Deps handed to the workload-side
// escalators — same wiring the dispatch path builds in Reconcile, minus
// the render hooks the escalators never invoke. The disposition uses
// Reader()/Client for the migration audit ledger and Recorder for the
// auto-migration event.
func (r *Reconciler) escalationDeps() workload.Deps {
	return workload.Deps{
		Client:       r.Client,
		APIReader:    r.APIReader,
		Recorder:     r.Recorder,
		Expectations: r.Expectations,
		Clock:        r.Clock,
	}
}

// reconcileHeldDeadlines parks/restarts InstanceReadyTimeout around either
// admission gating or an operator pause. Pausing holds every in-flight
// Instance; unpausing rearms each parked deadline from that point unless its
// pods remain admission-gated.
func (r *Reconciler) reconcileHeldDeadlines(ctx context.Context, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, byIndex map[int32][]*corev1.Pod, paused bool, timeout time.Duration) error {
	if ir == nil {
		return nil
	}
	held := make(map[int32]bool, len(byIndex))
	for idx, pods := range byIndex {
		for _, p := range pods {
			if workload.PodAdmissionGated(p) {
				held[idx] = true
				break
			}
		}
	}
	if paused {
		for i := range ir.Status.InstanceStatuses {
			held[ir.Status.InstanceStatuses[i].Index] = true
		}
	}
	return workload.ReconcileGatedDeadlines(ctx, r.buildEscalationInput(ir, parent, nil),
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses), held, timeout)
}

// buildEscalationInput builds the workload.ReconcileInput the two
// terminal-failure backstops (EscalateStuckPodFailures,
// ExpireOperationsIfDeadlinePassed) share. Beyond MutateInstance +
// WarnInstanceFailed, the deadline disposition needs the RetryBlock
// write capability (MutateRetryBlock + UpdateRetryPolicy + WarnRetryHeld),
// the identity Key (ledger entries are keyed on Component), the observed
// state (UpdateRevision fallback for Create-op dispositions), and the
// ledger owner (parent ISVC when resolvable, else the IR — matching
// buildReconcileInput). The RemoveInstance / WriteAggregateCondition
// stubs satisfy the panic-on-nil contract. Reuses
// buildMutateInstance so the escalation observes the same retry +
// in-memory-mirror semantics dispatch-time writes get.
//
// EventTarget defers to the parent ISVC when available so user-facing
// events stay coherent under `kubectl describe isvc`, matching the
// EventTarget rule in buildReconcileInput. Taking the typed
// *InferenceService is deliberate (typed-nil hazard — see
// buildReconcileInput).
func (r *Reconciler) buildEscalationInput(ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, retryPolicy *workload.RetryPolicy) workload.ReconcileInput {
	eventTarget := client.Object(ir)
	if parent != nil {
		eventTarget = parent
	}
	input := workload.ReconcileInput{
		OwnerObject:             ir,
		OwnerGVK:                irGVK,
		EventTarget:             eventTarget,
		Key:                     buildKey(ir),
		ObservedState:           observedFromIR(ir),
		Clock:                   r.Clock,
		MutateInstance:          buildMutateInstance(r.Client, ir),
		MutateRetryBlock:        buildMutateRetryBlock(r.Client, ir),
		UpdateRetryPolicy:       retryPolicy,
		RemoveInstance:          func(_ context.Context, _ int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(_ context.Context, _ metav1.Condition) error { return nil },
		WarnRetryHeld:           r.warnRetryHeldFunc(ir, eventTarget),
		// podName is empty for the deadline backstop (no single pod to
		// blame); reason carries the cause either way (kubelet
		// waiting-state reason for the stuck-pod path, "DeadlineExceeded:
		// <op>/<step> ..." for the deadline path).
		WarnInstanceFailed: r.warnInstanceFailedFunc(ir, eventTarget, instanceFailedEscalationMsg),
	}
	// The migration audit ledger lives on the user-facing parent ISVC
	// when resolvable (same rule as buildReconcileInput); nil parent
	// falls back to the IR via the workload-side owner resolution.
	if parent != nil {
		input.LedgerOwner = parent
		input.LedgerOwnerGVK = isvcGVK
	}
	return input
}

// mirrorInstanceCounters copies the per-Instance pod counter fields
// (PodCount / ReadyPodCount / ServingPodCount / ScheduledPodCount /
// AvailablePodCount / NodesOccupied) from the just-committed
// InstanceStatuses onto the caller's in-memory IR. Matched on Index
// so a parallel mutator that inserted or removed an Instance between
// the Get and Update doesn't end up writing counters onto the wrong
// slot. The lifecycle fields (Phase, Operation, Incarnation,
// RunningRevision, TargetRevision, ActiveOrdinal) are owned by the
// workload per-op writers, not the aggregator — leaving them
// untouched on the mirror avoids clobbering an in-flight per-op
// write.
func mirrorInstanceCounters(status *v1beta1.InferenceReplicaStatus, fresh []v1beta1.OMENativeInstanceStatus) {
	if status == nil {
		return
	}
	freshByIdx := make(map[int32]v1beta1.OMENativeInstanceStatus, len(fresh))
	for _, s := range fresh {
		freshByIdx[s.Index] = s
	}
	for i := range status.InstanceStatuses {
		s := &status.InstanceStatuses[i]
		f, ok := freshByIdx[s.Index]
		if !ok {
			continue
		}
		s.PodCount = f.PodCount
		s.ReadyPodCount = f.ReadyPodCount
		s.ServingPodCount = f.ServingPodCount
		s.ScheduledPodCount = f.ScheduledPodCount
		s.AvailablePodCount = f.AvailablePodCount
		if f.NodesOccupied != nil {
			s.NodesOccupied = append([]string(nil), f.NodesOccupied...)
		} else {
			s.NodesOccupied = nil
		}
	}
}

// populateInstancePodCounters refreshes the per-Instance observability
// fields (PodCount / ReadyPodCount / ServingPodCount /
// ScheduledPodCount / AvailablePodCount / NodesOccupied) from the
// live pod set bucketed by Instance index. The per-pod counters feed
// the workload.CountReadyInstances / CountUpdatedInstances /
// CountAvailableInstances classifiers — without populating them the
// downstream rollups read zero on every Instance even when the
// underlying pods are alive and ContainersReady.
//
// Mirrors the omenative direct path's per-Instance counter loop
// (omenative/status_aggregate.go:83-93) byte-for-byte. The IR adapter
// uses workload.Count* helpers directly via workload.CountReadyPods
// / CountServingPods / etc., which is the same primitive set the
// direct path's local wrappers delegate to — so the two adapters
// produce byte-identical per-Instance counters off the same pod set.
//
// availableByPod is the AvailablePodSet (pod names appearing as Ready
// non-terminating endpoints in the headless Service's EndpointSlices).
// Empty map is OK — every Instance's AvailablePodCount drops to 0,
// which is the correct semantic when EndpointSlices haven't yet caught
// up with a fresh pod's Ready transition.
//
// Instance entries with no matching pods get all-zero counters —
// that's the correct semantic for an Instance whose pods haven't been
// created yet (Pending phase) or have all been deleted (post-drain,
// pre-recreate Phase B).
func populateInstancePodCounters(insts []v1beta1.OMENativeInstanceStatus, byIndex map[int32][]*corev1.Pod, availableByPod map[string]struct{}) {
	for i := range insts {
		s := &insts[i]
		instPods := byIndex[s.Index]
		s.PodCount = int32(len(instPods))
		s.ReadyPodCount = workload.CountReadyPods(instPods)
		s.ServingPodCount = workload.CountServingPods(instPods)
		s.ScheduledPodCount = workload.CountScheduledPods(instPods)
		s.AvailablePodCount = workload.CountAvailablePods(instPods, availableByPod)
		s.NodesOccupied = workload.UniqueNodes(instPods)
		s.Admitted = len(instPods) > 0 && !anyAdmissionGated(instPods)
	}
}

// anyAdmissionGated reports whether any pod still carries an admission
// scheduling gate (Kueue has not admitted the workload yet).
func anyAdmissionGated(pods []*corev1.Pod) bool {
	for _, p := range pods {
		if workload.PodAdmissionGated(p) {
			return true
		}
	}
	return false
}

// hasFailedInstance reports whether at least one Instance has been
// escalated to Phase=Failed (stuck-pod escalation, InstanceReadyTimeout).
// Used by computeReadyCondition to flip Ready=False when the rollout is
// permanently wedged.
func hasFailedInstance(insts []v1beta1.OMENativeInstanceStatus) bool {
	for _, s := range insts {
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			return true
		}
	}
	return false
}

// computeReadyCondition derives the Ready condition for the IR from
// already-rolled-up status counters + per-Instance phases. Caller passes
// a pre-aggregated IR.Status snapshot (Replicas, ReadyReplicas,
// CurrentRevision, UpdateRevision, InstanceStatuses must all reflect the
// post-counter state). Reason precedence:
//
//  1. InstanceFailed       — any Instance Phase=Failed (overrides all)
//  2. RolloutInProgress    — CurrentRevision != UpdateRevision (Unknown)
//  3. NoReplicas           — Replicas == 0 (False)
//  4. AllInstancesReady    — ReadyReplicas == Replicas (True)
//  5. ReplicaCountMismatch — ReadyReplicas < Replicas, no rollout (False)
//
// Returns a metav1.Condition with ObservedGeneration stamped off
// status.ObservedGeneration so consumers can correlate. The caller is
// responsible for invoking apimeta.SetStatusCondition to merge the
// returned condition into status.Conditions (which handles
// LastTransitionTime correctly).
// effectivePartition returns the IR's static rollingUpdate.partition, or 0
// when unset.
func effectivePartition(pacing *v1beta1.InferenceReplicaPacing) int32 {
	if pacing == nil || pacing.Partition == nil {
		return 0
	}
	return *pacing.Partition
}

// stagedAtPartition reports whether the IR has converged to a static,
// non-zero partition: (replicas-partition) Instances Ready on the target
// revision and `partition` Instances held Ready on the prior one. Zero
// partition (full rollout) is not staged — it converges via the normal
// promotion path. (#838)
func stagedAtPartition(status *v1beta1.InferenceReplicaStatus, pacing *v1beta1.InferenceReplicaPacing) bool {
	part := effectivePartition(pacing)
	if part <= 0 {
		return false
	}
	return workload.ReachedDesiredShape(
		v1beta1convert.InstanceStatusSliceToWorkload(status.InstanceStatuses),
		status.UpdateRevision, part, status.Replicas)
}

func computeReadyCondition(status *v1beta1.InferenceReplicaStatus, pacing *v1beta1.InferenceReplicaPacing) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionReady,
		ObservedGeneration: status.ObservedGeneration,
	}
	switch {
	case hasFailedInstance(status.InstanceStatuses):
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonInstanceFailed
		cond.Message = "At least one Instance has Phase=Failed"
	case stagedAtPartition(status, pacing):
		// Converged to a static partition: intentionally holding old-revision
		// Instances, all Ready. Ready=True (not RolloutInProgress/Unknown) —
		// the rollout is complete for the configured partition. (#838)
		part := effectivePartition(pacing)
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonStaged
		cond.Message = fmt.Sprintf("Staged at partition %d: %d/%d Instances on %s, %d held on the prior revision",
			part, status.Replicas-part, status.Replicas, status.UpdateRevision, part)
	case status.UpdateRevision != "" && status.CurrentRevision != status.UpdateRevision:
		cond.Status = metav1.ConditionUnknown
		cond.Reason = ReasonRolloutInProgress
		cond.Message = fmt.Sprintf("Rolling out %s (%d/%d Instances on target)",
			status.UpdateRevision, status.UpdatedReadyReplicas, status.Replicas)
	case status.Replicas == 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonNoReplicas
		cond.Message = "InferenceReplica has no desired Instances"
	case status.ReadyReplicas == status.Replicas:
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonAllInstancesReady
		cond.Message = fmt.Sprintf("%d/%d Instances Ready", status.ReadyReplicas, status.Replicas)
	default:
		// Not fully Ready and no rollout in flight. Readiness is
		// availability-based: Ready=True while the serving Instance count is at
		// or above the disruption floor derived from the rollout MaxUnavailable
		// budget (the only budget projected onto the IR — the PDB MinAvailable/
		// MaxUnavailable live on the ISVC). A drop below the floor is genuinely
		// stuck. Keeps the IR's Ready consistent with the ISVC's EngineReady.
		var fallback *intstr.IntOrString
		if pacing != nil {
			fallback = pacing.MaxUnavailable
		}
		floor := utils.AvailabilityFloor(status.Replicas, nil, nil, fallback)
		if status.ServingReplicas >= floor {
			cond.Status = metav1.ConditionTrue
			cond.Reason = ReasonMinimumAvailable
			cond.Message = fmt.Sprintf("%d/%d Instances serving (min %d)",
				status.ServingReplicas, status.Replicas, floor)
		} else {
			cond.Status = metav1.ConditionFalse
			cond.Reason = ReasonReplicaCountMismatch
			cond.Message = fmt.Sprintf("%d/%d Instances serving, need %d (no rollout in flight)",
				status.ServingReplicas, status.Replicas, floor)
		}
	}
	return cond
}

// irLabelSelectorString returns the canonical "k=v,k=v" selector
// string the HPA scale subresource consumes via
// IR.status.labelSelector. Formula matches the legacy ISVC-side
// componentLabelSelectorString byte-for-byte so existing HPAs
// continue to resolve unchanged after a workload migrates from
// ISVC-direct to IR-driven.
func irLabelSelectorString(isvc string, component v1beta1.ComponentType) string {
	return labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}).String()
}
