package ops

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/obsmetrics"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// CreateRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up.
const CreateRequeueInterval = 5 * time.Second

// Create drives Create / Scale-Up toward each desired Instance.
// Multi-pass:
//
//  1. Render missing pods with ome.io/serving=False; patch
//     InstanceStatus to Phase=Creating with the Create operation
//     stamped.
//  2. Wait for runtime Ready on every pod, flip ome.io/serving=True,
//     promote InstanceStatus to Phase=Ready (Operation=nil).
//
// When target != nil the Ready promote records
// RunningRevision=target.Name so detectUpdateTrigger's fast-path can
// short-circuit. Without it, per-pod podMatchesTarget false-positives
// against post-Render pod mutations and triggers a spurious recreate.
func Create(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	return createFiltered(ctx, deps, input, plan, target, func(int32) bool { return true })
}

// CreateFreshIndices runs the Create pass for ONLY surge-free Instance
// indices — those with no in-flight surge state the Update pass owns. It
// lets a scale-up of brand-new gangs proceed even while a rollout is
// mid-flight on another index, instead of starving scale-out behind the
// rollout (a 20-40 min cold-compile on TPU).
//
// A surge-free index is immune to both corruption modes the dispatcher's
// skip-Create-while-updating gate guards against: its ActiveOrdinal is a
// genuine 0 (never existed, not a stale snapshot of a flipped ordinal),
// and it has no RunningRevision to mis-stamp — its pods are created
// carrying the target rev-hash, so existingPodsMatchTargetRevision
// promotes correctly. See the surgeFreeIndex predicate.
//
// No-op (returns ctrl.Result{}, nil) when no index qualifies, so pure
// rollouts are unaffected.
func CreateFreshIndices(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	observed := input.ObservedState.InstanceStatuses
	eligible := false
	for _, inst := range plan.Instances {
		if surgeFreeIndex(observed, inst.Index) {
			eligible = true
			break
		}
	}
	if !eligible {
		return ctrl.Result{}, nil
	}
	return createFiltered(ctx, deps, input, plan, target, func(idx int32) bool {
		return surgeFreeIndex(observed, idx)
	})
}

// surgeFreeIndex reports whether the Instance at idx has NO in-flight
// surge state — i.e. its ObservedState entry is absent (never created)
// or still Phase=Creating. Updating / Migrating carry an in-flight surge
// (stale ActiveOrdinal / RunningRevision hazard); Ready-but-degraded is
// Restart/Recreate territory. Such indices are excluded.
func surgeFreeIndex(observed []workload.InstanceStatus, idx int32) bool {
	s := findInstanceStatus(observed, idx)
	return s == nil || s.Phase == workload.InstancePhaseCreating
}

// createFiltered is Create restricted to the Instance indices for which
// keep returns true. Create delegates with an always-true keep;
// CreateFreshIndices delegates with the surge-free predicate. Indices
// that keep rejects are left entirely untouched this pass (not listed,
// not promoted, not duplicated).
func createFiltered(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, keep func(idx int32) bool) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("Create: nil client")
	}

	existing, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, true)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Create: list pods: %w", err)
	}
	byInstance := query.BucketPodsByInstanceIdx(existing)

	allReady := true
	// retryBlockWait is the earliest not-yet-due Backoff RetryBlock
	// wake-up reported by a gated instance; folded into the pass result
	// below so the gate is re-evaluated exactly on time (never a busy
	// poll, never a missed wake-up).
	var retryBlockWait time.Duration
	for _, inst := range plan.Instances {
		if !keep(inst.Index) {
			continue
		}
		ready, retryAfter, err := reconcileInstance(ctx, deps, input, plan, inst, byInstance[inst.Index], target)
		if err != nil {
			return ctrl.Result{}, err
		}
		if retryAfter > 0 && (retryBlockWait == 0 || retryAfter < retryBlockWait) {
			retryBlockWait = retryAfter
		}
		if !ready {
			allReady = false
		}
	}

	res := ctrl.Result{}
	if !allReady {
		res.RequeueAfter = CreateRequeueInterval
	}
	// Additive fold, mirroring the dispatcher's foldRetryAfter: take the
	// earlier wake-up, never remove one.
	if retryBlockWait > 0 && (res.RequeueAfter == 0 || retryBlockWait < res.RequeueAfter) {
		res.RequeueAfter = retryBlockWait
	}
	return res, nil
}

// reconcileInstance drives one Instance toward Ready. Returns ready=true
// only when every pod is runtime-ready and ome.io/serving=True. target has
// the same nil-OK semantics as Create. retryAfter > 0 means the instance
// was skipped by a not-yet-due Backoff RetryBlock for the target
// revision; re-evaluate then.
func reconcileInstance(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, existing []*corev1.Pod, target *appsv1.ControllerRevision) (ready bool, retryAfter time.Duration, err error) {
	desired := expectedPodNamesForInstance(input, plan, inst)
	existingByName := query.IndexPodsByName(existing)

	missing := make([]podTarget, 0)
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}

	// Only patch Creating when there's something to create — otherwise the
	// next pass would oscillate status back to Creating after Ready.
	if len(missing) > 0 {
		// Disposed fresh-start gate. A deadline-disposed create leaves the
		// instance Failed-with-no-Operation — a fresh start — so without
		// this gate the pass would re-materialize pods at the same bad
		// revision every reconcile, bypassing the RetryBlock the
		// disposition recorded. Gate on the block for the revision these
		// pods would stamp (target). An absent status slot gates too: a
		// scale-up onto a blocked revision materializes the same wedged
		// pods. Instances with no block for the target — genuinely-new
		// scale-ups — pass untouched. Denial is a skip, not un-readiness:
		// Held has no wake-up (a corrected revision re-triggers via watch)
		// and Backoff surfaces its NextRetryAt to the caller.
		if target != nil {
			s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
			freshStart := s == nil || (s.Phase == workload.InstancePhaseFailed && s.Operation == nil)
			if freshStart {
				if b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name); b != nil {
					attemptInFlight := anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, target.Name) ||
						anyInFlightCreateAttempt(input.ObservedState.InstanceStatuses)
					if denied, wait := evaluateRetryBlockGate(b, input.Now(), attemptInFlight); denied {
						return true, wait, nil
					}
				}
			}
		}
		// RecreateInstance owns partial-Instance recovery. When an Instance
		// that already reached Ready loses a pod (some present, some missing),
		// recreating only the missing pod here races the Restart pass — which
		// drains and recreates the WHOLE Instance (the gang) per the policy
		// contract. When this self-heal wins that race it also stamps
		// Phase=Creating, which latches the outcome by making
		// DetectRestartTrigger skip on every later pass — so RecreateInstance
		// silently degrades to "recreate just the dead pod" (the leader and
		// surviving workers keep their incarnation). Defer to Restart for an
		// already-Ready Instance; fresh/empty Instances (initial create +
		// scale-up) and partial Instances under other restart policies remain
		// Create's responsibility.
		if plan.RestartPolicy == workload.RestartPolicyRecreateInstance && len(existing) > 0 {
			if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseReady {
				return false, 0, nil
			}
		}
		// Capture before patch — findInstanceStatus reads ObservedState,
		// so the patch below doesn't backfill this view.
		isFirstCreate := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index) == nil
		if err := patchInstanceStatusCreating(ctx, input, inst.Index, inst.Incarnation, plan.InstanceReadyTimeout, revisionNameFromTarget(target)); err != nil {
			return false, 0, fmt.Errorf("patch status Creating (instance=%d): %w", inst.Index, err)
		}
		if isFirstCreate {
			recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				instanceKey(input.Key.Component, inst.Index), len(missing))
		}

		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, 0, nil
		}

		if _, err := createMissingPods(ctx, deps, input, plan, inst, inst.Index, missing, revisionHashFromTarget(target)); err != nil {
			return false, 0, err
		}
		return false, 0, nil
	}

	// All desired pods exist. Are they runtime-Ready?
	if !query.AllPodsRuntimeReady(existing) {
		return false, 0, nil
	}

	// Flip ome.io/serving=True. For freshly-created pods the condition
	// doesn't exist yet; the Lifecycle writer creates it on first flip.
	for _, pod := range existing {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, 0, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Promote to Ready. Fire InstanceReady only on transition.
	wasReady := false
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseReady {
		wasReady = true
	}
	var promoteErr error
	// Only stamp RunningRevision=target.Name when the existing pods
	// actually carry that revision hash. The Create pass can run after a
	// surge promote in the SAME reconcile (with stale ObservedState
	// showing the pre-promote ActiveOrdinal) or in a follow-up reconcile
	// where target has since moved past the in-flight surge's pinned
	// rev — both are the X-2 bump-during-bump scenario. Naively
	// promoting to target.Name here writes
	// `RunningRevision=<latest-target>` while the pod is on the prior
	// rev. The next reconcile sees RunningRev==target and short-circuits
	// detectUpdateTrigger, leaving the pod stranded on the intermediate
	// revision forever. Falling back to patchInstanceStatusReady (no
	// RunningRevision write) preserves whatever revision the surge
	// promote actually recorded; the next reconcile's detectUpdateTrigger
	// observes the genuine RunningRev != target mismatch and fires a
	// fresh surge cycle.
	if target != nil && existingPodsMatchTargetRevision(existing, target) {
		promoteErr = patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, target.Name)
	} else {
		promoteErr = patchInstanceStatusReady(ctx, input, inst.Index)
	}
	if promoteErr != nil {
		return false, 0, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, promoteErr)
	}
	if !wasReady {
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInstanceReady,
			"OMENative %s is Ready (%d pod(s) serving)",
			instanceKey(input.Key.Component, inst.Index), len(existing))
		// Pod-create-to-ready latency: gap between the earliest pod's
		// creation timestamp and this first Ready promotion. Recorded
		// only on the not-ready→ready edge so re-reconciles of an
		// already-Ready Instance don't re-observe. Labeled by component
		// (bounded), not per-isvc.
		if earliest := earliestPodCreation(existing); !earliest.IsZero() {
			obsmetrics.RecordPodCreateToReady(string(input.Key.Component), time.Since(earliest).Seconds())
		}
	}
	return true, 0, nil
}

// earliestPodCreation returns the oldest CreationTimestamp across the
// Instance's pods, or the zero time when the slice is empty / unstamped.
func earliestPodCreation(pods []*corev1.Pod) time.Time {
	var earliest time.Time
	for _, p := range pods {
		ct := p.CreationTimestamp.Time
		if ct.IsZero() {
			continue
		}
		if earliest.IsZero() || ct.Before(earliest) {
			earliest = ct
		}
	}
	return earliest
}

// existingPodsMatchTargetRevision reports whether every existing pod
// for an Instance carries the rev-hash label of `target`. Used by the
// Create pass's Ready promote to avoid stamping
// RunningRevision=target.Name on pods that are actually on a different
// (typically intermediate) revision — the X-2 bump-during-bump
// corruption mode.
//
// Returns true when:
//   - existing is non-empty AND
//   - every pod's `ome.io/revision-hash` label equals
//     RevisionHashFromControllerRevisionName(target.Name).
//
// Returns false otherwise. An empty existing slice returns false (no
// pods to promote against; caller short-circuits earlier on the
// no-missing-pods branch anyway, so this only safeguards the path
// where the slice is unexpectedly empty).
//
// A pod missing the rev-hash label is treated as a mismatch — we
// can't prove it's on target, so refuse to promote. Legacy pods
// (pre-LabelRevisionHash) would hit this path and stay on their
// prior RunningRevision; a fresh recreate would label them and the
// next pass would promote correctly.
func existingPodsMatchTargetRevision(existing []*corev1.Pod, target *appsv1.ControllerRevision) bool {
	if len(existing) == 0 || target == nil {
		return false
	}
	tgt := query.RevisionOf(target)
	if tgt.IsZero() {
		return false
	}
	for _, pod := range existing {
		if pod == nil {
			return false
		}
		if !query.RevisionFromPod(pod).Same(tgt) {
			return false
		}
	}
	return true
}

// podTarget describes one pod to create within an Instance.
type podTarget struct {
	Name    string
	Runner  workload.RunnerPlan
	Ordinal int32
}

// expectedPodNamesForInstance enumerates every (Runner, ordinal)
// tuple the Instance plan asks for.
//
// Single-pod Runners (Size==1) read the ordinal slot from the
// Instance's recorded ActiveOrdinal — SurgeThenDrain alternates
// between slots 0 and 1 across rollouts. Without this lookup the
// post-surge Create pass would hard-code ordinal 0 and create a
// duplicate pod alongside the (now ordinal-1) canonical pod.
//
// Multi-pod Runners (Size>1) keep enumerating every 0..Size-1
// ordinal; per-gang-member surge allocation isn't yet plumbed.
func expectedPodNamesForInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) []podTarget {
	var targets []podTarget
	for _, runner := range inst.Runners {
		if runner.Size == 1 {
			ordinal := activeOrdinalForInstance(input.ObservedState.InstanceStatuses, inst.Index)
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, ordinal),
				Runner:  runner,
				Ordinal: ordinal,
			})
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, o),
				Runner:  runner,
				Ordinal: o,
			})
		}
	}
	return targets
}

// activeOrdinalForInstance returns the InstanceStatus.ActiveOrdinal
// for idx, or 0 when no status exists yet. Single-pod
// Create / Restart / recreateUpdate paths consult this to address the
// post-surge canonical slot (which may be 1, not 0).
func activeOrdinalForInstance(observed []workload.InstanceStatus, idx int32) int32 {
	if s := findInstanceStatus(observed, idx); s != nil {
		return s.ActiveOrdinal
	}
	return 0
}

// findInstanceStatus returns a pointer to the matching InstanceStatus
// in the observed slice, or nil if absent. Callers must NOT mutate
// the returned pointer — it aliases ObservedState.
func findInstanceStatus(observed []workload.InstanceStatus, idx int32) *workload.InstanceStatus {
	for i := range observed {
		if observed[i].Index == idx {
			return &observed[i]
		}
	}
	return nil
}

// createMissingPods renders and creates targets, owning the per-pod
// ExpectCreates bookkeeping. idx is the expectations bucket — pass
// inst.Index for steady-state Create / Restart Phase B. Callers must
// NOT call ExpectCreates themselves. revisionHash, when non-empty,
// stamps ome.io/revision-hash on every created pod so per-revision
// Services can select it.
func createMissingPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, idx int32, targets []podTarget, revisionHash string) (int, error) {
	created := 0
	// Prime the gang's peer-DNS host list once. It's identical for every
	// pod in the Instance, so caching it here turns the per-pod O(gangsize)
	// rebuild inside Render into O(gangsize) total per gang. inst is a value
	// copy, so this mutation stays loop-local. Single-pod Instances get no
	// peer list (Render's len<2 gate makes it a no-op anyway).
	if inst.PeerHostnames == nil && inst.TotalPods() > 1 {
		inst.PeerHostnames = buildInstancePeerHostnames(input.Key.OwnerName, plan.Component, inst)
	}
	for _, t := range targets {
		template := input.DesiredSpec.PodSpec
		if t.Runner.Name == "worker" && input.DesiredSpec.WorkerPodSpec != nil {
			template = input.DesiredSpec.WorkerPodSpec
		}

		pod, err := RenderWithRevision(
			input.OwnerObject,
			input.OwnerGVK,
			input.Key,
			template,
			input.DesiredSpec.PodTemplateObjectMeta,
			plan,
			inst,
			t.Runner,
			t.Ordinal,
			revisionHash,
			deps.RenderHook,
		)
		if err != nil {
			return created, fmt.Errorf("render pod %s: %w", t.Name, err)
		}

		// Advisory (non-blocking): a multi-node gang worker rendered with no
		// co-location podAffinity — no resolved topologyKey and no
		// operator-supplied term — may schedule across topology domains and
		// break the runtime's collectives. Warn once per (owner, Component).
		maybeWarnGangSplitRisk(deps.Recorder, input, plan, inst, t.Runner, pod)

		// EXPECT-ORDER: expectation before RPC, rollback via ObservedCreate
		// on error — a failed Create fires no watch event to decrement it.
		deps.ExpectationsCache().ExpectCreates(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx, 1)
		if err := deps.Client.Create(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedCreate(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
			if apierrors.IsAlreadyExists(err) {
				// Prior reconcile created it; cache hasn't caught up yet.
				continue
			}
			return created, fmt.Errorf("create pod %s: %w", t.Name, err)
		}
		created++
	}
	return created, nil
}

// revisionHashFromTarget extracts the hash suffix from a target
// ControllerRevision. Returns "" when target is nil so callers can
// pass-through to RenderWithRevision; an empty hash skips the
// ome.io/revision-hash label.
func revisionHashFromTarget(target *appsv1.ControllerRevision) string {
	return query.RevisionOf(target).Hash()
}

// revisionNameFromTarget returns target.Name, or "" for a nil target.
func revisionNameFromTarget(target *appsv1.ControllerRevision) string {
	return query.RevisionOf(target).Name()
}

// patchInstanceStatusCreating idempotently stamps Phase=Creating with the
// Create operation and a Deadline of StartedAt+timeout. incarnation is
// written on first-touch; existing nonzero values are preserved so a
// post-restart reconcile doesn't downgrade the lifecycle token.
//
// targetRev is the revision the created pods will carry. Like the
// Update stampers, the attempt stamp — not the gate — flips a due
// Backoff RetryBlock to RetryInProgress: the flip must be
// co-transactional with the attempt actually starting, so wave counting
// can't double-count a pass that gated-allowed but never stamped.
func patchInstanceStatusCreating(ctx context.Context, input workload.ReconcileInput, idx int32, incarnation int64, timeout time.Duration, targetRev string) error {
	now := metav1.NewTime(input.Now())
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseCreating &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationCreate &&
			s.Incarnation > 0 {
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = incarnation
		}
		s.Phase = workload.InstancePhaseCreating
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationCreate,
			Step:           "CreatePods",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
	if err != nil {
		return err
	}
	if targetRev == "" {
		return nil
	}
	return flipRetryBlockOnAttemptStart(ctx, input, targetRev)
}

// patchInstanceStatusReady idempotently moves to Phase=Ready, clears Operation.
func patchInstanceStatusReady(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady && s.Operation == nil {
			return false
		}
		s.Phase = workload.InstancePhaseReady
		s.Operation = nil
		return true
	})
}

// patchInstanceStatusReadyOnRevision idempotently moves to Phase=Ready
// with RunningRevision=rev; clears Operation + TargetRevision. The
// post-promote anchor that lets detectUpdateTrigger's fast-path
// short-circuit on revision-equality before per-pod diffs. Success at
// rev also prunes rev's RetryBlock — a converged subject leaves no
// active block.
func patchInstanceStatusReadyOnRevision(ctx context.Context, input workload.ReconcileInput, idx int32, rev string) error {
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" {
			return false
		}
		s.Phase = workload.InstancePhaseReady
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		return true
	})
	if err != nil {
		return err
	}
	return pruneRetryBlockOnPromote(ctx, input, rev)
}
