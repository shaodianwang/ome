package ops

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/evidence"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/status"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// RestartRequeueInterval is the wait between passes while a Restart is
// in flight, from the operator's lifecycle.requeue.operation. Exported
// so the dispatcher's pacing stays in lockstep. Zero means
// unconfigured: the caller requeues on the controller's rate-limited
// backoff instead.
func RestartRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

// DetectRestartTrigger fires when the Instance is mid-restart, when
// Phase=Ready and a pod is Failed / the live pod count is below
// desired, when a row demoted for losing every pod (types.DemotedReady)
// is still short of its pod set, or when a materialized Instance has
// lost a gang member in any phase (see instanceLostGangMember). A
// Migrate-owned Instance is suppressed because Migrate's source-pod
// deletion would otherwise trip the "pod count below desired" trigger on
// the source.
//
// The restart policy is read here rather than by the caller: all of the
// above is RestartPolicyRecreateInstance's, while driving an
// open repair and the operation-free crash-loop repair
// (crashLoopRepairReason) belong to every policy.
//
// Ownership: Create materializes an Instance, Restart repairs one that
// was already materialized. Gating repair on Phase=Ready is right for a
// single-pod Instance and backwards for a gang — a gang that loses a
// member before it ever reaches Ready is the case that can never recover
// on its own, because the recovery is gated on a Ready it can no longer
// attain. Only Restart bumps the Incarnation, and only the bump drains
// the surviving members, so any other pass that fills the gap leaves the
// survivors pinned to a topology domain the replacement cannot enter.
//
// Returns (needsRestart, reason, err). When needsRestart is true the
// reason lands on Operation.Reason on the first pass through Restart.
// The trigger reads the row against its own revision; whether that
// revision is still the Component's target is the selection's question
// (RepairYieldsToTarget), asked with the roll target in hand.
//
// Self-lists this Instance's pods live. The dispatcher's per-reconcile
// restart pass instead calls DetectRestartTriggerWithPods after a single
// live List + bucket, so a Component with N gangs costs one live List per
// reconcile instead of N — see that variant.
func DetectRestartTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (bool, string, error) {
	s := input.ObservedState.Instance(inst.Index)
	// Restarting and the absent/Migrate-owned cases decide without reading
	// pods at all, so the live List is wasted there. Every other phase can
	// reach a pod-set comparison.
	if s == nil || isMigrateOwnedStatus(s) || s.Phase == workload.InstancePhaseRestarting {
		needs, reason := DetectRestartTriggerWithPods(input, plan, inst, nil)
		return needs, reason, nil
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, "", err
	}
	needs, reason := DetectRestartTriggerWithPods(input, plan, inst, pods)
	return needs, reason, nil
}

// DetectRestartTriggerWithPods is DetectRestartTrigger with this
// Instance's pods supplied by the caller — the dispatcher does a single
// per-Component live List + bucket once per reconcile and threads each
// Instance's slice here, instead of one live List per Instance.
// instancePods must already be filtered to inst.Index. Semantics are
// identical to DetectRestartTrigger; only the read source differs.
func DetectRestartTriggerWithPods(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, instancePods []*corev1.Pod) (bool, string) {
	s := input.ObservedState.Instance(inst.Index)
	if s == nil {
		return false, ""
	}
	if isMigrateOwnedStatus(s) {
		return false, ""
	}
	// An open Restart is driven to completion whatever the policy says:
	// the policy governs whether a repair STARTS, and a half-drained
	// Instance abandoned mid-repair is worse than one never repaired.
	if s.Phase == workload.InstancePhaseRestarting {
		return true, ""
	}
	expected := inst.TotalPods()
	// A repair parked at Failed has its own two exits, whatever the policy
	// says: the policy governs whether a repair STARTS, and this one did.
	if SpentRepair(s) {
		if reason, fires := spentRepairTrigger(input, s, expected, instancePods); fires {
			return true, reason
		}
	}
	// A pod wedged in a terminal kubelet waiting reason cannot recover on
	// its own and no other pass owns an operation-free Ready row, so the
	// repair starts regardless of the restart policy.
	if reason, wedged := crashLoopRepairReason(input, s, expected, instancePods); wedged {
		return true, reason
	}
	if plan.RestartPolicy != workload.RestartPolicyRecreateInstance {
		return false, ""
	}
	if s.Phase != workload.InstancePhaseReady {
		// Below Ready, gang-member loss triggers. Pod-level failure
		// evidence stays Ready-gated: a container that dies while the
		// Instance is still forming is the ordinary boot path.
		if reason, lost := instanceLostGangMember(input, plan, s, expected, instancePods); lost {
			return true, reason
		}
		// A row demoted for losing every pod (types.DemotedReady) is read
		// by its pod count below, as Ready is: the running revision it
		// kept is what the repair rebuilds at, and pods that turn out to
		// be present return it to Ready through the promote path. Below
		// Ready the rebuild answers to that revision's RetryBlock, as
		// every re-materialization does. Every other row below Ready is
		// its own pass's to materialize.
		if !workload.DemotedReady(s) || rebuildRetryBlockDenies(input, s) {
			return false, ""
		}
	} else {
		for _, pod := range instancePods {
			if reason, restarted := RunnerRestartedSinceReady(pod, s.ReadySince); restarted {
				return true, reason
			}
			if pod.Status.Phase == corev1.PodFailed {
				// Build a richer reason from the failed pod's container
				// termination so the RestartTriggered event names the actual
				// cause (OOMKilled exit 137, CrashLoopBackOff, ...) instead of
				// the bare "pod X Failed". Falls back to the pod name when no
				// per-container detail is available.
				if t := workload.PodTermination(pod, metav1.NewTime(input.Now())); t != nil {
					return true, t.ShortString()
				}
				return true, fmt.Sprintf("pod %s Failed", pod.Name)
			}
		}
	}
	// Terminal pods are absent for the count: a Succeeded pod, or a Failed
	// one that carried no termination detail, still leaves the Instance
	// short of its desired set.
	if live := len(query.ExcludeTerminalPods(instancePods)); int32(live) < expected {
		return true, fmt.Sprintf("pod count %d below desired %d", live, expected)
	}
	return false, ""
}

// instanceLostGangMember reports whether a committed multi-pod Instance is
// missing a member and no other operation owns its pod churn. Create commits
// CreatePods before issuing Pod creates, so an interrupted partial create is
// safely repaired as a whole Instance. Failed also proves that an attempt ran;
// unrecognized or operation-free states use PodCount as a legacy fallback.
//
// Requiring a survivor (len(pods) > 0) confines this to partial loss —
// the shape where the survivors hold capacity the replacement needs.
// Total loss stays with Create's fresh-start path, which already honors
// the RetryBlock recorded against a bad revision. It also excludes
// single-pod Instances without a policy check, since 0 < n < 1 is
// unsatisfiable.
//
// Nothing here reads readiness, container state, deadlines or operation
// age: a slow boot must not be expressible in the inputs, or a gang that
// merely takes hours to load weights would be recycled underneath itself.
func instanceLostGangMember(input workload.ReconcileInput, plan workload.ComponentPlan, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if s == nil || expected <= 0 {
		return "", false
	}
	if plan.RestartPolicy != workload.RestartPolicyRecreateInstance {
		return "", false
	}
	// Only the phases where Create or Restart owns the pod set. Updating,
	// Migrating, Restarting and Deleting are mid-flight through another
	// pass's own drain/recreate, and their transient pod sets are not loss.
	switch s.Phase {
	case workload.InstancePhaseEmpty, workload.InstancePhasePending,
		workload.InstancePhaseCreating, workload.InstancePhaseFailed,
		workload.InstancePhaseReady:
	default:
		return "", false
	}
	if isMigrateOwnedStatus(s) {
		return "", false
	}
	// A preserved Update or Migrate operation keeps its own pass as owner
	// whatever the phase says. A Restart operation parked at Failed is a
	// spent attempt, not an owner: its deadline elapsed with a member still
	// missing, and only another Restart can rebuild the gang as a whole
	// (incarnation bump, survivors drained), so it may re-arm. Each cycle
	// is bounded by the attempt deadline.
	if s.Operation != nil && s.Operation.Type != workload.InstanceOperationCreate &&
		!(s.Operation.Type == workload.InstanceOperationRestart && s.Phase == workload.InstancePhaseFailed) {
		return "", false
	}
	createCommitted := s.Operation != nil && s.Operation.Step == status.CreateStepCreatePods
	publishedPods, _ := workload.AdapterPublished(s)
	if !createCommitted && s.Phase != workload.InstancePhaseFailed && publishedPods < expected {
		return "", false
	}
	// A terminal pod is not a survivor: it holds no capacity and pins no
	// topology, so it counts as lost, not live.
	live := len(query.ExcludeTerminalPods(pods))
	if live == 0 || int32(live) >= expected {
		return "", false
	}
	if rebuildRetryBlockDenies(input, s) {
		return "", false
	}
	return fmt.Sprintf("gang member lost: %d of %d pods present", live, expected), true
}

// rebuildRetryBlockDenies reports whether a RetryBlock forbids rebuilding
// this Instance right now. Restart has no RetryBlock gate of its own
// because a Ready Instance is by definition running a revision that
// works; repairing a never-Ready one can re-materialize a revision the
// disposition held, so it has to answer to the same authority Create does.
func rebuildRetryBlockDenies(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	rev := s.RunningRevision
	if rev == "" {
		rev = s.TargetRevision
	}
	if rev == "" {
		rev = input.ObservedState.UpdateRevision
	}
	if rev == "" {
		return false
	}
	b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	if b == nil {
		return false
	}
	denied, _ := evaluateRetryBlockGate(b, input.Now(), anyInFlightCreateAttempt(input, allInstances))
	return denied
}

// restartDrainKey identifies a Restart drain writer against one Instance
// materialization. Same shape as the Update drain key, but the writer
// userAgent differs so the two can't cancel each other out.
func restartDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// Restart drives one Instance through Restart-on-pod-loss. Multi-pass,
// idempotent. Three phases:
//
//	A. Drain + delete every pod at the old incarnation.
//	B. Recreate the pod set at the bumped incarnation
//	   (distinguished by the ome.io/instance-incarnation label).
//	C. Wait for runtime ready, flip ome.io/serving=True, promote to Ready.
//
// reason lands on Operation.Reason on the first pass only; later passes
// preserve it via the skip-write guard.
//
// Returns done=true once the Instance is back at Phase=Ready with
// Operation=nil; done=false means the caller should requeue and resume.
//
// target is the Component's roll target, nil when it has none. A rebuild
// that has created no pod yet follows it when the row is off it
// (rebuildFollowsTarget); every other rebuild renders the revision its
// row records.
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types — today the InferenceReplica adapter
// (inferencereplica.buildReconcileInput) wires this entry point.
func Restart(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, reason string) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}

	// Use the post-patch Incarnation, not inst.Incarnation — BuildPlan
	// ran against an earlier read.
	observed := input.ObservedState.Instance(inst.Index)
	wasNotRestarting := observed == nil || observed.Phase != workload.InstancePhaseRestarting
	var newInc int64
	switch {
	case RepairResumes(observed, inst.TotalPods(), pods):
		// The rebuilt set of a spent repair came up on its own. No new
		// attempt opens: the pods already carry the row's incarnation, and
		// the repair ends with the promote below.
		newInc = observed.Incarnation
		wasNotRestarting = false
	case spentRepairParked(input, observed, inst.TotalPods(), pods):
		// Wedged and not yet due on the retry ladder: the trigger declined
		// this row, and a pass that reaches it anyway leaves it parked.
		return false, nil
	default:
		newInc, err = status.StampRestarting(ctx, input, inst.Index, reason, plan.InstanceReadyTimeout)
		if err != nil {
			return false, fmt.Errorf("patch status Restarting (instance=%d): %w", inst.Index, err)
		}
		if wasNotRestarting {
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonRestartTriggered,
				"OMENative %s restart triggered: %s (incarnation=%d)",
				workload.InstanceKey(input.Key.Component, inst.Index), reason, newInc)
		}
	}

	// On the first pass, preserve the failing pod's diagnostics into
	// InstanceStatus.LastFailure BEFORE Phase A drains and deletes it —
	// otherwise the container termination reason / exit code vanish with
	// the pod and a gang that keeps restarting leaves no failure trace.
	// Only a genuine pod-failure restart populates this; a "pod count
	// below desired" restart (the pod already vanished) finds nothing to
	// capture and leaves any prior LastFailure intact.
	if wasNotRestarting {
		if t := firstFailedPodTermination(pods); t != nil {
			if err := status.RecordLastFailure(ctx, input, inst.Index, t); err != nil {
				return false, fmt.Errorf("record LastFailure (instance=%d): %w", inst.Index, err)
			}
		}
	}

	oldPods, newPods, unknownPods := query.PartitionPodsByIncarnation(pods, newInc)

	// Surface orphan pods (label-missing) as warnings and refuse to delete.
	// A stripped label or a third-party operator stamping the OMENative
	// managed-by label would otherwise get torn down by Phase A. Operator
	// must re-classify or delete the pod for Restart to proceed.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
				workload.InstanceKey(input.Key.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// Phase A: drain + delete old pods.
	if len(oldPods) > 0 {
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterRestartDrain, restartDrainKey(inst.Index, newInc)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}

		// Live reader on drain check so kube-proxy isn't still routing.
		// Drain target is the per-revision *routed* Service. Read from
		// the pod's ome.io/revision-hash label (stamped by Render). The
		// per-Component headless Service publishes not-ready endpoints
		// so peer-discovery DNS resolves during gang init — using it
		// here would make IsPodDrained wait forever.
		//
		// Batcher lists each per-revision Service's EndpointSlices once
		// and reuses them across the gang, avoiding the N+1 per-pod LIST.
		drainer := drain.NewBatcher(deps.Reader(), input.Key.Namespace)
		for _, pod := range oldPods {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			serviceName := query.PerRevisionServiceName(input.Key.OwnerName, plan.Component, hash)
			drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}

		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase B: recreate at the bumped Incarnation.
	// Live-read the OLD pod set first — a stable-name Create with the
	// cache stale would AlreadyExists against the still-terminating
	// previous incarnation. Matches K8s foreground-deletion propagation.
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("Restart: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	// Override Incarnation on the ad-hoc plan so Render stamps the new label.
	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(input, plan, newInst)
	// A new-incarnation pod that died (rejected at admission, evicted) is
	// absent: it holds the stable name but will never run. Recycle it so
	// the name frees up; its target is created on a later pass.
	recycling, err := recycleTerminalPods(ctx, deps, input, inst.Index, inst.Index, workload.InstanceOperationRestart, terminalTargetPods(newPods, desired))
	if err != nil {
		return false, fmt.Errorf("Restart: recycle terminal pods (instance=%d): %w", inst.Index, err)
	}
	if recycling {
		return false, nil
	}
	// A pod whose node stopped reporting it is not dead evidence: the
	// name stays occupied until the force-delete sweep proves the node
	// gone, and the repair reports that wait instead of polling a name no
	// kubelet is answering for. The policy boundary that wait ends on
	// needs no plumbing here: an unfinished Restart requeues at
	// RestartRequeueInterval, well inside any node-death threshold.
	holding, _, err := recoverUnknownPhaseTargets(ctx, deps, input, inst.Index, newPods, desired)
	if err != nil {
		return false, fmt.Errorf("Restart: recover unknown-phase pods (instance=%d): %w", inst.Index, err)
	}
	if holding {
		return false, nil
	}
	existingByName := query.IndexPodsByName(query.ExcludeTerminalPods(newPods))
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		// The rebuild renders the revision it stamps. A rebuild that has
		// created no pod yet follows a roll target the row is off: the row
		// records that revision before the first create, so a pass
		// interrupted between the two still renders what the row says, and
		// the update trigger reads the Instance as on target once it is
		// Ready. Every other rebuild renders the stored template of the
		// revision its row records: a repair under a pause or a Held
		// target, and one whose pods already exist, which the roll replaces
		// from Ready.
		var tmpl podTemplate
		found := true
		if rebuildFollowsTarget(input, plan, inst.Index, target, newPods) {
			if err := status.StampRestartRevision(ctx, input, inst.Index, target.Name); err != nil {
				return false, fmt.Errorf("Restart: record rebuild revision (instance=%d): %w", inst.Index, err)
			}
			tmpl = desiredTemplate(input, plan, query.RevisionOf(target))
		} else {
			tmpl, found, err = rebuildTemplate(ctx, deps, input, plan, target, inst.Index)
			if err != nil {
				return false, fmt.Errorf("Restart: resolve rebuild template (instance=%d): %w", inst.Index, err)
			}
		}
		if !found {
			return false, announceRevisionGone(ctx, deps, input, inst.Index, rebuildRevision(input.ObservedState.Instance(inst.Index)))
		}
		if _, err := createMissingPods(ctx, deps, input, plan, newInst, inst.Index, missing, tmpl); err != nil {
			if createRejectionHandled(err) {
				return false, nil
			}
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, then hold at the shared promote bar. The gate
	// write comes first because the rebuilt pods carry the lifecycle hold, so
	// PodReady cannot become true until it is released.
	if !query.AllPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		// Lifecycle key on fresh pods; the Restart-drain key only lived
		// on the now-deleted OLD pods.
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	if promotable, wait := query.PodSetPromotable(newPods, plan.MinReadySeconds, input.Now()); !promotable {
		input.PromoteWindow.Observe(wait)
		return false, nil
	}

	if err := status.StampReady(ctx, input, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRestartCompleted,
		"OMENative %s restart complete (incarnation=%d)",
		workload.InstanceKey(input.Key.Component, inst.Index), newInc)
	return true, nil
}

// firstFailedPodTermination returns the InstanceTermination of the first
// pod that is Phase=Failed or carries any container-termination diagnostics
// worth preserving across the recreate. Returns nil when no pod yields a
// signal (e.g. the failing pod already vanished — the "pod count below
// desired" trigger). Order-deterministic by the input slice.
func firstFailedPodTermination(pods []*corev1.Pod) *workload.InstanceTermination {
	now := metav1.Now()
	// Prefer an explicitly Failed pod — that's the pod the trigger fired on.
	for _, pod := range pods {
		if pod != nil && pod.Status.Phase == corev1.PodFailed {
			if t := workload.PodTermination(pod, now); t != nil {
				return t
			}
		}
	}
	// Otherwise capture any pod showing a crash / wedge in its container
	// states (a not-yet-Failed-phase pod mid-CrashLoopBackOff still counts
	// as the cause when Restart was triggered for it).
	for _, pod := range pods {
		if pod == nil || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if t := workload.PodTermination(pod, now); t != nil {
			return t
		}
	}
	return nil
}

// rebuildTemplate is the template a repair of Instance idx renders:
// the stored template of the revision it rebuilds at (rebuildRevision),
// stamped with that revision. target is the roll target the stored
// template is composed against. found is false when that revision's
// ControllerRevision is gone. A row that records no revision is rebuilt
// from the current template with no revision label, as it was created.
func rebuildTemplate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, idx int32) (podTemplate, bool, error) {
	rev := rebuildRevision(input.ObservedState.Instance(idx))
	if rev == "" {
		return desiredTemplate(input, plan, query.RevisionID{}), true, nil
	}
	return storedTemplate(ctx, deps.Reader(), input, target, rev)
}

// rebuildRevision names the revision a repair of this row would rebuild
// at: the revision the Instance runs, else the one it was converging to,
// else the one its attempt pinned — an Instance that never ran a revision
// (a gang rebuilt after losing a member while still forming) has only the
// pin on its Operation. Empty when nothing records a revision.
func rebuildRevision(s *workload.InstanceStatus) string {
	if s == nil {
		return ""
	}
	if s.RunningRevision != "" {
		return s.RunningRevision
	}
	if s.TargetRevision != "" {
		return s.TargetRevision
	}
	if s.Operation != nil {
		return s.Operation.TargetRevision
	}
	return ""
}

// RepairYieldsToTarget reports whether a restart selection for inst is
// left to the pass that rebuilds the Instance at the Component's roll
// target instead of opening a repair: the revision the repair would
// rebuild (rebuildRevision) is not target. A crash loop, a lost pod or a
// lost gang member on such a row is unavailability its roll must cross
// anyway, so the update pass replacing the pod set at the target opens
// no further outage, while a repair would rebuild a revision the
// Component does not want and the roll would tear it down again.
// The update pass takes a Ready or Failed row, the Create pass retires a
// first materialization pinned to a superseded revision and materializes
// a demoted row that has no pod left.
//
// Three rows are never yielded: an open Restart, which is driven on — its
// rebuild follows the target itself while it has created no pod
// (rebuildFollowsTarget); a Migrate-owned row, which its record owns; and
// a row demoted for losing every pod that holds pods again, which no
// update trigger reads, so its repair rebuilds the revision it records and
// the roll follows once it is Ready. A row that records no revision, or a
// pass with no target, has nothing to compare.
//
// The roll the row is left to must be one that can start (rollTargetOpen):
// an Instance kept dark for the length of a pause or a hold is worse than
// one rebuilt at the revision the roll is held on. The roll takes the row
// once the pause or the hold clears.
func RepairYieldsToTarget(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) bool {
	if !rollTargetOpen(input, plan, target) {
		return false
	}
	s := input.ObservedState.Instance(inst.Index)
	if s == nil || s.Phase == workload.InstancePhaseRestarting || isMigrateOwnedStatus(s) {
		return false
	}
	rev := rebuildRevision(s)
	if rev == "" || rev == target.Name {
		return false
	}
	if workload.DemotedReady(s) && len(query.ExcludeTerminalPods(instancePods)) > 0 {
		return false
	}
	return true
}

// rollTargetOpen reports whether target is a roll target the roll could
// start on: the Component has one, is not paused, and the RetryBlock does
// not hold it. A pause withholds the roll, and a Held target admits no
// fresh attempt until an operator releases it; a Backoff block merely
// paces the start.
func rollTargetOpen(input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) bool {
	if target == nil || plan.Paused {
		return false
	}
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name)
	return block == nil || block.State != workload.RetryBlockHeld
}

// rebuildFollowsTarget reports whether the rebuild of an open repair
// renders the roll target instead of the revision its row records: no pod
// of the rebuild exists yet and the row is off a target the roll could
// start on. The Instance serves nothing, so the rebuild is the pass that
// puts it on the target and no pod set is torn down for it. A rebuild
// whose pods exist finishes on the revision they carry: replacing a
// starting pod set is the roll's job, under its budget.
func rebuildFollowsTarget(input workload.ReconcileInput, plan workload.ComponentPlan, idx int32, target *appsv1.ControllerRevision, rebuiltPods []*corev1.Pod) bool {
	if len(query.ExcludeTerminalPods(rebuiltPods)) > 0 || !rollTargetOpen(input, plan, target) {
		return false
	}
	rev := rebuildRevision(input.ObservedState.Instance(idx))
	return rev != "" && rev != target.Name
}

// RunnerRestartedSinceReady reports whether pod's runner container carries
// restart evidence dated after readySince: a current run that began after the
// Instance entered Ready, or a termination that finished after it. Either
// proves the process group formed at Ready has lost this member's original
// process, even though kubelet's in-place restart left the Pod Running under
// the same UID. Sidecar and init containers never count — kubelet's in-place
// restart is their recovery path and does not break the Instance's process
// group. A nil readySince (Instance was last promoted before the field
// existed) reports false; the anchor appears on the next Ready transition.
func RunnerRestartedSinceReady(pod *corev1.Pod, readySince *metav1.Time) (string, bool) {
	if pod == nil || readySince == nil || readySince.IsZero() || pod.DeletionTimestamp != nil {
		return "", false
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name != constants.MainContainerName {
			continue
		}
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(readySince.Time) {
			reason := t.Reason
			if reason == "" {
				reason = "Error"
			}
			return fmt.Sprintf("pod %s container %s restarted after Ready: %s (exit %d)",
				pod.Name, cs.Name, reason, t.ExitCode), true
		}
		if r := cs.State.Running; r != nil && r.StartedAt.After(readySince.Time) {
			return fmt.Sprintf("pod %s container %s restarted after Ready", pod.Name, cs.Name), true
		}
	}
	return "", false
}

// RunnerRestartedAgainSinceReady reports whether pod's runner container
// has restarted more than once since readySince: the run its last
// termination records had itself begun after the Instance entered Ready,
// so it was already a restart's run, and it ended too. The kubelet keeps
// only the latest termination, and that run's start is what dates the
// restart before it. A container that keeps dying under load reads this
// way from its second crash on; one the kubelet restarted once never
// does, because the run that crashed is the run that was promoted. The
// nil and zero readySince rules of RunnerRestartedSinceReady apply.
func RunnerRestartedAgainSinceReady(pod *corev1.Pod, readySince *metav1.Time) bool {
	if pod == nil || readySince == nil || readySince.IsZero() || pod.DeletionTimestamp != nil {
		return false
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name != constants.MainContainerName {
			continue
		}
		t := cs.LastTerminationState.Terminated
		return t != nil && t.StartedAt.After(readySince.Time)
	}
	return false
}

// Crash-loop repair: a wedge no other pass owns.
//
// No pass owns an operation-free Ready row, so this trigger does: the
// stuck-pod fast escalator qualifies such a row only through the
// wedged-pod shape, which is a revision-hash disagreement, so a pod
// crash-looping on the revision the Instance is supposed to be running
// reaches no escalation path. The trigger belongs to every restart
// policy; RestartPolicy=None keeps its meaning for mere container
// restarts, because a container that died and came back is not a wedge.
//
// The repair is a Restart, not a Failed stamp: the pod set can be
// rebuilt. Routing it through the restart pass means a rebuild that
// takes a serving pod offline is admitted by the per-Component
// unavailability budget before it opens, while a rebuild of a pod set
// that serves nothing removes no serving capacity and is not. The
// coordination gate is not asked either way: the parked member that
// makes the wedge already puts the Instance inside the gate's
// serving-based unavailability. The RetryBlock recorded against the
// revision denies either once the attempts are spent, so a revision that
// keeps crash-looping is held rather than recycled forever.

// crashLoopRepairReason reports whether this row holds a crash-loop
// wedge the restart pass may repair: the wedge itself is
// evidence.CrashLoopWedge, and the RetryBlock held against the revision
// is what denies the rebuild — the same authority a repair of a
// never-Ready Instance answers to. A pod parked inside the grace is not
// a wedge yet, and the grace ending raises no watch event, so the pass
// deposits the grace left and comes back when the repair can open.
func crashLoopRepairReason(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	reason, wedged, graceLeft := evidence.CrashLoopWedge(input, s, expected, pods)
	if !wedged {
		if graceLeft > 0 {
			input.PassWake.Observe(graceLeft)
		}
		return "", false
	}
	if rebuildRetryBlockDenies(input, s) {
		return "", false
	}
	return reason, true
}

// RestartOpensRepair reports whether a restart selection for inst would
// OPEN a fresh crash-loop repair this pass, as opposed to driving one
// already in flight or recovering capacity that is already gone: a
// Restart already in flight must be driven to completion, and the
// pod-loss and lost-member triggers repair an outage rather than causing
// one. Only a fresh open counts against the per-pass repair batch.
func RestartOpensRepair(input workload.ReconcileInput, inst workload.InstancePlan, instancePods []*corev1.Pod) bool {
	s := input.ObservedState.Instance(inst.Index)
	_, wedged := crashLoopRepairReason(input, s, inst.TotalPods(), instancePods)
	return wedged
}

// RestartOpensUnavailability reports whether a restart selection for
// inst would OPEN a fresh crash-loop repair this pass that takes serving
// capacity offline. Only such an open is put to the per-Component
// unavailability budget: a wedged pod set that serves nothing
// (evidence.PodSetServesNothing, the reading the update pass makes for a
// dark row) removes no serving capacity, so its rebuild is neither
// admitted nor charged.
func RestartOpensUnavailability(input workload.ReconcileInput, inst workload.InstancePlan, instancePods []*corev1.Pod) bool {
	return RestartOpensRepair(input, inst, instancePods) && !evidence.PodSetServesNothing(instancePods)
}

// A spent repair: a Restart parked at Failed by the deadline or a wedged
// pod. No pass drives the row, but the repair it records keeps two exits
// here. A rebuilt set that comes up on its own — the kubelet's retry
// succeeded once a missing image tag or ConfigMap key was back — resumes
// the promote at the incarnation the pods already carry, so the Instance
// returns to rotation on its running revision with no operator action. A
// set that stays wedged is rebuilt again on the operator's retry ladder
// (lifecycle.updateRetry, the one ladder every attempt is paced by),
// measured from the recorded failure and bounded by its attempt count, so
// a cause that needs a fresh pod set to clear — a crash loop, a gang
// whose members hung, a node-local fault — heals once it is gone. A park
// whose recorded cause is workload-caused (workload.RepairWaitsOnWorkload:
// the container config or image the pod template names) is owed no
// re-arm at all: the kubelet retries that cause in place and a fresh set
// would wedge on it identically, so the row stays Failed with its reason
// until the configuration or image is fixed and takes the first exit
// when the pods come up. No ladder configured means no re-arm either:
// the row waits for the reset mailbox or a new revision.

// SpentRepair reports whether the row is a Restart parked at Failed.
func SpentRepair(s *workload.InstanceStatus) bool {
	return s != nil && s.Phase == workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationRestart
}

// repairPodSet is the pods a spent repair's exits are judged on: live,
// not deleting, not terminal. A pod still terminating holds its name and
// belongs to no exit until it is gone.
func repairPodSet(pods []*corev1.Pod) []*corev1.Pod {
	set := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range query.ExcludeTerminalPods(pods) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		set = append(set, pod)
	}
	return set
}

// RepairResumes reports whether a spent repair's rebuilt set is one
// promote away: complete, every pod at the row's incarnation, and every
// pod ContainersReady. A set carrying an older incarnation is a drain the
// attempt never finished, not a set that came up, and is rebuilt instead.
func RepairResumes(s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) bool {
	if !SpentRepair(s) || expected <= 0 {
		return false
	}
	set := repairPodSet(pods)
	if int32(len(set)) < expected {
		return false
	}
	for _, pod := range set {
		if inc, ok := query.InstanceIncarnationFromLabels(pod); !ok || inc != s.Incarnation {
			return false
		}
	}
	return query.AllPodsRuntimeReady(set)
}

// RepairRetryAt returns the instant a spent repair may re-arm — the
// recorded failure plus the ladder's delay for the next attempt — and
// false when no re-arm is owed: no spent repair on the row, a park on a
// cause the kubelet retries in place (workload.RepairWaitsOnWorkload),
// no ladder configured, or every re-arm spent.
func RepairRetryAt(input workload.ReconcileInput, s *workload.InstanceStatus) (time.Time, bool) {
	policy := input.UpdateRetryPolicy
	if !SpentRepair(s) || workload.RepairWaitsOnWorkload(s) || policy == nil || policy.Exhausted(s.Operation.RetryCount) {
		return time.Time{}, false
	}
	return repairParkedAt(s).Add(policy.NextRetryDelay(s.Operation.RetryCount + 1)), true
}

// repairParkedAt is when the repair parked: the recorded failure, which
// the Failed stamps write in the same transaction as the phase, else the
// operation's own timestamps for a row that recorded no failure.
func repairParkedAt(s *workload.InstanceStatus) time.Time {
	if s.LastFailure != nil && !s.LastFailure.Time.IsZero() {
		return s.LastFailure.Time.Time
	}
	if !s.Operation.LastProgressAt.IsZero() {
		return s.Operation.LastProgressAt.Time
	}
	return s.Operation.StartedAt.Time
}

// RepairRetriesExhausted reports whether a spent repair has spent every
// re-arm the operator's ladder allows, so the row stays parked until an
// operator resets it or a new revision arrives. A park on a cause the
// kubelet retries in place is not held by the ladder — it waits for the
// configuration or image and resumes on its own — so it is never read as
// exhausted, whatever its attempt count.
func RepairRetriesExhausted(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	return SpentRepair(s) && !workload.RepairWaitsOnWorkload(s) && input.UpdateRetryPolicy != nil &&
		input.UpdateRetryPolicy.Exhausted(s.Operation.RetryCount)
}

// RepairWaitingNote is what a park on a workload-caused failure records
// on its failure message and says in its event: the cause is the
// configuration or image the pod template names, which the kubelet
// retries in place, so the row waits for the fix and no re-arm is owed.
const RepairWaitingNote = "waiting for the configuration or image the pod needs; the kubelet retries in place"

// spentRepairTrigger decides the exits of a spent repair whose pod set is
// complete. An incomplete set is not decided here: a gang short of a
// member re-arms through the lost-member trigger, and a row with no pod
// left is the Create pass's fresh start.
//
// A resume stamps no operation, so it carries no reason. A re-arm that is
// owed but not yet due deposits its wake-up on the pass: the ladder
// coming due raises no watch event.
func spentRepairTrigger(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if expected <= 0 || int32(len(repairPodSet(pods))) < expected {
		return "", false
	}
	if RepairResumes(s, expected, pods) {
		return "", true
	}
	if rebuildRetryBlockDenies(input, s) {
		return "", false
	}
	at, owed := RepairRetryAt(input, s)
	if !owed {
		return "", false
	}
	if now := input.Now(); now.Before(at) {
		input.PassWake.Observe(at.Sub(now))
		return "", false
	}
	return repairRearmReason(s), true
}

// spentRepairParked reports whether a spent repair with a complete pod set
// has nothing to do this pass: the set is wedged and the ladder has not
// come due, is spent, owes nothing to a park the kubelet retries in
// place, or is denied by the revision's RetryBlock.
func spentRepairParked(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) bool {
	if !SpentRepair(s) || expected <= 0 || int32(len(repairPodSet(pods))) < expected {
		return false
	}
	if RepairResumes(s, expected, pods) {
		return false
	}
	if rebuildRetryBlockDenies(input, s) {
		return true
	}
	at, owed := RepairRetryAt(input, s)
	return !owed || input.Now().Before(at)
}

// repairRearmReason names the re-arm on the new attempt: which attempt it
// is and the failure that parked the last one.
func repairRearmReason(s *workload.InstanceStatus) string {
	return fmt.Sprintf("repair re-armed (attempt %d) after %s", s.Operation.RetryCount+1, RepairFailureSummary(s))
}

// RepairFailureSummary names what parked the repair, for events and the
// re-armed attempt's reason: the recorded failure when there is one — by
// pod when it names one, by its own message otherwise, as an elapsed
// deadline does — else the reason the repair opened with.
func RepairFailureSummary(s *workload.InstanceStatus) string {
	switch {
	case s == nil:
		return ""
	case s.LastFailure == nil:
		if s.Operation != nil {
			return s.Operation.Reason
		}
		return ""
	case s.LastFailure.PodName != "":
		return s.LastFailure.ShortString()
	case s.LastFailure.Message != "":
		return s.LastFailure.Message
	default:
		return s.LastFailure.Reason
	}
}
