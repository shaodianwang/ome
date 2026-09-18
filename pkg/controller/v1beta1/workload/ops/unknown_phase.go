// unknown_phase.go — what a rebuild does about a pod that holds one of
// its target names in phase Unknown.
//
// Unknown is not terminal: the node stopped reporting, and the container
// may still be running. Recycling the name the way a Failed or Succeeded
// occupant is recycled would risk two pods of the same identity, so the
// name is only freed by the force-delete sweep, on the same node-death
// evidence and the same operator-configured policy that frees a
// stuck-Terminating pod. Until that evidence exists the step reports the
// wait on the operation instead of spending its deadline in silence.
package ops

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// unknownPhaseTargetPods returns the pods in phase Unknown occupying one
// of the desired target names, lowest name first. Name order — not the
// informer's map iteration order — decides which pod the hold's evidence
// names, so re-observing the same wait writes the same record.
func unknownPhaseTargetPods(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	if len(existing) == 0 || len(targets) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		wanted[t.Name] = struct{}{}
	}
	var quiet []*corev1.Pod
	for _, pod := range existing {
		if !workload.PodPhaseUnknown(pod) {
			continue
		}
		if _, ok := wanted[pod.Name]; ok {
			quiet = append(quiet, pod)
		}
	}
	sort.Slice(quiet, func(i, j int) bool { return quiet[i].Name < quiet[j].Name })
	return quiet
}

// nodeUnknownHoldWritable reports whether this authority may own the
// operation's Waiting token.
//
// A Delete is excluded: DeleteBatch owns a teardown's progress and its
// own stuck-pod escalation, and a pod on its way out occupies no name
// anyone is waiting to rebuild. An operation already parked on a
// DIFFERENT external wait keeps it — that token's writer owns what the
// row reports it is queued behind.
func nodeUnknownHoldWritable(op *workload.InstanceOperation) bool {
	if op == nil || op.Type == workload.InstanceOperationDelete {
		return false
	}
	return op.Waiting == "" || workload.OperationNodeUnknown(op)
}

// recoverUnknownPhaseTargets routes every target name held in phase
// Unknown through the force-delete sweep and reports the outcome to its
// caller: holding=true means the step must not create or promote this
// pass, because a name it needs is still occupied or a delete of one has
// just been issued.
//
// With no force-delete policy configured, or with the node still alive,
// nothing frees the name — the wait is recorded on the operation instead
// (Waiting token plus the pod and node on LastFailure), which parks the
// attempt deadline and keeps the wait legible in `kubectl describe`. The
// token is released by this same authority as soon as no target name is
// held in that phase.
//
// requeueAt is the next moment the evidence could turn actionable on its
// own. A node that has stopped reporting emits no event, so a caller
// whose pass has no poll of its own must fold it into its requeue, or
// the sweep fires only when something unrelated wakes the owner.
func recoverUnknownPhaseTargets(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, existing []*corev1.Pod, targets []podTarget) (bool, time.Time, error) {
	quiet := unknownPhaseTargetPods(existing, targets)
	if len(quiet) == 0 {
		return false, time.Time{}, clearNodeUnknownHold(ctx, input, idx)
	}
	var nextEvidenceAt time.Time
	held := make([]*corev1.Pod, 0, len(quiet))
	for _, pod := range quiet {
		gone, at, err := sweepUnknownPhasePod(ctx, deps, input, pod, idx)
		if err != nil {
			return true, time.Time{}, fmt.Errorf("sweep unknown-phase pod %s: %w", pod.Name, err)
		}
		if !at.IsZero() && (nextEvidenceAt.IsZero() || at.Before(nextEvidenceAt)) {
			nextEvidenceAt = at
		}
		if !gone {
			held = append(held, pod)
		}
	}
	if len(held) == 0 {
		return true, nextEvidenceAt, clearNodeUnknownHold(ctx, input, idx)
	}
	if _, _, err := workload.MarkExternalHold(ctx, input, idx, workload.WaitingReasonNodeUnknown,
		workload.NodeUnknownTermination(held[0]), nodeUnknownHoldWritable); err != nil {
		return true, nextEvidenceAt, fmt.Errorf("record node-unknown hold (instance=%d): %w", idx, err)
	}
	return true, nextEvidenceAt, nil
}

// sweepUnknownPhasePod picks the arm of the force-delete sweep the pod's
// own state calls for. One that is Terminating as well as Unknown is the
// stuck-teardown case, which owns the graceful-shutdown window and the
// finalizer report; neither arm reaches these callers otherwise, so
// without this such a pod would hold its name with the attempt deadline
// parked and nothing left to free it.
//
// Reports whether the name is free afterwards, re-read rather than
// assumed: the Terminating arm deletes in place and tells the caller
// nothing, and a pod inside its grace or pinned by a finalizer is still
// there.
func sweepUnknownPhasePod(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) (bool, time.Time, error) {
	if pod.DeletionTimestamp == nil {
		return forceDeleteOnNodeDeath(ctx, deps, input, pod, idx)
	}
	at, err := escalateStuckTerminatingWithDeadline(ctx, deps, input, pod, idx)
	if err != nil {
		return false, time.Time{}, err
	}
	gone, err := podGone(ctx, deps, pod)
	return gone, at, err
}

// podGone reports whether the pod object has left the apiserver, read
// live so a lagging cache cannot report a name as free while it is not.
func podGone(ctx context.Context, deps workload.Deps, pod *corev1.Pod) (bool, error) {
	err := deps.Reader().Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return false, nil
}

// recoverUnknownPhaseInstances applies recoverUnknownPhaseTargets across
// a whole Create pass. Returns the Instances the pass must leave alone
// this time round and the earliest policy boundary among them.
func recoverUnknownPhaseInstances(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, keep func(int32) bool, byInstance map[int32][]*corev1.Pod) (map[int32]bool, time.Time, error) {
	var held map[int32]bool
	var nextEvidenceAt time.Time
	for _, inst := range plan.Instances {
		if !keep(inst.Index) {
			continue
		}
		holding, at, err := recoverUnknownPhaseTargets(ctx, deps, input, inst.Index,
			byInstance[inst.Index], expectedPodNamesForInstance(input, plan, inst))
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("Create: recover unknown-phase pods (instance=%d): %w", inst.Index, err)
		}
		if !at.IsZero() && (nextEvidenceAt.IsZero() || at.Before(nextEvidenceAt)) {
			nextEvidenceAt = at
		}
		if holding {
			if held == nil {
				held = make(map[int32]bool)
			}
			held[inst.Index] = true
		}
	}
	return held, nextEvidenceAt, nil
}

// until converts an absolute policy boundary into the delay a requeue
// takes. A zero or already-passed boundary is no delay at all — there is
// nothing left to wait for, and asking for a zero RequeueAfter would
// read as "no requeue requested".
func until(now, at time.Time) time.Duration {
	if at.IsZero() {
		return 0
	}
	if wait := at.Sub(now); wait > 0 {
		return wait
	}
	return 0
}

// clearNodeUnknownHold releases the token once no target name is held in
// phase Unknown. LastFailure is left in place: it is the record of the
// wait that just ended, and the next real failure overwrites it.
func clearNodeUnknownHold(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	return workload.ClearExternalHold(ctx, input, idx, workload.WaitingReasonNodeUnknown)
}
