package workload

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
)

// StuckPodFailureGracePeriod is the wait window after a pod's
// CreationTimestamp before a terminal kubelet waiting state
// (ImagePullBackOff, ErrImagePull, InvalidImageName,
// CreateContainerConfigError, CreateContainerError) escalates the
// owning Instance to Phase=Failed. 60 seconds is comfortable
// headroom over a 5–10 second pull cycle while debouncing transient
// ContainerCreating / ImagePulling states.
//
// Exported so tests can lower the value to keep test runtime sane.
var StuckPodFailureGracePeriod = 60 * time.Second

// DefaultRevisionRetention caps non-live ControllerRevisions per
// workload (StatefulSet default; rollback headroom without unbounded
// etcd growth).
const DefaultRevisionRetention = 10

// terminalPullFailureReasons enumerates kubelet container
// state.Waiting reasons that cannot self-recover. Every entry below
// has been observed in the wild as a wedge that requires either a
// spec change (fix the image tag) or a node-level fix (registry
// creds).
//
// The set covers both pull-side failures (image cannot be obtained)
// and post-pull failures (image obtained but the container cannot
// run or stay running). Naming retains the historical "pull" prefix
// for backwards compatibility, but the classifier is the generic
// terminal-waiting-reason matcher the escalator delegates to —
// every reason that lands a container in Waiting forever without
// hope of self-recovery belongs here.
var terminalPullFailureReasons = map[string]struct{}{
	"ErrImagePull":               {},
	"ImagePullBackOff":           {},
	"InvalidImageName":           {},
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
	// CrashLoopBackOff lands when the container repeatedly exits
	// non-zero and kubelet's restart backoff has converged on its
	// per-pod cap (~5 minutes between attempts). Common operator-
	// reported triggers: bad command-line args, missing config, image
	// with broken entrypoint. Permanent until the spec changes — even
	// if the pod briefly transitions through Running between attempts,
	// the kubelet's stable steady-state for a wedged container is
	// Waiting{Reason=CrashLoopBackOff}. Closes the bug #52 follow-up
	// where a bumped image that pulls cleanly but crashes immediately
	// was not classified as terminal and the rollout sat at
	// Phase=Updating for the full 30-minute InstanceReadyTimeout.
	"CrashLoopBackOff": {},
	// RunContainerError signals the container runtime rejected the
	// start (corrupt manifest, missing required shared libraries,
	// exec-format mismatch — e.g., amd64 image on arm64 node).
	// Permanent until the image is fixed; the controller cannot
	// retry past a broken binary.
	"RunContainerError": {},
}

// PodStuckPullFailure returns (true, reason) when pod has at least
// one container in a terminal pull-failure waiting state that has
// been stuck for ≥ grace from `now`, otherwise (false, "").
//
// Stuck duration is measured from CreationTimestamp (kubelet doesn't
// expose per-state transition time). For ImagePullBackOff kubelet
// retries at least once before flipping to BackOff (~30s on a 404),
// so a 60s grace catches stuck pulls while ignoring the brief
// PodInitializing / ContainerCreating / ImagePulling window.
func PodStuckPullFailure(pod *corev1.Pod, now time.Time, grace time.Duration) (bool, string) {
	if pod == nil {
		return false, ""
	}
	if pod.CreationTimestamp.IsZero() {
		return false, ""
	}
	age := now.Sub(pod.CreationTimestamp.Time)
	if age < grace {
		return false, ""
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		if _, ok := terminalPullFailureReasons[cs.State.Waiting.Reason]; ok {
			return true, cs.State.Waiting.Reason
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		if _, ok := terminalPullFailureReasons[cs.State.Waiting.Reason]; ok {
			return true, cs.State.Waiting.Reason
		}
	}
	return false, ""
}

// AvailablePodSet returns pod names that are Ready and non-terminating
// in any EndpointSlice for serviceName. drain.EndpointAvailable
// excludes terminating-but-Ready endpoints — they're not getting new
// traffic. Single slice list avoids the N+1 reads
// drain.IsPodInRotation per pod would do.
//
// Adapter-agnostic: takes the namespace and headless-service name
// directly, no v1beta1 ISVC handle needed.
func AvailablePodSet(ctx context.Context, reads client.Reader, namespace, serviceName string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	slices, err := drain.EndpointSlicesForService(ctx, reads, namespace, serviceName)
	if err != nil {
		return nil, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Kind != "" && ep.TargetRef.Kind != "Pod" {
				continue
			}
			if !drain.EndpointAvailable(ep) {
				continue
			}
			out[ep.TargetRef.Name] = struct{}{}
		}
	}
	return out, nil
}

// CountReadyPods returns the number of pods whose ContainersReady
// condition is True.
func CountReadyPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if podreadiness.IsContainersReady(p) {
			n++
		}
	}
	return n
}

// CountServingPods counts pods that are BOTH ContainersReady AND have
// the controller's serving gate set to True — i.e., pods actually in
// the load-balancer rotation. This is the count MaxUnavailable budgets
// in coordination/ratio.go gate against; ContainersReady alone misses
// the case where the controller has flipped serving=False (in-place
// update drain, recreate Phase A) while containers technically remain
// Ready.
func CountServingPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if podreadiness.IsContainersReady(p) && podreadiness.IsServing(p) {
			n++
		}
	}
	return n
}

// CountScheduledPods returns the number of pods with Spec.NodeName set.
func CountScheduledPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if p.Spec.NodeName != "" {
			n++
		}
	}
	return n
}

// CountAvailablePods returns the number of pods named in
// availableByName. Used in conjunction with AvailablePodSet to compute
// per-Instance availability.
func CountAvailablePods(pods []*corev1.Pod, availableByName map[string]struct{}) int32 {
	var n int32
	for _, p := range pods {
		if _, ok := availableByName[p.Name]; ok {
			n++
		}
	}
	return n
}

// UniqueNodes returns the deterministically-sorted set of node names
// hosting at least one pod. Returns nil for empty / all-unscheduled
// so NodesOccupied round-trips through nil ↔ nil rather than nil ↔
// empty-slice.
func UniqueNodes(pods []*corev1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, p := range pods {
		if p.Spec.NodeName == "" {
			continue
		}
		seen[p.Spec.NodeName] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// DesiredPodCountByInstance maps each planned Instance index to its
// desired canonical pod count (sum of Runner sizes). Returns nil for
// an empty plan so callers fall back to the observed PodCount
// comparison (during scale-down with stale status entries).
func DesiredPodCountByInstance(plan ComponentPlan) map[int32]int32 {
	if len(plan.Instances) == 0 {
		return nil
	}
	out := make(map[int32]int32, len(plan.Instances))
	for _, inst := range plan.Instances {
		out[inst.Index] = inst.TotalPods()
	}
	return out
}

// DesiredFor returns the desired pod count for idx — the plan entry's
// value when present, otherwise the observed PodCount (no plan info ⇒
// use the observed count, for stale-status scale-down Instances).
func DesiredFor(desiredByIdx map[int32]int32, idx int32, observedPodCount int32) int32 {
	if desiredByIdx == nil {
		return observedPodCount
	}
	if d, ok := desiredByIdx[idx]; ok {
		return d
	}
	return observedPodCount
}

// InstanceMeetsThreshold is the shared classifier the Count*Instances
// helpers use: an Instance counts when at least its desired pod count
// of pods satisfy the relevant predicate (Ready / Serving / Available).
//
// Surge-tolerant: a naive `observed == PodCount` would mis-classify
// mid-surge Instances as not-Ready the whole time the +1 surge pod is
// starting up — the visible serving dip we explicitly want to avoid.
//
// desired falls back to observedPodCount when the plan has no entry
// for this index (scale-down with stale status).
func InstanceMeetsThreshold(observedPodCount, observedSatisfying, desired int32) bool {
	if observedPodCount == 0 {
		return false
	}
	if desired <= 0 {
		return observedSatisfying == observedPodCount
	}
	return observedSatisfying >= desired
}

// CountReadyInstances counts Instances whose desired-many pods are
// ContainersReady=True. See InstanceMeetsThreshold for the
// surge-tolerant semantic.
//
// Operates on workload.InstanceStatus so both adapters share the
// same classifier without converting back to a v1beta1 shape.
func CountReadyInstances(insts []InstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if InstanceMeetsThreshold(s.PodCount, s.ReadyPodCount, desired) {
			n++
		}
	}
	return n
}

// CountServingInstances counts Instances whose desired-many pods are
// BOTH ContainersReady AND serving=True. This is the count
// CheckUnavailabilityGate uses — "instances actually in the load
// balancer rotation right now."
func CountServingInstances(insts []InstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if InstanceMeetsThreshold(s.PodCount, s.ServingPodCount, desired) {
			n++
		}
	}
	return n
}

// CountAvailableInstances counts Instances whose desired-many pods
// are in EndpointSlice rotation. Strict sub-condition of Ready —
// kube-proxy only publishes Ready pods.
func CountAvailableInstances(insts []InstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if InstanceMeetsThreshold(s.PodCount, s.AvailablePodCount, desired) {
			n++
		}
	}
	return n
}

// CountUpdatedInstances returns (UpdatedReplicas, UpdatedReadyReplicas)
// for Instances on targetRevName. UpdatedReadyReplicas adds the Ready
// gate — what HPA's scale-aware rollout pacing reads. The Ready check
// shares the same surge-tolerant classifier as CountReadyInstances.
func CountUpdatedInstances(insts []InstanceStatus, targetRevName string, desiredByIdx map[int32]int32) (updated, updatedReady int32) {
	for _, s := range insts {
		if s.RunningRevision != targetRevName {
			continue
		}
		updated++
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if InstanceMeetsThreshold(s.PodCount, s.ReadyPodCount, desired) {
			updatedReady++
		}
	}
	return updated, updatedReady
}

// RolloutComplete is the CurrentRevision promotion gate: every Instance
// on targetRevName AND Phase=Ready.
//
// Operates on workload.InstanceStatus so both adapters share the same
// gate without converting back to a v1beta1 shape.
func RolloutComplete(insts []InstanceStatus, targetRevName string) bool {
	if len(insts) == 0 {
		return false
	}
	for _, s := range insts {
		if s.Phase != InstancePhaseReady {
			return false
		}
		if s.RunningRevision != targetRevName {
			return false
		}
	}
	return true
}

// FirstStuckPodForInstance returns the first pod in pods that is stuck
// in a terminal kubelet waiting state past the grace window, plus the
// kubelet reason string. Returns (nil, "") when no pod is stuck —
// caller skips escalation.
//
// "First" is order-deterministic by the input slice; callers that need
// a stable index should sort pods before calling. For event emission
// the order doesn't matter (the message names the specific pod).
//
// Adapter-agnostic counterpart of the omenative-side helper; takes
// nothing v1beta1-shaped.
func FirstStuckPodForInstance(pods []*corev1.Pod, now time.Time, grace time.Duration) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		// Skip pods marked for deletion — kubelet may be tearing them
		// down, and "ImagePullBackOff on a deleting pod" is not an
		// escalation signal (the pod is about to be gone anyway).
		if pod.DeletionTimestamp != nil {
			continue
		}
		if stuck, reason := PodStuckPullFailure(pod, now, grace); stuck {
			return pod, reason
		}
	}
	return nil, ""
}

// HasWedgedPodAgainstCurrent reports whether any pod carries a
// revision-hash label disagreeing with currentHash — i.e., a pod
// created for a revision the controller doesn't yet (or no longer)
// recognize as current.
//
// Pods missing the label are skipped (no signal). Empty currentHash
// means no revision has been promoted yet — treat every pod hash as
// matching so the wedged-recovery branch doesn't over-fire on the
// initial Create.
//
// Used by the wedged-pod recovery branch of the escalator: catches
// the post-surge shape where the state machine has been reset out of
// the transient phase set but a real wedged surge pod still exists.
func HasWedgedPodAgainstCurrent(pods []*corev1.Pod, currentHash, revisionHashLabel string) bool {
	if currentHash == "" || revisionHashLabel == "" {
		return false
	}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		hash := pod.Labels[revisionHashLabel]
		if hash == "" {
			continue
		}
		if hash != currentHash {
			return true
		}
	}
	return false
}

// CountersForInstance computes the per-Instance counter set from the
// live pod bucket. availableByPod is the map returned by
// AvailablePodSet.
func CountersForInstance(pods []*corev1.Pod, availableByPod map[string]struct{}) InstanceCounters {
	return InstanceCounters{
		PodCount:          int32(len(pods)),
		ReadyPodCount:     CountReadyPods(pods),
		ServingPodCount:   CountServingPods(pods),
		ScheduledPodCount: CountScheduledPods(pods),
		AvailablePodCount: CountAvailablePods(pods, availableByPod),
		NodesOccupied:     UniqueNodes(pods),
	}
}

// InstanceCounters is the field-level result of CountersForInstance.
// The caller maps fields onto the per-Instance status by assignment.
type InstanceCounters struct {
	PodCount          int32
	ReadyPodCount     int32
	ServingPodCount   int32
	ScheduledPodCount int32
	AvailablePodCount int32
	NodesOccupied     []string
}

// ComponentCounters is the field-level result of
// ComputeComponentCounters. The aggregator caller maps the fields onto
// the per-Component status block.
type ComponentCounters struct {
	Replicas             int32
	ReadyReplicas        int32
	ServingReplicas      int32
	AvailableReplicas    int32
	UpdatedReplicas      int32
	UpdatedReadyReplicas int32
}

// ComputeComponentCounters computes the Component-level counter set
// from the per-Instance statuses (already populated with per-Instance
// counters by CountersForInstance + caller assignment) and the desired
// per-Instance pod count map. When targetRevName is non-empty, the
// Updated* fields are also computed; an empty targetRevName leaves
// them at zero (the caller is responsible for not writing them in
// that case).
func ComputeComponentCounters(insts []InstanceStatus, desiredByIdx map[int32]int32, targetRevName string) ComponentCounters {
	out := ComponentCounters{
		Replicas:          int32(len(insts)),
		ReadyReplicas:     CountReadyInstances(insts, desiredByIdx),
		ServingReplicas:   CountServingInstances(insts, desiredByIdx),
		AvailableReplicas: CountAvailableInstances(insts, desiredByIdx),
	}
	if targetRevName != "" {
		out.UpdatedReplicas, out.UpdatedReadyReplicas = CountUpdatedInstances(insts, targetRevName, desiredByIdx)
	}
	return out
}

// String formats InstanceCounters for debug output.
func (c InstanceCounters) String() string {
	return fmt.Sprintf("pods=%d ready=%d serving=%d scheduled=%d available=%d nodes=%v",
		c.PodCount, c.ReadyPodCount, c.ServingPodCount, c.ScheduledPodCount, c.AvailablePodCount, c.NodesOccupied)
}

// EscalationParams bundles the inputs the stuck-pod escalator needs.
//
// Instances is observed AFTER any retry-loop status commit has
// landed. PodsByIndex is the per-Instance pod bucket the caller
// already computed for counters. CurrentRevisionHash is the
// ControllerRevision-name suffix (empty hash suppresses the wedged
// branch; nothing has been promoted yet). RevisionHashLabel is the
// adapter-specific pod-label key holding the per-pod revision hash.
type EscalationParams struct {
	Instances           []InstanceStatus
	PodsByIndex         map[int32][]*corev1.Pod
	CurrentRevisionHash string
	RevisionHashLabel   string
}

// EscalateStuckPodFailures walks every InstanceStatus and escalates to
// Phase=Failed when one of two conditions holds:
//
//  1. (Transient-phase path) The Instance has an in-flight Operation AND
//     its Phase is one of {Creating, Updating, Restarting, Migrating}
//     AND at least one of its pods has been in a terminal kubelet
//     waiting state past StuckPodFailureGracePeriod.
//
//  2. (Wedged-pod recovery path) The Instance has at least one pod
//     whose revision-hash label disagrees with CurrentRevisionHash AND
//     that pod is stuck past the grace window. Catches the post-surge
//     cluster shape where the per-Instance state machine has been
//     reset out of the transient phase set but a real wedged surge
//     pod still exists.
//
// Idempotent: a second pass on an already-Failed Instance is a no-op
// (no status write, no event). The coordination layer sees the
// Phase=Failed flip on the next read and lifts it to Component.Failed
// → group Phase=Failed.
//
// Adapter-agnostic by construction: writes Phase=Failed via
// input.MutateInstance, emits the operator-facing event via
// input.WarnInstanceFailed. The ISVC adapter wires both behind
// status.MutateInstance / status.RecordWarning; the IR adapter will
// wire its own equivalents.
func EscalateStuckPodFailures(ctx context.Context, input ReconcileInput, p EscalationParams) error {
	now := time.Now()
	for _, s := range p.Instances {
		if !ShouldCheckForStuckPods(&s, p.PodsByIndex[s.Index], p.CurrentRevisionHash, p.RevisionHashLabel) {
			continue
		}
		stuckPod, reason := FirstStuckPodForInstance(p.PodsByIndex[s.Index], now, StuckPodFailureGracePeriod)
		if stuckPod == nil {
			continue
		}
		// Idempotent: skip the mutate when already at Phase=Failed so
		// the operator-facing event fires exactly once per escalation.
		if s.Phase == InstancePhaseFailed {
			continue
		}
		if err := StampInstanceFailedForStuckPod(ctx, input, s.Index); err != nil {
			return fmt.Errorf("stamp Phase=Failed (instance=%d): %w", s.Index, err)
		}
		input.WarnInstanceFailed(s.Index, stuckPod.Name, reason)
	}
	return nil
}

// ShouldCheckForStuckPods reports whether s qualifies for the
// fast-failure check. Two paths qualify:
//
//   - Transient-phase path: an Instance with an in-flight Operation in
//     the {Creating, Updating, Restarting, Migrating} set.
//
//   - Wedged-pod recovery path: an Instance with at least one pod
//     whose revision-hash label disagrees with currentHash, regardless
//     of Phase / Operation. Catches the wedged-state recovery case
//     where the controller's per-Instance bookkeeping has been reset
//     while a real stuck pod still exists.
//
// Skips Failed (already done) and Deleting (Delete pass owns its own
// deadline). Empty currentHash suppresses the wedged branch (no
// rollout has ever promoted → over-firing risk during initial Create).
func ShouldCheckForStuckPods(s *InstanceStatus, pods []*corev1.Pod, currentHash, revisionHashLabel string) bool {
	if s == nil {
		return false
	}
	if s.Phase == InstancePhaseFailed || s.Phase == InstancePhaseDeleting {
		return false
	}
	if s.Operation != nil {
		switch s.Phase {
		case InstancePhaseCreating,
			InstancePhaseUpdating,
			InstancePhaseRestarting,
			InstancePhaseMigrating:
			return true
		}
	}
	return HasWedgedPodAgainstCurrent(pods, currentHash, revisionHashLabel)
}

// StampInstanceFailedForStuckPod escalates Phase to Failed via
// input.MutateInstance. Mirrors the omenative-side
// stampInstanceFailedForStuckPod guard logic: a fresh-empty slot
// (Phase=="") is treated as a sentinel for "resurrected slot, don't
// touch", a Phase=Failed slot is a no-op, and any other Phase flips to
// Failed.
//
// The Operation field is preserved (when present) — operators want to
// see WHAT was in flight when the kubelet wedge surfaced.
func StampInstanceFailedForStuckPod(ctx context.Context, input ReconcileInput, idx int32) error {
	return input.MutateInstance(ctx, idx, func(s *InstanceStatus) bool {
		if s.Phase == "" {
			// Fresh-empty slot from MutateInstance's append path — the
			// real InstanceStatus was deleted out from under us
			// (migrate-abandon cleanup, etc.). Don't resurrect.
			return false
		}
		if s.Phase == InstancePhaseFailed {
			return false
		}
		s.Phase = InstancePhaseFailed
		return true
	})
}
