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
// CreateContainerConfigError, CreateContainerError) escalates the owning
// Instance to Phase=Failed. Tuned to debounce the transient
// ContainerCreating / ImagePulling states that a real kubelet briefly
// passes through on initial pod placement — 60 seconds is comfortable
// headroom for a 5–10 second pull cycle on a healthy node before
// committing to "this is genuinely stuck."
//
// Exported so tests can lower the value to keep test runtime sane while
// still exercising the real escalation path (a hardcoded constant would
// force tests to spin for 60+ seconds per case, gating the suite on a
// wall-clock that adds nothing to coverage).
//
// Lives in workload/ rather than the ISVC-shaped omenative/ because the
// IR adapter will consume the same escalator behavior; the value is
// shared across both adapters.
var StuckPodFailureGracePeriod = 60 * time.Second

// DefaultRevisionRetention caps non-live ControllerRevisions per
// workload (StatefulSet default; rollback headroom without unbounded
// etcd growth). Exposed for adapters that drive
// revision.RetainControllerRevisions from their own retention policy.
const DefaultRevisionRetention = 10

// terminalPullFailureReasons enumerates kubelet container state.Waiting
// reasons that cannot self-recover. Distinct from PodInitializing or
// ContainerCreating, which are transient and resolve once the kubelet
// finishes its setup. Conservative list — every entry below has been
// observed in the wild as a wedge that requires either a spec change
// (fix the image tag) or a node-level fix (registry creds); neither is
// something the controller can do on its own.
//
// Mirrors the same constant in omenative/status/pod_failures.go so the
// workload-side PodStuckPullFailure has no dependency on the omenative
// status package.
var terminalPullFailureReasons = map[string]struct{}{
	"ErrImagePull":               {},
	"ImagePullBackOff":           {},
	"InvalidImageName":           {},
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
}

// PodStuckPullFailure returns true when pod has at least one container
// in a terminal pull-failure waiting state AND that container has been
// stuck for ≥ grace from `now`. The grace window debounces transient
// kubelet states (ContainerCreating → ImagePullBackOff transitions on
// slow registries) so the controller doesn't fire on a pod that's about
// to recover within seconds.
//
// Returns (true, reason) when stuck, (false, "") otherwise. The reason
// is the kubelet's Waiting.Reason string for the first stuck container,
// suitable for the operator-facing event message.
//
// Timing source: stuck duration is measured from the pod's
// CreationTimestamp, NOT from the container's individual state
// transition time (which kubelet doesn't expose per-state). For
// ImagePullBackOff specifically, kubelet has already retried at least
// once before flipping to BackOff (typically ~30s after pod creation
// on a 404), so a 60s grace from pod creation reliably catches stuck
// pulls while ignoring the brief startup window where the container
// transitions through normal Waiting states (PodInitializing,
// ContainerCreating, ImagePulling).
//
// Adapter-agnostic counterpart of omenative/status.PodStuckPullFailure;
// the omenative copy delegates to this one so both adapters share a
// single classifier.
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
// in any EndpointSlice for serviceName. drain.EndpointAvailable excludes
// terminating-but-Ready endpoints — they're not getting new traffic.
// Single slice list (vs. drain.IsPodInRotation per pod) avoids N+1 reads.
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
// condition is True. Workload-agnostic counter consumed by the
// per-Instance aggregator.
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
// hosting at least one pod in pods. Unscheduled pods are skipped.
// Returns nil for an empty input or an all-unscheduled set so the
// per-Instance NodesOccupied field round-trips through nil ↔ nil
// rather than nil ↔ empty-slice.
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
// comparison (pre-existing behavior for Instances absent from the
// plan — e.g., during scale-down with stale status entries).
//
// Workload-typed counterpart of the omenative-side helper; takes the
// workload.ComponentPlan so both adapters' aggregators consume the
// same shape.
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
// use the observed count, matching pre-fix behavior for stale-status
// scale-down Instances).
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
// helpers share: an Instance "counts" when at least its desired pod
// count of pods satisfy the relevant predicate (Ready / Serving /
// Available). Differs from the pre-fix `observed == PodCount`
// comparison: SurgeThenDrain transiently puts an extra pod alive on
// the Instance, lifting PodCount above desired. The old comparison
// would mis-classify mid-surge Instances as not-Ready / not-Serving
// the whole time the surge pod is starting up — that's the user-
// visible serving dip seen in PR #238 sampler runs.
//
// The desired value falls back to observedPodCount when the plan
// has no entry for this index (scale-down with stale status).
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

// HasWedgedPodAgainstCurrent reports whether any pod in pods carries a
// revision-hash label that disagrees with currentHash — i.e., the pod
// was created for a revision the controller no longer (or doesn't yet)
// recognize as current. The label key is taken from selectorLabels —
// the workload adapter populates input.Key.SelectorLabels with the
// adapter-specific revision-hash key, and the workload-side escalator
// reads the value through that label.
//
// Pods missing the label are skipped (no signal). Empty currentHash
// means the workload has never promoted a revision — treat every pod
// hash as matching so the wedged-recovery branch doesn't over-fire on
// the initial Create.
//
// Used by the wedged-pod recovery branch of the escalator (task #52
// follow-up to PR #257): catches the post-surge cluster shape where
// the per-Instance state machine has been reset out of the transient
// phase set but a real wedged surge pod still exists.
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

// CountersForInstance computes the per-Instance counter set the
// aggregator stamps onto an InstanceStatus from the live pod bucket.
// Returns the field values in the order the consumer assigns them.
//
// availableByPod is the map of EndpointSlice-Ready pod names returned
// by AvailablePodSet.
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
// The aggregator caller maps the fields onto the per-Instance status
// (v1beta1.OMENativeInstanceStatus for the ISVC adapter,
// InferenceReplicaInstanceStatus for the IR adapter) by assignment.
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

// String formats InstanceCounters for debug / log output. Useful in
// tests that want a single-line representation of the counter set.
func (c InstanceCounters) String() string {
	return fmt.Sprintf("pods=%d ready=%d serving=%d scheduled=%d available=%d nodes=%v",
		c.PodCount, c.ReadyPodCount, c.ServingPodCount, c.ScheduledPodCount, c.AvailablePodCount, c.NodesOccupied)
}

// EscalationParams bundles the inputs the workload-side stuck-pod
// escalator needs. Pulled into its own struct so the (already-wide)
// EscalateStuckPodFailures signature stays single-line.
//
// Instances is the workload-typed view of the per-Component
// InstanceStatuses the caller observes AFTER any retry-loop status
// commit has landed. The escalator reads Phase / Operation / Index off
// each entry to decide whether to escalate.
//
// PodsByIndex is the per-Instance pod bucket the caller already
// computed for counters (see query.BucketPodsByInstanceIdx). The
// escalator reads it via the InstanceStatus.Index to avoid re-listing
// pods.
//
// CurrentRevisionHash is the workload's CurrentRevision hash (the
// ControllerRevision-name suffix, NOT the full name). The wedged-pod
// recovery branch compares it against each pod's revision-hash label
// to detect post-#257 cluster shapes where a stuck surge pod still
// exists but the per-Instance state machine has been reset. Empty
// hash suppresses the wedged branch (no rollout has ever promoted →
// nothing to escalate from).
//
// RevisionHashLabel is the pod-label key the wedged-pod branch reads
// the per-pod revision hash from. The caller (the ISVC adapter today,
// the IR adapter tomorrow) passes the adapter-specific constant — for
// ISVC this is `ome.io/revision-hash` via workload/query.LabelRevisionHash.
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
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(s.Index, stuckPod.Name, reason)
		}
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
