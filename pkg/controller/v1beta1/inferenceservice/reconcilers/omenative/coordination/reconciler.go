package coordination

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

// ReconcileInputs is the input bag the top-level OMENative dispatcher
// passes into the coordination layer once per ISVC reconcile (after
// every Component's Loop A has run).
type ReconcileInputs struct {
	// ISVC is the parent InferenceService. The coordination layer
	// reads ISVC.Spec.RolloutCoordination and writes
	// ISVC.Status.RolloutCoordination + per-Component
	// Status.Components.<c>.Traffic.
	ISVC *v1beta1.InferenceService

	// Client is the cached controller-runtime client. Used for
	// per-revision Service ensure/GC and live pod reads.
	Client client.Client

	// Reader is the live API reader for correctness-critical pod
	// reads.
	Reader client.Reader

	// Recorder emits K8s Events for coordination transitions.
	Recorder record.EventRecorder

	// Now is the wall-clock used for LastTransitionTime; callers may
	// override for tests. Zero defaults to time.Now().
	Now time.Time
}

// Result reports what the coordination layer did this reconcile. The
// caller uses it to:
//   - decide whether to write status (Status* fields)
//   - emit metrics for surge skew / failures
//   - log structured progress lines
type Result struct {
	// GroupStatuses are the per-group statuses freshly computed this
	// reconcile.
	GroupStatuses []v1beta1.RolloutCoordinationGroupStatus

	// PerRevisionServicesEnsured counts how many per-revision Services
	// the reconcile created or updated. Surfaces in logs.
	PerRevisionServicesEnsured int

	// SkewRejections counts the Components whose surge attempts were
	// rejected by RatioBalanced pacing this reconcile.
	SkewRejections int
}

// Reconcile is the entry point called by the top-level OMENative
// reconciler once per ISVC reconcile, after every Component's
// Loop A has run. It:
//
//  1. resolves the spec.rolloutCoordination.groups[] declaration
//  2. observes each Component's current per-revision pod state
//  3. drives the per-policy state machine
//  4. ensures per-revision Services
//  5. writes Status.Components.<c>.Traffic + RolloutCoordination
//  6. records metrics + emits events
//
// Returns a non-nil Result on success and a non-nil error on failure.
// On error, the caller surfaces the error to the parent reconcile
// chain — coordination is a status-only layer so a partial reconcile
// is safe to retry next pass.
func Reconcile(ctx context.Context, in ReconcileInputs) (*Result, error) {
	if in.ISVC == nil {
		return nil, fmt.Errorf("coordination.Reconcile: nil ISVC")
	}
	if in.Client == nil {
		return nil, fmt.Errorf("coordination.Reconcile: nil client")
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	groups := ResolveGroups(in.ISVC.Spec.RolloutCoordination)
	result := &Result{}

	// Coordination is opt-in via spec.rolloutCoordination.groups[].
	// When unset, the entire layer is a no-op: no per-revision
	// Services are emitted, no Status.Traffic[] is written, no
	// per-group state machine runs. This preserves the per-reconcile
	// I/O cost and observable-status shape from before coordination
	// existed, so ISVCs that haven't opted in see zero behavior
	// change.
	if len(groups) == 0 {
		return result, nil
	}

	componentsTouched := collectOMENativeComponents(in.ISVC)
	if len(componentsTouched) == 0 {
		return result, nil
	}

	perRevisionPods, err := observePerRevisionPods(ctx, in.Reader, in.ISVC, componentsTouched)
	if err != nil {
		return nil, fmt.Errorf("observe per-revision pods: %w", err)
	}

	// Ensure per-revision Services for every (component, revisionHash)
	// pair that has at least one live pod. Services are additive K8s
	// resources that cost almost nothing; emit them whenever a group
	// is declared so the HTTPRoute weighted-backendRef consumer has
	// the per-revision backends to point at.
	for _, c := range componentsTouched {
		for hash := range perRevisionPods[c] {
			if hash == "" {
				continue
			}
			if _, err := EnsurePerRevisionServices(ctx, in.Client, in.ISVC, c, hash); err != nil {
				return nil, fmt.Errorf("ensure per-revision service for %s/%s: %w", c, hash, err)
			}
			result.PerRevisionServicesEnsured++
		}
		SetPerRevisionServiceCount(in.ISVC.Name, string(c), float64(len(perRevisionPods[c])))
	}

	// Write per-Component Traffic[] from observed pod distribution
	// (producer-side contract feeding the HTTPRoute weighted-
	// backendRef consumer).
	updateTrafficStatus(in.ISVC, componentsTouched, perRevisionPods)

	// Drive the per-group state machine.
	for _, g := range groups {
		if err := ValidateGroupShape(g); err != nil {
			return nil, fmt.Errorf("invalid coordination group shape: %w", err)
		}
		obs := buildGroupObservation(in.ISVC, g, perRevisionPods)
		tr := ComputeTransition(obs)
		groupStatus := BuildGroupStatus(g, tr, nil, now)

		// Record observability for this group.
		RecordGroupPhase(in.ISVC.Name, g.Name, string(tr.Phase))
		if tr.Phase == v1beta1.CoordinationPhaseFailed {
			RecordGroupFailure(in.ISVC.Name, g.Name, tr.Message)
		}

		// Event emission on phase boundary. Compare to previously-
		// observed phase.
		emitPhaseEvents(in.Recorder, in.ISVC, g, tr)

		result.GroupStatuses = append(result.GroupStatuses, groupStatus)
	}

	mergeAndPersistGroupStatuses(in.ISVC, result.GroupStatuses)

	// Stamp the RolloutCoordinationReady condition for groups that
	// have been observed at least once.
	if len(result.GroupStatuses) > 0 {
		SetRolloutCoordinationReady(in.ISVC, result.GroupStatuses, now)
	}

	return result, nil
}

// collectOMENativeComponents returns the Components on the ISVC that
// declare ome.io/deploymentMode=OMENative. Only those Components
// participate in coordination's per-revision service emission /
// traffic write.
func collectOMENativeComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	if isvc == nil {
		return nil
	}
	out := make([]v1beta1.ComponentType, 0, 3)
	if isvc.Spec.Engine != nil && componentIsOMENative(&isvc.Spec.Engine.ComponentExtensionSpec) {
		out = append(out, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil && componentIsOMENative(&isvc.Spec.Decoder.ComponentExtensionSpec) {
		out = append(out, v1beta1.DecoderComponent)
	}
	if isvc.Spec.Router != nil && componentIsOMENative(&isvc.Spec.Router.ComponentExtensionSpec) {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}

// componentIsOMENative reports whether the Component declares
// ome.io/deploymentMode=OMENative via its Annotations.
func componentIsOMENative(ext *v1beta1.ComponentExtensionSpec) bool {
	if ext == nil {
		return false
	}
	return ext.Annotations[constants.DeploymentMode] == string(constants.OMENative)
}

// observePerRevisionPods returns per-Component, per-revisionHash pod
// counts (both total and Ready). Reads pods via the live reader so
// observations don't lag behind reality.
func observePerRevisionPods(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType) (map[v1beta1.ComponentType]map[string]int32, error) {
	if reads == nil {
		return map[v1beta1.ComponentType]map[string]int32{}, nil
	}
	out := make(map[v1beta1.ComponentType]map[string]int32, len(components))
	for _, c := range components {
		pods := &corev1.PodList{}
		sel := labels.SelectorFromSet(labels.Set{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(c),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		})
		if err := reads.List(ctx, pods, client.InNamespace(isvc.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
			return nil, fmt.Errorf("list pods for %s/%s: %w", isvc.Namespace, c, err)
		}
		byHash := make(map[string]int32, 4)
		for i := range pods.Items {
			hash := pods.Items[i].Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			byHash[hash]++
		}
		out[c] = byHash
	}
	return out, nil
}

// updateTrafficStatus writes Status.Components.<c>.Traffic[] from
// observed per-revision pod counts. Idempotent; no-op writes are
// skipped to avoid bouncing the apiserver.
func updateTrafficStatus(isvc *v1beta1.InferenceService, components []v1beta1.ComponentType, perRevisionPods map[v1beta1.ComponentType]map[string]int32) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	for _, c := range components {
		hashes := perRevisionPods[c]
		if len(hashes) == 0 {
			continue
		}
		var total int32
		for _, n := range hashes {
			total += n
		}
		latestHash := latestRevisionHash(isvc, c)
		weights := ComputeWeightsFromPods(hashes, total, latestHash)
		traffic := BuildTrafficTargets(isvc.Name, c, weights)
		// Only write when something changed.
		cs := isvc.Status.Components[c]
		if !TrafficDiffersMeaningfully(traffic, cs.Traffic) {
			continue
		}
		cs.Traffic = traffic
		if latestHash != "" {
			cs.LatestReadyRevision = PerRevisionServiceName(isvc.Name, c, latestHash)
		}
		// LatestRolledoutRevision advances to the per-revision Service
		// name once the rollout is at 100% on a single revision.
		if len(hashes) == 1 {
			for hash := range hashes {
				cs.LatestRolledoutRevision = PerRevisionServiceName(isvc.Name, c, hash)
			}
		}
		isvc.Status.Components[c] = cs
	}
}

// latestRevisionHash returns the ControllerRevision hash this
// Component is converging toward. Read from
// OMENativeComponentStatus.UpdateRevision; falls back to
// CurrentRevision.
func latestRevisionHash(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if isvc == nil {
		return ""
	}
	cs, ok := isvc.Status.Components[c]
	if !ok || cs.OMENative == nil {
		return ""
	}
	if name := cs.OMENative.UpdateRevision; name != "" {
		return revisionHashFromName(isvc.Name, c, name)
	}
	if name := cs.OMENative.CurrentRevision; name != "" {
		return revisionHashFromName(isvc.Name, c, name)
	}
	return ""
}

// revisionHashFromName extracts the hash suffix from a
// ControllerRevision name of the form `<isvc>-<component>-<hash>`.
// Returns "" when the name doesn't match the expected shape.
func revisionHashFromName(isvc string, c v1beta1.ComponentType, name string) string {
	prefix := isvc + "-" + string(c) + "-"
	if len(name) <= len(prefix) {
		return ""
	}
	if name[:len(prefix)] != prefix {
		return ""
	}
	return name[len(prefix):]
}

// buildGroupObservation builds the GroupObservation the state machine
// consumes from observed pod counts + OMENativeComponentStatus.
func buildGroupObservation(isvc *v1beta1.InferenceService, g ResolvedGroup, perRevisionPods map[v1beta1.ComponentType]map[string]int32) GroupObservation {
	obs := GroupObservation{
		Group:      g,
		Components: make(map[v1beta1.ComponentType]ComponentObservation, len(g.Components)),
	}
	for _, c := range g.Components {
		obs.Components[c] = buildComponentObservation(isvc, c, perRevisionPods[c])
	}
	if isvc.Annotations[paussedAnnotationKey()] == "true" {
		obs.PausedGlobal = true
	}
	return obs
}

// paussedAnnotationKey returns the ome.io/rollout-paused annotation
// key. Declared inline rather than in constants.go to keep the
// coordination subpackage's surface small.
func paussedAnnotationKey() string {
	return "ome.io/rollout-paused"
}

// buildComponentObservation aggregates one Component's observation from
// OMENativeComponentStatus and per-revision pod counts.
func buildComponentObservation(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, perRevisionPods map[string]int32) ComponentObservation {
	out := ComponentObservation{
		Component: c,
	}
	cs, ok := isvc.Status.Components[c]
	if !ok || cs.OMENative == nil {
		return out
	}
	out.DesiredReplicas = cs.OMENative.Replicas
	out.TotalPods = sumPodCounts(perRevisionPods)
	out.ReadyPods = cs.OMENative.ReadyReplicas
	out.NewRevisionPods = cs.OMENative.UpdatedReplicas
	out.NewRevisionReadyPods = cs.OMENative.UpdatedReadyReplicas
	currentHash := revisionHashFromName(isvc.Name, c, cs.OMENative.CurrentRevision)
	targetHash := revisionHashFromName(isvc.Name, c, cs.OMENative.UpdateRevision)
	out.CurrentRevisionHash = currentHash
	out.TargetRevisionHash = targetHash
	out.RolloutInFlight = targetHash != "" && targetHash != currentHash
	return out
}

// sumPodCounts sums all values in a per-revision pod count map.
func sumPodCounts(in map[string]int32) int32 {
	var sum int32
	for _, n := range in {
		sum += n
	}
	return sum
}

// emitPhaseEvents fires K8s events on phase transitions for the
// observer to understand what the coordination layer is doing.
// Stateless — the same event may fire multiple times across
// reconciles if the phase doesn't change. Callers can dedupe via
// the recorder's standard event aggregation.
func emitPhaseEvents(recorder record.EventRecorder, isvc *v1beta1.InferenceService, g ResolvedGroup, tr GroupTransition) {
	if recorder == nil {
		return
	}
	switch tr.Phase {
	case v1beta1.CoordinationPhaseSurging:
		recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonGroupSurging,
			"coordination group %s surging: %s", g.Name, tr.Message)
	case v1beta1.CoordinationPhaseShifting:
		recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonGroupShifting,
			"coordination group %s shifting: %s", g.Name, tr.Message)
	case v1beta1.CoordinationPhaseFailed:
		recorder.Eventf(isvc, corev1.EventTypeWarning, EventReasonGroupFailed,
			"coordination group %s failed: %s", g.Name, tr.Message)
	case v1beta1.CoordinationPhaseIdle:
		recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonGroupCompleted,
			"coordination group %s idle: %s", g.Name, tr.Message)
	}
	if tr.CompositePhase == v1beta1.CompositePhaseSequentialAwaiting {
		recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonSequentialAwaitingNext,
			"sequential group %s awaiting next Component: %s", g.Name, tr.Message)
	}
}

// mergeAndPersistGroupStatuses installs the freshly-computed group
// statuses into ISVC.Status.RolloutCoordination. Preserves transition
// timestamps when the phase didn't change.
func mergeAndPersistGroupStatuses(isvc *v1beta1.InferenceService, fresh []v1beta1.RolloutCoordinationGroupStatus) {
	if len(fresh) == 0 {
		return
	}
	merged := isvc.Status.RolloutCoordination
	for _, g := range fresh {
		merged = MergeGroupStatus(merged, g)
	}
	// Stable order by group name for deterministic status writes.
	if merged != nil {
		sort.SliceStable(merged.Groups, func(i, j int) bool {
			return merged.Groups[i].Name < merged.Groups[j].Name
		})
	}
	isvc.Status.RolloutCoordination = merged
}
