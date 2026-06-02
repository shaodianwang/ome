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
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// ReconcileInputs is the input bag the top-level OMENative dispatcher
// passes into the coordination layer once per ISVC reconcile (after
// every Component's per-Component reconciler has run).
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
//
// RatioBalanced skew is published exclusively via the ratio_skew_total
// Prometheus counter and the RatioSkewRejected event — see the
// RatioSkewRejected branch in Reconcile — so Result intentionally does
// not carry a per-reconcile skew count.
type Result struct {
	// GroupStatuses are the per-group statuses freshly computed this
	// reconcile.
	GroupStatuses []v1beta1.RolloutCoordinationGroupStatus

	// PerRevisionServicesEnsured counts how many per-revision Services
	// the reconcile created or updated. Surfaces in logs.
	PerRevisionServicesEnsured int
}

// Reconcile is the entry point called by the top-level OMENative
// reconciler once per ISVC reconcile, after every Component's
// per-Component reconciler has run. It:
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

	// Bind a logger keyed to the ISVC so every downstream V(1)
	// breadcrumb (per-group transition, per-revision Service GC,
	// skew rejection) inherits the same anchor and operators can
	// `grep isvc=foo` across the coordination layer.
	logger := log.FromContext(ctx).WithValues(
		"isvc", client.ObjectKeyFromObject(in.ISVC),
		"groupCount", len(groups))
	ctx = log.IntoContext(ctx, logger)

	// Per-revision Service / Traffic[] producer: emitted for EVERY
	// OMENative-mode Component, with or without a rolloutCoordination
	// group declared. These artifacts are the data-plane for the
	// HTTPRoute weighted-backendRef consumer; an opt-in gate would
	// break the traffic layer for ISVCs that haven't (yet) declared a
	// coord group but still want per-revision routing.
	//
	// The per-group state machine (Status.RolloutCoordination,
	// RolloutCoordinationReady condition, group events) remains
	// opt-in — those only run when groups are declared.
	componentsTouched := collectOMENativeComponents(in.ISVC)
	if len(componentsTouched) == 0 {
		logger.V(1).Info("Reconcile entry; no OMENative Components on ISVC, nothing to coordinate")
		return result, nil
	}
	logger.V(1).Info("Reconcile entry",
		"componentCount", len(componentsTouched),
		"components", componentsTouched)

	// Informational hint: ISVC has multiple OMENative Components but
	// no rolloutCoordination block declared. Default behavior rolls
	// each Component independently, which can break PD pairs or
	// shared-state workloads. Operator may want a coord group.
	// K8s event recorder aggregates by (reason, involvedObject) so
	// repeated firings collapse to one event with a count.
	if len(componentsTouched) >= 2 && len(groups) == 0 && in.Recorder != nil {
		in.Recorder.Eventf(in.ISVC, corev1.EventTypeNormal, EventReasonConsiderCoordinationGroup,
			"InferenceService has %d OMENative Components (%v) but no rolloutCoordination block declared; consider adding a coord group to define rollout behavior across them",
			len(componentsTouched), componentsTouched)
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
		// Compute the multi-pod predicate once per Component — the
		// answer is identical across every revisionHash on that
		// Component (Worker.Size is a per-Component spec field, not
		// per-revision). Multi-pod Components route traffic only to
		// rank-0; the routing Service selector picks that up.
		multiPod := isvcutils.IsMultiPodComponent(in.ISVC, c)
		for hash := range perRevisionPods[c] {
			if hash == "" {
				continue
			}
			if _, err := EnsurePerRevisionServices(ctx, in.Client, in.ISVC, c, hash, multiPod); err != nil {
				return nil, fmt.Errorf("ensure per-revision service for %s/%s: %w", c, hash, err)
			}
			result.PerRevisionServicesEnsured++
		}
		SetPerRevisionServiceCount(in.ISVC.Name, string(c), float64(len(perRevisionPods[c])))

		// Delete per-revision Service pairs whose revision-hash has no
		// live pods. Safe because we Ensure only for hashes with pods —
		// a hash with zero pods is genuinely past retention. Pods added
		// between the list and the delete re-Ensure on the next reconcile.
		if err := gcOrphanedPerRevisionServices(ctx, in.Client, in.ISVC, c, perRevisionPods[c]); err != nil {
			return nil, fmt.Errorf("gc per-revision services for %s: %w", c, err)
		}
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
		// Snapshot Original on first observation, then preserve it
		// across reconciles via the previously-written ObservedRatio.
		// Always refresh Current + NewPods from the live observation.
		ratio := buildRatioState(in.ISVC, g, obs, perRevisionPods)
		obs.OriginalReplicas = ratio.Original
		obs.Now = now
		obs.PreviousPhaseEnteredAt = previousPhaseEnteredAt(in.ISVC, g.Name)
		tr := ComputeTransition(obs)
		groupStatus := BuildGroupStatus(g, tr, ratio, now)

		// Record observability for this group.
		prevPhase := previousGroupPhase(in.ISVC, g.Name)
		RecordGroupPhase(in.ISVC.Name, g.Name, string(tr.Phase))
		// Steady-state breadcrumb (no transition): V(1) so operators
		// can `grep group=foo` without drowning the log under normal
		// load. Includes phase + policy + componentCount + rollout
		// count so post-mortem on a misfire can correlate the inputs
		// the per-policy transition function saw.
		rolloutInFlight := 0
		for _, comp := range obs.Components {
			if comp.RolloutInFlight {
				rolloutInFlight++
			}
		}
		logger.V(1).Info("Group transition computed",
			"group", g.Name,
			"policy", g.Policy,
			"phase", tr.Phase,
			"componentCount", len(g.Components),
			"rolloutInFlight", rolloutInFlight,
			"ratioSkewRejected", tr.RatioSkewRejected)
		if prevPhase != "" && string(prevPhase) != string(tr.Phase) {
			RecordGroupTransition(in.ISVC.Name, g.Name, string(prevPhase), string(tr.Phase))
			// Real Phase transition: Info-level so it shows up in
			// operator-facing dashboards alongside the K8s event the
			// emitPhaseEvents call below records.
			logger.Info("Group phase transition",
				"group", g.Name,
				"policy", g.Policy,
				"fromPhase", prevPhase,
				"toPhase", tr.Phase,
				"message", tr.Message)
		}
		if tr.Phase == v1beta1.CoordinationPhaseFailed {
			RecordGroupFailure(in.ISVC.Name, g.Name, tr.Message)
		}

		// RollingUpdate mixed-pairing observability: emit when distinct
		// revision hashes coexist across Components (RollingUpdate
		// permits this but operators want to know).
		if g.Policy == v1beta1.CoordinationPolicyRollingUpdate && hasMixedPairing(g, perRevisionPods) {
			RecordMixedPairing(in.ISVC.Name, g.Name)
			if in.Recorder != nil {
				in.Recorder.Eventf(in.ISVC, corev1.EventTypeNormal, EventReasonMixedPairingObserved,
					"RollingUpdate group %s observed distinct per-Component revision hashes", g.Name)
			}
		}

		// RatioBalanced skew rejection: fire metric + event when
		// EvaluateSurge zeroed at least one Component's surge budget
		// to preserve the cross-Component ratio.
		if tr.RatioSkewRejected {
			RecordRatioSkew(in.ISVC.Name, g.Name)
			if in.Recorder != nil {
				in.Recorder.Eventf(in.ISVC, corev1.EventTypeNormal, EventReasonRatioSkewRejected,
					"RatioBalanced pacing rejected surge in group %s to preserve cross-Component ratio", g.Name)
			}
		}

		emitPhaseEvents(in.Recorder, in.ISVC, g, tr)
		emitSequentialDeferredEvent(in.Recorder, in.ISVC, g, obs, previousActiveComponent(in.ISVC, g.Name))

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
// LifecycleStatus.UpdateRevision; falls back to
// CurrentRevision.
func latestRevisionHash(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if isvc == nil {
		return ""
	}
	cs, ok := isvc.Status.Components[c]
	if !ok || cs.Lifecycle == nil {
		return ""
	}
	if name := cs.Lifecycle.UpdateRevision; name != "" {
		return revisionHashFromName(isvc.Name, c, name)
	}
	if name := cs.Lifecycle.CurrentRevision; name != "" {
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
// consumes from observed pod counts + LifecycleStatus.
func buildGroupObservation(isvc *v1beta1.InferenceService, g ResolvedGroup, perRevisionPods map[v1beta1.ComponentType]map[string]int32) GroupObservation {
	obs := GroupObservation{
		Group:      g,
		Components: make(map[v1beta1.ComponentType]ComponentObservation, len(g.Components)),
	}
	for _, c := range g.Components {
		obs.Components[c] = buildComponentObservation(isvc, c, perRevisionPods[c])
	}
	if isvc.Annotations[PausedRolloutAnnotation] == "true" {
		obs.PausedGlobal = true
	}
	return obs
}

// PausedRolloutAnnotation is the ISVC annotation operators set to
// "true" to freeze every coordination group at its current phase.
// Declared inline rather than in pkg/constants to keep the
// coordination subpackage's surface small.
const PausedRolloutAnnotation = "ome.io/rollout-paused"

// buildComponentObservation aggregates one Component's observation from
// LifecycleStatus and per-revision pod counts.
func buildComponentObservation(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, perRevisionPods map[string]int32) ComponentObservation {
	out := ComponentObservation{
		Component: c,
	}
	cs, ok := isvc.Status.Components[c]
	if !ok || cs.Lifecycle == nil {
		return out
	}
	out.DesiredReplicas = cs.Lifecycle.Replicas
	out.TotalPods = sumPodCounts(perRevisionPods)
	out.ReadyPods = cs.Lifecycle.ReadyReplicas
	out.ServingPods = cs.Lifecycle.ServingReplicas
	out.NewRevisionPods = cs.Lifecycle.UpdatedReplicas
	out.NewRevisionReadyPods = cs.Lifecycle.UpdatedReadyReplicas
	currentHash := revisionHashFromName(isvc.Name, c, cs.Lifecycle.CurrentRevision)
	targetHash := revisionHashFromName(isvc.Name, c, cs.Lifecycle.UpdateRevision)
	out.CurrentRevisionHash = currentHash
	out.TargetRevisionHash = targetHash
	out.RolloutInFlight = targetHash != "" && targetHash != currentHash

	// Per-Instance failure rollup: any Instance escalated to
	// Phase=Failed by ExpireOperationsIfDeadlinePassed (or any other
	// terminal-failure path) marks the whole Component Failed. The
	// per-policy state machines (BlueGreen, RollingUpdate, Sequential,
	// Independent) all read Component.Failed and translate it into the
	// group's Failed phase — operators see Phase=Failed instead of the
	// rollout sitting at Surging / Waiting indefinitely after a per-
	// Instance deadline blew past.
	failedInstances := collectFailedInstanceIndices(cs.Lifecycle.InstanceStatuses)
	if len(failedInstances) > 0 {
		out.Failed = true
		out.FailureMessage = formatFailedInstancesMessage(c, failedInstances)
	}
	return out
}

// collectFailedInstanceIndices returns the indices of every Instance
// currently in Phase=Failed. Order matches the InstanceStatuses slice;
// callers that need a sorted list can sort the result.
func collectFailedInstanceIndices(instances []v1beta1.OMENativeInstanceStatus) []int32 {
	var out []int32
	for _, s := range instances {
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			out = append(out, s.Index)
		}
	}
	return out
}

// formatFailedInstancesMessage builds the operator-facing summary the
// coordination state machine carries on Component.FailureMessage. Stays
// short — the per-Instance breadcrumb (Operation.Reason +
// OperationStuck condition) carries the full detail.
func formatFailedInstancesMessage(c v1beta1.ComponentType, indices []int32) string {
	if len(indices) == 1 {
		return fmt.Sprintf("%s instance %d in Phase=Failed", c, indices[0])
	}
	return fmt.Sprintf("%s instances %v in Phase=Failed", c, indices)
}

// sumPodCounts sums all values in a per-revision pod count map.
func sumPodCounts(in map[string]int32) int32 {
	var sum int32
	for _, n := range in {
		sum += n
	}
	return sum
}

// gcOrphanedPerRevisionServices deletes per-revision Service pairs
// whose revision-hash has no live pods for `component`.
//
// The sweep lists Services in the ISVC's namespace that carry the
// (inferenceservice + component + revision-hash) selector keys, then
// drops every pair whose revision-hash isn't in `liveHashes`.
//
// Idempotent: deleting an already-deleted Service is a NotFound
// success per GCPerRevisionServices.
func gcOrphanedPerRevisionServices(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, liveHashes map[string]int32) error {
	if c == nil {
		return nil
	}
	live := make(map[string]struct{}, len(liveHashes))
	for h := range liveHashes {
		if h == "" {
			continue
		}
		live[h] = struct{}{}
	}
	svcs := &corev1.ServiceList{}
	if err := c.List(ctx, svcs,
		client.InNamespace(isvc.Namespace),
		client.MatchingLabels{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(component),
		},
	); err != nil {
		return fmt.Errorf("list per-revision services: %w", err)
	}
	for i := range svcs.Items {
		hash := svcs.Items[i].Spec.Selector[query.LabelRevisionHash]
		if hash == "" {
			continue // not a per-revision Service
		}
		if _, alive := live[hash]; alive {
			continue
		}
		// Skip headless variant — GCPerRevisionServices already
		// handles both halves of the pair when called with the
		// routing hash. The routing variant's name is the bare
		// `<isvc>-<c>-rev-<hash>` shape; headless adds `-headless`.
		if svcs.Items[i].Name != PerRevisionServiceName(isvc.Name, component, hash) {
			continue
		}
		if err := GCPerRevisionServices(ctx, c, isvc.Namespace, isvc.Name, component, hash); err != nil {
			return fmt.Errorf("gc service for %s/%s: %w", component, hash, err)
		}
	}
	return nil
}

// buildRatioState assembles the per-group RatioState that BuildGroupStatus
// turns into Status.RolloutCoordination.Groups[i].ObservedRatio.
// SnapshotOriginal runs on first observation; Original is preserved
// across reconciles via the previously-written ObservedRatio so the
// anchor doesn't shift when MinReplicas changes mid-rollout.
//
// Always populated even for non-RatioBalanced pacing (cheap; BuildGroupStatus
// suppresses it on the status side based on policy).
//
// Snapshot deferral: the first reconcile of a freshly-created ISVC observes
// every Component with status.OMENative.Replicas == 0 because the OMENative
// status writer hasn't run yet (Create happens first; the status write lands
// on a subsequent pass). Snapshotting from that empty status would anchor
// Original to {Engine: 1, Decoder: 1} via SnapshotOriginal's zero-clamp,
// which then deadlocks RatioBalanced rollouts the moment real replica counts
// disagree (e.g., MinReplicas=4:2 produces a live 2:1 ratio that immediately
// skews against the bogus 1:1 anchor). Defer the snapshot until every
// Component reports non-zero status.OMENative.Replicas — by then the live
// counts ARE the desired counts and Original anchors correctly.
func buildRatioState(isvc *v1beta1.InferenceService, g ResolvedGroup, obs GroupObservation, perRevisionPods map[v1beta1.ComponentType]map[string]int32) *RatioState {
	state := &RatioState{
		Current: make(map[v1beta1.ComponentType]int32, len(g.Components)),
		NewPods: make(map[v1beta1.ComponentType]int32, len(g.Components)),
	}
	// Current = total live pods this Component, any revision.
	// NewPods = pods on target revision (matched by Component obs).
	for _, c := range g.Components {
		hashes := perRevisionPods[c]
		var total int32
		for _, n := range hashes {
			total += n
		}
		state.Current[c] = total
		cObs := obs.Components[c]
		if cObs.TargetRevisionHash != "" {
			state.NewPods[c] = hashes[cObs.TargetRevisionHash]
		}
	}
	// Original: read from existing Status, snapshot from current
	// observed replicas if not yet recorded (first observation that
	// also has all Components populated in status).
	state.Original = existingOriginalReplicas(isvc, g.Name)
	if len(state.Original) == 0 {
		desired := make(map[v1beta1.ComponentType]int32, len(g.Components))
		allPopulated := true
		for _, c := range g.Components {
			d := obs.Components[c].DesiredReplicas
			if d <= 0 {
				// Status hasn't been written for this Component yet. Don't
				// snapshot — defer to the next reconcile after the status
				// aggregator writes the real Replicas.
				allPopulated = false
				break
			}
			desired[c] = d
		}
		if allPopulated {
			state.Original = SnapshotOriginal(desired)
		}
	}
	return state
}

// existingOriginalReplicas reads the previously-recorded Original
// from the ISVC's status, if any. Empty when first observation of the
// group.
func existingOriginalReplicas(isvc *v1beta1.InferenceService, groupName string) map[v1beta1.ComponentType]int32 {
	if isvc.Status.RolloutCoordination == nil {
		return nil
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name != groupName || gs.ObservedRatio == nil {
			continue
		}
		if len(gs.ObservedRatio.Original) == 0 {
			return nil
		}
		out := make(map[v1beta1.ComponentType]int32, len(gs.ObservedRatio.Original))
		for k, v := range gs.ObservedRatio.Original {
			out[k] = v
		}
		return out
	}
	return nil
}

// previousGroupPhase returns the Phase observed for `groupName` in the
// most recent reconcile (read from the ISVC's existing status). Empty
// when no prior observation exists — caller treats that as "no
// transition" so the very first reconcile doesn't synthesize a
// spurious from="" → to=Idle transition increment.
func previousGroupPhase(isvc *v1beta1.InferenceService, groupName string) v1beta1.CoordinationPhase {
	if isvc.Status.RolloutCoordination == nil {
		return ""
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName {
			return gs.Phase
		}
	}
	return ""
}

// previousActiveComponent returns the CurrentComponent observed for
// `groupName` in the most recent reconcile. Used by the Sequential
// deferred-event emitter to dedupe re-fires while the same active
// Component continues rolling.
func previousActiveComponent(isvc *v1beta1.InferenceService, groupName string) v1beta1.ComponentType {
	if isvc.Status.RolloutCoordination == nil {
		return ""
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName {
			return gs.CurrentComponent
		}
	}
	return ""
}

// previousPhaseEnteredAt returns the timestamp the group last entered
// its current Phase, as captured in LastTransitionTime on the
// previously-written group status. Returns zero time when no prior
// status exists — callers (Sequential soak gate) treat zero as
// "no clock running yet."
func previousPhaseEnteredAt(isvc *v1beta1.InferenceService, groupName string) time.Time {
	if isvc.Status.RolloutCoordination == nil {
		return time.Time{}
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName && gs.LastTransitionTime != nil {
			return gs.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// hasMixedPairing reports whether the RollingUpdate group's Components
// observe distinct revision hashes — the cross-revision pairing case
// RollingUpdate permits but operators want surfaced.
//
// A pairing is "mixed" when at least one Component in the group has
// pods on 2+ revisions OR when different Components in the group are
// on different revisions. Both indicate ad-hoc cross-revision pod
// interaction that's worth flagging.
func hasMixedPairing(g ResolvedGroup, perRevisionPods map[v1beta1.ComponentType]map[string]int32) bool {
	if len(g.Components) < 2 {
		return false
	}
	// Single-Component multi-hash: any Component with >1 hash present.
	for _, c := range g.Components {
		hashes := perRevisionPods[c]
		nonZero := 0
		for _, n := range hashes {
			if n > 0 {
				nonZero++
			}
		}
		if nonZero > 1 {
			return true
		}
	}
	// Cross-Component hash mismatch: pick the first non-empty Component's
	// only hash and compare to others.
	var ref string
	for _, c := range g.Components {
		hashes := perRevisionPods[c]
		for h, n := range hashes {
			if n == 0 {
				continue
			}
			if ref == "" {
				ref = h
				break
			}
			if h != ref {
				return true
			}
		}
	}
	return false
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

// emitSequentialDeferredEvent fires SequentialNextSpecBumpDeferred
// when an operator bumped a later-in-Order Component's spec while an
// earlier Component is still rolling — Sequential semantics silently
// defer the later rollout, and the operator gets a warning event.
//
// Detection (pure, stateless): for each Component at index j > i
// where i is the active rolling Component's index in Order, if the
// later Component has RolloutInFlight=true, emit the event. No-op
// for non-Sequential policies and for groups with no Component rolling.
//
// Dedup: skips the emit when activeComponent matches the previous
// reconcile's CurrentComponent. The first reconcile of a given
// activeComponent emits; subsequent reconciles of the same active
// Component don't re-emit. When the active Component advances (e.g.,
// decoder finishes → engine becomes active), the dedup clears and
// the event fires once for the new active Component.
func emitSequentialDeferredEvent(recorder record.EventRecorder, isvc *v1beta1.InferenceService, g ResolvedGroup, obs GroupObservation, prevActive v1beta1.ComponentType) {
	if recorder == nil || g.Policy != v1beta1.CoordinationPolicySequential {
		return
	}
	if len(g.Order) == 0 {
		return
	}
	activeIdx := -1
	for i, c := range g.Order {
		comp, ok := obs.Components[c]
		if !ok {
			continue
		}
		if comp.RolloutInFlight {
			activeIdx = i
			break
		}
	}
	if activeIdx < 0 {
		return
	}
	activeC := g.Order[activeIdx]
	if prevActive == activeC {
		// Already emitted (or attempted) for this activeComponent in a
		// prior reconcile. K8s event recorder also aggregates by
		// (involvedObject, reason) but skipping the call entirely is
		// cleaner.
		return
	}
	for j := activeIdx + 1; j < len(g.Order); j++ {
		laterC := g.Order[j]
		laterObs, ok := obs.Components[laterC]
		if !ok || !laterObs.RolloutInFlight {
			continue
		}
		recorder.Eventf(isvc, corev1.EventTypeWarning, EventReasonSequentialNextSpecBumpDeferred,
			"sequential group %s: spec bump on later Component %s deferred while active Component %s is still rolling",
			g.Name, laterC, activeC)
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
