package canary

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/rollout"
)

// canaryWeights builds the two-revision external weight split for a step: the
// canary revision receives `weight` percent, the stable revision the remainder.
// The canary entry is marked LatestRevision. (coordination.BuildTrafficTargets
// drops any entry with Percent<=0, so a 0%/100% step naturally collapses to a
// single target.) Each entry carries its revision's pairing protocol so the
// routing consumer can pair engine/decoder targets by equal values.
func canaryWeights(canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) []coordination.RevisionWeight {
	if canaryHash != "" && canaryHash == stableHash {
		return []coordination.RevisionWeight{{
			RevisionHash: canaryHash, Percent: 100, Tag: "canary", LatestRevision: true, PairingProtocol: canaryProtocol,
		}}
	}
	return []coordination.RevisionWeight{
		{RevisionHash: canaryHash, Percent: weight, Tag: "canary", LatestRevision: true, PairingProtocol: canaryProtocol},
		{RevisionHash: stableHash, Percent: 100 - weight, Tag: "stable", PairingProtocol: stableProtocol},
	}
}

// servingStable resolves the revision a split's remainder is written on, and
// its pairing protocol. The stable revision carries the remainder while it
// has a serving pod. A stable revision with none, whose instances rolled onto
// another revision, hands the remainder to that revision: the one serving the
// most pods off the canary, under no protocol (pairs with anything), so the
// remainder never points at capacity that moved. With nothing serving in its
// place, or no pod view, the stable revision keeps its share: a pod loss is
// the workload's repair, not a traffic decision.
func servingStable(stableHash, stableProtocol, canaryHash string, readyPods map[string]int32) (string, string) {
	if stableHash == "" || stableHash == canaryHash || readyPods == nil || readyPods[stableHash] > 0 {
		return stableHash, stableProtocol
	}
	substitute, most := "", int32(0)
	for hash, count := range readyPods {
		if hash == "" || hash == canaryHash || count <= 0 {
			continue
		}
		if count > most || (count == most && hash < substitute) {
			substitute, most = hash, count
		}
	}
	if substitute == "" {
		return stableHash, stableProtocol
	}
	return substitute, ""
}

// memberWeights is a secondary's split at a step: its target revision at
// `weight` percent and its stable revision at the remainder, each under the
// protocol it was minted with; readyPods is the member's serving pods per
// revision, which decide the revision the remainder rests on. A member whose
// target is its stable revision was not bumped, and a member with no stable
// revision has nothing to shift traffic from; both serve their target alone.
// No target yields no weights.
func memberWeights(m MemberRevisions, weight int32, readyPods map[string]int32) []coordination.RevisionWeight {
	if m.CanaryRevisionHash == "" {
		return nil
	}
	if m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
		return canaryWeights(m.CanaryRevisionHash, m.CanaryRevisionHash, m.CanaryPairingProtocol, m.CanaryPairingProtocol, weight)
	}
	stable, protocol := servingStable(m.StableRevisionHash, m.StablePairingProtocol, m.CanaryRevisionHash, readyPods)
	return canaryWeights(m.CanaryRevisionHash, stable, m.CanaryPairingProtocol, protocol, weight)
}

// memberStableWeights is a rolled-back secondary's traffic: its stable
// revision alone, as the primary's. A member with no stable revision is sent
// no revert, so it yields no weights and keeps the targets it has.
func memberStableWeights(m MemberRevisions) []coordination.RevisionWeight {
	if m.StableRevisionHash == "" {
		return nil
	}
	return []coordination.RevisionWeight{{
		RevisionHash: m.StableRevisionHash, Percent: 100, Tag: "stable", PairingProtocol: m.StablePairingProtocol,
	}}
}

// setTraffic writes one Component's targets onto Status.Components.<c>.Traffic,
// the entries the HTTPRoute weighted-backendRef consumer reads.
func setTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, targets []v1beta1.ComponentTrafficTarget) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	cs.Traffic = targets
	isvc.Status.Components[c] = cs
}

// applyTraffic programs the step's external traffic weight on the primary: it
// writes the per-revision weights onto its traffic targets and records the
// applied weight on the canary status.
func applyTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) {
	setTraffic(isvc, c, coordination.BuildTrafficTargets(isvc.Name, c, canaryWeights(canaryHash, stableHash, canaryProtocol, stableProtocol, weight)))
	if cs := rollout.CanaryStatusFor(&isvc.Status, c); cs != nil {
		cs.ObservedTrafficWeight = weight
	}
}

// applyMemberTraffic writes one secondary's targets from its weights. No
// weights leaves the member's traffic as it is.
func applyMemberTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, weights []coordination.RevisionWeight) {
	if len(weights) == 0 {
		return
	}
	setTraffic(isvc, c, coordination.BuildTrafficTargets(isvc.Name, c, weights))
}

// applyGroupTraffic programs one step weight onto every member of the canary
// unit: the primary's split on its own revision pair and each secondary's on
// its own. The weights are the same; only the revisions they name differ, so
// a routing consumer finds a target for every member at every step. Each
// member's remainder rests on the revision servingStable resolves from that
// member's serving pods.
func applyGroupTraffic(in ReconcileInputs, weight int32) {
	stable, protocol := servingStable(in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash, in.PerRevisionPods)
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, stable, in.CanaryPairingProtocol, protocol, weight)
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberWeights(m, weight, in.GroupReadyPerRevisionPods[c]))
	}
}

// returnTrafficToStable writes the split a superseded ladder left behind back
// onto each member's stable revision, as servingStable resolves it, with the
// new target at nothing. A member with no targets written, or no stable
// revision distinct from its target, has nothing to return and keeps what it
// has.
func returnTrafficToStable(in ReconcileInputs) {
	if len(in.ISVC.Status.Components[in.Component].Traffic) > 0 &&
		in.StableRevisionHash != "" && in.StableRevisionHash != in.CanaryRevisionHash {
		stable, protocol := servingStable(in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash, in.PerRevisionPods)
		applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, stable, in.CanaryPairingProtocol, protocol, 0)
	}
	for c, m := range in.Secondaries {
		if len(in.ISVC.Status.Components[c].Traffic) == 0 ||
			m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
			continue
		}
		applyMemberTraffic(in.ISVC, c, memberWeights(m, 0, in.GroupReadyPerRevisionPods[c]))
	}
}

// applyGroupStableTraffic returns every secondary to its stable revision
// alongside the primary's rollback write. A member whose stable revision is
// observed with no ready pod keeps its programmed split until one serves.
func applyGroupStableTraffic(in ReconcileInputs) {
	for c, m := range in.Secondaries {
		if ready, observed := in.GroupReadyPerRevisionPods[c]; observed && ready[m.StableRevisionHash] == 0 {
			continue
		}
		applyMemberTraffic(in.ISVC, c, memberStableWeights(m))
	}
}
