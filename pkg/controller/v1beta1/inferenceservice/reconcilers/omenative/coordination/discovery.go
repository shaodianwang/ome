package coordination

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// PeerEndpointEnv is the set of generation-scoped peer-discovery env
// vars OMENative stamps on every pod for one peer Component. Two
// vars are emitted per peer:
//
//   - OME_<PEER>_ENDPOINT carries the generic, revision-agnostic
//     Service DNS name. Used by runtimes in Independent or
//     RollingUpdate groups where cross-revision peer calls are
//     allowed.
//   - OME_<PEER>_REVISION_ENDPOINT carries the per-revision Service
//     DNS name, scoped to the pod's own revision. Used by runtimes
//     in BlueGreen groups where only same-revision peer calls are
//     allowed.
//
// The runtime selects which one to honor based on its own config; both
// are always populated. Env vars are read-once at pod startup which
// matches the LLM-runtime pattern.
type PeerEndpointEnv struct {
	// Peer is the peer Component these vars target.
	Peer v1beta1.ComponentType

	// GenericName is the OME_<PEER>_ENDPOINT env var name.
	GenericName string

	// GenericValue is the DNS name pointing at the revision-agnostic
	// per-Component Service (`<isvc>-<peer>`).
	GenericValue string

	// RevisionName is the OME_<PEER>_REVISION_ENDPOINT env var name.
	RevisionName string

	// RevisionValue is the DNS name pointing at the per-revision
	// Service for this pod's revision
	// (`<isvc>-<peer>-rev-<revision-hash>`).
	RevisionValue string
}

// BuildPeerEndpointEnv computes the env vars OMENative injects into a
// pod for one peer Component. The peer's revision-hash is the pod's
// own revision — peer pods talk to peers in their own revision when
// the runtime honors the per-revision form.
//
// isvc + namespace identify the InferenceService; the function does
// not do I/O.
func BuildPeerEndpointEnv(isvcName, namespace string, peer v1beta1.ComponentType, podRevisionHash string) PeerEndpointEnv {
	upper := strings.ToUpper(string(peer))
	return PeerEndpointEnv{
		Peer:          peer,
		GenericName:   fmt.Sprintf("OME_%s_ENDPOINT", upper),
		GenericValue:  genericPeerDNS(isvcName, peer, namespace),
		RevisionName:  fmt.Sprintf("OME_%s_REVISION_ENDPOINT", upper),
		RevisionValue: revisionPeerDNS(isvcName, peer, podRevisionHash, namespace),
	}
}

// genericPeerDNS returns the in-cluster DNS for the revision-agnostic
// per-Component Service. The OMENative reconciler emits one of these
// per Component (`<isvc>-<peer>`).
func genericPeerDNS(isvc string, peer v1beta1.ComponentType, namespace string) string {
	return fmt.Sprintf("%s-%s.%s.svc.cluster.local", isvc, peer, namespace)
}

// revisionPeerDNS returns the in-cluster DNS for the per-revision peer
// Service. The OMENative coordination layer creates one of these per
// (Component, revisionHash) pair.
func revisionPeerDNS(isvc string, peer v1beta1.ComponentType, revisionHash, namespace string) string {
	return fmt.Sprintf("%s-%s-rev-%s.%s.svc.cluster.local", isvc, peer, revisionHash, namespace)
}

// InjectPeerEnv overlays the OME_<PEER>_ENDPOINT and
// OME_<PEER>_REVISION_ENDPOINT env vars onto every container in pod
// for each peer Component the pod is told about. Existing env vars
// with the same Name are replaced — coordination's values are
// authoritative.
//
// If revisionHashFor is nil, the function emits only the generic env
// var (the revision-aware form requires a known revision hash for
// each peer; in single-revision steady state callers may either pass
// the steady revision hash or skip the per-revision form).
//
// The peers slice should be deduplicated and stable in order;
// callers typically pass coordination.PeerComponents(...) sorted.
func InjectPeerEnv(pod *corev1.Pod, isvc, namespace string, peers []v1beta1.ComponentType, revisionHashFor func(v1beta1.ComponentType) string) {
	if pod == nil || len(peers) == 0 {
		return
	}
	envs := make([]corev1.EnvVar, 0, 2*len(peers))
	owned := make(map[string]struct{}, 2*len(peers))
	for _, peer := range peers {
		hash := ""
		if revisionHashFor != nil {
			hash = revisionHashFor(peer)
		}
		peerEnv := BuildPeerEndpointEnv(isvc, namespace, peer, hash)
		envs = append(envs, corev1.EnvVar{Name: peerEnv.GenericName, Value: peerEnv.GenericValue})
		owned[peerEnv.GenericName] = struct{}{}
		if hash != "" {
			envs = append(envs, corev1.EnvVar{Name: peerEnv.RevisionName, Value: peerEnv.RevisionValue})
			owned[peerEnv.RevisionName] = struct{}{}
		}
	}
	for i := range pod.Spec.Containers {
		kept := pod.Spec.Containers[i].Env[:0]
		for _, e := range pod.Spec.Containers[i].Env {
			if _, replace := owned[e.Name]; replace {
				continue
			}
			kept = append(kept, e)
		}
		pod.Spec.Containers[i].Env = append(kept, envs...)
	}
}

// SortedPeers returns the peer set for c with declaration order
// preserved within a group, then deduplicated and sorted. Callers
// that want stable env-var ordering across reconciles use this rather
// than raw PeerComponents output.
func SortedPeers(groups []ResolvedGroup, c v1beta1.ComponentType, fallbackPeers []v1beta1.ComponentType) []v1beta1.ComponentType {
	if peers := PeerComponents(groups, c); len(peers) > 0 {
		return sortedComponents(peers)
	}
	return sortedComponents(fallbackPeers)
}

// sortedComponents returns a deduplicated, lexicographically-sorted
// copy of in. Used to give env-var injection a stable order so a pod
// re-render doesn't produce noise.
func sortedComponents(in []v1beta1.ComponentType) []v1beta1.ComponentType {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[v1beta1.ComponentType]struct{}, len(in))
	out := make([]v1beta1.ComponentType, 0, len(in))
	for _, c := range in {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
