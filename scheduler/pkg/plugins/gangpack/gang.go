package gangpack

import v1 "k8s.io/api/core/v1"

// podGroupLabel is the scheduler-plugins standard pod label naming a pod's
// PodGroup. It is an ecosystem standard (sigs.k8s.io/scheduler-plugins), not an
// OME convention, so it is used as-is — not made configurable.
const podGroupLabel = "scheduling.x-k8s.io/pod-group"

// topologyKeyAnnotation is the plugin's one workload-API key: a PodGroup carries
// this annotation to declare which node label groups its gang into a domain —
// exactly as a topologySpreadConstraint names its own topologyKey. The value is
// fully per-workload (any fabric's label: an NVLink clique, a TPU slice, a
// rack); only this key name belongs to the plugin, and it is trivially
// renameable. The scheduler is told no domain label globally.
const topologyKeyAnnotation = "ome.io/topology-key"

// gangInfo is everything the plugin needs to place a gang, all declared by the
// workload (never global config): the gang's key, how many members must land
// together, and which node label denotes its domain.
type gangInfo struct {
	key         string // namespace/podgroup-name
	minMember   int
	topologyKey string
}

// podGroupReader resolves a PodGroup into its placement facts (minMember + the
// declared topology key from its annotation). It is an interface so the plugin
// logic is unit-testable without a live PodGroup informer; the informer-backed
// implementation lands separately.
type podGroupReader interface {
	get(namespace, name string) (minMember int, topologyKey string, found bool)
}

// podGroupNameOf returns the (namespace, name) of a pod's PodGroup — namespace
// is the pod's own, name is the standard pod-group label value — and false when
// the pod carries no pod-group label (not a gang member).
func podGroupNameOf(pod *v1.Pod) (namespace, name string, ok bool) {
	if pod == nil {
		return "", "", false
	}
	name = pod.Labels[podGroupLabel]
	if name == "" {
		return "", "", false
	}
	return pod.Namespace, name, true
}

// resolveGang assembles a pod's gangInfo from its PodGroup. It returns false —
// meaning "not a domain-pinned gang, leave placement unconstrained" — when the
// pod is not a gang member, its PodGroup is absent, its minMember is
// non-positive, or the PodGroup declares no topology key (nothing to pin to).
func resolveGang(pod *v1.Pod, r podGroupReader) (gangInfo, bool) {
	ns, name, ok := podGroupNameOf(pod)
	if !ok {
		return gangInfo{}, false
	}
	minMember, topologyKey, found := r.get(ns, name)
	if !found || minMember <= 0 || topologyKey == "" {
		return gangInfo{}, false
	}
	return gangInfo{key: ns + "/" + name, minMember: minMember, topologyKey: topologyKey}, true
}
