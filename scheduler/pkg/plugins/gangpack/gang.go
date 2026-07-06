package gangpack

import (
	"time"

	v1 "k8s.io/api/core/v1"
)

// defaultPermitTimeout is the safety-valve wait a gang member holds at the gate
// when its PodGroup declares no ScheduleTimeoutSeconds. The workload's own value
// (the standard PodGroup field) overrides it; this is only the fallback so a
// misconfigured gang can't wait forever.
const defaultPermitTimeout = 600 * time.Second

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

// gangInfo is everything the plugin needs to place and gate a gang, all declared
// by the workload (never global config): the gang's key, how many members must
// land together, which node label denotes its domain, and how long a member may
// wait at the gate for its siblings.
type gangInfo struct {
	key         string // namespace/podgroup-name
	minMember   int
	topologyKey string
	timeout     time.Duration
}

// podGroupReader resolves a PodGroup into its placement facts (minMember, the
// declared topology key, and the gate timeout). It is an interface so the plugin
// logic is unit-testable without a live PodGroup informer.
type podGroupReader interface {
	get(namespace, name string) (minMember int, topologyKey string, timeout time.Duration, found bool)
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
	minMember, topologyKey, timeout, found := r.get(ns, name)
	if !found || minMember <= 0 || topologyKey == "" {
		return gangInfo{}, false
	}
	return gangInfo{key: ns + "/" + name, minMember: minMember, topologyKey: topologyKey, timeout: timeout}, true
}
