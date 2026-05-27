// Package status owns the per-Component / per-Instance status surface
// for the OMENative reconciler. pod_failures.go classifies kubelet pod
// failure modes that cannot self-recover (bad image, image not found,
// invalid image name) so the controller can escalate the owning Instance
// to Phase=Failed without waiting for the InstanceReadyTimeout deadline
// (default 30 min).
//
// Operator-mental-model justification: an ImagePullBackOff signals a
// permanent failure (typo in image tag, registry auth issue, image
// genuinely missing). No amount of waiting will fix it — kubelet's
// exponential backoff converges to one retry every ~5 minutes. Surfacing
// the failure quickly (within ~60s of the pull failing) is strictly
// better than sitting at Phase=Updating / Phase=Creating until the
// 30-minute timeout fires, because the rollout is wedged either way
// and the operator needs to see the wedge to act on it.
//
// Pure: no I/O, no status writes. The status_aggregate caller is
// responsible for translating the classifier's verdict into an
// Instance Phase mutation via MutateInstance.

package status

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// terminalPullFailureReasons enumerates kubelet container state.Waiting
// reasons that cannot self-recover. Distinct from PodInitializing or
// ContainerCreating, which are transient and resolve once the kubelet
// finishes its setup. The list is conservative — every entry below has
// been observed in the wild as a wedge that requires either a spec
// change (fix the image tag) or a node-level fix (registry creds);
// neither is something the controller can do on its own.
var terminalPullFailureReasons = map[string]struct{}{
	// ErrImagePull is kubelet's initial signal that the registry pull
	// failed (404, unauthorized, network timeout). After a few retries
	// it transitions to ImagePullBackOff.
	"ErrImagePull": {},
	// ImagePullBackOff is the long-term steady state for a failing pull
	// — kubelet retries with exponential backoff up to ~5 minutes
	// between attempts. It never converges to Ready unless the image
	// actually becomes available (rare; image typos don't fix
	// themselves).
	"ImagePullBackOff": {},
	// InvalidImageName fires on syntactically malformed image references
	// (e.g., trailing space, illegal characters). kubelet refuses to
	// even try the pull. Permanent until the spec is fixed.
	"InvalidImageName": {},
	// CreateContainerConfigError indicates the pod spec is rejected at
	// runtime config assembly (e.g., a Secret reference that doesn't
	// resolve). Permanent until the missing dependency lands.
	"CreateContainerConfigError": {},
	// CreateContainerError is the catch-all for kubelet/runtime
	// rejecting the create. Often transient on apiserver/etcd hiccups
	// but stays wedged on persistent failures (image manifest issues,
	// missing volume mounts, etc.). The grace window distinguishes
	// transient hiccups from persistent failures.
	"CreateContainerError": {},
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
// Timing source: we measure "stuck duration" from the pod's
// CreationTimestamp, NOT from the container's individual state
// transition time (which kubelet doesn't expose per-state). For
// ImagePullBackOff specifically, kubelet has already retried at least
// once before flipping to BackOff (typically ~30s after pod creation
// on a 404), so a 60s grace from pod creation reliably catches stuck
// pulls while ignoring the brief startup window where the container
// transitions through normal Waiting states (PodInitializing,
// ContainerCreating, ImagePulling). For ImagePullBackOff specifically
// on a tag that doesn't exist, the pod is in the failing state from
// ~T+5s onward; 60s is comfortable headroom.
func PodStuckPullFailure(pod *corev1.Pod, now time.Time, grace time.Duration) (bool, string) {
	if pod == nil {
		return false, ""
	}
	// Only evaluate pods that have lived long enough for kubelet to
	// have attempted a pull and surfaced a stable reason. Without this,
	// a pod created < grace seconds ago could be in the legitimate
	// "ContainerCreating" transient state and we'd false-fire.
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
	// Init containers can wedge the same way (typo in init image, missing
	// init image tag). Check them too so the init-container failure mode
	// doesn't fall through to the slow timeout.
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
