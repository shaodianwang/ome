// Package podreadiness implements the OEP-0007 multi-writer readiness
// gate protocol for OMENative's `ome.io/serving` pod condition.
//
// Pattern ported from RBG (`sigs.k8s.io/rbgs/pkg/inplace/pod/readiness`).
// The condition's Message field carries a JSON list of {UserAgent, Key}
// entries — one per writer that wants the pod NotReady right now.
// Status=True iff the list is empty AND containers are ready (the
// latter is kubelet's responsibility via the standard readiness gate
// machinery).
//
// Why multi-writer: OMENative has at least four overlapping reasons
// to want a pod NotReady — migration source drain, in-place update
// drain, restart drain, scale-down drain — that may be in flight on
// the same pod simultaneously. A single binary True/False gate has
// the writers race to overwrite each other; the multi-writer protocol
// lets them coexist as independent message-list entries.
//
// All writes use Status().Patch with strategic merge on the condition
// list (patch-merge-key=type), so kubelet's concurrent writes to
// PodScheduled / ContainersReady / PodReady are preserved. All
// writes are wrapped in retry.RetryOnConflict.
package podreadiness

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConditionType is the controller-owned readiness gate OMENative
// stamps on every managed pod. The same value Render appends to
// pod.spec.readinessGates so kubelet AND's it into PodReady.
const ConditionType corev1.PodConditionType = "ome.io/serving"

// Message identifies one writer that wants the pod NotReady. The
// pair {UserAgent, Key} is unique per logical reason — e.g.,
// {"Update-in-place", "0-3"} for an in-place update on Instance 0
// incarnation 3.
type Message struct {
	UserAgent string `json:"userAgent"`
	Key       string `json:"key"`
}

// AddNotReadyKey appends msg to the readiness condition's message list
// and sets Status=False. No-op if msg is already present. Patches only
// the single condition slot so kubelet's concurrent status writes are
// preserved; wrapped in retry.RetryOnConflict.
func AddNotReadyKey(ctx context.Context, c client.Client, pod *corev1.Pod, msg Message) error {
	if pod == nil {
		return fmt.Errorf("AddNotReadyKey: nil pod")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &corev1.Pod{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			return fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		cond := findCondition(fresh, ConditionType)
		var base string
		if cond != nil {
			base = cond.Message
		}
		changed, list := addMessage(base, msg)
		if !changed {
			return nil
		}
		return patchCondition(ctx, c, fresh, corev1.PodCondition{
			Type:               ConditionType,
			Status:             corev1.ConditionFalse,
			Reason:             "NotReady",
			Message:            list.dump(),
			LastTransitionTime: metav1.Now(),
		})
	})
}

// RemoveNotReadyKey removes msg from the readiness condition's
// message list. If the list becomes empty (or the condition doesn't
// exist yet — fresh pod case), sets Status=True. Otherwise keeps
// Status=False with the shrunken list.
//
// Wrapped in retry.RetryOnConflict; patches only the single
// condition slot.
func RemoveNotReadyKey(ctx context.Context, c client.Client, pod *corev1.Pod, msg Message) error {
	if pod == nil {
		return fmt.Errorf("RemoveNotReadyKey: nil pod")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &corev1.Pod{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
			return fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		cond := findCondition(fresh, ConditionType)
		if cond == nil {
			// Fresh pod, no condition yet. Removing a key that isn't
			// present is vacuously "no writers hold it NotReady"
			// → write Status=True.
			return patchCondition(ctx, c, fresh, corev1.PodCondition{
				Type:               ConditionType,
				Status:             corev1.ConditionTrue,
				Reason:             "Serving",
				LastTransitionTime: metav1.Now(),
			})
		}
		changed, list := removeMessage(cond.Message, msg)
		if !changed && cond.Status == corev1.ConditionTrue {
			// Key wasn't in the list AND condition already True.
			// Nothing to do.
			return nil
		}
		status := corev1.ConditionTrue
		reason := "Serving"
		message := ""
		if len(list) > 0 {
			status = corev1.ConditionFalse
			reason = "NotReady"
			message = list.dump()
		}
		// If only the status field needs updating, still issue a
		// patch — the list dedup is in the helper.
		return patchCondition(ctx, c, fresh, corev1.PodCondition{
			Type:               ConditionType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})
	})
}

// ContainsNotReadyKey reports whether the readiness condition
// currently holds msg in its message list. Read-side check used by
// callers that want to skip the redundant Add/Remove call.
func ContainsNotReadyKey(pod *corev1.Pod, msg Message) bool {
	if pod == nil {
		return false
	}
	cond := findCondition(pod, ConditionType)
	if cond == nil || cond.Status == corev1.ConditionTrue || cond.Message == "" {
		return false
	}
	list := parseList(cond.Message)
	for _, m := range list {
		if m == msg {
			return true
		}
	}
	return false
}

// IsServing reports whether the condition exists with Status=True.
// Equivalent to the old binary `IsServing` semantics — true iff no
// writer currently holds the pod NotReady.
func IsServing(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	cond := findCondition(pod, ConditionType)
	return cond != nil && cond.Status == corev1.ConditionTrue
}

// IsContainersReady reports whether the kubelet-owned ContainersReady
// condition is True — i.e., all containers in the pod have passed their
// readiness probes.
//
// Distinct from corev1.PodReady (which ANDs ContainersReady with every
// readiness gate including ome.io/serving). Gating MarkPodServing on
// PodReady would deadlock: kubelet won't flip PodReady=True until
// ome.io/serving=True, and OMENative won't write ome.io/serving=True
// until PodReady=True. ContainersReady reflects only probe-level
// readiness and is the correct signal that the runtime is healthy
// enough to receive traffic.
func IsContainersReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// MarkPodServing removes the {userAgent, key} entry from the
// ome.io/serving condition's writer list. If the resulting list is
// empty (or the condition didn't exist yet — fresh pod case), the
// condition is written with Status=True and the pod becomes eligible
// for Service rotation.
//
// Idempotent: removing a key that isn't present is a no-op (no patch
// issued) when the condition is already Status=True.
func MarkPodServing(ctx context.Context, c client.Client, pod *corev1.Pod, userAgent, key string) error {
	return RemoveNotReadyKey(ctx, c, pod, Message{UserAgent: userAgent, Key: key})
}

// MarkPodNotServing adds the {userAgent, key} entry to the
// ome.io/serving condition's writer list. Status becomes False (because
// the list is non-empty). The pod is removed from Service rotation via
// kube-proxy's standard PodReady gate machinery.
//
// Idempotent: re-adding the same key is a no-op.
func MarkPodNotServing(ctx context.Context, c client.Client, pod *corev1.Pod, userAgent, key string) error {
	return AddNotReadyKey(ctx, c, pod, Message{UserAgent: userAgent, Key: key})
}

// Writer userAgents used across the OMENative ops. Centralized here so
// a stray drift in any one site is easy to catch in `git grep`.
const (
	// WriterLifecycle flips a fresh pod's gate to Status=True once all
	// Instance siblings reach ContainersReady. The key
	// (KeyLifecycleInstanceReady) is the same across all sites — the
	// userAgent + key tuple is the lookup; we never need to differentiate.
	WriterLifecycle = "Lifecycle"

	// WriterUpdateInPlace drains a pod for in-place container image
	// update. Same key on Add (drain) and Remove (un-drain) on the
	// same pod.
	WriterUpdateInPlace = "Update-in-place"

	// WriterUpdateRecreateDrain drains the OLD pods for a recreate
	// update. The NEW pods are fresh and go through WriterLifecycle.
	WriterUpdateRecreateDrain = "Update-recreate-drain"

	// WriterRestartDrain drains the OLD pods for an Instance restart.
	// The NEW pods are fresh and go through WriterLifecycle.
	WriterRestartDrain = "Restart-drain"

	// WriterDeleteDrain drains a pod for scale-down deletion. The key
	// is removed by virtue of the pod being deleted; no explicit
	// RemoveNotReadyKey is required.
	WriterDeleteDrain = "Delete-drain"

	// WriterMigrateSourceDrain drains the source pods of a surge
	// migration. The key is removed by virtue of the source pods being
	// deleted at the end of the migration.
	WriterMigrateSourceDrain = "Migrate-source-drain"

	// WriterUpdateSurgeDrain drains the OLD pod during a SurgeThenDrain
	// rollout — after the surge pod (at the other ordinal slot) reaches
	// Ready, the old pod is drained via this writer then deleted. The
	// surge pod is fresh and goes through WriterLifecycle.
	WriterUpdateSurgeDrain = "Update-surge-drain"
)

// KeyLifecycleInstanceReady is the universal Lifecycle key. Matches
// RBG's convention. The pair (WriterLifecycle, KeyLifecycleInstanceReady)
// always means "the gate is unheld; if no other writer holds it, status
// is True".
const KeyLifecycleInstanceReady = "InstanceReady"

// patchCondition issues a strategic-merge patch updating only the
// single ome.io/serving condition. Kubelet's concurrent writes to
// the other Pod.Status.Conditions entries (PodScheduled, Initialized,
// ContainersReady, Ready, ...) are preserved because patch-merge-key
// on PodCondition is `type`.
func patchCondition(ctx context.Context, c client.Client, pod *corev1.Pod, cond corev1.PodCondition) error {
	patch := map[string]any{
		"status": map[string]any{
			"conditions": []corev1.PodCondition{cond},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal condition patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Status().Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("patch %s on pod %s/%s: %w", ConditionType, pod.Namespace, pod.Name, err)
	}
	return nil
}

// findCondition returns a pointer to the condition matching condType
// in pod.Status.Conditions, or nil. Used by the multi-writer helpers
// to extract the current message list.
func findCondition(pod *corev1.Pod, condType corev1.PodConditionType) *corev1.PodCondition {
	if pod == nil {
		return nil
	}
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// addMessage appends msg to the list parsed from base (an empty
// string is treated as an empty list). Returns changed=false if msg
// is already in the list.
func addMessage(base string, msg Message) (bool, messageList) {
	list := parseList(base)
	for _, m := range list {
		if m == msg {
			return false, list
		}
	}
	list = append(list, msg)
	return true, list
}

// removeMessage drops msg from the list parsed from base. Returns
// changed=false if msg isn't in the list.
func removeMessage(base string, msg Message) (bool, messageList) {
	list := parseList(base)
	var out messageList
	var removed bool
	for _, m := range list {
		if m == msg {
			removed = true
			continue
		}
		out = append(out, m)
	}
	return removed, out
}

// parseList safely decodes a message list from its JSON serialization.
// An empty or malformed string yields nil — the helpers treat that as
// "no writers hold the gate."
func parseList(raw string) messageList {
	if raw == "" {
		return nil
	}
	var list messageList
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return nil
	}
	return list
}

// messageList is a sortable slice of Messages. Stored in a stable
// order so the serialized form is deterministic — operators
// inspecting `kubectl get pod -o yaml` see the same list across
// reconciles when nothing changed.
type messageList []Message

func (l messageList) Len() int      { return len(l) }
func (l messageList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l messageList) Less(i, j int) bool {
	if l[i].UserAgent == l[j].UserAgent {
		return l[i].Key < l[j].Key
	}
	return l[i].UserAgent < l[j].UserAgent
}

// dump serializes the list in stable {UserAgent, Key} sort order.
func (l messageList) dump() string {
	if len(l) == 0 {
		return ""
	}
	sort.Sort(l)
	raw, _ := json.Marshal(l)
	return string(raw)
}
