// Package query holds the read-side primitives shared between the
// workload pipeline (workload/aggregate.go, workload/ops/...) and the
// per-Component dispatch surfaces still on the ISVC controller
// (omenative/services.go, watches.go). Lives as a leaf so the
// workload package can import it without closing a cycle.
package query

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// ListOMENativePodsByName returns every pod the OMENative selector matches
// for (namespace, isvcName, component). Takes a Reader so callers can use
// either the cached client or the live reader (destructive paths need live
// — see LiveListPodsForInstance below).
//
// Adapters with a typed owner handle (e.g. ISVC, IR) compose the namespace
// + name + component arguments from their own source-of-truth shape; this
// helper stays owner-agnostic so the workload pipeline can call it from
// either adapter path.
func ListOMENativePodsByName(ctx context.Context, c client.Reader, namespace, isvcName string, component workload.ComponentType) ([]*corev1.Pod, error) {
	pods := &corev1.PodList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		LabelManagedBy:                        ManagedByOMENative,
	})
	if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		out = append(out, &pods.Items[i])
	}
	return out, nil
}

// LiveListPodsForInstance returns the pods matching the (component,
// idx) tuple via the supplied live API reader. Use this from any
// destructive code path — Delete, the zero-old-pods confirmation
// between recreate/restart Phase A and Phase B, Migrate's
// source-drain and source-delete steps — so the controller doesn't
// act on a stale cache view that says "pods gone" while the API
// server still holds them.
//
// Cache-driven ListOMENativePodsByName is fine for plan computation and
// status aggregation (stale reads are acceptable for non-destructive
// operations), but every step that mutates pod state needs a live read
// first.
//
// Takes primitive args (reader + namespace + isvcName + component) so
// the workload/query leaf doesn't take on a backward import to either
// workload.Deps / workload.Key or to a Component-spec carrier type —
// keeps the leaf importable by both adapter paths without closing a
// cycle. Adapters compose these from their own Deps + key.
func LiveListPodsForInstance(ctx context.Context, reader client.Reader, namespace, isvcName string, component workload.ComponentType, idx int32) ([]*corev1.Pod, error) {
	pods, err := ListOMENativePodsByName(ctx, reader, namespace, isvcName, component)
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if i, ok := InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out, nil
}

// BucketPodsByInstanceIdx groups pods by their ome.io/instance-index
// label. Pods missing the label are dropped — they were never created
// by OMENative.
func BucketPodsByInstanceIdx(pods []*corev1.Pod) map[int32][]*corev1.Pod {
	by := make(map[int32][]*corev1.Pod)
	for _, pod := range pods {
		idx, ok := InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		by[idx] = append(by[idx], pod)
	}
	return by
}

// IndexPodsByName returns a name -> *Pod lookup. Used by Create's
// desired-vs-existing diff and Recreate's Phase-B race-on-create check.
func IndexPodsByName(pods []*corev1.Pod) map[string]*corev1.Pod {
	m := make(map[string]*corev1.Pod, len(pods))
	for _, pod := range pods {
		m[pod.Name] = pod
	}
	return m
}

// InstanceIdxFromLabels returns (0, false) when the ome.io/instance-index
// label is absent or malformed.
func InstanceIdxFromLabels(pod *corev1.Pod) (int32, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[LabelInstanceIdx]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

// InstanceIncarnationFromLabels returns (0, false) when the
// ome.io/instance-incarnation label is absent or malformed.
func InstanceIncarnationFromLabels(pod *corev1.Pod) (int64, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[LabelInstanceIncarnation]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// PodOrdinalFromLabels returns the pod-naming ordinal stamped via
// LabelPodOrdinal. Pre-feature pods lack the label and fall back to 0
// — matches their actual ordinal suffix in the pod name. Returns
// (0, false) only on malformed values so callers can distinguish
// "legacy pod, ordinal 0" from a parse error.
func PodOrdinalFromLabels(pod *corev1.Pod) (int32, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[LabelPodOrdinal]
	if !ok {
		return 0, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

// PartitionPodsByIncarnation splits pods by the ome.io/instance-incarnation
// label:
//   - old: value < current — safe to delete (prior materialization).
//   - fresh: value >= current — the new set, kept even if strictly above
//     (deleting future-labeled pods could lose work).
//   - unknown: label missing/malformed — orphan candidates. Callers refuse
//     to delete and emit FoundOrphan; an operator must re-classify the
//     pod before Restart can proceed. Without this guard, a stripped
//     label or third-party reuse of managed-by would silently tear down
//     foreign pods.
func PartitionPodsByIncarnation(pods []*corev1.Pod, current int64) (old, fresh, unknown []*corev1.Pod) {
	for _, pod := range pods {
		inc, ok := InstanceIncarnationFromLabels(pod)
		if !ok {
			unknown = append(unknown, pod)
			continue
		}
		if inc < current {
			old = append(old, pod)
			continue
		}
		fresh = append(fresh, pod)
	}
	return
}

// AllPodsRuntimeReady reports whether every pod has the ContainersReady
// PodCondition. Empty input returns false (nothing is Ready).
func AllPodsRuntimeReady(pods []*corev1.Pod) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if !podreadiness.IsContainersReady(pod) {
			return false
		}
	}
	return true
}

// AllTerminating reports whether every pod in the slice carries a
// DeletionTimestamp. Used by recreate / restart Phase B to allow
// moving on to Create even when the API server hasn't fully GC'd
// the previous incarnation's pods yet — terminating-but-not-yet-gone
// is a safe state because the previous Delete call already issued
// the foreground propagation.
//
// Empty slice returns true (nothing to terminate, trivially done).
func AllTerminating(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			return false
		}
	}
	return true
}

// LiveOldPodsClearedForRecreate live-reads pods for the Instance,
// partitions by incarnation against currentInc, and reports whether
// every OLD pod (incarnation < currentInc OR missing the label) is
// either gone or terminating. New-incarnation pods (those Phase B
// may have already created on a prior pass) are NOT inspected here —
// the Phase B caller does its own desired-vs-existing diff against
// them.
//
// The boundary check between Phase A (drain + delete) and Phase B
// (create) in recreate / restart ops. Without it, the cache may show
// stale old pods that Phase A already deleted (blocking Phase B
// indefinitely), or the cache may show zero pods while the API
// server still holds them (letting Phase B issue a stable-name
// Create that AlreadyExists-conflicts against the still-terminating
// previous incarnation). Tolerating terminating-but-not-yet-gone
// matches K8s foreground propagation semantics.
//
// Same primitive-arg shape as LiveListPodsForInstance (see comment
// there for the rationale).
func LiveOldPodsClearedForRecreate(ctx context.Context, reader client.Reader, namespace, isvcName string, component workload.ComponentType, idx int32, currentInc int64) (bool, error) {
	pods, err := LiveListPodsForInstance(ctx, reader, namespace, isvcName, component, idx)
	if err != nil {
		return false, err
	}
	// Orphans don't block recreate — they aren't the prior incarnation.
	// Restart's caller routes them through the FoundOrphan event path.
	oldPods, _, _ := PartitionPodsByIncarnation(pods, currentInc)
	return AllTerminating(oldPods), nil
}
