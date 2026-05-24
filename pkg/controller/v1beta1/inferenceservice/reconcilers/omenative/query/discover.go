// Package query holds the read-side primitives shared between the
// top-level reconciler dispatch (discover.go, render.go, services.go,
// status_aggregate.go, watches.go) and the ops/ subpackage. Lives as
// a leaf so ops/ can import it without pulling in the top-level
// omenative package — that would close a cycle since omenative
// dispatches to ops.ReconcileComponentOps.
package query

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
)

// ListOMENativePods returns every pod the OMENative selector matches for
// (isvc, component). Takes a Reader so callers can use either the cached
// client or the live reader (destructive paths need live — see
// LiveListPodsForInstance below).
func ListOMENativePods(ctx context.Context, c client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) ([]*corev1.Pod, error) {
	pods := &corev1.PodList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		LabelManagedBy:                        ManagedByOMENative,
	})
	if err := c.List(ctx, pods, client.InNamespace(isvc.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		out = append(out, &pods.Items[i])
	}
	return out, nil
}

// ListPodsForInstance returns pods for one Instance. Uses ListOMENativePods'
// selector plus an ome.io/instance-index filter — prefer over bucketing the
// full list when only one index is in play. Reader choice: cached for
// reads, live for destructive paths.
func ListPodsForInstance(ctx context.Context, c client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) ([]*corev1.Pod, error) {
	pods, err := ListOMENativePods(ctx, c, isvc, component)
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

// LiveListPodsForInstance returns the pods matching the (component,
// idx) tuple via the live API reader (p.Reader()). Use this from any
// destructive code path — Delete, the zero-old-pods confirmation
// between recreate/restart Phase A and Phase B, Migrate's
// source-drain and source-delete steps — so the controller doesn't
// act on a stale cache view that says "pods gone" while the API
// server still holds them.
//
// Cache-driven ListPodsForInstance is fine for plan computation and
// status aggregation (the OEP §8.3 "Cache Lag Rule"), but every step
// that mutates pod state needs a live read first.
func LiveListPodsForInstance(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) ([]*corev1.Pod, error) {
	return ListPodsForInstance(ctx, p.Reader(), p.ISVC, component, idx)
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
func LiveOldPodsClearedForRecreate(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, currentInc int64) (bool, error) {
	pods, err := LiveListPodsForInstance(ctx, p, component, idx)
	if err != nil {
		return false, err
	}
	// Orphans don't block recreate — they aren't the prior incarnation.
	// Restart's caller routes them through the FoundOrphan event path.
	oldPods, _, _ := PartitionPodsByIncarnation(pods, currentInc)
	return AllTerminating(oldPods), nil
}

// FindInstanceStatus returns the cached InstanceStatus entry, nil if
// absent. Callers must NOT mutate — it aliases the cached ISVC.
func FindInstanceStatus(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	for i := range cs.OMENative.InstanceStatuses {
		if cs.OMENative.InstanceStatuses[i].Index == idx {
			return &cs.OMENative.InstanceStatuses[i]
		}
	}
	return nil
}

// TotalPodsForInstance sums desired pod count across Runners — 1 for
// single-pod, 1+N for leader+worker.
func TotalPodsForInstance(inst core.InstancePlan) int32 {
	var n int32
	for _, r := range inst.Runners {
		n += r.Size
	}
	return n
}
