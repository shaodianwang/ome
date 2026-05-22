package omenative

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// liveListPodsForInstance returns the pods matching the (component,
// idx) tuple via the live API reader (p.reader()). Use this from any
// destructive code path — Delete, the zero-old-pods confirmation
// between recreate/restart Phase A and Phase B, Migrate's
// source-drain and source-delete steps — so the controller doesn't
// act on a stale cache view that says "pods gone" while the API
// server still holds them.
//
// Cache-driven listPodsForInstance is fine for plan computation and
// status aggregation (the OEP §8.3 "Cache Lag Rule"), but every step
// that mutates pod state needs a live read first.
func liveListPodsForInstance(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32) ([]*corev1.Pod, error) {
	return listPodsForInstance(ctx, p.reader(), p.ISVC, component, idx)
}

// allTerminating reports whether every pod in the slice carries a
// DeletionTimestamp. Used by recreate / restart Phase B to allow
// moving on to Create even when the API server hasn't fully GC'd
// the previous incarnation's pods yet — terminating-but-not-yet-gone
// is a safe state because the previous Delete call already issued
// the foreground propagation.
//
// Empty slice returns true (nothing to terminate, trivially done).
func allTerminating(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			return false
		}
	}
	return true
}

// liveOldPodsClearedForRecreate live-reads pods for the Instance,
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
func liveOldPodsClearedForRecreate(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, currentInc int64) (bool, error) {
	pods, err := liveListPodsForInstance(ctx, p, component, idx)
	if err != nil {
		return false, err
	}
	oldPods, _ := partitionPodsByIncarnation(pods, currentInc)
	return allTerminating(oldPods), nil
}
