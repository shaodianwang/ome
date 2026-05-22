package omenative

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
)

// IsContainersReady reports whether the kubelet-owned ContainersReady
// condition is True — i.e., all containers in the pod have passed their
// readiness probes.
//
// This is deliberately not corev1.PodReady. PodReady is the AND of
// ContainersReady and every readiness gate, including the controller-
// owned ome.io/serving gate. Gating MarkPodServing on PodReady would
// deadlock: kubelet won't flip PodReady=True until ome.io/serving=True,
// and OMENative won't write ome.io/serving=True until PodReady=True.
// ContainersReady reflects only probe-level readiness and is the correct
// signal that the runtime is healthy enough to receive traffic.
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

// IsServing reports whether the ome.io/serving condition has been
// written as True — i.e., no writer currently holds the pod NotReady
// via the multi-writer protocol in podreadiness.
func IsServing(pod *corev1.Pod) bool {
	return podreadiness.IsServing(pod)
}

// MarkPodServing removes the {userAgent, key} entry from the
// ome.io/serving condition's writer list. If the resulting list is
// empty (or the condition didn't exist yet — fresh pod case), the
// condition is written with Status=True and the pod becomes eligible
// for Service rotation.
//
// Idempotent: removing a key that isn't present is a no-op (no
// patch issued) when the condition is already Status=True.
func MarkPodServing(ctx context.Context, c client.Client, pod *corev1.Pod, userAgent, key string) error {
	return podreadiness.RemoveNotReadyKey(ctx, c, pod, podreadiness.Message{UserAgent: userAgent, Key: key})
}

// MarkPodNotServing adds the {userAgent, key} entry to the
// ome.io/serving condition's writer list. Status becomes False
// (because the list is non-empty). The pod is removed from Service
// rotation via kube-proxy's standard PodReady gate machinery.
//
// Idempotent: re-adding the same key is a no-op.
func MarkPodNotServing(ctx context.Context, c client.Client, pod *corev1.Pod, userAgent, key string) error {
	return podreadiness.AddNotReadyKey(ctx, c, pod, podreadiness.Message{UserAgent: userAgent, Key: key})
}

// Writer userAgents used across the OMENative ops. Centralized here
// so a stray drift in any one site is easy to catch in `git grep`.
const (
	// WriterLifecycle is what flips a fresh pod's gate to Status=True
	// once all Instance siblings reach ContainersReady. The key
	// (InstanceReady) is the same across all sites — the userAgent +
	// key tuple is the lookup; we never need to differentiate.
	WriterLifecycle = "Lifecycle"

	// WriterUpdateInPlace drains a pod for in-place container image
	// update. Same key on Add (drain) and Remove (un-drain) on the
	// same pod.
	WriterUpdateInPlace = "Update-in-place"

	// WriterUpdateRecreateDrain drains the OLD pods for a recreate
	// update. The NEW pods are fresh and go through Lifecycle.
	WriterUpdateRecreateDrain = "Update-recreate-drain"

	// WriterRestartDrain drains the OLD pods for an Instance restart.
	// The NEW pods are fresh and go through Lifecycle.
	WriterRestartDrain = "Restart-drain"

	// WriterDeleteDrain drains a pod for scale-down deletion. The
	// key is removed by virtue of the pod being deleted; no explicit
	// RemoveNotReadyKey is required.
	WriterDeleteDrain = "Delete-drain"

	// WriterMigrateSourceDrain drains the source pods of a surge
	// migration. The key is removed by virtue of the source pods
	// being deleted at the end of the migration.
	WriterMigrateSourceDrain = "Migrate-source-drain"
)

// KeyLifecycleInstanceReady is the universal Lifecycle key. Matches
// RBG's convention. The pair (WriterLifecycle, KeyLifecycleInstanceReady)
// always means "the gate is unheld; if no other writer holds it,
// status is True".
const KeyLifecycleInstanceReady = "InstanceReady"
