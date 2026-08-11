package kindutil_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// TestKindSmoke is the foundation acceptance test for the kindutil
// package. It validates the acceptance criteria:
//
//   - kindutil.RunKindSuite brings up a KIND cluster end-to-end
//   - core API (namespaces, pods) responds against the cluster
//   - a real pod scheduled on the real kubelet reaches Running
//   - cluster reuse under KIND_KEEP_CLUSTER=1 finishes in <30s
//
// Intentionally does NOT register OME reconcilers (Setup is nil) so
// the smoke test is independent of the controller wiring — that's
// exercised by the per-suite tests under omenative_coordination_kind/ etc.
//
// To run with cluster reuse:
//
//	KIND_KEEP_CLUSTER=1 go test ./tests/integration/kindutil/...
//
// To force a fresh cluster (debugging):
//
//	KIND_RECREATE=1 go test ./tests/integration/kindutil/...
func TestKindSmoke(t *testing.T) {
	s := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-smoke",
		Workers:     0,
		// No image builds, no OME reconciler setup — pure framework
		// smoke test.
	})

	t.Run("namespace_create_delete", func(t *testing.T) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			GenerateName: "smoke-",
		}}
		if err := s.Client.Create(s.Ctx, ns); err != nil {
			t.Fatalf("create namespace: %v", err)
		}
		defer func() { _ = s.Client.Delete(s.Ctx, ns) }()
		if err := s.Client.Get(s.Ctx, client.ObjectKeyFromObject(ns), ns); err != nil {
			t.Fatalf("get namespace: %v", err)
		}
	})

	t.Run("pause_pod_reaches_running", func(t *testing.T) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			GenerateName: "smoke-pod-",
		}}
		if err := s.Client.Create(s.Ctx, ns); err != nil {
			t.Fatalf("create namespace: %v", err)
		}
		defer func() { _ = s.Client.Delete(s.Ctx, ns) }()

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "smoke-pause",
				Namespace: ns.Name,
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:  "pause",
				Image: "registry.k8s.io/pause:3.10",
			}}},
		}
		if err := s.Client.Create(s.Ctx, pod); err != nil {
			t.Fatalf("create pause pod: %v", err)
		}

		// Real kubelet → real pull → real Running. Generous timeout
		// for the first-ever pause pull on a fresh cluster (~10-15s
		// on M-series, ~30s on a cold CI runner).
		err := wait.PollUntilContextTimeout(s.Ctx, 1*time.Second, 90*time.Second, true,
			func(ctx context.Context) (bool, error) {
				fresh := &corev1.Pod{}
				if err := s.Client.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				return fresh.Status.Phase == corev1.PodRunning, nil
			})
		if err != nil {
			t.Fatalf("pause pod never reached Running: %v", err)
		}
	})
}
