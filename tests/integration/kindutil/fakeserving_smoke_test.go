package kindutil_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// TestKindSmokeFakeServing validates the BuildImages + kind-load
// path end-to-end:
//
//  1. RunKindSuite builds the fake-serving image
//  2. kind load docker-image copies it into the cluster's containerd
//  3. A test pod with imagePullPolicy: IfNotPresent finds the image
//     locally and reaches Ready (kubelet's ContainersReady)
//  4. The pod's /healthz responds 200
//
// Together with TestKindSmoke this closes the P1 acceptance
// criterion "fake-serving image builds + kind-loads successfully."
//
// Skipped automatically when docker isn't available so the test file
// is safe to keep in the package on systems without a working
// container runtime (e.g. pre-Colima Mac dev machines).
func TestKindSmokeFakeServing(t *testing.T) {
	s := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-smoke-fs",
		Workers:     0,
		BuildImages: []kindutil.ImageBuildSpec{{
			ContextDir: "tests/integration/kindutil/fake-serving",
			Tag:        "fake-serving:v1",
			ExtraTags:  []string{"fake-serving:v2"}, // verify alias-tagging works
		}},
	})

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "fs-",
	}}
	if err := s.Client.Create(s.Ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Cleanup(func() { _ = s.Client.Delete(context.Background(), ns) })

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fake-serving-smoke",
			Namespace: ns.Name,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:            "fake-serving",
			Image:           "fake-serving:v1",
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: 8080,
			}},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/healthz",
						Port: intstr.FromInt(8080),
					},
				},
				InitialDelaySeconds: 1,
				PeriodSeconds:       1,
			},
		}}},
	}
	if err := s.Client.Create(s.Ctx, pod); err != nil {
		t.Fatalf("create fake-serving pod: %v", err)
	}

	err := wait.PollUntilContextTimeout(s.Ctx, 1*time.Second, 90*time.Second, true,
		func(ctx context.Context) (bool, error) {
			fresh := &corev1.Pod{}
			if err := s.Client.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			// ContainersReady is the readiness-probe-aggregate kubelet
			// condition; we use it here rather than PodReady because
			// no readiness gate is in play (no OMENative serving-gate
			// on this raw test pod).
			for _, cond := range fresh.Status.Conditions {
				if cond.Type == corev1.ContainersReady && cond.Status == corev1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		})
	if err != nil {
		// Surface pod status for diagnostic value.
		fresh := &corev1.Pod{}
		_ = s.Client.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh)
		t.Fatalf("fake-serving pod never became ContainersReady: %v\npod status: %+v", err, fresh.Status)
	}
}
