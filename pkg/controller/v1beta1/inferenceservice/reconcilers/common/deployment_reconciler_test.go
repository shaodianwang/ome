package common

import (
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	istiov1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	lwsapi "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/istiosidecar"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/lws"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/multinode"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/podmonitor"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
)

// newMultiNodeReconcilerForRefTest assembles a MultiNodeReconciler whose
// sub-reconcilers hold only the exported child objects setMultiNodeReferences
// touches. The sidecar arg is nil when sidecar injection is disabled, which
// exercises the nil-guard path.
func newMultiNodeReconcilerForRefTest(meta metav1.ObjectMeta, sidecar *istiov1beta1.Sidecar) *multinode.MultiNodeReconciler {
	return &multinode.MultiNodeReconciler{
		LWS: &lws.LWSReconciler{
			LWS: &lwsapi.LeaderWorkerSet{ObjectMeta: meta},
		},
		Service: &service.ServiceReconciler{
			Service: &corev1.Service{ObjectMeta: meta},
		},
		PodMonitor: &podmonitor.PodMonitorReconciler{
			PodMonitor: &monitoringv1.PodMonitor{ObjectMeta: meta},
		},
		IstioSidecar: &istiosidecar.IstioSidecarReconciler{
			Sidecar: sidecar,
		},
	}
}

func ownsISVC(refs []metav1.OwnerReference, isvc *v1beta1.InferenceService) bool {
	for _, ref := range refs {
		if ref.Kind == "InferenceService" && ref.Name == isvc.Name &&
			ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

// TestSetMultiNodeReferences_SidecarOwnerRef pins the fix for the orphaned
// Istio Sidecar leak: when the MultiNode path builds a Sidecar (injection
// enabled), setMultiNodeReferences must stamp a controller owner-ref pointing
// at the ISVC so the Sidecar is garbage-collected when the ISVC is deleted.
// The sibling resources (LWS, Service, PodMonitor) already get one; the
// Sidecar was previously omitted.
func TestSetMultiNodeReferences_SidecarOwnerRef(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
	}
	meta := metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"}

	r := &DeploymentReconciler{
		Scheme: scheme,
		Log:    ctrl.Log.WithName("test"),
	}

	t.Run("sidecar present gets owner-ref to ISVC", func(t *testing.T) {
		sidecar := &istiov1beta1.Sidecar{ObjectMeta: meta}
		mnr := newMultiNodeReconcilerForRefTest(meta, sidecar)

		require.NoError(t, r.setMultiNodeReferences(isvc, mnr))

		// The fix under test.
		assert.True(t, ownsISVC(mnr.IstioSidecar.Sidecar.OwnerReferences, isvc),
			"Istio Sidecar must carry a controller owner-ref to the ISVC (else it leaks on delete)")

		// Siblings keep their refs too (regression guard).
		assert.True(t, ownsISVC(mnr.LWS.LWS.OwnerReferences, isvc), "LWS owner-ref missing")
		assert.True(t, ownsISVC(mnr.Service.Service.OwnerReferences, isvc), "Service owner-ref missing")
		assert.True(t, ownsISVC(mnr.PodMonitor.PodMonitor.OwnerReferences, isvc), "PodMonitor owner-ref missing")
	})

	t.Run("nil sidecar is a no-op (injection disabled)", func(t *testing.T) {
		mnr := newMultiNodeReconcilerForRefTest(meta, nil)

		// Must not panic and must still ref the siblings.
		require.NoError(t, r.setMultiNodeReferences(isvc, mnr))
		assert.True(t, ownsISVC(mnr.LWS.LWS.OwnerReferences, isvc), "LWS owner-ref missing")
		assert.True(t, ownsISVC(mnr.Service.Service.OwnerReferences, isvc), "Service owner-ref missing")
		assert.True(t, ownsISVC(mnr.PodMonitor.PodMonitor.OwnerReferences, isvc), "PodMonitor owner-ref missing")
	})
}
