// Package inferencereplicastandalone hosts the envtest suite for a
// standalone InferenceReplica: one created directly, with rendered runners
// and no InferenceService. Only the IR reconciler runs (no ISVC controller,
// no kubelet), so the specs assert the objects the controller derives from
// the replica's own name and where its events land; readiness is covered by
// the KIND spec of the same name.
package inferencereplicastandalone

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

func TestInferenceReplicaStandalone(t *testing.T) {
	util.RunSuite(t, "InferenceReplica Standalone Integration Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// The lifecycle block carries the batch sizes SetupInferenceReplica
	// reads at startup and deliberately no instanceReadyTimeout, so the
	// controller raises its "no readiness deadline" warning; where that
	// warning lands is one of the assertions.
	ginkgo.By("seeding inferenceservice-config")
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Succeed())
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			controllerconfig.LifecycleConfigName:       `{"scaleUpPodBatchSize":100,"scaleDownPodBatchSize":100,"requeue":{"operation":"5s","gate":"3s"}}`,
			controllerconfig.OMENativeStatusConfigName: util.OMENativeStatusConfigJSON(),
		},
	})).To(gomega.Succeed())

	ginkgo.By("registering the InferenceReplica reconciler")
	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// The workload pipeline lists pods through this cache field
		// index (mirrors the production manager wiring).
		if err := query.RegisterOMENativePodIndex(ctx, mgr.GetFieldIndexer()); err != nil {
			return err
		}
		return components.SetupInferenceReplica(ctx, mgr)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})
