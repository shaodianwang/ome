// Package inferenceservicestatuscontracts hosts integration tests for
// the OMENative status-projection contracts: IR.Status → ISVC Lifecycle
// subtree, the derived top-level Component conditions, and the no-op
// write-avoidance guarantee.
//
// The suite runs its own manager (separate from the general
// inferenceservice suite) because the OMENative reconcile path requires
// the OMENative pod field index on the manager cache — mirroring
// cmd/manager — before coordination and the status rollup can run. The
// IR controller is deliberately NOT registered: specs write IR.Status by
// hand so the assertions isolate the ISVC-side projection layer.
package inferenceservicestatuscontracts

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestInferenceServiceStatusContracts is the Go entrypoint Ginkgo hooks into.
func TestInferenceServiceStatusContracts(t *testing.T) {
	util.RunSuite(t, "InferenceService Status Contracts Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{
			// KEDA keeps the autoscaler dispatch path identical to the
			// general inferenceservice suite even though every spec here
			// pins AutoscalerNone.
			framework.KEDA,
			// PrometheusOperator registers PodMonitor (scheme + CRD); the
			// ISVC reconcile path touches PodMonitor unconditionally.
			framework.PrometheusOperator,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	ginkgo.By("seeding inferenceservice-config so the reconciler can read it at reconcile time")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	ginkgo.By("registering the BaseModel + InferenceService reconcilers with the OMENative pod index")
	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// The coordination layer's pod observation lists via the
		// OMENative pod field index; register it exactly as cmd/manager
		// does or every OMENative reconcile aborts before the rollup.
		if err := query.RegisterOMENativePodIndex(ctx, mgr.GetFieldIndexer()); err != nil {
			return err
		}
		if err := components.SetupBaseModel(ctx, mgr); err != nil {
			return err
		}
		return components.SetupInferenceService(ctx, mgr)
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
