// Package benchmarkjob hosts integration tests for the
// BenchmarkJobReconciler. The reconciler creates a batch/v1 Job
// per BenchmarkJob CR and tracks status; specs here exercise the
// finalizer + Job-creation path end-to-end.
package benchmarkjob

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestBenchmarkJobController(t *testing.T) {
	util.RunSuite(t, "BenchmarkJob Controller Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	ginkgo.By("seeding benchmarkjob-config so the reconciler can read its pod-template defaults")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// BaseModel + InferenceService reconcilers required for
		// specs that reference an existing ISVC via
		// Endpoint.InferenceService — the BenchmarkJob reconciler
		// looks up the ISVC and re-queues until isvc.Status.IsReady.
		if err := components.SetupBaseModel(ctx, mgr); err != nil {
			return err
		}
		if err := components.SetupInferenceService(ctx, mgr); err != nil {
			return err
		}
		return components.SetupBenchmarkJob(ctx, mgr)
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
