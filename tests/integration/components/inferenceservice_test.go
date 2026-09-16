package components_test

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// SetupInferenceService smoke test: registering the reconciler must
// not error and the manager must start cleanly. End-to-end reconcile
// behavior is covered in tests/integration/singlecluster/controller/
// inferenceservice/.
var _ = ginkgo.Describe("components.SetupInferenceService", func() {
	ginkgo.It("registers the InferenceServiceReconciler and manager starts cleanly", func() {
		s, err := framework.NewSuite(&framework.Framework{})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		// The reconciler reads the omenativeStatus block at setup, as the
		// manager binary does, so the config must exist before registration.
		Expect(util.SeedDefaultConfigMaps(s.Ctx, s.Client)).To(Succeed())

		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			return components.SetupInferenceService(ctx, mgr)
		}
		Expect(s.Fwk.StartManager(s.Ctx, s.Cfg, mgrSetup)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.StopManager(s.Ctx) })
	})
})
