// Package runtimerevision hosts integration tests for the OEP-0014
// Phase 3 GC controller (deletes stale ControllerRevisions in the OME
// namespace).
package runtimerevision

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestRuntimeRevisionGCController(t *testing.T) {
	util.RunSuite(t, "RuntimeRevision GC Controller Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// Install the immutability webhook alongside the GC controller.
	// Without this the GC suite is blind to the real production
	// interaction: the webhook denies UPDATEs, GC's Mark is an UPDATE.
	// Reusing the webhook suite's testdata keeps a single source of
	// truth for the ValidatingWebhookConfiguration shape.
	s, err := framework.NewSuite(&framework.Framework{
		WebhookPath: "tests/integration/singlecluster/webhook/runtimerevision/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// Ensure the OME namespace exists for the controller to write to.
	err = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		if err := components.SetupRuntimeRevisionWebhooks(mgr); err != nil {
			return err
		}
		// Compressed retention + grace so specs can observe Mark →
		// Wait → Delete transitions in seconds. Grace is long enough
		// that specs can reliably catch the marked state before delete
		// races them, but short enough that the delete spec doesn't
		// stretch the suite by minutes.
		return components.SetupRuntimeRevisionGC(ctx, mgr,
			components.WithRetention(2),
			components.WithGracePeriod(10*time.Second),
			components.WithResync(1*time.Second),
		)
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
