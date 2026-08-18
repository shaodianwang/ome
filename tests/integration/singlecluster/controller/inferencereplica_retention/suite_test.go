// Package inferencereplicaretention hosts the envtest suite for the
// InferenceReplica controller's ControllerRevision retention sweep.
// Only the IR reconciler runs (no ISVC controller, no kubelet): the
// specs create InferenceReplica objects directly, seed extra non-live
// ControllerRevisions, and assert the sweep prunes them down to the
// resolved limit while live revisions survive. Retention does not
// depend on pod readiness, so envtest is sufficient and stable here.
package inferencereplicaretention

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// configuredRetentionLimit is the operator-level default retention cap
// this suite writes into the inferenceservice-config lifecycle block —
// the config-driven default the binary itself does not carry.
const configuredRetentionLimit = 2

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

func TestInferenceReplicaRetention(t *testing.T) {
	util.RunSuite(t, "InferenceReplica Revision Retention Integration Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// The ConfigMap must exist before the manager starts:
	// SetupInferenceReplica reads the lifecycle block at setup, and the
	// retention sweep re-reads it per reconcile.
	ginkgo.By("seeding inferenceservice-config with lifecycle.revisionHistoryLimit")
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Succeed())
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			"lifecycle": fmt.Sprintf(
				`{"scaleUpPodBatchSize":100,"scaleDownPodBatchSize":100,"revisionHistoryLimit":%d}`,
				configuredRetentionLimit),
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
