// Package pod hosts integration tests for the pod-mutator
// admission webhook (the same handler that runs in production
// against every pod create with an InferenceService or TrainingJob
// label). Boots envtest with a stripped-down testdata/webhooks.yaml
// that registers ONLY /mutate-pods — full manifests.yaml installs
// the ISVC validator + defaulter alongside, neither of which the
// pod mutator suite needs.
package pod

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestPodWebhook(t *testing.T) {
	util.RunSuite(t, "Pod Webhook Integration Suite")
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
		WebhookPath: "tests/integration/singlecluster/webhook/pod/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// Pod mutator reads inferenceservice-config from the OME
	// namespace at every admission request — without this seed,
	// every pod create on a labeled namespace returns a 500 from
	// the webhook (failurePolicy=Fail) and the spec setup fails
	// before reaching its assertion.
	ginkgo.By("seeding inferenceservice-config so the mutator can read its injector configs")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	// SeedDefaultConfigMaps seeds ingress/deploy/multinodeProber/
	// omeAgent — but NOT the per-injector keys the pod mutator's
	// chained injectors expect. The model-init injector's
	// validate() rejects the request unless its config block has
	// Image / CompartmentId / AuthType / VaultId all populated;
	// without this patch, the model-init specs fail at admission
	// with a 500 instead of getting an opportunity to assert.
	ginkgo.By("patching inferenceservice-config with a valid modelInit block")
	cm := &corev1.ConfigMap{}
	gomega.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
		Name:      constants.InferenceServiceConfigMapName,
		Namespace: constants.OMENamespace,
	}, cm)).To(gomega.Succeed())
	cm.Data["modelInit"] = `{
  "image": "ghcr.io/test/model-init:test",
  "compartmentId": "ocid1.compartment.test",
  "authType": "InstancePrincipal",
  "vaultId": "ocid1.vault.test",
  "memoryRequest": "100Mi",
  "memoryLimit": "1Gi",
  "cpuRequest": "100m",
  "cpuLimit": "1"
}`
	gomega.Expect(k8sClient.Update(suiteCtx, cm)).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupPodMutatorWebhook(mgr)
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
