// Package basemodel hosts integration tests for the BaseModel /
// ClusterBaseModel reconcilers driven against envtest via the shared
// framework.
package basemodel

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/modelcache"
	mcfake "github.com/ome-projects/ome/pkg/modelcache/fake"

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestBaseModelController(t *testing.T) {
	util.RunSuite(t, "BaseModel Controller Integration Suite")
}

// Suite-wide state. Populated by BeforeSuite, used by each Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	suiteCfg    *rest.Config
	k8sClient   client.Client

	// suiteFakeMC is exposed package-private so the Sharded specs can
	// re-script its CheckSourceFunc / StartPrewarmFunc /
	// GetPrewarmStatusFunc per-test. PVC specs don't touch it because
	// the PVC reconcile branch never invokes the modelcache provider.
	suiteFakeMC *mcfake.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	suiteCfg = s.Cfg
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	suiteFakeMC = &mcfake.Client{}
	provider := modelcache.NewProvider("fake", suiteFakeMC)

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupBaseModel(ctx, mgr,
			components.WithOmeAgentConfig(&controllerconfig.OmeAgentConfig{
				Image:          "ghcr.io/test/ome-agent:test",
				ServiceAccount: "ome-model-metadata",
			}),
			components.WithModelCacheProvider(provider))
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())

	// Pre-create the OME namespace so per-PVC ConfigMaps land somewhere.
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Or(gomega.BeNil(), gomega.MatchError(gomega.ContainSubstring("already exists"))))
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
