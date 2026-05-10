package basemodel

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/modelcache"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/components"
	"github.com/sgl-project/ome/tests/integration/util"
)

// Sharded WITHOUT a model-cache provider — restart the manager with
// no WithModelCacheProvider option so applyProviderNotConfiguredStatus
// fires. Restoring the suite-level provider in AfterAll keeps the
// rest of the suite running normally.
//
// The Ordered + BeforeAll/AfterAll restart dance is needed because
// the no-provider config is fundamentally different from every other
// Sharded spec; there's no way to express both in one manager.
var _ = ginkgo.Describe("Sharded BaseModel without a configured model-cache provider", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(func() {
		ginkgo.By("stopping the suite-level manager and restarting with no WithModelCacheProvider")
		Expect(suiteFwk.StopManager(suiteCtx)).To(Succeed())

		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			// Same as suite_test.go's BeforeSuite minus the
			// WithModelCacheProvider call → provider.Configured()
			// returns false → applyProviderNotConfiguredStatus fires.
			return components.SetupBaseModel(ctx, mgr,
				components.WithOmeAgentConfig(&controllerconfig.OmeAgentConfig{
					Image:          "ghcr.io/test/ome-agent:test",
					ServiceAccount: "ome-model-metadata",
				}))
		}
		Expect(suiteFwk.StartManager(suiteCtx, suiteCfg, mgrSetup)).To(Succeed())
	})

	ginkgo.AfterAll(func() {
		ginkgo.By("restoring the suite's default manager (with provider injected) for subsequent specs")
		Expect(suiteFwk.StopManager(suiteCtx)).To(Succeed())

		provider := modelcache.NewProvider("fake", suiteFakeMC)
		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			return components.SetupBaseModel(ctx, mgr,
				components.WithOmeAgentConfig(&controllerconfig.OmeAgentConfig{
					Image:          "ghcr.io/test/ome-agent:test",
					ServiceAccount: "ome-model-metadata",
				}),
				components.WithModelCacheProvider(provider))
		}
		Expect(suiteFwk.StartManager(suiteCtx, suiteCfg, mgrSetup)).To(Succeed())
	})

	ginkgo.It("Sharded BaseModel without a provider → all conditions reason=ProviderNotConfigured", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("sharded-no-provider").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-no-provider"}, got)).To(Succeed())
			for _, condType := range []string{
				v1beta1.ModelConditionSourceReachable,
				v1beta1.ModelConditionCached,
				v1beta1.ModelConditionReady,
			} {
				cond := meta.FindStatusCondition(got.Status.Conditions, condType)
				g.Expect(cond).NotTo(BeNil(), "condition %q must be set", condType)
				g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonProviderNotConfigured),
					"condition %q must carry reason=ProviderNotConfigured", condType)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
