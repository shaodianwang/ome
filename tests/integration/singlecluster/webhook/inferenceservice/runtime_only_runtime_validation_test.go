package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

func waitForClusterServingRuntimeCache(runtime *v1beta1.ClusterServingRuntime) {
	ginkgo.GinkgoHelper()
	cachedRuntime := &v1beta1.ClusterServingRuntime{}
	Eventually(func() error {
		return suiteFwk.Manager().GetClient().Get(suiteCtx, client.ObjectKey{Name: runtime.Name}, cachedRuntime)
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
}

var _ = ginkgo.Describe("Runtime-only runtime validation", func() {
	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("accepts an enabled live-synced runtime", func() {
		runtime := v1beta1testing.MakeClusterServingRuntime("runtime-only-enabled").Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })
		waitForClusterServingRuntimeCache(runtime)

		isvc := v1beta1testing.MakeInferenceService("runtime-only-enabled", "isvc-webhook").
			Runtime(runtime.Name).
			Engine(&v1beta1.EngineSpec{}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("rejects a disabled live-synced runtime", func() {
		runtime := v1beta1testing.MakeClusterServingRuntime("runtime-only-disabled").
			Disabled(true).
			Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })
		waitForClusterServingRuntimeCache(runtime)

		isvc := v1beta1testing.MakeInferenceService("runtime-only-disabled", "isvc-webhook").
			Runtime(runtime.Name).
			AutoSync(true).
			Engine(&v1beta1.EngineSpec{}).
			Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("disabled"))
	})

	ginkgo.It("accepts a missing live-synced runtime", func() {
		isvc := v1beta1testing.MakeInferenceService("runtime-only-missing", "isvc-webhook").
			Runtime("runtime-only-missing").
			Engine(&v1beta1.EngineSpec{}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("accepts a disabled source when autoSync is false", func() {
		runtime := v1beta1testing.MakeClusterServingRuntime("runtime-only-pinned").
			Disabled(true).
			Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })
		waitForClusterServingRuntimeCache(runtime)

		isvc := v1beta1testing.MakeInferenceService("runtime-only-pinned", "isvc-webhook").
			Runtime(runtime.Name).
			AutoSync(false).
			Engine(&v1beta1.EngineSpec{}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})
})
