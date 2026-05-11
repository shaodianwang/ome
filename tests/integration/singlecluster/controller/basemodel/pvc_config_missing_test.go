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
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/components"
	"github.com/sgl-project/ome/tests/integration/util"
)

// PVC reconcile path WITHOUT a configured ome-agent image. pvc.go's
// ensureMetadataJob bails at "cfg.Image == \"\"" with a terminal
// failure (PVCConfigMissing) because the metadata extraction Job
// can't be built without an image — and a missing image is an
// operator-side config error, not something the BaseModel CR itself
// can fix. Only recoverable by editing inferenceservice-config and
// restarting the controller.
//
// Same Ordered + BeforeAll/AfterAll restart dance as
// sharded_no_provider_test.go: the no-config setup is fundamentally
// different from every other PVC spec; can't express both in one
// manager.
var _ = ginkgo.Describe("BaseModel PVC reconcile without ome-agent config", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(func() {
		ginkgo.By("stopping the suite-level manager and restarting with NO WithOmeAgentConfig")
		Expect(suiteFwk.StopManager(suiteCtx)).To(Succeed())

		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			// Sharded provider stays so unrelated specs in the suite
			// continue to work; OmeAgentConfig is intentionally not
			// passed → cfg.Image is empty → PVCConfigMissing fires.
			return components.SetupBaseModel(ctx, mgr,
				components.WithModelCacheProvider(modelcache.NewProvider("fake", suiteFakeMC)))
		}
		Expect(suiteFwk.StartManager(suiteCtx, suiteCfg, mgrSetup)).To(Succeed())
	})

	ginkgo.AfterAll(func() {
		ginkgo.By("restoring the suite's default manager (with both ome-agent config and provider) for subsequent specs")
		Expect(suiteFwk.StopManager(suiteCtx)).To(Succeed())

		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			return components.SetupBaseModel(ctx, mgr,
				components.WithOmeAgentConfig(&controllerconfig.OmeAgentConfig{
					Image:          "ghcr.io/test/ome-agent:test",
					ServiceAccount: "ome-model-metadata",
				}),
				components.WithModelCacheProvider(modelcache.NewProvider("fake", suiteFakeMC)))
		}
		Expect(suiteFwk.StartManager(suiteCtx, suiteCfg, mgrSetup)).To(Succeed())
	})

	ginkgo.It("PVC-backed BaseModel without ome-agent config → state=Failed, Ready=False reason=PVCConfigMissing", func() {
		ns := "pvc-no-cfg"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "pvc-no-cfg", ns)
		bindPVC(suiteCtx, k8sClient, "pvc-no-cfg", ns)

		bm := v1beta1testing.MakeBaseModel("pvc-no-cfg-llama", ns).
			StorageURI("pvc://pvc-no-cfg/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "pvc-no-cfg-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed),
				"missing ome-agent image must surface as terminal Failed (no Job can be built)")

			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPVCConfigMissing))
			g.Expect(ready.Message).To(ContainSubstring("ome-agent image is not configured"),
				"message must point operators at the omeAgent config block, not at the BaseModel itself")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("non-PVC BaseModel works normally even without ome-agent config (PVCConfigMissing only blocks PVC path)", func() {
		// Defensive guard: the per-node download path does NOT need
		// the ome-agent image (real model-agent has its own
		// configuration loaded from the daemonset's environment).
		// PVCConfigMissing is a PVC-only constraint; specs using
		// other storage types must keep working under this manager
		// config.
		ns := "pvc-no-cfg-oci"
		makeNamespace(suiteCtx, k8sClient, ns)

		bm := v1beta1testing.MakeBaseModel("oci-no-cfg", ns).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		// State stays at the per-node default (InTransit) until an
		// agent reports — proves the controller did NOT force-fail
		// this BM with PVCConfigMissing.
		Consistently(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "oci-no-cfg", Namespace: ns}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			if ready != nil {
				g.Expect(ready.Reason).NotTo(Equal(v1beta1.ModelConditionReasonPVCConfigMissing),
					"non-PVC BMs must not pick up PVCConfigMissing under any reconciler config")
			}
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
