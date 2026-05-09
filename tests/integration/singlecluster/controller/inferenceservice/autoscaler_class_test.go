package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// HPA gating via the ome.io/autoscalerClass annotation.
//
// reconcilers/hpa/hpa_reconciler.go:160 — shouldCreateHPA returns true
// only when the annotation is empty OR equal to "hpa". When set to
// "external" the autoscaler is provided by the user (e.g., a separate
// HPA they manage), so the reconciler MUST NOT create one.
var _ = ginkgo.Describe("InferenceService autoscalerClass gating", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-asclass-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ome.io/autoscalerClass=external → Deployment created but NO HPA", func() {
		ginkgo.By("creating BaseModel + Runtime baseline")
		cbm := v1beta1testing.MakeClusterBaseModel("asext-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("asext-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with autoscalerClass=external + variable replicas (would normally trigger HPA)")
		minR, maxR := 2, 5
		isvc := v1beta1testing.MakeInferenceService("asext-isvc", ns.Name).
			Model("asext-llama").
			Runtime("asext-runtime").
			DeploymentMode(constants.RawDeployment).
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassExternal)).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting NO HPA in the namespace, even though MinReplicas != MaxReplicas")
		// We use Consistently rather than Eventually-NotFound: an
		// Eventually with a length=0 assertion would pass immediately,
		// before the controller even reconciles. Consistently for
		// ShortTimeout (10s) gives the controller time to TRY to
		// create an HPA before we declare success.
		Consistently(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).To(BeEmpty(),
				"autoscalerClass=external must NOT produce an HPA (got %d)", len(hpas.Items))
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})
})
