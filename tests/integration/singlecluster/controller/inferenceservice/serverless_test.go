package inferenceservice

import (
	"strconv"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Serverless-mode InferenceService specs.
//
// Trigger condition (utils/deployment.go:DetermineEngineDeploymentMode):
//
//	engine.MinReplicas != nil && *engine.MinReplicas == 0  →  Serverless
//
// In Serverless mode the controller produces a Knative Service
// (serving.knative.dev/v1) instead of a regular Deployment. The
// Knative bundle MUST be installed in the suite (suite_test.go opts
// in via OptionalCRDs); without it, handleServerlessPrerequisites
// returns a terminal error.
var _ = ginkgo.Describe("InferenceService Serverless", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-serverless-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		// Best-effort cleanup of any KSvcs in the test namespace
		// before the namespace itself is dropped.
		ksvcs := &knservingv1.ServiceList{}
		_ = k8sClient.List(suiteCtx, ksvcs, client.InNamespace(ns.Name))
		for i := range ksvcs.Items {
			_ = k8sClient.Delete(suiteCtx, &ksvcs.Items[i])
		}
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("MinReplicas=0 → Knative Service with min/max-scale annotations, runtime image, and 100%-latest traffic", func() {
		ginkgo.By("creating BaseModel + Runtime baseline (engineConfig.runner is the canonical place for the engine container)")
		cbm := v1beta1testing.MakeClusterBaseModel("serverless-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("serverless-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:serverless"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC with Engine.MinReplicas=0 + MaxReplicas=5 to opt into Serverless")
		const minReplicas, maxReplicas = 0, 5
		isvc := v1beta1testing.MakeInferenceService("serverless-isvc", ns.Name).
			Model("serverless-llama-7b").
			Runtime("serverless-runtime").
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To(minReplicas),
					MaxReplicas: maxReplicas,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting EXACTLY one Knative Service with the expected traffic + scale annotations + runtime image")
		Eventually(func(g Gomega) {
			ksvcs := &knservingv1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, ksvcs, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(ksvcs.Items).To(HaveLen(1),
				"Serverless mode must produce exactly one Knative Service")

			ksvc := &ksvcs.Items[0]

			// Scale annotations live on the RevisionTemplateSpec
			// (template.metadata.annotations), per
			// reconcilers/knative/ksvc_reconciler.go:155.
			templAnno := ksvc.Spec.Template.ObjectMeta.Annotations
			g.Expect(templAnno).To(HaveKeyWithValue(constants.MinScaleAnnotationKey, strconv.Itoa(minReplicas)),
				"%s must reflect Engine.MinReplicas", constants.MinScaleAnnotationKey)
			g.Expect(templAnno).To(HaveKeyWithValue(constants.MaxScaleAnnotationKey, strconv.Itoa(maxReplicas)),
				"%s must reflect Engine.MaxReplicas", constants.MaxScaleAnnotationKey)

			// Default (no canary) traffic split is 100% → latest revision.
			g.Expect(ksvc.Spec.Traffic).To(HaveLen(1),
				"non-canary KSvc must have a single traffic target")
			t := ksvc.Spec.Traffic[0]
			g.Expect(t.LatestRevision).NotTo(BeNil())
			g.Expect(*t.LatestRevision).To(BeTrue(),
				"the single traffic target must point at the latest revision")
			g.Expect(t.Percent).NotTo(BeNil())
			g.Expect(*t.Percent).To(BeEquivalentTo(100),
				"100%% of traffic should go to the latest revision in default rollout")

			// The KSvc's pod template must have inherited the runtime's
			// engineConfig.runner image — proves the canonical
			// engineConfig.runner pattern flows through Serverless mode too.
			containers := ksvc.Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty(),
				"KSvc template must contain at least one container")
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/runtime:serverless"),
				"KSvc container[0] must come from the runtime's engineConfig.runner image")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
