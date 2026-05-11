package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// ensureNamespace creates the "isvc-webhook" namespace used by every
// spec's namespaced ISVC. Shared by every Describe via BeforeEach so
// each Describe's specs work in isolation under -ginkgo.focus.
func ensureNamespace() {
	ginkgo.GinkgoHelper()
	_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-webhook"},
	})
}

// Webhook envtest specs for the InferenceService validating webhook.
// Each spec sends a Create (or Update) through the apiserver and
// asserts the admission decision — same logic the unit tests in
// pkg/webhook/admission/isvc/ cover, but verified end-to-end with
// the real apiserver invoking the webhook over TLS, with real
// BaseModel/ClusterBaseModel lookups against the API server.
var _ = ginkgo.Describe("InferenceService webhook validation", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("rejects ISVC with invalid name (uppercase / dots)", func() {
		// IsvcRegexp enforces DNS-style: lowercase alphanumeric +
		// hyphens, no dots. Uppercase is the most common operator
		// mistake.
		isvc := v1beta1testing.MakeInferenceService("Bad-ISVC-Name", "isvc-webhook").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Bad-ISVC-Name"),
			"webhook must reject ISVC names that violate IsvcRegexp")
	})

	ginkgo.It("rejects ISVC with Decoder set but Engine nil", func() {
		// validateEngineDecoderConfiguration: a Decoder is meaningless
		// without an Engine to disaggregate from. Most operators who
		// hit this set the wrong field by mistake.
		isvc := v1beta1testing.MakeInferenceService("decoder-only", "isvc-webhook").
			Model("any-model").
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("decoder cannot be specified without engine"))
	})

	ginkgo.It("rejects ISVC with unknown autoscalerClass annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("bad-as-class", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, "made-up-class").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not a supported autoscaler class type"))
	})

	ginkgo.It("rejects ISVC with HPA autoscaler + invalid metric annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("bad-hpa-metric", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassHPA)).
			Annotation(constants.AutoscalerMetrics, "fictional-metric").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("is not a supported metric"))
	})

	ginkgo.It("rejects ISVC with KEDA autoscaler + invalid scaling operator annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("bad-keda-op", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Annotation(constants.KedaScalingOperator, "EqualToButNotReally").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid KEDA scaling operator"))
	})

	ginkgo.It("rejects ISVC referencing a model that doesn't exist", func() {
		// validateModelExists: the BaseModel/ClusterBaseModel lookup
		// hits the apiserver, so this is the most-valuable
		// integration check (unit tests use mocks).
		isvc := v1beta1testing.MakeInferenceService("missing-model", "isvc-webhook").
			Model("does-not-exist-anywhere").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("referenced model"))
		Expect(err.Error()).To(ContainSubstring("not found"))
	})

	ginkgo.It("accepts ISVC referencing an existing ClusterBaseModel + matching ClusterServingRuntime", func() {
		// Symmetric positive case for the model-existence check.
		// validateRuntimeAndModelResolution additionally requires
		// either a runtime that supports the model's format OR an
		// engine with hasFullRunnerConfig — we provide the runtime,
		// matching the realistic operator setup.
		cbm := v1beta1testing.MakeClusterBaseModel("ok-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("ok-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("ok-isvc", "isvc-webhook").
			Model("ok-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("rejects ISVC referencing CBM whose PVC URI points at a different namespace", func() {
		// validateClusterBaseModelPVCNamespace: a pod can't mount a
		// PVC from another namespace, so an ISVC + CBM combo where
		// the CBM's PVC URI specifies a different namespace from the
		// ISVC would silently break at scheduling time without this
		// check.
		cbm := v1beta1testing.MakeClusterBaseModel("cross-ns-cbm").
			StorageURI("pvc://elsewhere:my-pvc/models/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		isvc := v1beta1testing.MakeInferenceService("cross-ns-isvc", "isvc-webhook").
			Model("cross-ns-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("references PVC in namespace"))
		Expect(err.Error()).To(ContainSubstring("isvc-webhook"))
	})

	ginkgo.It("validates Updates too: changing model to a missing one is rejected", func() {
		// The webhook is registered for verbs=create;update so the
		// existing-model check must also fire on Updates. CBM + CSR
		// satisfy the runtime-resolution check on the initial Create.
		cbm := v1beta1testing.MakeClusterBaseModel("update-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("update-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("update-isvc", "isvc-webhook").
			Model("update-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		isvc.Spec.Model.Name = "definitely-not-here"
		err := k8sClient.Update(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("referenced model"))
		Expect(err.Error()).To(ContainSubstring("not found"))
	})
})
