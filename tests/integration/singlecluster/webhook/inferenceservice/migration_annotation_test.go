package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Webhook envtest specs for the migration-request annotation mailbox:
// annotations ADDED by an Update are validated at admission (schema,
// component existence, instance index); unchanged pre-existing values
// are not re-validated and deletions are always admitted so the
// controller can consume the mailbox.
var _ = ginkgo.Describe("InferenceService webhook migration-request annotations", func() {

	ginkgo.BeforeEach(ensureNamespace)

	// makeMigrationISVC creates a CBM + CSR + valid ISVC the specs can
	// Update against, with cleanup registered.
	makeMigrationISVC := func(name string) *v1beta1.InferenceService {
		ginkgo.GinkgoHelper()
		cbm := v1beta1testing.MakeClusterBaseModel(name + "-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime(name+"-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService(name, "isvc-webhook").
			Model(name + "-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		return isvc
	}

	ginkgo.It("rejects an update adding a garbage migration-request annotation", func() {
		isvc := makeMigrationISVC("mig-garbage")
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		isvc.Annotations[audit.MigrationRequestAnnotationPrefix+"g1"] = `{not-json`
		err := k8sClient.Update(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("migration request g1"))
	})

	ginkgo.It("rejects a request naming a component the ISVC lacks", func() {
		isvc := makeMigrationISVC("mig-nocomp")
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		isvc.Annotations[audit.MigrationRequestAnnotationPrefix+"c1"] =
			`{"schemaVersion":"v1","component":"decoder","instance":0,"from_node":"n"}`
		err := k8sClient.Update(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`component "decoder" does not exist`))
	})

	ginkgo.It("accepts a valid request annotation and admits its later deletion", func() {
		isvc := makeMigrationISVC("mig-valid")
		key := audit.MigrationRequestAnnotationPrefix + "v1"
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		isvc.Annotations[key] =
			`{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"node-a"}`
		Expect(k8sClient.Update(suiteCtx, isvc)).To(Succeed())

		// The consume side of the mailbox: deleting the key must pass.
		delete(isvc.Annotations, key)
		Expect(k8sClient.Update(suiteCtx, isvc)).To(Succeed())
	})

	ginkgo.It("rejects a garbage migration annotation at create and admits clean unrelated updates", func() {
		// Create and update are both validated for ADDED keys, so garbage
		// cannot enter through the API. (Objects predating the webhook can
		// still carry garbage; the unchanged-key skip that protects their
		// unrelated updates is pinned by the validator unit table — it
		// cannot be seeded end-to-end here because admission is closed.)
		cbm := v1beta1testing.MakeClusterBaseModel("mig-preexist-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })
		csr := v1beta1testing.MakeClusterServingRuntime("mig-preexist-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		garbage := v1beta1testing.MakeInferenceService("mig-preexist-bad", "isvc-webhook").
			Model("mig-preexist-cbm").
			Annotation(audit.MigrationRequestAnnotationPrefix+"old", `{not-json`).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, garbage)
		Expect(err).To(HaveOccurred(), "garbage migration annotation must be rejected at create")
		Expect(err.Error()).To(ContainSubstring("migration request old"))

		isvc := v1beta1testing.MakeInferenceService("mig-preexist", "isvc-webhook").
			Model("mig-preexist-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		if isvc.Labels == nil {
			isvc.Labels = map[string]string{}
		}
		isvc.Labels["unrelated"] = "change"
		Expect(k8sClient.Update(suiteCtx, isvc)).To(Succeed(),
			"unrelated updates with no migration annotations must not be gated")
	})
})
