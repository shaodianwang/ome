package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Additional webhook specs covering the corners of ParsePVCStorageURI
// that the baseline file (validation_test.go) doesn't reach. The
// validator runs on every Create AND Update; this file also verifies
// the Update path, which the baseline only touches via a single
// happy-path Create.
var _ = ginkgo.Describe("BaseModel webhook additional validation", func() {

	ginkgo.BeforeEach(ensureModelsNamespace)

	ginkgo.It("rejects pvc URI with empty PVC name after colon (CBM)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("cbm-empty-pvcname").
			StorageURI("pvc://ns1:/models/llama").Obj()
		err := k8sClient.Create(suiteCtx, cbm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("empty PVC name after colon"),
			"webhook must reject pvc://{ns}:/{path} with empty PVC name")
	})

	ginkgo.It("rejects pvc URI with empty namespace before colon (CBM)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("cbm-empty-ns").
			StorageURI("pvc://:my-pvc/models/llama").Obj()
		err := k8sClient.Create(suiteCtx, cbm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("empty namespace before colon"),
			"webhook must reject pvc://:{pvc-name}/{path} with empty namespace")
	})

	ginkgo.It("rejects pvc URI with multiple colons (CBM)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("cbm-multicolon").
			StorageURI("pvc://ns1:my-pvc:extra/models/llama").Obj()
		err := k8sClient.Create(suiteCtx, cbm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("multiple colons not allowed"),
			"webhook must reject pvc://ns:pvc:extra/path")
	})

	ginkgo.It("rejects pvc URI with invalid namespace (uppercase) on CBM", func() {
		// isValidNamespace requires lowercase alphanumeric + hyphens.
		cbm := v1beta1testing.MakeClusterBaseModel("cbm-bad-ns").
			StorageURI("pvc://BadNS:my-pvc/models/llama").Obj()
		err := k8sClient.Create(suiteCtx, cbm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid namespace"),
			"webhook must reject pvc URI with non-DNS-1123 namespace")
	})

	ginkgo.It("rejects pvc URI with '..' segment in subpath (path-traversal guard)", func() {
		bm := v1beta1testing.MakeBaseModel("bm-traversal", "models").
			StorageURI("pvc://my-pvc/models/../../etc/passwd").Obj()
		err := k8sClient.Create(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("'..' segments"),
			"webhook must reject pvc URI with parent-directory segments")
	})

	ginkgo.It("rejects pvc URI with empty content after prefix", func() {
		bm := v1beta1testing.MakeBaseModel("bm-empty", "models").
			StorageURI("pvc://").Obj()
		err := k8sClient.Create(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing content after prefix"),
			"webhook must reject bare pvc:// with no body")
	})

	ginkgo.It("Update is also validated: legal → illegal storage URI is rejected on update", func() {
		// Create with a legal URI, then try to Update to an illegal
		// one. The webhook is registered for verbs=create;update so
		// the Update must be denied with the same error class as the
		// equivalent Create.
		bm := v1beta1testing.MakeBaseModel("bm-update-validate", "models").
			StorageURI("pvc://my-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		// Mutate to an illegal URI (namespaced BM with NS prefix).
		bad := "pvc://otherns:my-pvc/models/llama"
		bm.Spec.Storage.StorageUri = &bad
		err := k8sClient.Update(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("namespaced BaseModel PVC URI must not specify a namespace"),
			"webhook must validate Update payloads, not just Creates")
	})

	ginkgo.It("Update is also validated: enabling Distribution=Sharded on a PVC-backed BM is rejected", func() {
		// Symmetric to the above — different rule, same path. Common
		// operator mistake: deploy a PVC model, then try to flip it
		// to Sharded later.
		bm := v1beta1testing.MakeBaseModel("bm-update-sharded", "models").
			StorageURI("pvc://my-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		dist := v1beta1.DistributionSharded
		bm.Spec.Distribution = &dist
		err := k8sClient.Update(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("PVC storage URIs are not compatible with distribution=Sharded"),
			"webhook must reject post-create Distribution=Sharded on PVC-backed BM")
	})

	ginkgo.It("non-pvc URI (s3://) is accepted; validator only enforces PVC rules", func() {
		// Regression guard for over-rejection. The baseline file
		// covers oci:// + hf://; this adds s3:// which has its own
		// parser rules but should pass through the BaseModel
		// validator (only PVC URIs are gated).
		bm := v1beta1testing.MakeBaseModel("bm-s3", "models").
			StorageURI("s3://bucket/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })
	})
})
