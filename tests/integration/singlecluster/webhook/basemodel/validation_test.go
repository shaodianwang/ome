package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// ensureModelsNamespace creates the "models" namespace used by every
// webhook spec's BaseModel (BMs are namespaced; the apiserver returns
// "namespace not found" on Create if the NS isn't there). Shared by
// every Describe in this package via BeforeEach so each Describe's
// specs work in isolation when run with -ginkgo.focus.
func ensureModelsNamespace() {
	ginkgo.GinkgoHelper()
	_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "models"},
	})
}

// Webhook envtest specs for BaseModel + ClusterBaseModel validators.
// Each spec sends a Create through the apiserver and asserts the
// admission rejection (or acceptance) — same logic the unit tests in
// pkg/webhook/admission/basemodel/basemodel_webhook_test.go cover,
// but verified end-to-end with the real apiserver invoking the
// webhook over TLS.
var _ = ginkgo.Describe("BaseModel webhook validation", func() {

	ginkgo.BeforeEach(ensureModelsNamespace)

	ginkgo.It("namespaced BaseModel rejects pvc URI with namespace prefix", func() {
		bm := v1beta1testing.MakeBaseModel("bad-bm-1", "models").
			StorageURI("pvc://otherns:my-pvc/models/llama").Obj()
		err := k8sClient.Create(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("namespaced BaseModel PVC URI must not specify a namespace"),
			"webhook must reject ns-prefixed PVC URI on namespaced BaseModel")
	})

	ginkgo.It("namespaced BaseModel accepts pvc URI without namespace prefix", func() {
		bm := v1beta1testing.MakeBaseModel("good-bm-1", "models").
			StorageURI("pvc://my-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })
	})

	ginkgo.It("ClusterBaseModel rejects pvc URI without namespace prefix", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("bad-cbm-1").
			StorageURI("pvc://my-pvc/models/llama").Obj()
		err := k8sClient.Create(suiteCtx, cbm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ClusterBaseModel PVC URI must specify a namespace"),
			"webhook must reject missing-ns PVC URI on ClusterBaseModel")
	})

	ginkgo.It("ClusterBaseModel accepts pvc URI with namespace prefix", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("good-cbm-1").
			StorageURI("pvc://shared:my-pvc/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })
	})

	ginkgo.It("PVC + distribution=Sharded combination is rejected on BaseModel", func() {
		bm := v1beta1testing.MakeBaseModel("bad-bm-sharded", "models").
			StorageURI("pvc://my-pvc/models/llama").
			Distribution(v1beta1.DistributionSharded).Obj()
		err := k8sClient.Create(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("PVC storage URIs are not compatible with distribution=Sharded"),
			"webhook must reject PVC + Sharded combination")
	})

	ginkgo.It("PVC + distribution=Sharded combination is rejected on ClusterBaseModel", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("bad-cbm-sharded").
			StorageURI("pvc://shared:my-pvc/models/llama").
			Distribution(v1beta1.DistributionSharded).Obj()
		err := k8sClient.Create(suiteCtx, cbm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("PVC storage URIs are not compatible with distribution=Sharded"),
			"webhook must reject PVC + Sharded combination on cluster-scoped CR")
	})

	ginkgo.It("non-PVC URIs are accepted unchanged (regression guard for over-rejection)", func() {
		bm := v1beta1testing.MakeBaseModel("ok-oci-bm", "models").
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, bm) })

		cbm := v1beta1testing.MakeClusterBaseModel("ok-oci-cbm").
			StorageURI("hf://meta-llama/Llama-2-7b").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })
	})

	ginkgo.It("malformed PVC URI rejected (no subpath)", func() {
		bm := v1beta1testing.MakeBaseModel("bad-bm-nosubpath", "models").
			StorageURI("pvc://my-pvc").Obj() // missing /subpath
		err := k8sClient.Create(suiteCtx, bm)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid PVC storage URI"),
			"webhook must reject PVC URI without subpath")
	})

	_ = util.MediumTimeout // keep util imported for status-condition polling specs
})
