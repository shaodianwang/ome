package v1beta1testing_test

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/modelagent"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/framework"
	"github.com/sgl-project/ome/tests/integration/util"
)

// TestV1beta1Builders is the Ginkgo entrypoint for the builder
// package's self-tests.
func TestV1beta1Builders(t *testing.T) {
	util.RunSuite(t, "v1beta1 Builder Self-Test Suite")
}

var (
	suiteCtx    context.Context
	suiteFwk    *framework.Framework
	suiteClient client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	s, err := framework.NewSuite(&framework.Framework{})
	Expect(err).NotTo(HaveOccurred())
	suiteFwk = s.Fwk
	suiteClient = s.Client
	suiteCtx = s.Ctx

	// Pre-create the OME namespace so SimulatePVCAgent* writes land
	// somewhere.
	_ = suiteClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})
})

var _ = ginkgo.AfterSuite(func() {
	if suiteFwk != nil {
		_ = suiteFwk.Teardown()
	}
})

var _ = ginkgo.Describe("v1beta1testing.MakeBaseModel", func() {
	ginkgo.It("chained setters produce a CR the apiserver round-trips", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "p1-3-bm-")
		Expect(err).NotTo(HaveOccurred())

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns.Name).
			StorageURI("pvc://my-pvc/models/llama").
			Distribution(v1beta1.DistributionPerNode).
			Label("test-marker", "p1-3").
			Obj()
		Expect(suiteClient.Create(suiteCtx, bm)).To(Succeed())

		got := &v1beta1.BaseModel{}
		Expect(suiteClient.Get(suiteCtx, types.NamespacedName{Name: "llama-7b", Namespace: ns.Name}, got)).To(Succeed())
		Expect(got.Spec.Storage).NotTo(BeNil())
		Expect(got.Spec.Storage.StorageUri).NotTo(BeNil())
		Expect(*got.Spec.Storage.StorageUri).To(Equal("pvc://my-pvc/models/llama"))
		Expect(got.Labels).To(HaveKeyWithValue("test-marker", "p1-3"))
	})
})

var _ = ginkgo.Describe("v1beta1testing.SimulatePVCAgent helpers", func() {

	ginkgo.It("Ready writes a ConfigMap with the right labels + ModelEntry shape", func() {
		cfg := &modelagent.ModelConfig{
			ModelType:         "llama",
			ModelArchitecture: "LlamaForCausalLM",
			MaxTokens:         4096,
		}
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, suiteClient,
			"llama-7b-sim-ready", "p1-3-sim-ns", false, cfg)).To(Succeed())

		cmName := constants.GetPVCMetadataConfigMapName("llama-7b-sim-ready", "p1-3-sim-ns", false)
		cm := &corev1.ConfigMap{}
		Expect(suiteClient.Get(suiteCtx, types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).To(Succeed())
		Expect(cm.Labels[constants.PVCStorageConfigMapLabel]).To(Equal("true"))
		Expect(cm.Labels[constants.PVCMetadataScopeLabel]).To(Equal("namespaced"))
		Expect(cm.Data).To(HaveKey(constants.GetModelConfigMapKey("p1-3-sim-ns", "llama-7b-sim-ready", false)))
	})

	ginkgo.It("Failed writes the error annotation under the dedicated key", func() {
		Expect(v1beta1testing.SimulatePVCAgentFailed(suiteCtx, suiteClient,
			"llama-7b-sim-failed", "p1-3-sim-ns2", false, "config.json missing")).To(Succeed())
		cmName := constants.GetPVCMetadataConfigMapName("llama-7b-sim-failed", "p1-3-sim-ns2", false)
		cm := &corev1.ConfigMap{}
		Expect(suiteClient.Get(suiteCtx, types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).To(Succeed())
		Expect(cm.Annotations[constants.PVCMetadataLastErrorAnnotation]).To(Equal("config.json missing"))
	})
})
