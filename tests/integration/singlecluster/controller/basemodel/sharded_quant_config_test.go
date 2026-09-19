package basemodel

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// hf_quant_config.json end-to-end: a sharded ClusterBaseModel whose
// cluster-cache provider returns HFQuantConfigJSON in InspectSource
// must surface the quantization scheme on the BaseModel spec. The
// modelparser's ParseModelConfigFromFiles handles the file directly;
// this spec proves the wiring all the way from ModelMetadataInputs
// through shardedMetadataInputsFromInspection to the parser to the
// CR spec write.
var _ = ginkgo.Describe("Sharded BaseModel hf_quant_config.json detection", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("hf_quant_config.json with NVFP4 → spec.Quantization=nvfp4", func() {
		// Override the suite's default InspectSource to inject
		// HFQuantConfigJSON. config.json is intentionally without a
		// quantization_config block — the only signal for NVFP4 is
		// the sibling hf_quant_config.json.
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			return &sharded.SourceInspection{
				Objects: []sharded.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: sharded.ModelMetadataInputs{
					ConfigJSON: []byte(`{
						"model_type": "llama4_text",
						"architectures": ["Llama4ForCausalLM"],
						"max_position_embeddings": 32768
					}`),
					HFQuantConfigJSON: []byte(`{
						"quant_method": "modelopt",
						"quantization": {
							"quant_algo": "NVFP4",
							"group_size": 16,
							"kv_cache_quant_algo": "FP8"
						}
					}`),
				},
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-quant-nvfp4").
			StorageURI("oci://n/test/b/models/o/llama4-nvfp4").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-quant-nvfp4"}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))

			g.Expect(got.Spec.Quantization).NotTo(BeNil(),
				"hf_quant_config.json's NVFP4 must propagate to spec.Quantization end-to-end through the cluster-cache → parser → spec-update path")
			g.Expect(*got.Spec.Quantization).To(Equal(v1beta1.ModelQuantizationNVFP4))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
