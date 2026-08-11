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

// Sharded metadata-parsing edge specs. The sharded reconcile path
// (sharded_cache_status.go) calls the cluster-cache provider's
// InspectSource, hands the returned ModelMetadataInputs through
// modelparser.ParseModelConfigFromFiles, and then feeds the parsed
// ModelMetadata into BaseModel.Spec via parser.ApplyModelMetadata.
//
// Two distinct things go wrong in production around this path:
//  1. the provider returns metadata that the parser can't consume
//     (malformed JSON, missing required files);
//  2. the parser succeeds but writes fields the operator already set.
//
// Existing sharded specs use a hand-minimal config.json
// (`{"model_type":"phi","architectures":["PhiForCausalLM"]}`) and
// don't assert that the parsed values actually land in the Spec —
// they only check the Cached/Ready conditions. These specs cover
// the spec-population side and the parse-failure side.
var _ = ginkgo.Describe("Sharded BaseModel metadata parsing", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("realistic config.json drives Spec.ModelType, ModelArchitecture, MaxTokens auto-fill", func() {
		// This config.json mirrors pkg/modelconfig/testdata/tiny-random-PhiModel/config.json
		// — the same fixture the modelparser unit tests use. The
		// parser should extract:
		//   - ModelType         = "phi"           (from model_type)
		//   - ModelArchitecture = "PhiModel"      (from architectures[0])
		//   - MaxTokens         = 512             (from max_position_embeddings)
		//
		// updateShardedModelMetadataWithRetry (sharded_cache_status.go:207)
		// calls parser.ApplyModelMetadata which writes these into spec
		// as a Spec update (NOT status), separate from the status writes
		// that drive the Cached/Ready conditions. This spec proves the
		// Spec write actually happens — none of the existing sharded
		// tests assert on Spec fields after the metadata reparse.
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			return &sharded.SourceInspection{
				Objects: []sharded.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: sharded.ModelMetadataInputs{
					ConfigJSON: []byte(`{
						"architectures": ["PhiModel"],
						"model_type": "phi",
						"hidden_size": 32,
						"max_position_embeddings": 512,
						"num_attention_heads": 4,
						"num_hidden_layers": 2,
						"num_key_value_heads": 4,
						"transformers_version": "4.40.0.dev0",
						"vocab_size": 1024
					}`),
				},
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-realistic-meta").
			StorageURI("oci://n/test/b/models/o/llama-realistic").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Wait for Ready=True (post-metadata-parse) so we know
		// updateShardedModelMetadataWithRetry has run end-to-end.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-realistic-meta"}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))

			// Spec fields must reflect parsed metadata.
			g.Expect(got.Spec.ModelType).NotTo(BeNil(), "ModelType should be auto-filled from config.json model_type")
			g.Expect(*got.Spec.ModelType).To(Equal("phi"))
			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil(), "ModelArchitecture should be auto-filled from config.json architectures[0]")
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("PhiModel"))
			g.Expect(got.Spec.MaxTokens).NotTo(BeNil(), "MaxTokens should be auto-filled from config.json max_position_embeddings")
			g.Expect(*got.Spec.MaxTokens).To(Equal(int32(512)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("malformed ConfigJSON → Ready=False reason=MetadataPending", func() {
		// reconcileShardedModelMetadataSpec calls InspectSource and
		// runs the result through the parser. When the parser errors,
		// markShardedModelMetadataPending sets Ready=False with
		// reason=MetadataPending and the controller will keep retrying
		// at the poll interval. There's no prewarm in the new flow, so
		// the metadata parse is the only gate between SourceReachable
		// and Ready.
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			return &sharded.SourceInspection{
				Objects: []sharded.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: sharded.ModelMetadataInputs{
					// Truncated mid-key — JSON parser will error.
					ConfigJSON: []byte(`{"model_type":"phi", "architectures": ["PhiMo`),
				},
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-malformed-meta").
			StorageURI("oci://n/test/b/models/o/llama-malformed").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Wait for the controller to reach the metadata-pending state.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-malformed-meta"}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataPending))
			g.Expect(ready.Message).To(ContainSubstring("model metadata"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("user-set Spec.ModelType + ModelArchitecture preserved when parser would write different values (fill-only contract)", func() {
		// Sibling specs already cover the analogous contract for the
		// PVC/agent path (updateSpecWithConfig in controller.go:897).
		// This spec covers the OTHER auto-fill code path — the sharded
		// flow uses parser.ApplyModelMetadata, which is a totally
		// different function in pkg/modelparser/config_parser.go:621.
		// Both must honor the same fill-only invariant: never overwrite
		// fields the operator already populated.
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			return &sharded.SourceInspection{
				Objects: []sharded.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: sharded.ModelMetadataInputs{
					// Parser would write ModelType="phi", ModelArchitecture="PhiModel"
					// — but spec already has user-curated values.
					ConfigJSON: []byte(`{
						"model_type": "phi",
						"architectures": ["PhiModel"],
						"max_position_embeddings": 4096,
						"hidden_size": 32,
						"num_attention_heads": 4,
						"num_hidden_layers": 2,
						"num_key_value_heads": 4,
						"transformers_version": "4.40.0.dev0",
						"vocab_size": 1024
					}`),
				},
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-user-set").
			StorageURI("oci://n/test/b/models/o/llama-user-set").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").
			ModelType("operator-curated-type").
			ModelArchitecture("operator-curated-arch").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-user-set"}, got)).To(Succeed())
			// Wait for full reconcile so we know ApplyModelMetadata ran.
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))

			// Pre-set fields stay as user wrote them.
			g.Expect(got.Spec.ModelType).NotTo(BeNil())
			g.Expect(*got.Spec.ModelType).To(Equal("operator-curated-type"),
				"parser.ApplyModelMetadata must NOT overwrite user-set ModelType")
			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil())
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("operator-curated-arch"),
				"parser.ApplyModelMetadata must NOT overwrite user-set ModelArchitecture")
			// Sanity: an UNSET field IS filled, proving the parse
			// actually consumed the config.json (not a no-op skip).
			g.Expect(got.Spec.MaxTokens).NotTo(BeNil(),
				"sanity: unset MaxTokens should populate from max_position_embeddings, proving parser ran")
			g.Expect(*got.Spec.MaxTokens).To(Equal(int32(4096)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
