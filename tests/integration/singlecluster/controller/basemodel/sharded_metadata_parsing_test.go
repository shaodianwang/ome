package basemodel

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"
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
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src modelcache.Source) (*modelcache.SourceInspection, error) {
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: modelcache.ModelMetadataInputs{
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

	ginkgo.It("malformed ConfigJSON → Ready=False reason=MetadataPending, Cached never flips True (prewarm never starts)", func() {
		// reconcileShardedModelMetadataSpec runs BEFORE StartPrewarm
		// (sharded_cache_status.go:77). If parser fails on the
		// inspection's metadata, markShardedModelMetadataPending sets
		// Ready=False reason=MetadataPending and the controller bails
		// without calling StartPrewarm — Cached stays at its initial
		// PrewarmPending value. This pins that ordering: the parse
		// gate is the FIRST gate after CheckSource, not a post-prewarm
		// concern.
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src modelcache.Source) (*modelcache.SourceInspection, error) {
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: modelcache.ModelMetadataInputs{
					// Truncated mid-key — JSON parser will error.
					ConfigJSON: []byte(`{"model_type":"phi", "architectures": ["PhiMo`),
				},
			}, nil
		}
		// If StartPrewarm is reached, this would store the URI. The
		// spec asserts it stays nil to prove the parse gate fired
		// FIRST and short-circuited the reconcile.
		var prewarmInvoked = false
		suiteFakeMC.StartPrewarmFunc = func(_ context.Context, _ modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			prewarmInvoked = true
			return &modelcache.PrewarmOperation{ID: "should-not-be-called"}, nil
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

			// Cached stays at initial PrewarmPending value — the
			// metadata gate fired before StartPrewarm.
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("False"))
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmPending))

			// Cache.PrewarmID must be empty — StartPrewarm was never
			// invoked, so no operation ID could be stashed.
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.PrewarmID).To(BeEmpty(),
				"StartPrewarm must NOT fire when metadata parse fails (parse is the gate)")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(prewarmInvoked).To(BeFalse(),
			"StartPrewarm must not be invoked when InspectSource returns metadata the parser cannot consume")
	})

	ginkgo.It("user-set Spec.ModelType + ModelArchitecture preserved when parser would write different values (fill-only contract)", func() {
		// Bucket G already covers the analogous contract for the
		// PVC/agent path (updateSpecWithConfig in controller.go:897).
		// This spec covers the OTHER auto-fill code path — the sharded
		// flow uses parser.ApplyModelMetadata, which is a totally
		// different function in pkg/modelparser/config_parser.go:621.
		// Both must honor the same fill-only invariant: never overwrite
		// fields the operator already populated.
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src modelcache.Source) (*modelcache.SourceInspection, error) {
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{
					{Source: src, Path: src.URI + "/config.json"},
				},
				Metadata: modelcache.ModelMetadataInputs{
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
