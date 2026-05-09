package metadata

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modelmetadata "github.com/sgl-project/ome/internal/ome-agent/model-metadata"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/logging"
	"github.com/sgl-project/ome/pkg/modelagent"
)

// Tier-1 metadata-agent specs. Each It uses a distinct model name so
// the deterministic per-PVC ConfigMap names don't collide on the
// shared envtest.
var _ = ginkgo.Describe("Metadata Agent PVC writes", ginkgo.Ordered, func() {

	ginkgo.It("writes a Ready ConfigMap with parsed metadata for a namespaced BaseModel", func() {
		cfg := &modelmetadata.Config{
			ModelPath:          writeMinimalLlamaConfig(),
			BaseModelName:      "llama-7b-ready",
			BaseModelNamespace: "models",
			ClusterScoped:      false,
			Logger:             logging.Discard(),
		}
		ex, err := modelmetadata.NewMetadataExtractor(cfg, k8sClient, zap.NewNop())
		Expect(err).NotTo(HaveOccurred())
		Expect(ex.Start()).NotTo(HaveOccurred())

		cmName := constants.GetPVCMetadataConfigMapName(cfg.BaseModelName, cfg.BaseModelNamespace, cfg.ClusterScoped)
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).
			To(Succeed())

		ginkgo.By("PVC ConfigMaps deliberately omit the per-node basemodel-status label")
		Expect(cm.Labels).NotTo(HaveKey(constants.ModelStatusConfigMapLabel))
		Expect(cm.Labels[constants.PVCStorageConfigMapLabel]).To(Equal("true"))
		Expect(cm.Labels[constants.PVCMetadataModelNameLabel]).To(Equal("llama-7b-ready"))
		Expect(cm.Labels[constants.PVCMetadataScopeLabel]).To(Equal("namespaced"))

		modelKey := constants.GetModelConfigMapKey(cfg.BaseModelNamespace, cfg.BaseModelName, cfg.ClusterScoped)
		Expect(cm.Data).To(HaveKey(modelKey))

		var entry modelagent.ModelEntry
		Expect(json.Unmarshal([]byte(cm.Data[modelKey]), &entry)).To(Succeed())
		Expect(entry.Status).To(Equal(modelagent.ModelStatusReady))
		Expect(entry.Config).NotTo(BeNil())
		Expect(entry.Config.ModelType).To(Equal("llama"))
		Expect(entry.Config.ModelArchitecture).To(Equal("LlamaForCausalLM"))
	})

	ginkgo.It("writes a Failed ConfigMap with the error annotation when parser fails", func() {
		cfg := &modelmetadata.Config{
			ModelPath:          ginkgo.GinkgoT().TempDir(), // empty dir → parser errors
			BaseModelName:      "llama-7b-failed",
			BaseModelNamespace: "models",
			Logger:             logging.Discard(),
		}
		ex, err := modelmetadata.NewMetadataExtractor(cfg, k8sClient, zap.NewNop())
		Expect(err).NotTo(HaveOccurred())
		Expect(ex.Start()).To(HaveOccurred(), "agent must exit non-zero on parse error")

		cmName := constants.GetPVCMetadataConfigMapName(cfg.BaseModelName, cfg.BaseModelNamespace, cfg.ClusterScoped)
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).
			To(Succeed(), "Failed-status ConfigMap must still be written so the controller can surface the error")
		Expect(cm.Annotations[constants.PVCMetadataLastErrorAnnotation]).NotTo(BeEmpty())

		modelKey := constants.GetModelConfigMapKey(cfg.BaseModelNamespace, cfg.BaseModelName, cfg.ClusterScoped)
		var entry modelagent.ModelEntry
		Expect(json.Unmarshal([]byte(cm.Data[modelKey]), &entry)).To(Succeed())
		Expect(entry.Status).To(Equal(modelagent.ModelStatusFailed))
	})

	ginkgo.It("a second extraction overwrites in place rather than duplicating", func() {
		cfg := &modelmetadata.Config{
			ModelPath:          writeMinimalLlamaConfig(),
			BaseModelName:      "llama-7b-idemp",
			BaseModelNamespace: "models",
			Logger:             logging.Discard(),
		}
		ex, err := modelmetadata.NewMetadataExtractor(cfg, k8sClient, zap.NewNop())
		Expect(err).NotTo(HaveOccurred())

		Expect(ex.Start()).NotTo(HaveOccurred())
		Expect(ex.Start()).NotTo(HaveOccurred(), "second run must overwrite via optimistic-locking retry, not error")

		ginkgo.By("List narrows by model-name label so sibling tests' ConfigMaps don't pollute the count")
		cms := &corev1.ConfigMapList{}
		Expect(k8sClient.List(context.Background(), cms,
			client.InNamespace(constants.OMENamespace),
			client.MatchingLabels{
				constants.PVCStorageConfigMapLabel:  "true",
				constants.PVCMetadataModelNameLabel: "llama-7b-idemp",
			},
		)).To(Succeed())
		Expect(cms.Items).To(HaveLen(1), "second extraction must not create a duplicate ConfigMap")
	})

	ginkgo.It("--cluster-scoped writes to a different ConfigMap-name keyspace and tags scope=cluster", func() {
		cfg := &modelmetadata.Config{
			ModelPath:     writeMinimalLlamaConfig(),
			BaseModelName: "shared-llama-cluster",
			ClusterScoped: true,
			Logger:        logging.Discard(),
		}
		ex, err := modelmetadata.NewMetadataExtractor(cfg, k8sClient, zap.NewNop())
		Expect(err).NotTo(HaveOccurred())
		Expect(ex.Start()).NotTo(HaveOccurred())

		cmName := constants.GetPVCMetadataConfigMapName(cfg.BaseModelName, "", true)
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).
			To(Succeed())
		Expect(cm.Labels[constants.PVCMetadataScopeLabel]).To(Equal("cluster"))
		Expect(cm.Data).To(HaveKey(constants.GetModelConfigMapKey("", cfg.BaseModelName, true)))
	})
})

// writeMinimalLlamaConfig writes a config.json under a fresh
// Ginkgo-managed temp dir and returns the dir path. The dir is
// cleaned up by Ginkgo when the spec finishes.
func writeMinimalLlamaConfig() string {
	ginkgo.GinkgoHelper()
	dir := ginkgo.GinkgoT().TempDir()
	cfg := map[string]any{
		"model_type":              "llama",
		"architectures":           []string{"LlamaForCausalLM"},
		"max_position_embeddings": 4096,
		"hidden_size":             4096,
		"num_hidden_layers":       32,
		"num_attention_heads":     32,
		"num_key_value_heads":     32,
		"intermediate_size":       11008,
		"vocab_size":              32000,
		"torch_dtype":             "float16",
	}
	data, err := json.Marshal(cfg)
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(filepath.Join(dir, "config.json"), data, 0o644)).To(Succeed())
	return dir
}
