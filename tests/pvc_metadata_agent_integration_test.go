package integration_tests

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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

// Tier-1 integration tests: drive MetadataExtractor end-to-end against a
// real apiserver (envtest). Catches bugs that the in-package fake-client
// tests can't — label validation, status subresource, optimistic-locking
// retry semantics, real ConfigMap upsert behavior.
//
// Plain go test (not Ginkgo) so failures surface without -ginkgo.* flags.

func TestPVCMetadataAgent_WritesReadyConfigMap_Envtest(t *testing.T) {
	g := NewGomegaWithT(t)
	_, kc, stop := startPVCEnvTest(t, g)
	defer stop()

	modelDir := writeMinimalLlamaConfigForIntegration(t)

	cfg := &modelmetadata.Config{
		ModelPath:          modelDir,
		BaseModelName:      "llama-7b",
		BaseModelNamespace: "models",
		ClusterScoped:      false,
		Logger:             logging.Discard(),
	}
	ex, err := modelmetadata.NewMetadataExtractor(cfg, kc, zap.NewNop())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ex.Start()).NotTo(HaveOccurred())

	cmName := constants.GetPVCMetadataConfigMapName(cfg.BaseModelName, cfg.BaseModelNamespace, cfg.ClusterScoped)
	cm := &corev1.ConfigMap{}
	g.Expect(kc.Get(context.Background(),
		types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).
		To(Succeed())

	// PVC ConfigMaps deliberately don't carry the per-node basemodel-status
	// label (would confuse processModelStatus's per-node List).
	g.Expect(cm.Labels).NotTo(HaveKey(constants.ModelStatusConfigMapLabel))
	g.Expect(cm.Labels[constants.PVCStorageConfigMapLabel]).To(Equal("true"))
	g.Expect(cm.Labels[constants.PVCMetadataModelNameLabel]).To(Equal("llama-7b"))
	g.Expect(cm.Labels[constants.PVCMetadataScopeLabel]).To(Equal("namespaced"))

	modelKey := constants.GetModelConfigMapKey(cfg.BaseModelNamespace, cfg.BaseModelName, cfg.ClusterScoped)
	g.Expect(cm.Data).To(HaveKey(modelKey))

	var entry modelagent.ModelEntry
	g.Expect(json.Unmarshal([]byte(cm.Data[modelKey]), &entry)).To(Succeed())
	g.Expect(entry.Status).To(Equal(modelagent.ModelStatusReady))
	g.Expect(entry.Config).NotTo(BeNil())
	g.Expect(entry.Config.ModelType).To(Equal("llama"))
	g.Expect(entry.Config.ModelArchitecture).To(Equal("LlamaForCausalLM"))
}

func TestPVCMetadataAgent_FailedConfigMapPath_Envtest(t *testing.T) {
	g := NewGomegaWithT(t)
	_, kc, stop := startPVCEnvTest(t, g)
	defer stop()

	cfg := &modelmetadata.Config{
		ModelPath:          t.TempDir(), // empty dir → parser errors
		BaseModelName:      "llama-7b",
		BaseModelNamespace: "models",
		Logger:             logging.Discard(),
	}
	ex, err := modelmetadata.NewMetadataExtractor(cfg, kc, zap.NewNop())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ex.Start()).To(HaveOccurred(), "agent must exit non-zero on parse error")

	cmName := constants.GetPVCMetadataConfigMapName(cfg.BaseModelName, cfg.BaseModelNamespace, cfg.ClusterScoped)
	cm := &corev1.ConfigMap{}
	g.Expect(kc.Get(context.Background(),
		types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).
		To(Succeed(), "Failed-status ConfigMap must still be written so the controller can surface the error")

	g.Expect(cm.Annotations[constants.PVCMetadataLastErrorAnnotation]).NotTo(BeEmpty())

	modelKey := constants.GetModelConfigMapKey(cfg.BaseModelNamespace, cfg.BaseModelName, cfg.ClusterScoped)
	var entry modelagent.ModelEntry
	g.Expect(json.Unmarshal([]byte(cm.Data[modelKey]), &entry)).To(Succeed())
	g.Expect(entry.Status).To(Equal(modelagent.ModelStatusFailed))
}

func TestPVCMetadataAgent_Idempotent_Envtest(t *testing.T) {
	g := NewGomegaWithT(t)
	_, kc, stop := startPVCEnvTest(t, g)
	defer stop()

	modelDir := writeMinimalLlamaConfigForIntegration(t)
	cfg := &modelmetadata.Config{
		ModelPath:          modelDir,
		BaseModelName:      "llama-7b",
		BaseModelNamespace: "models",
		Logger:             logging.Discard(),
	}
	ex, err := modelmetadata.NewMetadataExtractor(cfg, kc, zap.NewNop())
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(ex.Start()).NotTo(HaveOccurred())
	g.Expect(ex.Start()).NotTo(HaveOccurred(), "second run must overwrite via optimistic-locking retry, not error")

	// Filter by our exclusive label so envtest system ConfigMaps
	// (kube-root-ca etc.) don't pollute the count.
	cms := &corev1.ConfigMapList{}
	g.Expect(kc.List(context.Background(), cms,
		client.InNamespace(constants.OMENamespace),
		client.MatchingLabels{constants.PVCStorageConfigMapLabel: "true"},
	)).To(Succeed())
	g.Expect(cms.Items).To(HaveLen(1), "second extraction must not create a duplicate ConfigMap")
}

// TestPVCMetadataAgent_ClusterScoped_Envtest exercises the cluster-scoped
// flag path: no namespace, scope label = "cluster", different ConfigMap
// name keyspace.
func TestPVCMetadataAgent_ClusterScoped_Envtest(t *testing.T) {
	g := NewGomegaWithT(t)
	_, kc, stop := startPVCEnvTest(t, g)
	defer stop()

	cfg := &modelmetadata.Config{
		ModelPath:     writeMinimalLlamaConfigForIntegration(t),
		BaseModelName: "shared-llama",
		ClusterScoped: true,
		Logger:        logging.Discard(),
	}
	ex, err := modelmetadata.NewMetadataExtractor(cfg, kc, zap.NewNop())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ex.Start()).NotTo(HaveOccurred())

	cmName := constants.GetPVCMetadataConfigMapName(cfg.BaseModelName, "", true)
	cm := &corev1.ConfigMap{}
	g.Expect(kc.Get(context.Background(),
		types.NamespacedName{Namespace: constants.OMENamespace, Name: cmName}, cm)).
		To(Succeed())

	g.Expect(cm.Labels[constants.PVCMetadataScopeLabel]).To(Equal("cluster"))
	g.Expect(cm.Data).To(HaveKey(constants.GetModelConfigMapKey("", cfg.BaseModelName, true)))
}

// --- helpers --------------------------------------------------------------

func writeMinimalLlamaConfigForIntegration(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
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
	if err != nil {
		t.Fatalf("marshal config.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), data, 0o644); err != nil {
		t.Fatalf("write config.json: %v", err)
	}
	return dir
}
