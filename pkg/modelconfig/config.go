package modelconfig

import (
	"encoding/json"
	"fmt"
	"path/filepath"
)

// GenericModelConfig is the only HuggingFaceModel implementation for
// transformer config.json. It parses common fields, probes nested
// sub-configs (text_config, llm_config, language_config) for
// multimodal models, attempts to derive parameter count from
// safetensors files, and falls back to architecture-based estimation.
type GenericModelConfig struct {
	BaseModelConfig

	// Architecture dimensions.
	HiddenSize            int `json:"hidden_size"`
	NumHiddenLayers       int `json:"num_hidden_layers"`
	NumAttentionHeads     int `json:"num_attention_heads"`
	IntermediateSize      int `json:"intermediate_size"`
	MaxPositionEmbeddings int `json:"max_position_embeddings"`
	MaxSequenceLength     int `json:"max_sequence_length"`
	MaxSeqLen             int `json:"max_seq_len"`      // DBRX family
	SeqLength             int `json:"seq_length"`       // ChatGLM family
	ModelMaxLength        int `json:"model_max_length"` // Baichuan family
	VocabSize             int `json:"vocab_size"`

	// MoE fields (populated from top-level or nested config).
	NRoutedExperts      int `json:"n_routed_experts"`
	NSharedExperts      int `json:"n_shared_experts"`
	MoeIntermediateSize int `json:"moe_intermediate_size"`

	// Quantization config (optional).
	QuantizationConfig *QuantizationConfig `json:"quantization_config,omitempty"`

	// Set during loading when a vision shape signal is detected.
	hasVisionConfig bool
}

// GetParameterCount derives a parameter count: try safetensors first,
// fall back to architecture-based estimation (MoE-aware).
func (c *GenericModelConfig) GetParameterCount() int64 {
	if c.ConfigPath != "" {
		count, err := FindAndParseSafetensors(c.ConfigPath)
		if err == nil && count > 0 {
			return count
		}
	}

	if c.HiddenSize > 0 && c.NumHiddenLayers > 0 {
		if c.NRoutedExperts > 0 {
			return estimateMoEParams(c.HiddenSize, c.NumHiddenLayers, c.IntermediateSize,
				c.MoeIntermediateSize, c.NRoutedExperts, c.NSharedExperts, c.VocabSize)
		}
		return estimateGenericParams(c.HiddenSize, c.NumHiddenLayers, c.VocabSize)
	}

	return 0
}

// estimateGenericParams provides a rough parameter estimate for
// transformer models when safetensors metadata isn't available.
// The 12*hidden^2 per-layer term is the standard transformer
// approximation that already absorbs both attention and MLP weights,
// so intermediateSize doesn't show up explicitly here.
func estimateGenericParams(hiddenSize, numLayers, vocabSize int) int64 {
	embeddingParams := int64(vocabSize) * int64(hiddenSize)
	perLayerParams := int64(12) * int64(hiddenSize) * int64(hiddenSize)
	totalLayerParams := int64(numLayers) * perLayerParams
	return embeddingParams + totalLayerParams
}

// estimateMoEParams estimates parameter count for Mixture-of-Experts
// models, accounting for per-expert FFN weights, shared experts, and
// the router.
func estimateMoEParams(hiddenSize, numLayers, intermediateSize, moeIntermediateSize, nRoutedExperts, nSharedExperts, vocabSize int) int64 {
	if moeIntermediateSize == 0 {
		moeIntermediateSize = intermediateSize
	}

	params := int64(hiddenSize * vocabSize)
	params += int64(numLayers) * (
	// Self-attention.
	int64(4*hiddenSize*hiddenSize) +
		// Shared experts.
		int64(nSharedExperts*2*hiddenSize*intermediateSize) +
		// Routed experts.
		int64(nRoutedExperts*2*hiddenSize*moeIntermediateSize) +
		// Router.
		int64(hiddenSize*nRoutedExperts) +
		// Layer norms.
		int64(2*hiddenSize))

	return params
}

// nestedLLMConfigKeys lists the JSON keys under which multimodal
// models commonly store their language/LLM sub-configuration.
var nestedLLMConfigKeys = []string{"text_config", "llm_config", "language_config"}

// probeNestedConfig fills zero-valued fields in config from nested
// sub-configurations commonly found in multimodal model configs. It
// only fills fields that are zero/empty, so it is safe to call
// unconditionally. It also detects vision multimodality from JSON
// shape and resolves top-level MoE field name variants.
func probeNestedConfig(data []byte, config *GenericModelConfig) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}

	for _, key := range nestedLLMConfigKeys {
		sub, ok := raw[key]
		if !ok {
			continue
		}
		var nested struct {
			Architectures         []string `json:"architectures"`
			HiddenSize            int      `json:"hidden_size"`
			NumHiddenLayers       int      `json:"num_hidden_layers"`
			NumAttentionHeads     int      `json:"num_attention_heads"`
			IntermediateSize      int      `json:"intermediate_size"`
			MaxPositionEmbeddings int      `json:"max_position_embeddings"`
			VocabSize             int      `json:"vocab_size"`
			TransformersVersion   string   `json:"transformers_version"`
			TorchDtype            string   `json:"torch_dtype"`
			Dtype                 string   `json:"dtype"` // qwen3_5 family stores dtype here
			// MoE fields — try all common JSON key names via multiple fields.
			NRoutedExperts      int `json:"n_routed_experts"`
			NumLocalExperts     int `json:"num_local_experts"`
			NumExperts          int `json:"num_experts"`
			NSharedExperts      int `json:"n_shared_experts"`
			MoeIntermediateSize int `json:"moe_intermediate_size"`
		}
		if err := json.Unmarshal(sub, &nested); err != nil {
			continue
		}

		if len(config.Architectures) == 0 && len(nested.Architectures) > 0 {
			config.Architectures = nested.Architectures
		}
		if config.HiddenSize == 0 {
			config.HiddenSize = nested.HiddenSize
		}
		if config.NumHiddenLayers == 0 {
			config.NumHiddenLayers = nested.NumHiddenLayers
		}
		if config.NumAttentionHeads == 0 {
			config.NumAttentionHeads = nested.NumAttentionHeads
		}
		if config.IntermediateSize == 0 {
			config.IntermediateSize = nested.IntermediateSize
		}
		if config.MaxPositionEmbeddings == 0 {
			config.MaxPositionEmbeddings = nested.MaxPositionEmbeddings
		}
		if config.VocabSize == 0 {
			config.VocabSize = nested.VocabSize
		}
		if config.TransformerVersion == "" {
			config.TransformerVersion = nested.TransformersVersion
		}
		if config.TorchDtype == "" {
			config.TorchDtype = nested.TorchDtype
		}
		if config.TorchDtype == "" && nested.Dtype != "" {
			config.TorchDtype = nested.Dtype
		}

		if config.NRoutedExperts == 0 {
			switch {
			case nested.NRoutedExperts > 0:
				config.NRoutedExperts = nested.NRoutedExperts
			case nested.NumLocalExperts > 0:
				config.NRoutedExperts = nested.NumLocalExperts
			case nested.NumExperts > 0:
				config.NRoutedExperts = nested.NumExperts
			}
		}
		if config.NSharedExperts == 0 {
			config.NSharedExperts = nested.NSharedExperts
		}
		if config.MoeIntermediateSize == 0 {
			config.MoeIntermediateSize = nested.MoeIntermediateSize
		}

		break // First matching nested config wins.
	}

	// Resolve top-level MoE field name variants (num_local_experts,
	// num_experts) that don't match the GenericModelConfig JSON tags.
	if config.NRoutedExperts == 0 {
		var topLevel struct {
			NumLocalExperts int `json:"num_local_experts"`
			NumExperts      int `json:"num_experts"`
		}
		if err := json.Unmarshal(data, &topLevel); err == nil {
			switch {
			case topLevel.NumLocalExperts > 0:
				config.NRoutedExperts = topLevel.NumLocalExperts
			case topLevel.NumExperts > 0:
				config.NRoutedExperts = topLevel.NumExperts
			}
		}
	}

	// Detect vision multimodality from JSON shape. Any of these
	// top-level keys is enough on its own; we don't combine them.
	//   - vision_config: most VL models (Qwen-VL, Gemma3, Llama4,
	//     MLlama, DeepSeek-VL, Janus, ...).
	//   - mm_vision_tower: pre-vision_config LLaVA convention
	//     (LLaVA v1.5 raw checkpoints).
	//   - image_token_id / image_token_index: VL models that thread
	//     a single image token through tokenization (Qwen2/3-VL,
	//     Gemma3, Llama4, LLaVA-1.5-HF, MLlama).
	//   - img_processor: Phi-3 Vision style (nests CLIP vision
	//     model spec under this key).
	visionShapeKeys := []string{
		"vision_config",
		"mm_vision_tower",
		"image_token_id",
		"image_token_index",
		"img_processor",
	}
	for _, key := range visionShapeKeys {
		if _, ok := raw[key]; ok {
			config.hasVisionConfig = true
			break
		}
	}
}

func (c *GenericModelConfig) GetQuantizationType() string {
	if c.QuantizationConfig != nil && c.QuantizationConfig.QuantMethod != "" {
		return c.QuantizationConfig.QuantMethod
	}
	return ""
}

func (c *GenericModelConfig) GetHFQuantConfig() *HFQuantConfig {
	return c.QuantizationConfig.ToHFQuantConfig()
}

// GetContextLength returns the first non-zero candidate from the
// cascade: max_position_embeddings, max_sequence_length, max_seq_len
// (DBRX), seq_length (ChatGLM), model_max_length (Baichuan).
func (c *GenericModelConfig) GetContextLength() int {
	for _, candidate := range []int{
		c.MaxPositionEmbeddings,
		c.MaxSequenceLength,
		c.MaxSeqLen,
		c.SeqLength,
		c.ModelMaxLength,
	} {
		if candidate > 0 {
			return candidate
		}
	}
	return 0
}

// HasVision returns true if a vision shape signal was detected during
// loading.
func (c *GenericModelConfig) HasVision() bool {
	return c.hasVisionConfig
}

// GetCapabilities returns the model's classified capabilities by
// running the modelconfig rule dispatcher against this config.
// Overrides BaseModelConfig's [CapabilityUnknown] default.
func (c *GenericModelConfig) GetCapabilities() []Capability {
	return classifyCapabilities(c)
}

func (c *GenericModelConfig) GetModelSizeBytes() int64 {
	paramCount := c.GetParameterCount()
	if paramCount == 0 {
		return 0
	}
	return EstimateModelSizeBytes(paramCount, c.TorchDtype)
}

// parseGenericModelConfig dispatches between transformer config.json
// (parsed into GenericModelConfig) and diffusion model_index.json
// (parsed into GenericDiffusionModelConfig).
func parseGenericModelConfig(input ModelConfigInput) (HuggingFaceModel, error) {
	label := modelConfigInputLabel(input.Path)
	data := SanitizeJSONBytes(input.Data)

	if filepath.Base(input.Path) == "model_index.json" {
		pipeline, err := ParseDiffusionPipelineSpec(data)
		if err != nil {
			return nil, err
		}

		config := &GenericDiffusionModelConfig{
			DiffusersVersion:  pipeline.DiffusersVersion,
			DiffusionPipeline: pipeline,
		}
		config.ConfigPath = input.Path
		config.ModelType = "diffusers"
		if pipeline.ClassName != "" {
			config.Architectures = []string{pipeline.ClassName}
		}
		return config, nil
	}

	var config GenericModelConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON from '%s': %w", label, err)
	}
	if config.ModelType == "" {
		return nil, fmt.Errorf("model_type field is missing or empty in config file '%s'", label)
	}

	config.ConfigPath = input.Path

	// Probe nested sub-configs (text_config, llm_config, language_config)
	// to fill zero-valued fields for multimodal models.
	probeNestedConfig(data, &config)

	return &config, nil
}

func modelConfigInputLabel(path string) string {
	if path != "" {
		return path
	}
	return "model config input"
}
