package modelconfig

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// DiffusionComponentSpec captures an individual component used by a diffusion pipeline.
type DiffusionComponentSpec struct {
	Library string
	Type    string
}

// DiffusionPipelineSpec captures pipeline-specific metadata for diffusion models.
type DiffusionPipelineSpec struct {
	ClassName            string
	DiffusersVersion     string
	Scheduler            *DiffusionComponentSpec
	TextEncoder          *DiffusionComponentSpec
	Tokenizer            *DiffusionComponentSpec
	Transformer          *DiffusionComponentSpec
	VAE                  *DiffusionComponentSpec
	AdditionalComponents map[string]DiffusionComponentSpec
}

// LoadDiffusionPipelineSpec loads and parses a diffusers model_index.json file.
func LoadDiffusionPipelineSpec(modelIndexPath string) (*DiffusionPipelineSpec, error) {
	if modelIndexPath == "" {
		return nil, fmt.Errorf("model index path cannot be empty")
	}

	data, err := os.ReadFile(modelIndexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read model_index.json at %s: %w", modelIndexPath, err)
	}

	data = SanitizeJSONBytes(data)

	pipeline, err := ParseDiffusionPipelineSpec(data)
	if err != nil {
		return nil, err
	}

	return pipeline, nil
}

// ParseDiffusionPipelineSpec parses a diffusers model_index.json payload.
func ParseDiffusionPipelineSpec(data []byte) (*DiffusionPipelineSpec, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse model_index.json: %w", err)
	}

	pipeline := &DiffusionPipelineSpec{}
	pipeline.ClassName = parseJSONStringField(raw, "_class_name", "class_name", "className")
	pipeline.DiffusersVersion = parseJSONStringField(raw, "_diffusers_version", "diffusers_version")

	additional := map[string]DiffusionComponentSpec{}
	for key, value := range raw {
		if strings.HasPrefix(key, "_") {
			continue
		}

		component, ok := parseDiffusersComponent(value)
		if !ok {
			continue
		}

		switch strings.ToLower(key) {
		case "scheduler":
			pipeline.Scheduler = component
		case "text_encoder":
			pipeline.TextEncoder = component
		case "tokenizer":
			pipeline.Tokenizer = component
		case "transformer", "unet":
			pipeline.Transformer = component
		case "vae":
			pipeline.VAE = component
		default:
			additional[key] = *component
		}
	}

	if len(additional) > 0 {
		pipeline.AdditionalComponents = additional
	}

	if pipeline.ClassName == "" &&
		pipeline.Scheduler == nil &&
		pipeline.TextEncoder == nil &&
		pipeline.Tokenizer == nil &&
		pipeline.Transformer == nil &&
		pipeline.VAE == nil &&
		len(pipeline.AdditionalComponents) == 0 {
		return nil, fmt.Errorf("model_index.json did not contain diffusion pipeline metadata")
	}

	return pipeline, nil
}

func parseDiffusersComponent(raw json.RawMessage) (*DiffusionComponentSpec, bool) {
	var parts []string
	if err := json.Unmarshal(raw, &parts); err == nil {
		switch len(parts) {
		case 0:
			return nil, false
		case 1:
			return &DiffusionComponentSpec{Type: parts[0]}, true
		default:
			return &DiffusionComponentSpec{Library: parts[0], Type: parts[1]}, true
		}
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false
	}

	className := parseJSONStringField(obj, "_class_name", "class_name", "className", "type")
	library := parseJSONStringField(obj, "_library", "library")
	if className == "" && library == "" {
		return nil, false
	}

	return &DiffusionComponentSpec{Library: library, Type: className}, true
}

func parseJSONStringField(values map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := values[key]
		if !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && value != "" {
			return value
		}
	}
	return ""
}

// GenericDiffusionModelConfig is the HuggingFaceDiffusionModel
// implementation for diffusers model_index.json files. It walks the
// pipeline's component sub-directories (text_encoder, transformer or
// unet, vae, plus any AdditionalComponents) and sums their parameter
// counts via safetensors. Context length comes from the text_encoder's
// own config.json.
type GenericDiffusionModelConfig struct {
	BaseModelConfig

	DiffusersVersion  string
	DiffusionPipeline *DiffusionPipelineSpec
}

func (c *GenericDiffusionModelConfig) GetDiffusionModel() *DiffusionPipelineSpec {
	return c.DiffusionPipeline
}

func (c *GenericDiffusionModelConfig) GetParameterCount() int64 {
	if c.ConfigPath == "" {
		return 0
	}

	baseDir := filepath.Dir(c.ConfigPath)
	textEncoderConfig := filepath.Join(baseDir, "text_encoder", "config.json")
	transformerConfig := filepath.Join(baseDir, "transformer", "config.json")
	if _, err := os.Stat(transformerConfig); err != nil {
		unetConfig := filepath.Join(baseDir, "unet", "config.json")
		if _, unetErr := os.Stat(unetConfig); unetErr == nil {
			transformerConfig = unetConfig
		}
	}
	vaeConfig := filepath.Join(baseDir, "vae", "config.json")

	total := int64(0)

	add := func(label, configPath string, required bool) bool {
		if configPath == "" {
			if required {
				fmt.Printf("Warning: %s config path is empty\n", label)
				return false
			}
			return true
		}
		if _, err := os.Stat(configPath); err != nil {
			if required {
				fmt.Printf("Warning: %s config not found at '%s': %v\n", label, configPath, err)
				return false
			}
			return true
		}

		count, err := FindAndParseSafetensors(configPath)
		if err != nil {
			if required {
				fmt.Printf("Warning: failed to parse %s safetensors: %v\n", label, err)
				return false
			}
			return true
		}
		if total > math.MaxInt64-count {
			fmt.Printf("Warning: parameter count overflow when adding %s\n", label)
			return false
		}
		total += count
		return true
	}

	if !add("text_encoder", textEncoderConfig, true) {
		return 0
	}
	if !add("transformer", transformerConfig, true) {
		return 0
	}
	if !add("vae", vaeConfig, true) {
		return 0
	}

	for name := range c.DiffusionPipeline.AdditionalComponents {
		configPath := filepath.Join(baseDir, name, "config.json")
		if !add(name, configPath, false) {
			return 0
		}
	}

	return total
}

func (c *GenericDiffusionModelConfig) GetQuantizationType() string {
	// Quantization metadata for diffusers isn't standardized in HF.
	return ""
}

func (c *GenericDiffusionModelConfig) GetHFQuantConfig() *HFQuantConfig {
	// Diffusion pipelines don't carry HF-style quantization_config.
	return nil
}

func (c *GenericDiffusionModelConfig) GetContextLength() int {
	if c.ConfigPath == "" {
		return 0
	}

	textEncoderConfig := filepath.Join(filepath.Dir(c.ConfigPath), "text_encoder", "config.json")
	data, err := os.ReadFile(textEncoderConfig)
	if err != nil {
		return 0
	}

	data = SanitizeJSONBytes(data)

	var meta struct {
		MaxPositionEmbeds int `json:"max_position_embeddings"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0
	}
	return meta.MaxPositionEmbeds
}

func (c *GenericDiffusionModelConfig) GetModelSizeBytes() int64 {
	paramCount := c.GetParameterCount()
	if paramCount == 0 {
		return 0
	}
	return EstimateModelSizeBytes(paramCount, c.TorchDtype)
}

func (c *GenericDiffusionModelConfig) HasVision() bool {
	return true
}

// GetCapabilities returns the model's classified capabilities by
// running the modelconfig rule dispatcher against this config.
// Overrides BaseModelConfig's [CapabilityUnknown] default. The
// diffusionRule will fire (since this type satisfies
// HuggingFaceDiffusionModel) and use the pipeline class name to
// classify.
func (c *GenericDiffusionModelConfig) GetCapabilities() []Capability {
	return classifyCapabilities(c)
}

func (c *GenericDiffusionModelConfig) IsEmbedding() bool {
	return false
}
