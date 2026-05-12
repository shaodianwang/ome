package modelconfig

import (
	"fmt"
	"regexp"
	"strings"
)

// AutoMap defines the mapping of model classes for custom Hugging Face
// models. Used when models require custom code (e.g., trust_remote_code=True).
type AutoMap struct {
	AutoConfig           string `json:"AutoConfig,omitempty"`
	AutoModel            string `json:"AutoModel,omitempty"`
	AutoModelForCausalLM string `json:"AutoModelForCausalLM,omitempty"`
}

// BaseModelConfig defines fields shared across all Hugging Face model
// configurations and provides default HuggingFaceModel methods that
// concrete config types can rely on by embedding.
type BaseModelConfig struct {
	ModelType          string   `json:"model_type"`
	Architectures      []string `json:"architectures"`
	TorchDtype         string   `json:"torch_dtype"`
	TransformerVersion string   `json:"transformers_version"`

	// Internal field; not in JSON.
	ConfigPath string `json:"-"`
}

func (c *BaseModelConfig) GetModelType() string {
	return c.ModelType
}

func (c *BaseModelConfig) GetTransformerVersion() string {
	return c.TransformerVersion
}

func (c *BaseModelConfig) GetArchitecture() string {
	if len(c.Architectures) > 0 {
		return c.Architectures[0]
	}
	return ""
}

func (c *BaseModelConfig) GetTorchDtype() string {
	return c.TorchDtype
}

// HasVision defaults to false. GenericModelConfig overrides this with
// JSON-shape detection (vision_config / mm_vision_tower / image_token_*
// / img_processor); GenericDiffusionModelConfig overrides to true.
func (c *BaseModelConfig) HasVision() bool {
	return false
}

// IsEmbedding defaults to false. The capability classifier in this
// package detects embedding models from architecture name (BertModel,
// SentenceTransformerModel, etc.), so few configs need to override.
func (c *BaseModelConfig) IsEmbedding() bool {
	return false
}

// GetCapabilities defaults to CapabilityUnknown. GenericModelConfig
// and GenericDiffusionModelConfig override this with the real
// classifier; types that embed BaseModelConfig without overriding
// will surface as Unknown so the gap is visible in logs.
func (c *BaseModelConfig) GetCapabilities() []Capability {
	return []Capability{CapabilityUnknown}
}

// QuantizationConfig captures the embedded "quantization_config" block
// inside config.json — the HF-native convention used by mxfp4
// (gpt-oss), fbgemm_fp8 (Llama-3.1-405B), gptq, awq, and plain fp8
// (kimi-k2). Distinct from HFQuantConfig (NVIDIA ModelOpt's separate
// hf_quant_config.json file in quant_config.go).
//
// Producers can't agree on a single key name for the excluded-modules
// list: HF-native uses modules_to_not_convert, compressed-tensors uses
// "ignore", and some neuralmagic builds use "ignored_layers". We accept
// all three and surface them via ExcludePatterns(). Field names mirror
// the JSON exactly; the union covers every variant observed in
// pkg/modelconfig/testdata/quant/.
type QuantizationConfig struct {
	// QuantMethod is the algorithm name ("fp8", "mxfp4", "fbgemm_fp8",
	// "gptq", "awq", "compressed-tensors", etc.). Maps to a packing
	// factor via QuantPackingFactor and to OME's enum via the parser's
	// QuantAlgoToOMEEnum.
	QuantMethod string `json:"quant_method"`
	// ModulesToNotConvert is the HF-native excluded-modules key. Used
	// by mxfp4 (gpt-oss), fbgemm_fp8, awq. Patterns may be substrings
	// OR globs (mxfp4 uses "model.layers.*.self_attn"); IsExcludedTensor
	// handles both.
	ModulesToNotConvert []string `json:"modules_to_not_convert,omitempty"`
	// Ignore is the compressed-tensors excluded-modules key. Same
	// semantics as ModulesToNotConvert but a different field name.
	Ignore []string `json:"ignore,omitempty"`
	// IgnoredLayers is the neuralmagic excluded-modules key. Same
	// semantics, third name.
	IgnoredLayers []string `json:"ignored_layers,omitempty"`
	// ActivationScheme — fp8 / kimi-k2 use "dynamic" / "static".
	// Informational only today.
	ActivationScheme string `json:"activation_scheme,omitempty"`
	// Format — fp8 / kimi-k2 use "e4m3" / "e5m2"; compressed-tensors
	// uses "naive-quantized" / "int-quantized". Informational.
	Format string `json:"fmt,omitempty"`
	// WeightBlockSize — fp8 group size, e.g. [128, 128] for
	// kimi-k2's per-block FP8. Informational.
	WeightBlockSize []int `json:"weight_block_size,omitempty"`
}

// ExcludePatterns returns the first non-empty excluded-modules list
// across the three known field-name conventions. Returns nil when no
// list is set OR when the receiver is nil.
//
// Resolution order is the documented frequency in production
// fixtures: HF-native is most common, then compressed-tensors, then
// neuralmagic. If a producer ships multiple lists in the same config
// (no real example yet), the most-common name wins.
func (c *QuantizationConfig) ExcludePatterns() []string {
	if c == nil {
		return nil
	}
	if len(c.ModulesToNotConvert) > 0 {
		return c.ModulesToNotConvert
	}
	if len(c.Ignore) > 0 {
		return c.Ignore
	}
	if len(c.IgnoredLayers) > 0 {
		return c.IgnoredLayers
	}
	return nil
}

// ToHFQuantConfig converts an embedded HF-native QuantizationConfig
// into the standalone HFQuantConfig shape so the safetensors counting
// pipeline can use a single code path regardless of the producer
// convention. Returns nil if the embedded block has no quant_method
// (i.e., the model isn't quantized).
//
// The conversion is lossy in the OTHER direction (HFQuantConfig has
// fields like KVCacheQuantAlgo and group_size that the embedded
// block doesn't carry), but that's fine — the param-count pipeline
// only reads QuantAlgo + ExcludeModules, which are the two fields
// covered by both shapes.
func (c *QuantizationConfig) ToHFQuantConfig() *HFQuantConfig {
	if c == nil || strings.TrimSpace(c.QuantMethod) == "" {
		return nil
	}
	return &HFQuantConfig{
		QuantMethod: c.QuantMethod,
		Quantization: HFQuantConfigDetails{
			QuantAlgo:      c.QuantMethod,
			ExcludeModules: c.ExcludePatterns(),
		},
	}
}

// RopeScalingConfig captures RoPE (Rotary Position Embedding) scaling
// configuration. Several variants of fields exist across model
// families; the union here covers what's been observed in fixtures.
type RopeScalingConfig struct {
	Type                          string  `json:"type,omitempty"`
	RopeType                      string  `json:"rope_type,omitempty"`
	Factor                        float64 `json:"factor"`
	LowFreqFactor                 float64 `json:"low_freq_factor,omitempty"`
	HighFreqFactor                float64 `json:"high_freq_factor,omitempty"`
	BetaFast                      float64 `json:"beta_fast,omitempty"`
	BetaSlow                      float64 `json:"beta_slow,omitempty"`
	MScale                        float64 `json:"mscale,omitempty"`
	MScaleAllDim                  float64 `json:"mscale_all_dim,omitempty"`
	OriginalMaxPositionEmbeddings int     `json:"original_max_position_embeddings"`
}

// DtypeSizeBytes maps torch data types to their size in bytes per parameter.
var DtypeSizeBytes = map[string]float64{
	"float32":  4.0,
	"float":    4.0,
	"bfloat16": 2.0,
	"bf16":     2.0,
	"float16":  2.0,
	"fp16":     2.0,
	"half":     2.0,
	"int8":     1.0,
	"fp8":      1.0,
	"float8":   1.0,
	"e4m3":     1.0,
	"int4":     0.5,
	"4bit":     0.5,
}

// FormatSize formats a byte size in a human-readable form (B, KB, MB, GB, TB).
func FormatSize(size int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)

	switch {
	case size < kb:
		return fmt.Sprintf("%d B", size)
	case size < mb:
		return fmt.Sprintf("%.2f KB", float64(size)/float64(kb))
	case size < gb:
		return fmt.Sprintf("%.2f MB", float64(size)/float64(mb))
	case size < tb:
		return fmt.Sprintf("%.2f GB", float64(size)/float64(gb))
	default:
		return fmt.Sprintf("%.2f TB", float64(size)/float64(tb))
	}
}

// FormatParamCount converts a parameter count to a human-readable string.
// Examples: 1000 -> "1K", 1500000 -> "1.5M", 685000000000 -> "685B".
func FormatParamCount(count int64) string {
	const (
		thousand = 1_000
		million  = 1_000_000
		billion  = 1_000_000_000
		trillion = 1_000_000_000_000
	)

	var value float64
	var suffix string

	switch {
	case count >= trillion:
		value = float64(count) / float64(trillion)
		suffix = "T"
	case count >= billion:
		value = float64(count) / float64(billion)
		suffix = "B"
	case count >= million:
		value = float64(count) / float64(million)
		suffix = "M"
	case count >= thousand:
		value = float64(count) / float64(thousand)
		suffix = "K"
	default:
		return fmt.Sprintf("%d", count)
	}

	if value == float64(int64(value)) {
		return fmt.Sprintf("%d%s", int64(value), suffix)
	}
	if value*100 == float64(int64(value*100)) {
		return fmt.Sprintf("%.1f%s", value, suffix)
	}
	return fmt.Sprintf("%.2f%s", value, suffix)
}

// EstimateModelSizeBytes estimates model size in bytes from parameter
// count and torch dtype. Falls back to float32 (4 bytes) for unknown
// dtypes.
func EstimateModelSizeBytes(paramCount int64, dtype string) int64 {
	sizePerParam, ok := DtypeSizeBytes[strings.ToLower(dtype)]
	if !ok {
		sizePerParam = 4.0
	}
	return int64(float64(paramCount) * sizePerParam)
}

// Regex patterns for sanitizing non-standard JSON values.
var (
	infinityRegex    = regexp.MustCompile(`([:,\[]\s*)Infinity(\s*[,\]\}])`)
	negInfinityRegex = regexp.MustCompile(`([:,\[]\s*)-Infinity(\s*[,\]\}])`)
	nanRegex         = regexp.MustCompile(`([:,\[]\s*)NaN(\s*[,\]\}])`)
)

// SanitizeJSONBytes replaces JavaScript/Python-style special float
// values (Infinity, -Infinity, NaN) with JSON-compatible substitutes.
// Some configs (e.g., NVIDIA Nemotron) emit these; Python's json
// module accepts them but Go's does not.
func SanitizeJSONBytes(data []byte) []byte {
	s := string(data)
	s = infinityRegex.ReplaceAllString(s, "${1}1e308${2}")
	s = negInfinityRegex.ReplaceAllString(s, "${1}-1e308${2}")
	s = nanRegex.ReplaceAllString(s, "${1}null${2}")
	return []byte(s)
}
