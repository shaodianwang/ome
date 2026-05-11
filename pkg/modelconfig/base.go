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

// IsEmbedding defaults to false. Capability-side classification in
// pkg/modelparser does its own architecture-string sniffing for
// embedding models, so leaving this as false for transformer configs
// is fine.
func (c *BaseModelConfig) IsEmbedding() bool {
	return false
}

// QuantizationConfig captures the quantization settings most HF
// configs expose.
type QuantizationConfig struct {
	ActivationScheme string `json:"activation_scheme"`
	Format           string `json:"fmt"`
	QuantMethod      string `json:"quant_method"`
	WeightBlockSize  []int  `json:"weight_block_size"`
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
