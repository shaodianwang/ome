package modelparser

import (
	"strings"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// QuantAlgoToOMEEnum maps a known quantization algorithm name to the
// OME ModelQuantization enum. Accepts values from either source the
// parser reads:
//
//   - config.json's "quantization_config.quant_method" (HF native;
//     surfaced via modelconfig.HuggingFaceModel.GetQuantizationType)
//   - hf_quant_config.json's "quantization.quant_algo" (NVIDIA ModelOpt
//     and TensorRT-LLM convention; surfaced via
//     modelconfig.ParseHFQuantConfig)
//
// Returns the empty string when the value isn't recognized — callers
// should leave BaseModel.spec.quantization nil rather than write a
// nonsensical placeholder.
//
// The matching is fuzzy (case-insensitive, prefix/contains) because
// upstream tools mix conventions: "fp8", "FP8", "fp8_per_tensor",
// "INT4_AWQ", "w4a16_awq" all map to the same OME enum value.
func QuantAlgoToOMEEnum(s string) v1beta1.ModelQuantization {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "" {
		return ""
	}
	switch {
	case v == "fbgemm_fp8":
		return v1beta1.ModelQuantizationFbgemmFP8
	case v == "nvfp4":
		return v1beta1.ModelQuantizationNVFP4
	case v == "mxfp4":
		return v1beta1.ModelQuantizationMXFP4
	case strings.Contains(v, "int4") || strings.Contains(v, "w4a"):
		// covers int4, int4_awq, int4_gptq, w4a8_awq, w4a16_awq, etc.
		return v1beta1.ModelQuantizationINT4
	case strings.Contains(v, "fp8"):
		// covers fp8, fp8_per_tensor, fp8_e4m3, etc. fbgemm_fp8 is
		// matched above before this fallthrough.
		return v1beta1.ModelQuantizationFP8
	}
	return ""
}
