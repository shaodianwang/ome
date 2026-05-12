package modelconfig

import (
	"strings"
)

// IsScaleTensor reports whether the given tensor name is a quantization
// scale, zero-point, or other calibration tensor — i.e., NOT a real
// model parameter and should be excluded from parameter-count totals.
//
// Counted naively, a quantized model's safetensors include scale tensors
// (one per group of 16 weights for NVFP4, for example) which inflate the
// "parameter count" with values that aren't real parameters. Skipping
// them gives a faithful logical count.
//
// The check matches on the LAST dot-separated segment (the basename in
// the dot-path convention safetensors uses). This catches names like
// "model.layers.5.mlp.gate_proj.weight_scale" (basename =
// "weight_scale") without misclassifying real weights whose names
// happen to contain "scale" elsewhere (e.g., "model.scaled_weight").
//
// Recognized basenames:
//
//   - ModelOpt / TensorRT-LLM: weight_scale, weight_scale_2,
//     input_scale, kv_scale, kv_cache_scaling_factor, weight_zero_point
//   - AWQ / GPTQ: scales, zeros, qzeros
func IsScaleTensor(tensorName string) bool {
	basename := tensorName
	if idx := strings.LastIndex(tensorName, "."); idx >= 0 {
		basename = tensorName[idx+1:]
	}
	_, ok := scaleTensorBasenames[basename]
	return ok
}

var scaleTensorBasenames = map[string]struct{}{
	"weight_scale":            {},
	"weight_scale_2":          {},
	"input_scale":             {},
	"kv_scale":                {},
	"kv_cache_scaling_factor": {},
	"weight_zero_point":       {},
	"scales":                  {},
	"zeros":                   {},
	"qzeros":                  {},
}

// IsExcludedTensor reports whether the given tensor name matches any
// of the patterns in hf_quant_config.json's "exclude_modules". These
// modules are kept at higher precision (FP16/BF16) and are NOT
// packed-quantized — so their naive numel IS the real parameter count.
//
// ModelOpt's exclude_modules entries are substring patterns —
// "self_attn" matches any tensor whose name contains "self_attn",
// "layers.0." matches any tensor in the first layer block. Empty
// pattern is ignored.
func IsExcludedTensor(tensorName string, excludeModules []string) bool {
	for _, pattern := range excludeModules {
		if pattern == "" {
			continue
		}
		if strings.Contains(tensorName, pattern) {
			return true
		}
	}
	return false
}

// QuantPackingFactor returns the number of logical parameters stored
// per element of the given safetensors dtype, given the quantization
// algorithm in use.
//
// Returns 1 for the "no packing" case (full-precision storage, or
// no quantization) so callers can multiply unconditionally.
//
// Examples:
//
//	NVFP4 / MXFP4 / FP4 (4-bit weights packed 2 per byte):
//	  dtype=U8 → factor 2 (each byte holds 2 logical params)
//	  dtype=F4 → factor 1 (native 4-bit dtype, no packing)
//
//	INT4_AWQ / INT4_GPTQ (4-bit weights packed 8 per int32):
//	  dtype=I32 → factor 8
//	  dtype=U8/I8 → factor 2
//
//	FP8 (8-bit weights, no packing):
//	  dtype=F8_E4M3/F8_E5M2/U8/I8 → factor 1
//
// Unknown combinations return 1 — the function is conservative and
// will under-count rather than over-count when the combination is
// unfamiliar.
func QuantPackingFactor(quantAlgo, dtype string) int64 {
	algo := strings.ToLower(strings.TrimSpace(quantAlgo))
	dt := strings.ToUpper(strings.TrimSpace(dtype))

	if algo == "" {
		return 1
	}

	switch {
	case algo == "nvfp4" || algo == "mxfp4" || algo == "fp4":
		switch dt {
		case "U8", "I8", "UINT8", "INT8":
			return 2
		}
	case strings.Contains(algo, "int4") || strings.Contains(algo, "w4a"):
		switch dt {
		case "I32", "U32", "INT32", "UINT32":
			return 8
		case "I16", "U16", "INT16", "UINT16":
			return 4
		case "U8", "I8", "UINT8", "INT8":
			return 2
		}
	}
	return 1
}

// CountTensorParams returns the logical parameter count for one tensor
// given its shape, dtype, and the optional hf_quant_config that
// declares the model's quantization scheme. The function is the heart
// of the per-tensor classification:
//
//  1. If the name matches a scale/zero-point pattern → 0 (skipped).
//  2. Compute the storage element count = product of shape dims.
//  3. If quantConfig declares a packed scheme AND the tensor is NOT
//     in exclude_modules → multiply by QuantPackingFactor(algo, dtype).
//  4. Otherwise → use the storage count as-is (full precision, or
//     excluded module).
//
// Returns 0 if shape is empty (treated as scalar/metadata, like the
// "__metadata__" entry).
func CountTensorParams(name string, shape []int64, dtype string, quantConfig *HFQuantConfig) int64 {
	if len(shape) == 0 {
		return 0
	}

	count := int64(1)
	for _, dim := range shape {
		if dim <= 0 {
			return 0
		}
		count *= dim
	}

	if quantConfig == nil {
		// Backward compat: legacy ParseSafetensorsHeader callers
		// still get the naive sum of shape products, which includes
		// any tensors a model happens to ship in its header.
		// Unquantized models don't have *_weight_scale tensors, so
		// not skipping here doesn't change their counts.
		return count
	}
	if IsScaleTensor(name) {
		// Only skip scale/zero-point tensors for quantized models.
		// They're calibration data, not real parameters — counting
		// them inflates a quantized model's reported param count by
		// roughly group_count / group_size, which is small but
		// non-trivial.
		return 0
	}
	if IsExcludedTensor(name, quantConfig.Quantization.ExcludeModules) {
		return count
	}
	factor := QuantPackingFactor(quantConfig.Quantization.QuantAlgo, dtype)
	return count * factor
}
