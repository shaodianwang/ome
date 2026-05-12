package modelconfig

import (
	"regexp"
	"strings"
	"sync"
)

// IsScaleTensor reports whether the given tensor name is a quantization
// scale, zero-point, or other calibration tensor — i.e., NOT a real
// model parameter and should be excluded from parameter-count totals.
//
// Counted naively, a quantized model's safetensors include scale
// tensors (one per group of 16 weights for NVFP4, for example) which
// inflate the "parameter count" with values that aren't real
// parameters. Skipping them gives a faithful logical count.
//
// Two naming conventions exist in the wild, and we have to handle
// both because they coexist on real production models:
//
//	dot-convention (ModelOpt / AWQ / GPTQ / FP8):
//	  the scale lives as its own dot-separated segment, e.g.
//	  "model.layers.0.mlp.gate_proj.weight_scale" → basename
//	  "weight_scale". Matched via exact-equality on the basename
//	  against scaleExactBasenames.
//
//	underscore-convention (mxfp4 / gpt-oss MoE):
//	  the scale name is concatenated to the projection name with
//	  an underscore, e.g. "model.layers.0.mlp.experts.gate_up_proj_scales"
//	  → basename "gate_up_proj_scales". Matched via HasSuffix on
//	  the basename against scaleSuffixes (each leading "_" anchors
//	  to a real word boundary, avoiding false positives like
//	  "model.scaled_weight").
//
// Both checks operate on the basename (last dot-segment), never on
// the full path — that's what prevents misclassifying weights whose
// dotted module path happens to contain "scale" elsewhere.
func IsScaleTensor(tensorName string) bool {
	basename := tensorName
	if idx := strings.LastIndex(tensorName, "."); idx >= 0 {
		basename = tensorName[idx+1:]
	}
	if _, ok := scaleExactBasenames[basename]; ok {
		return true
	}
	for _, suffix := range scaleSuffixes {
		if strings.HasSuffix(basename, suffix) {
			return true
		}
	}
	return false
}

// scaleExactBasenames catches the ModelOpt + AWQ/GPTQ dot-convention
// where the scale (or other non-parameter calibration tensor) is its
// own dot-segment (basename equals one of these).
//
// Verified against real fixtures in testdata/quant/:
//
//	weight_scale, weight_scale_2 — ModelOpt NVFP4 (NVIDIA Llama-3.1-8B-FP4)
//	weight_scale_inv             — kimi-k2 FP8 (DeepSeek/Kimi block fp8 stores 1/scale)
//	input_scale                  — both above
//	scales, zeros, qzeros        — AWQ / GPTQ TheBloke variants
//	g_idx                        — GPTQ-only group index (int32 column-permutation)
//	k_scale, v_scale             — ModelOpt KV-cache scales on self_attn.{k,v}_proj
//	kv_cache_scaling_factor, weight_zero_point — ModelOpt extras
//
// Notably NOT in this set: "qweight" (AWQ/GPTQ packed weight — IS a
// real parameter, just stored in I32-packed form; counted via
// QuantPackingFactor with factor=8 for I32).
var scaleExactBasenames = map[string]struct{}{
	"weight_scale":            {},
	"weight_scale_2":          {},
	"weight_scale_inv":        {},
	"input_scale":             {},
	"k_scale":                 {},
	"v_scale":                 {},
	"kv_scale":                {},
	"kv_cache_scaling_factor": {},
	"weight_zero_point":       {},
	"scales":                  {},
	"zeros":                   {},
	"qzeros":                  {},
	"g_idx":                   {},
}

// scaleSuffixes catches the mxfp4 underscore-convention where the
// scale name is concatenated to the module name with "_". Each entry
// MUST start with "_" so we anchor at a word boundary — otherwise a
// real weight named "model.scaled_weight" would falsely match "_scale".
//
// Verified against testdata/quant/openai-gpt-oss-20b-mxfp4: the MoE
// expert weights are named "*_blocks" (real weights) paired with
// "*_scales" (scales). The "_blocks" suffix is intentionally NOT in
// this list — it's the actual quantized-weight tensor.
var scaleSuffixes = []string{
	"_scale",
	"_scales",
	"_scale_2",
	"_scale_inv",
	"_input_scale",
	"_weight_scale",
	"_weight_scale_2",
	"_weight_scale_inv",
	"_weight_zero_point",
	"_kv_scale",
	"_kv_cache_scaling_factor",
}

// IsExcludedTensor reports whether the given tensor name matches any
// of the patterns in the model's exclude/ignore list. Excluded modules
// are kept at higher precision (FP16/BF16) and are NOT packed-quantized
// — so their naive numel IS the real parameter count.
//
// Pattern semantics differ across producers (verified in testdata/quant/):
//
//   - ModelOpt exclude_modules:        substring patterns ("self_attn",
//     "lm_head", "layers.0.")  →  fall through to strings.Contains.
//   - fbgemm_fp8 modules_to_not_convert: full module paths
//     ("model.layers.0.mlp.down_proj")  →  also substring (the path is
//     a prefix of "model.layers.0.mlp.down_proj.weight").
//   - mxfp4 modules_to_not_convert:    glob patterns
//     ("model.layers.*.self_attn")  →  "*" expands to one or more
//     non-dot chars (matches a single dot-segment).
//
// Pattern detection: a pattern is treated as a glob iff it contains
// "*". Substring is the default — it's the lowest-common-denominator
// matcher that works for the first two conventions and is cheap.
func IsExcludedTensor(tensorName string, excludeModules []string) bool {
	for _, pattern := range excludeModules {
		if pattern == "" {
			continue
		}
		if strings.ContainsRune(pattern, '*') {
			if matchExcludeGlob(tensorName, pattern) {
				return true
			}
			continue
		}
		if strings.Contains(tensorName, pattern) {
			return true
		}
	}
	return false
}

// matchExcludeGlob compiles `pattern` (with "*" → "[^.]+" for one
// dot-segment) and tests whether `tensorName` contains a substring
// matching it. Compiled regexes are cached because the same pattern
// fires for every tensor in a multi-shard model — recompiling per
// tensor would be O(numTensors × numPatterns × patternLen).
func matchExcludeGlob(tensorName, pattern string) bool {
	rx, ok := excludeGlobCache.Load(pattern)
	if !ok {
		parts := strings.Split(pattern, "*")
		quoted := make([]string, len(parts))
		for i, p := range parts {
			quoted[i] = regexp.QuoteMeta(p)
		}
		compiled, err := regexp.Compile(strings.Join(quoted, `[^.]+`))
		if err != nil {
			// Treat compile failure as "this pattern doesn't match
			// anything" rather than crashing — operators see the
			// model misbehave (under-counts via missed exclusion)
			// instead of a controller crash.
			return false
		}
		excludeGlobCache.Store(pattern, compiled)
		rx = compiled
	}
	return rx.(*regexp.Regexp).MatchString(tensorName)
}

// excludeGlobCache memoizes regexp.Compile across IsExcludedTensor
// calls. Reset only on process restart — patterns are model-config
// values, expected to be stable for the lifetime of an OME pod.
var excludeGlobCache sync.Map

// QuantPackingFactor returns the number of logical parameters held
// per safetensors storage element for the given (quant algo, dtype)
// pair. Multiplier of 1 means "no packing" (full precision, or
// unrecognized combination — defaults conservatively to under-count
// rather than over-count).
//
// Coverage (verified in QuantPackingFactor's tests):
//
//	NVFP4 / MXFP4 / FP4 in U8/I8 → 2  (4-bit packed 2 per byte)
//	INT4_* / W4A* in I32         → 8  (4-bit packed 8 per int32, AWQ/GPTQ)
//	INT4_* / W4A* in I16         → 4
//	INT4_* / W4A* in U8/I8       → 2
//	FP8                          → 1  (already 8 bits)
//	anything else                → 1
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
// given its shape, dtype, and the optional quant config. Order matters
// — backward compat constrains the nil-quantConfig path to behave
// exactly like the legacy naive sum, including counting any tensor a
// model happens to ship in its header:
//
//  1. shape empty / any non-positive dim → 0 (skipped, like
//     "__metadata__" or scalars in the legacy code).
//  2. compute storage element count = product of shape dims.
//  3. quantConfig nil → return naive count (legacy behavior).
//  4. quantConfig non-nil AND scale tensor → 0 (skipped).
//  5. quantConfig non-nil AND excluded module → return naive count
//     (full-precision tensor, no packing).
//  6. quantConfig non-nil AND packed-quant tensor → multiply by
//     QuantPackingFactor(algo, dtype).
//
// Steps 4-6 are the per-tensor classification proper; steps 1-3 are
// the validation + backward-compat guards.
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
		return count
	}
	if IsScaleTensor(name) {
		return 0
	}
	if IsExcludedTensor(name, quantConfig.Quantization.ExcludeModules) {
		return count
	}
	factor := QuantPackingFactor(quantConfig.Quantization.QuantAlgo, dtype)
	return count * factor
}
