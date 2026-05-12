package modelconfig

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsScaleTensor(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		// ModelOpt / TensorRT-LLM conventions
		{name: "weight_scale per-tensor", in: "model.layers.5.mlp.gate_proj.weight_scale", want: true},
		{name: "weight_scale_2 group scale", in: "model.layers.5.mlp.gate_proj.weight_scale_2", want: true},
		{name: "input_scale activation", in: "model.layers.5.mlp.gate_proj.input_scale", want: true},
		{name: "kv cache scaling factor", in: "model.layers.5.self_attn.k_proj.kv_cache_scaling_factor", want: true},
		// AWQ / GPTQ conventions
		{name: "AWQ scales", in: "model.layers.5.mlp.gate_proj.scales", want: true},
		{name: "AWQ qzeros", in: "model.layers.5.mlp.gate_proj.qzeros", want: true},
		{name: "zero_point", in: "model.layers.5.mlp.gate_proj.weight_zero_point", want: true},
		// Negatives — actual weights/biases must NOT be classified as scales
		{name: "regular weight", in: "model.layers.5.mlp.gate_proj.weight", want: false},
		{name: "regular bias", in: "model.layers.5.mlp.gate_proj.bias", want: false},
		{name: "AWQ qweight (real packed weight)", in: "model.layers.5.mlp.gate_proj.qweight", want: false},
		{name: "embedding", in: "model.embed_tokens.weight", want: false},
		{name: "lm_head weight", in: "lm_head.weight", want: false},
		// Edge cases
		{name: "empty string", in: "", want: false},
		// "scaled_weight" is a real weight type; the suffix rule is "_weight_scale"
		// (with trailing _scale), so this stays a real param. Belt-and-suspenders
		// for the suffix-not-substring choice.
		{name: "false positive guard: scaled_weight", in: "model.layers.5.scaled_weight", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsScaleTensor(tc.in); got != tc.want {
				t.Errorf("IsScaleTensor(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsExcludedTensor(t *testing.T) {
	excludeModules := []string{"layers.0.", "layers.61.", "self_attn", "lm_head"}

	tests := []struct {
		name   string
		tensor string
		want   bool
	}{
		// Layer prefix patterns
		{name: "first layer matches layers.0.", tensor: "model.layers.0.mlp.gate_proj.weight", want: true},
		{name: "last layer matches layers.61.", tensor: "model.layers.61.mlp.gate_proj.weight", want: true},
		// Module-name patterns (substring across the dot path)
		{name: "self_attn anywhere", tensor: "model.layers.5.self_attn.k_proj.weight", want: true},
		{name: "lm_head anywhere", tensor: "lm_head.weight", want: true},
		// Negatives — middle layers MLP weights are packed-quant, NOT excluded
		{name: "middle layer MLP weight", tensor: "model.layers.30.mlp.gate_proj.weight", want: false},
		{name: "middle layer router", tensor: "model.layers.30.block_sparse_moe.gate.weight", want: false},
		{name: "embedding stays excluded? no", tensor: "model.embed_tokens.weight", want: false},
		// Edge cases
		{name: "empty exclude list", tensor: "model.layers.5.mlp.gate_proj.weight", want: false},
		{name: "empty tensor name", tensor: "", want: false},
	}

	// Empty exclude list test gets a separate empty list (uses outer var otherwise).
	t.Run("empty exclude list", func(t *testing.T) {
		if got := IsExcludedTensor("model.layers.5.mlp.gate_proj.weight", nil); got {
			t.Errorf("expected false for nil exclude list")
		}
	})

	for _, tc := range tests {
		if tc.name == "empty exclude list" {
			continue // covered above
		}
		t.Run(tc.name, func(t *testing.T) {
			if got := IsExcludedTensor(tc.tensor, excludeModules); got != tc.want {
				t.Errorf("IsExcludedTensor(%q) = %v, want %v", tc.tensor, got, tc.want)
			}
		})
	}
}

func TestQuantPackingFactor(t *testing.T) {
	tests := []struct {
		name string
		algo string
		dt   string
		want int64
	}{
		// NVFP4 / MXFP4 / FP4 packed two per byte
		{name: "NVFP4 in U8 → 2", algo: "NVFP4", dt: "U8", want: 2},
		{name: "MXFP4 in U8 → 2", algo: "MXFP4", dt: "U8", want: 2},
		{name: "FP4 in I8 → 2", algo: "FP4", dt: "I8", want: 2},
		{name: "lowercase nvfp4 + uint8 alias → 2", algo: "nvfp4", dt: "uint8", want: 2},
		// INT4 packed differently depending on storage element size
		{name: "INT4_AWQ in I32 → 8", algo: "INT4_AWQ", dt: "I32", want: 8},
		{name: "INT4_GPTQ in I32 → 8", algo: "INT4_GPTQ", dt: "I32", want: 8},
		{name: "W4A8_AWQ in U8 → 2", algo: "W4A8_AWQ", dt: "U8", want: 2},
		{name: "INT4 in I16 → 4", algo: "INT4", dt: "I16", want: 4},
		// FP8 stored as F8 / U8 → 1 (no packing, 8 bits per element)
		{name: "FP8 in F8_E4M3 → 1", algo: "FP8", dt: "F8_E4M3", want: 1},
		{name: "FP8 in U8 → 1", algo: "FP8", dt: "U8", want: 1},
		// Native high-precision dtypes never get a multiplier
		{name: "NVFP4 weights stored as F16 (excluded module case) → 1", algo: "NVFP4", dt: "F16", want: 1},
		{name: "INT4_AWQ in BF16 → 1", algo: "INT4_AWQ", dt: "BF16", want: 1},
		// No quant config / unknown algo → 1
		{name: "empty algo → 1", algo: "", dt: "U8", want: 1},
		{name: "unknown algo → 1", algo: "some-future-format", dt: "U8", want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QuantPackingFactor(tc.algo, tc.dt); got != tc.want {
				t.Errorf("QuantPackingFactor(%q, %q) = %d, want %d", tc.algo, tc.dt, got, tc.want)
			}
		})
	}
}

// safetensorsHeaderForTest builds an in-memory safetensors header
// (the 8-byte length prefix + JSON tensor map) from a tensor list.
// Lets the param-count tests synthesize realistic fixtures without
// shipping multi-GB binary files.
func safetensorsHeaderForTest(t *testing.T, tensors map[string]map[string]any) []byte {
	t.Helper()
	jsonBytes, err := json.Marshal(tensors)
	require.NoError(t, err)
	out := make([]byte, 8+len(jsonBytes))
	binary.LittleEndian.PutUint64(out[:8], uint64(len(jsonBytes)))
	copy(out[8:], jsonBytes)
	return out
}

func TestParseSafetensorsHeaderQuantAware_NoQuantConfigEqualsNaiveCount(t *testing.T) {
	// Backward compat: when quantConfig is nil, the function must
	// return the same naive sum as the legacy ParseSafetensorsHeader.
	header := safetensorsHeaderForTest(t, map[string]map[string]any{
		"model.layers.0.weight": {
			"shape":        []int64{4096, 4096},
			"dtype":        "F16",
			"data_offsets": []int64{0, 4096 * 4096 * 2},
		},
		"model.layers.1.weight": {
			"shape":        []int64{1024, 1024},
			"dtype":        "F16",
			"data_offsets": []int64{0, 1024 * 1024 * 2},
		},
	})

	naive, err := ParseSafetensorsHeader(header[8:], "test")
	require.NoError(t, err)

	quantAware, err := ParseSafetensorsHeaderQuantAware(header[8:], "test", nil)
	require.NoError(t, err)

	assert.Equal(t, naive, quantAware,
		"with nil quantConfig the two functions must agree (backward compat)")
	assert.Equal(t, int64(4096*4096+1024*1024), quantAware)
}

func TestParseSafetensorsHeaderQuantAware_NVFP4PackedTensorsGet2xMultiplier(t *testing.T) {
	// NVFP4 weight stored as U8[out, in/2] — naive numel is HALF the
	// logical params. With a NVFP4 quant config the function must
	// return 2x the naive count.
	header := safetensorsHeaderForTest(t, map[string]map[string]any{
		// 1024 × 512 = 524288 storage bytes; logical = 1024 × 1024 = 1048576 params
		"model.layers.5.mlp.gate_proj.weight": {
			"shape":        []int64{1024, 512},
			"dtype":        "U8",
			"data_offsets": []int64{0, 1024 * 512},
		},
	})

	quantConfig := &HFQuantConfig{
		QuantMethod:  "modelopt",
		Quantization: HFQuantConfigDetails{QuantAlgo: "NVFP4"},
	}

	count, err := ParseSafetensorsHeaderQuantAware(header[8:], "test", quantConfig)
	require.NoError(t, err)
	assert.Equal(t, int64(1024*1024), count,
		"NVFP4 U8 storage must be multiplied by 2 to recover logical param count")
}

func TestParseSafetensorsHeaderQuantAware_MixedPackedAndExcludedAndScales(t *testing.T) {
	// Realistic mini-fixture: the user's Llama4x-NVFP4 model has
	// excluded modules (kept at FP16/BF16), packed NVFP4 weights, and
	// scale tensors. Verify the per-tensor classification handles all
	// three kinds in one safetensors header:
	//
	//   excluded FP16:   layers.0.self_attn.k_proj.weight     (10 params, full precision)
	//   packed NVFP4:    layers.30.mlp.gate_proj.weight       (200 params logical, 100 stored)
	//   scale tensor:    layers.30.mlp.gate_proj.weight_scale (skipped entirely)
	//
	// Expected total: 10 + 200 = 210 logical params.
	header := safetensorsHeaderForTest(t, map[string]map[string]any{
		"model.layers.0.self_attn.k_proj.weight": {
			"shape":        []int64{2, 5},
			"dtype":        "F16",
			"data_offsets": []int64{0, 20},
		},
		"model.layers.30.mlp.gate_proj.weight": {
			"shape":        []int64{10, 10}, // U8, NVFP4 → 100 storage * 2 = 200 logical
			"dtype":        "U8",
			"data_offsets": []int64{0, 100},
		},
		"model.layers.30.mlp.gate_proj.weight_scale": {
			"shape":        []int64{10}, // scale tensor, skip entirely
			"dtype":        "F32",
			"data_offsets": []int64{0, 40},
		},
	})

	quantConfig := &HFQuantConfig{
		QuantMethod: "modelopt",
		Quantization: HFQuantConfigDetails{
			QuantAlgo:      "NVFP4",
			ExcludeModules: []string{"layers.0.", "self_attn", "lm_head"},
		},
	}

	count, err := ParseSafetensorsHeaderQuantAware(header[8:], "test", quantConfig)
	require.NoError(t, err)
	assert.Equal(t, int64(210), count,
		"mixed fixture: excluded FP16 + packed NVFP4 + skipped scale should sum to 210 logical params")

	// Naive count for sanity: 10 (FP16) + 100 (storage) + 10 (scale) = 120 params,
	// which is what the legacy function returns.
	naive, err := ParseSafetensorsHeader(header[8:], "test")
	require.NoError(t, err)
	assert.Equal(t, int64(120), naive,
		"sanity: legacy naive count is 120 (sum of all shape products including the scale tensor)")
}

func TestParseSafetensorsHeaderQuantAware_ScaleTensorsSkippedRegardlessOfQuantConfig(t *testing.T) {
	// Even without a quant config, scale tensors should NOT be
	// skipped in the legacy path (backward compat: existing callers
	// expect the naive count for unquantized models, which historically
	// included anything in the safetensors header). With a quant
	// config, scales ARE skipped.
	header := safetensorsHeaderForTest(t, map[string]map[string]any{
		"model.layers.5.mlp.gate_proj.weight":       {"shape": []int64{100}, "dtype": "F16", "data_offsets": []int64{0, 200}},
		"model.layers.5.mlp.gate_proj.weight_scale": {"shape": []int64{10}, "dtype": "F32", "data_offsets": []int64{0, 40}},
	})

	naive, err := ParseSafetensorsHeader(header[8:], "test")
	require.NoError(t, err)
	assert.Equal(t, int64(110), naive, "naive includes the scale tensor (110 = 100 + 10)")

	withQuant, err := ParseSafetensorsHeaderQuantAware(header[8:], "test", &HFQuantConfig{
		Quantization: HFQuantConfigDetails{QuantAlgo: "NVFP4"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(100), withQuant, "with quant config the scale tensor is skipped")
}

func TestCountTensorParams_OverflowProtection(t *testing.T) {
	// Defensive: a corrupted shape with a tiny bad dim should produce
	// 0 (skipped) rather than a panic or wraparound.
	got := CountTensorParams("model.weight", []int64{0, 100}, "F16", nil)
	assert.Equal(t, int64(0), got, "zero dimension yields 0 (skipped)")

	got = CountTensorParams("model.weight", []int64{-1, 100}, "F16", nil)
	assert.Equal(t, int64(0), got, "negative dimension yields 0 (skipped)")

	got = CountTensorParams("model.weight", nil, "F16", nil)
	assert.Equal(t, int64(0), got, "nil shape yields 0")
}
