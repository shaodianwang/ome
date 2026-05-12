package modelconfig

import (
	"encoding/json"
	"fmt"
)

// HFQuantConfig is the schema for hf_quant_config.json, a sibling file
// to config.json shipped by NVIDIA ModelOpt and the TensorRT-LLM
// quantization workflow. It carries quantization metadata that the HF
// native pipelines surface inside config.json under the
// "quantization_config" key — but ModelOpt-style models don't always
// modify config.json itself, so the data lives in this separate file
// instead. Both conventions exist in the wild; OME readers should
// honor either.
//
// Real-world example (Llama4x NVFP4 PTQ):
//
//	{
//	  "quant_method": "modelopt",
//	  "quantization": {
//	    "quant_algo": "NVFP4",
//	    "group_size": 16,
//	    "kv_cache_quant_algo": "FP8",
//	    "exclude_modules": ["layers.0.", "layers.61.", "self_attn", "lm_head"]
//	  }
//	}
type HFQuantConfig struct {
	// QuantMethod is the producing tool ("modelopt" for the NVIDIA
	// flow). Other tools that emit this file may use other values.
	QuantMethod string `json:"quant_method"`
	// Quantization carries the actual scheme parameters.
	Quantization HFQuantConfigDetails `json:"quantization"`
}

// HFQuantConfigDetails carries the per-model quantization parameters
// inside hf_quant_config.json. Field names mirror ModelOpt's wire
// format. Many fields are optional and provider-dependent; OME only
// reads QuantAlgo today.
type HFQuantConfigDetails struct {
	// QuantAlgo is the weight quantization algorithm — typical values
	// include "FP8", "NVFP4", "MXFP4", "INT4_AWQ", "INT8_SQ", "W4A8_AWQ".
	// Drives BaseModel.spec.quantization via quantAlgoToOMEEnum.
	QuantAlgo string `json:"quant_algo"`
	// GroupSize is the per-group scale group size for grouped quant
	// formats (NVFP4 = 16 typical). Informational only today.
	GroupSize int `json:"group_size,omitempty"`
	// KVCacheQuantAlgo is the KV-cache quantization scheme (often "FP8"
	// when weights are NVFP4). Informational only today; future param-
	// count work may want it.
	KVCacheQuantAlgo string `json:"kv_cache_quant_algo,omitempty"`
	// ExcludeModules names tensor-name patterns kept at higher
	// precision (e.g., "self_attn", "lm_head", "layers.0.", etc.).
	// Required for accurate parameter counting on packed-quant models;
	// see Gap 2 in the metadata-parser TODO list.
	ExcludeModules []string `json:"exclude_modules,omitempty"`
}

// HFQuantConfigFileName is the canonical filename ModelOpt and similar
// tools use. Looked up by basename so subdirectory placement is
// immaterial.
const HFQuantConfigFileName = "hf_quant_config.json"

// ParseHFQuantConfig deserializes the JSON bytes of an
// hf_quant_config.json file. Returns nil + nil on empty input so
// callers can use it for the "file is optional" pattern.
func ParseHFQuantConfig(data []byte) (*HFQuantConfig, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var cfg HFQuantConfig
	if err := json.Unmarshal(SanitizeJSONBytes(data), &cfg); err != nil {
		return nil, fmt.Errorf("parse hf_quant_config.json: %w", err)
	}
	return &cfg, nil
}
