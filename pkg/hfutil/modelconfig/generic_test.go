package modelconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseModelConfigFromBytes(t *testing.T) {
	configPath := filepath.Join("testdata", "tiny-random-PhiModel", "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config fixture: %v", err)
	}

	model, err := ParseModelConfig(ModelConfigInput{
		Path: "config.json",
		Data: data,
	})
	if err != nil {
		t.Fatalf("ParseModelConfig returned error: %v", err)
	}

	if model.GetModelType() != "phi" {
		t.Fatalf("expected model type phi, got %q", model.GetModelType())
	}
	if model.GetArchitecture() != "PhiModel" {
		t.Fatalf("expected architecture PhiModel, got %q", model.GetArchitecture())
	}
}

func TestUnsupportedModelType(t *testing.T) {
	configPath := filepath.Join("testdata", "clip_vision_model.json")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skipf("Skipping test: file %s not found", configPath)
		return
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Expected no error when loading unsupported model type with generic fallback, but got: %v", err)
	}
	if config == nil {
		t.Fatalf("Expected a config but got nil")
	}

	modelType := config.GetModelType()
	if modelType != "clip_vision_model" {
		t.Errorf("Expected model type 'clip_vision_model' but got '%s'", modelType)
	}

	t.Logf("Generic config for unsupported model type: modelType=%s, params=%d, context=%d",
		config.GetModelType(), config.GetParameterCount(), config.GetContextLength())
}

func TestGenericConfigFallback(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Config with common fields but a model_type the package has no
	// per-model knowledge of — exercises the generic resolver end-to-end.
	configJSON := `{
		"model_type": "falcon",
		"architectures": ["FalconForCausalLM"],
		"hidden_size": 4544,
		"num_hidden_layers": 32,
		"num_attention_heads": 71,
		"intermediate_size": 18176,
		"max_position_embeddings": 2048,
		"vocab_size": 65024,
		"torch_dtype": "bfloat16",
		"transformers_version": "4.30.0"
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load generic config: %v", err)
	}

	if config.GetModelType() != "falcon" {
		t.Errorf("Expected model type 'falcon', got '%s'", config.GetModelType())
	}
	if config.GetArchitecture() != "FalconForCausalLM" {
		t.Errorf("Expected architecture 'FalconForCausalLM', got '%s'", config.GetArchitecture())
	}
	if config.GetContextLength() != 2048 {
		t.Errorf("Expected context length 2048, got %d", config.GetContextLength())
	}
	if config.GetTorchDtype() != "bfloat16" {
		t.Errorf("Expected torch_dtype 'bfloat16', got '%s'", config.GetTorchDtype())
	}
	if config.GetTransformerVersion() != "4.30.0" {
		t.Errorf("Expected transformers_version '4.30.0', got '%s'", config.GetTransformerVersion())
	}

	paramCount := config.GetParameterCount()
	if paramCount <= 0 {
		t.Errorf("Expected positive parameter count from estimation, got %d", paramCount)
	}
	t.Logf("Estimated parameter count for Falcon: %s (%d)", FormatParamCount(paramCount), paramCount)

	modelSize := config.GetModelSizeBytes()
	if modelSize <= 0 {
		t.Errorf("Expected positive model size, got %d", modelSize)
	}
	t.Logf("Estimated model size: %s", FormatSize(modelSize))
}

func TestGenericMultimodalFallback(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Nemotron-style config: top-level model_type with nested llm_config.
	configJSON := `{
		"architectures": ["Reasoning_V3"],
		"model_type": "Reasoning_V3",
		"torch_dtype": "bfloat16",
		"max_sequence_length": 131072,
		"llm_config": {
			"architectures": ["CausalLM"],
			"model_type": "multi_h",
			"transformers_version": "4.55.4",
			"torch_dtype": "bfloat16",
			"hidden_size": 2688,
			"num_hidden_layers": 52,
			"num_attention_heads": 32,
			"intermediate_size": 1856,
			"max_position_embeddings": 262144,
			"vocab_size": 131072,
			"n_routed_experts": 128,
			"n_shared_experts": 1,
			"moe_intermediate_size": 1856
		},
		"vision_config": {
			"architectures": ["RADIOModel"],
			"max_resolution": 2048
		},
		"sound_config": {
			"model_type": "parakeet",
			"hidden_size": 1024
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetModelType() != "Reasoning_V3" {
		t.Errorf("Expected model type 'Reasoning_V3', got '%s'", config.GetModelType())
	}
	if config.GetTransformerVersion() != "4.55.4" {
		t.Errorf("Expected transformers_version '4.55.4', got '%s'", config.GetTransformerVersion())
	}
	// Nested probe of llm_config fills MaxPositionEmbeddings (262144),
	// which beats the top-level max_sequence_length (131072) per the
	// cascade order in GetContextLength.
	if config.GetContextLength() != 262144 {
		t.Errorf("Expected context length 262144, got %d", config.GetContextLength())
	}
	if !config.HasVision() {
		t.Error("Expected HasVision() to return true")
	}
	if config.IsEmbedding() {
		t.Error("Expected IsEmbedding() to return false")
	}
	// Parameter count should use MoE estimation (~68.8B), not dense (~4.86B).
	paramCount := config.GetParameterCount()
	if paramCount < 60_000_000_000 || paramCount > 80_000_000_000 {
		t.Errorf("Expected MoE parameter count in range [60B, 80B], got %d (%s)",
			paramCount, FormatParamCount(paramCount))
	}
	t.Logf("Estimated parameter count: %s (%d)", FormatParamCount(paramCount), paramCount)
}

func TestGenericMultimodalFallback_TextConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	configJSON := `{
		"architectures": ["SomeVisionModel"],
		"model_type": "some_vision_model",
		"text_config": {
			"hidden_size": 4096,
			"num_hidden_layers": 32,
			"intermediate_size": 11008,
			"max_position_embeddings": 4096,
			"vocab_size": 32000,
			"transformers_version": "4.40.0",
			"torch_dtype": "float16"
		},
		"vision_config": {
			"hidden_size": 1024,
			"num_hidden_layers": 24
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetTransformerVersion() != "4.40.0" {
		t.Errorf("Expected transformers_version '4.40.0', got '%s'", config.GetTransformerVersion())
	}
	if config.GetContextLength() != 4096 {
		t.Errorf("Expected context length 4096, got %d", config.GetContextLength())
	}
	if config.GetTorchDtype() != "float16" {
		t.Errorf("Expected torch_dtype 'float16', got '%s'", config.GetTorchDtype())
	}
	if !config.HasVision() {
		t.Error("Expected HasVision() to return true")
	}
	if config.GetParameterCount() <= 0 {
		t.Error("Expected positive parameter count")
	}
}

func TestGenericMultimodalFallback_LanguageConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	configJSON := `{
		"architectures": ["SomeVLModel"],
		"model_type": "some_vl_model",
		"language_config": {
			"hidden_size": 2048,
			"num_hidden_layers": 24,
			"intermediate_size": 5504,
			"max_position_embeddings": 4096,
			"vocab_size": 100000,
			"transformers_version": "4.38.0"
		},
		"vision_config": {
			"hidden_size": 1152
		}
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetTransformerVersion() != "4.38.0" {
		t.Errorf("Expected transformers_version '4.38.0', got '%s'", config.GetTransformerVersion())
	}
	if config.GetContextLength() != 4096 {
		t.Errorf("Expected context length 4096, got %d", config.GetContextLength())
	}
	if !config.HasVision() {
		t.Error("Expected HasVision() to return true")
	}
}

func TestGenericMoEFallback(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Flat MoE config (Mixtral style) with the num_local_experts variant.
	configJSON := `{
		"architectures": ["SomeMoEModel"],
		"model_type": "some_moe",
		"hidden_size": 4096,
		"num_hidden_layers": 32,
		"intermediate_size": 14336,
		"max_position_embeddings": 32768,
		"vocab_size": 32000,
		"torch_dtype": "bfloat16",
		"transformers_version": "4.36.0",
		"num_local_experts": 8,
		"moe_intermediate_size": 14336
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Dense estimate would be ~12 * 4096^2 * 32 + embeddings ≈ 6.6B;
	// MoE estimate with 8 experts should be much higher.
	paramCount := config.GetParameterCount()
	if paramCount < 30_000_000_000 {
		t.Errorf("Expected MoE parameter count > 30B, got %d (%s)",
			paramCount, FormatParamCount(paramCount))
	}
	t.Logf("Estimated MoE parameter count: %s (%d)", FormatParamCount(paramCount), paramCount)
}

func TestGenericContextLengthMaxSequenceLength(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// max_sequence_length present but no max_position_embeddings.
	configJSON := `{
		"architectures": ["SomeModel"],
		"model_type": "some_model",
		"max_sequence_length": 131072,
		"hidden_size": 2048,
		"num_hidden_layers": 24,
		"vocab_size": 32000,
		"torch_dtype": "bfloat16",
		"transformers_version": "4.40.0"
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := LoadModelConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if config.GetContextLength() != 131072 {
		t.Errorf("Expected context length 131072 from max_sequence_length, got %d", config.GetContextLength())
	}
	if config.HasVision() {
		t.Error("Expected HasVision() to return false for non-multimodal config")
	}
}
