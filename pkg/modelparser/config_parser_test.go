package modelparser

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/client/clientset/versioned"
	"github.com/ome-projects/ome/pkg/modelconfig"
)

// mockHuggingFaceModel implements the modelconfig.HuggingFaceModel interface for testing
type mockHuggingFaceModel struct {
	modelType          string
	architecture       string
	parameterCount     int64
	contextLength      int
	transformerVersion string
	quantizationType   string
	torchDtype         string
	modelSizeBytes     int64
	hasVision          bool
	isEmbedding        bool
}

type mockDiffusionModel struct {
	mockHuggingFaceModel
	diffusionModel *modelconfig.DiffusionPipelineSpec
}

func (m *mockDiffusionModel) GetDiffusionModel() *modelconfig.DiffusionPipelineSpec {
	return m.diffusionModel
}

// Implement all methods of the HuggingFaceModel interface
func (m *mockHuggingFaceModel) GetModelType() string                         { return m.modelType }
func (m *mockHuggingFaceModel) GetArchitecture() string                      { return m.architecture }
func (m *mockHuggingFaceModel) GetParameterCount() int64                     { return m.parameterCount }
func (m *mockHuggingFaceModel) GetContextLength() int                        { return m.contextLength }
func (m *mockHuggingFaceModel) GetTransformerVersion() string                { return m.transformerVersion }
func (m *mockHuggingFaceModel) GetQuantizationType() string                  { return m.quantizationType }
func (m *mockHuggingFaceModel) GetTorchDtype() string                        { return m.torchDtype }
func (m *mockHuggingFaceModel) GetModelSizeBytes() int64                     { return m.modelSizeBytes }
func (m *mockHuggingFaceModel) HasVision() bool                              { return m.hasVision }
func (m *mockHuggingFaceModel) IsEmbedding() bool                            { return m.isEmbedding }
func (m *mockHuggingFaceModel) GetHFQuantConfig() *modelconfig.HFQuantConfig { return nil }
func (m *mockHuggingFaceModel) GetCapabilities() []modelconfig.Capability {
	// Synthesize capabilities approximating what modelconfig's
	// classifier would return for this mock's flags. Covers the
	// simple cases the existing mock-driven tests exercise; richer
	// scenarios should route through the real classifier via
	// modelconfig.LoadModelConfig + a fixture.
	if m.isEmbedding {
		return []modelconfig.Capability{modelconfig.CapabilityEmbedding}
	}
	if strings.Contains(strings.ToLower(m.modelType), "bert") ||
		strings.Contains(m.architecture, "Bert") {
		return []modelconfig.Capability{modelconfig.CapabilityEmbedding}
	}
	if m.hasVision {
		return []modelconfig.Capability{modelconfig.CapabilityImageTextToText}
	}
	return []modelconfig.Capability{modelconfig.CapabilityTextToText}
}

// Define a helper function to create a mock model with default values
func createDefaultMockModel() *mockHuggingFaceModel {
	return &mockHuggingFaceModel{
		modelType:          "llama",
		architecture:       "LlamaForCausalLM",
		parameterCount:     7000000000, // 7B
		contextLength:      4096,
		transformerVersion: "4.33.2",
		quantizationType:   "",
		torchDtype:         "float16",
		modelSizeBytes:     14000000000, // 14GB
		hasVision:          false,
		isEmbedding:        false,
	}
}

// TestExtractModelMetadataFromHF tests the extractModelMetadataFromHF function
func TestExtractModelMetadataFromHF(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer func() { _ = logger.Sync() }()

	// Create a ModelConfigParser with the logger
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Define test cases
	testCases := []struct {
		name                 string
		mockModel            *mockHuggingFaceModel
		expectedMetadata     func(metadata ModelMetadata) bool
		expectedCapability   string
		expectedQuantization v1beta1.ModelQuantization
	}{
		{
			name:      "Standard LLM Model",
			mockModel: createDefaultMockModel(),
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "llama" &&
					metadata.ModelArchitecture == "LlamaForCausalLM" &&
					metadata.ModelParameterSize == "7B" &&
					metadata.MaxTokens == 4096 &&
					metadata.ModelFramework.Name == "transformers" &&
					*metadata.ModelFramework.Version == "4.33.2" &&
					metadata.ModelFormat.Name == "safetensors" &&
					*metadata.ModelFormat.Version == "1.0.0"
			},
			expectedCapability:   string(v1beta1.ModelCapabilityTextToText),
			expectedQuantization: "", // No quantization
		},
		{
			name: "INT4 Quantized Model",
			mockModel: &mockHuggingFaceModel{
				modelType:          "mixtral",
				architecture:       "MixtralForCausalLM",
				parameterCount:     8000000000, // 8B
				contextLength:      32768,
				transformerVersion: "4.35.0",
				quantizationType:   "gptq_int4",
				torchDtype:         "int4",
				modelSizeBytes:     4000000000, // 4GB (reduced due to quantization)
				hasVision:          false,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "mixtral" &&
					metadata.ModelArchitecture == "MixtralForCausalLM" &&
					metadata.ModelParameterSize == "8B" &&
					metadata.MaxTokens == 32768
			},
			expectedCapability:   string(v1beta1.ModelCapabilityTextToText),
			expectedQuantization: v1beta1.ModelQuantizationINT4,
		},
		{
			name: "FP8 Quantized Model",
			mockModel: &mockHuggingFaceModel{
				modelType:          "phi",
				architecture:       "PhiForCausalLM",
				parameterCount:     2800000000, // 2.8B
				contextLength:      2048,
				transformerVersion: "4.34.1",
				quantizationType:   "fp8-e4m3",
				torchDtype:         "float8",
				modelSizeBytes:     6000000000,
				hasVision:          false,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "phi" &&
					metadata.ModelArchitecture == "PhiForCausalLM" &&
					metadata.ModelParameterSize == "2.8B"
			},
			expectedCapability:   string(v1beta1.ModelCapabilityTextToText),
			expectedQuantization: v1beta1.ModelQuantizationFP8,
		},
		{
			name: "Vision Model",
			mockModel: &mockHuggingFaceModel{
				modelType:          "clip",
				architecture:       "CLIPModel",
				parameterCount:     400000000, // 400M
				contextLength:      77,        // CLIP typically uses smaller context
				transformerVersion: "4.32.0",
				quantizationType:   "",
				torchDtype:         "float16",
				modelSizeBytes:     1500000000,
				hasVision:          true,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "clip" &&
					metadata.ModelArchitecture == "CLIPModel" &&
					metadata.ModelParameterSize == "400M"
			},
			expectedCapability:   string(v1beta1.ModelCapabilityImageTextToText),
			expectedQuantization: "",
		},
		{
			name: "Missing Transformer Version",
			mockModel: &mockHuggingFaceModel{
				modelType:          "bert",
				architecture:       "BertModel",
				parameterCount:     110000000, // 110M
				contextLength:      512,
				transformerVersion: "", // Missing transformer version
				quantizationType:   "",
				torchDtype:         "float32",
				modelSizeBytes:     440000000,
				hasVision:          false,
			},
			expectedMetadata: func(metadata ModelMetadata) bool {
				return metadata.ModelType == "bert" &&
					metadata.ModelArchitecture == "BertModel" &&
					metadata.ModelFramework.Name == "transformers" &&
					metadata.ModelFramework.Version == nil // Should be nil when missing
			},
			expectedCapability:   string(v1beta1.ModelCapabilityEmbedding),
			expectedQuantization: "",
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function under test
			metadata := parser.extractModelMetadataFromHF(tc.mockModel)
			// extractModelMetadataFromHF no longer writes Quantization
			// (see config_parser.go's "intentionally NOT set here"
			// comment) — the parser entry points apply it via
			// applyQuantizationFromConfig as a single source of truth.
			// Mimic that here so the existing assertion is meaningful.
			applyQuantizationFromConfig(&metadata, syntheticQuantConfig(tc.mockModel.quantizationType), parser.logger)

			// Verify the metadata using the custom validation function
			if !tc.expectedMetadata(metadata) {
				t.Errorf("Metadata does not match expected values for test case %s", tc.name)
			}

			// Check specific expected capabilities
			hasCapability := false
			for _, cap := range metadata.ModelCapabilities {
				if cap == tc.expectedCapability {
					hasCapability = true
					break
				}
			}
			assert.True(t, hasCapability, "Expected capability %s not found", tc.expectedCapability)

			// Check quantization
			assert.Equal(t, tc.expectedQuantization, metadata.Quantization,
				"Quantization mismatch for test case %s", tc.name)

			// Check that ModelConfiguration JSON is valid and contains the expected fields
			if len(metadata.ModelConfiguration) > 0 {
				var configData map[string]interface{}
				err := json.Unmarshal(metadata.ModelConfiguration, &configData)
				assert.NoError(t, err, "Failed to unmarshal ModelConfiguration JSON")

				assert.Equal(t, tc.mockModel.GetModelType(), configData["model_type"],
					"model_type mismatch in ModelConfiguration")
				assert.Equal(t, tc.mockModel.GetArchitecture(), configData["architecture"],
					"architecture mismatch in ModelConfiguration")
				assert.Equal(t, tc.mockModel.GetContextLength(), int(configData["context_length"].(float64)),
					"context_length mismatch in ModelConfiguration")
				assert.Equal(t, tc.mockModel.HasVision(), configData["has_vision"],
					"has_vision mismatch in ModelConfiguration")
			}
		})
	}
}

// Helper function to find test for the direct internal functions
// TestNewModelConfigParser tests the constructor
func TestNewModelConfigParser(t *testing.T) {
	// Create dependencies
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Test with nil client
	parser := NewModelConfigParser(nil, sugar)
	assert.NotNil(t, parser)
	assert.Nil(t, parser.omeClient)
	assert.Equal(t, sugar, parser.logger)

	// Test with a client
	client := &versioned.Clientset{}
	parser = NewModelConfigParser(client, sugar)
	assert.NotNil(t, parser)
	assert.Equal(t, client, parser.omeClient)
}

// TestFindConfigFile tests the findConfigFile method
func TestFindConfigFile(t *testing.T) {
	// Create a temp directory structure for testing
	tempDir, err := os.MkdirTemp("", "config-test")
	assert.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create parser
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Case 1: Config in root directory
	configPath := filepath.Join(tempDir, DefaultConfigFileName)
	_, err = os.Create(configPath)
	assert.NoError(t, err)

	resultPath, err := parser.findConfigFile(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, configPath, resultPath)

	// Case 2: Config in model subdirectory
	_ = os.Remove(configPath) // Remove from root
	modelDir := filepath.Join(tempDir, "model")
	err = os.Mkdir(modelDir, 0755)
	assert.NoError(t, err)
	configPath = filepath.Join(modelDir, DefaultConfigFileName)
	_, err = os.Create(configPath)
	assert.NoError(t, err)

	resultPath, err = parser.findConfigFile(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, configPath, resultPath)

	// Case 3: No config file
	_ = os.RemoveAll(tempDir)
	err = os.Mkdir(tempDir, 0755)
	assert.NoError(t, err)

	_, err = parser.findConfigFile(tempDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.json not found in")
}

// TestShouldSkipConfigParsing tests the shouldSkipConfigParsing method
func TestShouldSkipConfigParsing(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create parser
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Case 1: BaseModel with skip annotation
	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-base-model",
			Annotations: map[string]string{
				ConfigParsingAnnotation: "true",
			},
		},
	}

	result := parser.shouldSkipConfigParsing(baseModel, nil)
	assert.True(t, result)

	// Case 2: BaseModel without skip annotation
	baseModel.Annotations = map[string]string{}
	result = parser.shouldSkipConfigParsing(baseModel, nil)
	assert.False(t, result)

	// Case 3: ClusterBaseModel with skip annotation
	clusterBaseModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster-base-model",
			Annotations: map[string]string{
				ConfigParsingAnnotation: "true",
			},
		},
	}

	result = parser.shouldSkipConfigParsing(nil, clusterBaseModel)
	assert.True(t, result)

	// Case 4: ClusterBaseModel without skip annotation
	clusterBaseModel.Annotations = map[string]string{}
	result = parser.shouldSkipConfigParsing(nil, clusterBaseModel)
	assert.False(t, result)

	// Case 5: Case insensitive "TRUE" value
	baseModel.Annotations = map[string]string{
		ConfigParsingAnnotation: "TRUE",
	}
	result = parser.shouldSkipConfigParsing(baseModel, nil)
	assert.True(t, result)
}

// TestUpdateModelSpec tests the updateModelSpec method which only updates nil fields
func TestUpdateModelSpec(t *testing.T) {
	// Create a simple logger
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	// Create basic metadata with just string fields
	metadata := ModelMetadata{
		ModelType:          "llama",
		ModelArchitecture:  "LlamaForCausalLM",
		ModelParameterSize: "7B",
		MaxTokens:          4096,
		ModelCapabilities:  []string{string(v1beta1.ModelCapabilityTextToText)},
	}

	// Basic test: Empty spec gets fields set
	emptySpec := &v1beta1.BaseModelSpec{}
	parser.updateModelSpec(emptySpec, metadata)

	// Verify only string pointer fields that are guaranteed to be set
	assert.NotNil(t, emptySpec.ModelType)
	assert.Equal(t, "llama", *emptySpec.ModelType)

	assert.NotNil(t, emptySpec.ModelArchitecture)
	assert.Equal(t, "LlamaForCausalLM", *emptySpec.ModelArchitecture)

	assert.NotNil(t, emptySpec.ModelParameterSize)
	assert.Equal(t, "7B", *emptySpec.ModelParameterSize)

	assert.NotNil(t, emptySpec.MaxTokens)
	assert.Equal(t, int32(4096), *emptySpec.MaxTokens)

	// Verify slice
	assert.Equal(t, []string{string(v1beta1.ModelCapabilityTextToText)}, emptySpec.ModelCapabilities)

	// Test that existing values aren't overwritten
	existingType := "something-else"
	existingArch := "different-arch"
	existingMaxTokens := int32(2048)
	existingSpec := &v1beta1.BaseModelSpec{
		ModelType:         &existingType,
		ModelArchitecture: &existingArch,
		MaxTokens:         &existingMaxTokens,
		ModelCapabilities: []string{"EXISTING_CAP"},
	}

	parser.updateModelSpec(existingSpec, metadata)

	// Existing values shouldn't be overwritten
	assert.Equal(t, "something-else", *existingSpec.ModelType)
	assert.Equal(t, "different-arch", *existingSpec.ModelArchitecture)
	assert.Equal(t, int32(2048), *existingSpec.MaxTokens)
	assert.Equal(t, []string{"EXISTING_CAP"}, existingSpec.ModelCapabilities)

	// But nil values should be populated
	assert.NotNil(t, existingSpec.ModelParameterSize)
	assert.Equal(t, "7B", *existingSpec.ModelParameterSize)
}

func TestParseDiffusionPipelineSpec(t *testing.T) {
	data := []byte(`{
  "_class_name": "StableDiffusionPipeline",
  "_diffusers_version": "0.24.0",
  "scheduler": ["diffusers", "EulerDiscreteScheduler"],
  "text_encoder": ["transformers", "CLIPTextModel"],
  "tokenizer": ["transformers", "CLIPTokenizer"],
  "unet": ["diffusers", "UNet2DConditionModel"],
  "vae": ["diffusers", "AutoencoderKL"],
  "safety_checker": ["diffusers", "StableDiffusionSafetyChecker"]
}`)

	parsed, err := modelconfig.ParseDiffusionPipelineSpec(data)
	assert.NoError(t, err)
	pipeline := convertDiffusionPipelineSpec(parsed)
	if assert.NotNil(t, pipeline) {
		if assert.NotNil(t, pipeline.ClassName) {
			assert.Equal(t, "StableDiffusionPipeline", *pipeline.ClassName)
		}
		if assert.NotNil(t, pipeline.Scheduler) {
			assert.Equal(t, "diffusers", pipeline.Scheduler.Library)
			assert.Equal(t, "EulerDiscreteScheduler", pipeline.Scheduler.Type)
		}
		if assert.NotNil(t, pipeline.TextEncoder) {
			assert.Equal(t, "transformers", pipeline.TextEncoder.Library)
			assert.Equal(t, "CLIPTextModel", pipeline.TextEncoder.Type)
		}
		if assert.NotNil(t, pipeline.Tokenizer) {
			assert.Equal(t, "transformers", pipeline.Tokenizer.Library)
			assert.Equal(t, "CLIPTokenizer", pipeline.Tokenizer.Type)
		}
		if assert.NotNil(t, pipeline.Transformer) {
			assert.Equal(t, "diffusers", pipeline.Transformer.Library)
			assert.Equal(t, "UNet2DConditionModel", pipeline.Transformer.Type)
		}
		if assert.NotNil(t, pipeline.VAE) {
			assert.Equal(t, "diffusers", pipeline.VAE.Library)
			assert.Equal(t, "AutoencoderKL", pipeline.VAE.Type)
		}
		if assert.NotNil(t, pipeline.AdditionalComponents) {
			component, ok := pipeline.AdditionalComponents["safety_checker"]
			assert.True(t, ok)
			assert.Equal(t, "diffusers", component.Library)
			assert.Equal(t, "StableDiffusionSafetyChecker", component.Type)
		}
	}
}

// TestParseModelConfig tests part of the ParseModelConfig method logic,
// focusing on the parts that don't rely on the modelconfig.LoadModelConfig function
func TestParseModelConfig(t *testing.T) {
	// Create a temp directory structure for testing
	tempDir, err := os.MkdirTemp("", "model-config-test")
	assert.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	// Create parser
	parser := &ModelConfigParser{
		logger: sugar,
	}

	// Create config.json
	configDir := filepath.Join(tempDir, "model")
	err = os.MkdirAll(configDir, 0755)
	assert.NoError(t, err)
	configPath := filepath.Join(configDir, DefaultConfigFileName)
	configFile, err := os.Create(configPath)
	assert.NoError(t, err)
	_, _ = configFile.WriteString("{\"model_type\": \"llama\", \"architectures\": [\"LlamaForCausalLM\"]}\n")
	_ = configFile.Close()

	// Case 1: Non-existent directory
	metadata, err := parser.ParseModelConfig("/non-existent", nil, nil)
	assert.Nil(t, err)
	assert.Nil(t, metadata)

	// Case 2: Skip config parsing due to annotation
	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				ConfigParsingAnnotation: "true",
			},
		},
	}
	metadata, err = parser.ParseModelConfig(tempDir, baseModel, nil)
	assert.Nil(t, err)
	assert.Nil(t, metadata)

	// Note: We can't fully test cases 3 and 4 without mocking the private loadModelConfig field,
	// but we can at least verify the shouldSkipConfigParsing functionality
}

func TestParseModelConfig_PrefersModelIndex(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "model-index-test")
	assert.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	modelIndex := []byte(`{
  "_class_name": "StableDiffusionPipeline",
  "scheduler": ["diffusers", "EulerDiscreteScheduler"]
}`)
	modelIndexPath := filepath.Join(tempDir, DefaultModelIndexFileName)
	err = os.WriteFile(modelIndexPath, modelIndex, 0644)
	assert.NoError(t, err)

	vaeDir := filepath.Join(tempDir, "vae")
	err = os.MkdirAll(vaeDir, 0755)
	assert.NoError(t, err)
	configPath := filepath.Join(vaeDir, DefaultConfigFileName)
	err = os.WriteFile(configPath, []byte(`{"model_type":"vae","architectures":["AutoencoderKL"]}`), 0644)
	assert.NoError(t, err)

	loadCalled := false
	parser := &ModelConfigParser{
		logger: sugar,
		loadModelConfig: func(configPath string) (modelconfig.HuggingFaceModel, error) {
			loadCalled = true
			return &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "StableDiffusionPipeline",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{
					ClassName: "StableDiffusionPipeline",
					Scheduler: &modelconfig.DiffusionComponentSpec{Library: "diffusers", Type: "EulerDiscreteScheduler"},
				},
			}, nil
		},
	}

	metadata, parseErr := parser.ParseModelConfig(tempDir, nil, nil)
	assert.NoError(t, parseErr)
	if assert.NotNil(t, metadata) {
		assert.NotNil(t, metadata.DiffusionPipeline)
		if metadata.DiffusionPipeline != nil && metadata.DiffusionPipeline.ClassName != nil {
			assert.Equal(t, "StableDiffusionPipeline", *metadata.DiffusionPipeline.ClassName)
		}
		if assert.NotNil(t, metadata.ModelFramework) {
			assert.Equal(t, "diffusers", metadata.ModelFramework.Name)
		}
		assert.Equal(t, "diffusers", metadata.ModelFormat.Name)
	}
	assert.True(t, loadCalled, "loadModelConfig should be called when model_index.json is present")
}

func TestParseModelConfigFromFiles(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	modelDir := filepath.Join("..", "modelconfig", "testdata", "tiny-random-PhiModel")
	files := []ModelConfigFileInput{
		{Path: DefaultConfigFileName, Data: mustReadTestFile(t, filepath.Join(modelDir, DefaultConfigFileName))},
		{Path: "model-1-of-2.safetensors", Data: mustReadSafetensorsHeader(t, filepath.Join(modelDir, "model-1-of-2.safetensors"))},
		{Path: "model-2-of-2.safetensors", Data: mustReadSafetensorsHeader(t, filepath.Join(modelDir, "model-2-of-2.safetensors"))},
	}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), files, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, metadata) {
		assert.Equal(t, "phi", metadata.ModelType)
		assert.Equal(t, "PhiModel", metadata.ModelArchitecture)
		assert.Equal(t, modelconfig.FormatParamCount(92564), metadata.ModelParameterSize)
		assert.Equal(t, int32(512), metadata.MaxTokens)
		assert.NotNil(t, metadata.ModelFramework)
		assert.Equal(t, "transformers", metadata.ModelFramework.Name)
		assert.NotNil(t, metadata.ModelFramework.Version)
		assert.Equal(t, "4.40.0.dev0", *metadata.ModelFramework.Version)
		assert.Equal(t, "safetensors", metadata.ModelFormat.Name)
	}
}

// safetensorsHeaderBytesForTest builds an in-memory safetensors file
// (8-byte length prefix + JSON header). Used by parser-level tests
// that need to exercise the safetensors counting path with synthesized
// tensor metadata rather than shipping multi-GB binary fixtures.
func safetensorsHeaderBytesForTest(t *testing.T, tensors map[string]map[string]any) []byte {
	t.Helper()
	jsonBytes, err := json.Marshal(tensors)
	require.NoError(t, err)
	out := make([]byte, 8+len(jsonBytes))
	binary.LittleEndian.PutUint64(out[:8], uint64(len(jsonBytes)))
	copy(out[8:], jsonBytes)
	return out
}

func TestParseModelConfigFromFiles_QuantAwareSafetensorsCount(t *testing.T) {
	// End-to-end through ParseModelConfigFromFiles: config.json (no
	// quantization_config) + hf_quant_config.json (NVFP4) + a synthetic
	// safetensors file with one packed U8 tensor + one excluded F16
	// tensor + one scale. Verify the resulting metadata.ModelParameterSize
	// reflects the quant-aware count, not the legacy naive sum.
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	// Tensors:
	//   layers.0.self_attn.k_proj.weight: F16 [4, 8] = 32 params, EXCLUDED → naive
	//   layers.30.mlp.gate_proj.weight:   U8  [8, 8] = 64 storage, NVFP4 → ×2 = 128 logical
	//   layers.30.mlp.gate_proj.weight_scale: F32 [8] = scale, SKIPPED
	// Quant-aware total: 32 + 128 = 160. Legacy naive: 32 + 64 + 8 = 104.
	safetensorsBytes := safetensorsHeaderBytesForTest(t, map[string]map[string]any{
		"model.layers.0.self_attn.k_proj.weight": {
			"shape": []int64{4, 8}, "dtype": "F16", "data_offsets": []int64{0, 64},
		},
		"model.layers.30.mlp.gate_proj.weight": {
			"shape": []int64{8, 8}, "dtype": "U8", "data_offsets": []int64{0, 64},
		},
		"model.layers.30.mlp.gate_proj.weight_scale": {
			"shape": []int64{8}, "dtype": "F32", "data_offsets": []int64{0, 32},
		},
	})

	files := []ModelConfigFileInput{
		{Path: DefaultConfigFileName, Data: []byte(`{"model_type":"llama4x_text","architectures":["Llama4xForCausalLM"]}`)},
		{Path: "model.safetensors", Data: safetensorsBytes},
		{Path: modelconfig.HFQuantConfigFileName, Data: []byte(`{
			"quant_method": "modelopt",
			"quantization": {
				"quant_algo": "NVFP4",
				"exclude_modules": ["self_attn"]
			}
		}`)},
	}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), files, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	// Quant-aware: 32 (excluded F16) + 128 (NVFP4 ×2) = 160.
	// FormatParamCount renders 160 as "160" — no SI suffix because <1000.
	assert.Equal(t, modelconfig.FormatParamCount(160), metadata.ModelParameterSize,
		"hf_quant_config.json + NVFP4 + excluded module + scale tensor must yield quant-aware count, not naive sum")
	assert.Equal(t, v1beta1.ModelQuantizationNVFP4, metadata.Quantization,
		"sanity: spec.Quantization should also be set from the same hf_quant_config.json")
}

func TestParseModelConfigDir_HFQuantConfigFallbackFromDisk(t *testing.T) {
	// PVC agent path: model dir on disk has config.json (no
	// quantization_config) + hf_quant_config.json (NVFP4).
	// parseModelConfigDir is the function the agent's
	// MetadataExtractor calls; this proves the fallback wiring works
	// when the file lives on a real filesystem (not just in-memory
	// inputs through ParseModelConfigFromFiles).
	tempDir, err := os.MkdirTemp("", "hf-quant-fallback")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	require.NoError(t, os.WriteFile(
		filepath.Join(tempDir, DefaultConfigFileName),
		[]byte(`{"model_type":"llama4x_text","architectures":["Llama4xForCausalLM"],"max_position_embeddings":32768}`),
		0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(tempDir, modelconfig.HFQuantConfigFileName),
		[]byte(`{"quant_method":"modelopt","quantization":{"quant_algo":"NVFP4","group_size":16}}`),
		0o644,
	))

	logger, _ := zap.NewDevelopment()
	parser := &ModelConfigParser{
		logger:          logger.Sugar(),
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	metadata, err := parser.ParseModelConfig(tempDir, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, v1beta1.ModelQuantizationNVFP4, metadata.Quantization,
		"hf_quant_config.json on disk should populate metadata.Quantization via parseModelConfigDir")
}

func TestParseModelConfigFromFiles_HFQuantConfigFallback(t *testing.T) {
	// config.json without a quantization_config block + a sibling
	// hf_quant_config.json with NVFP4 → metadata.Quantization should
	// pick up NVFP4 from the hf_quant_config fallback path.
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	configJSON := []byte(`{"model_type":"llama4x_text","architectures":["Llama4xForCausalLM"],"max_position_embeddings":32768}`)
	hfQuantJSON := []byte(`{"quant_method":"modelopt","quantization":{"quant_algo":"NVFP4","group_size":16,"kv_cache_quant_algo":"FP8"}}`)

	files := []ModelConfigFileInput{
		{Path: DefaultConfigFileName, Data: configJSON},
		{Path: modelconfig.HFQuantConfigFileName, Data: hfQuantJSON},
	}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), files, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, metadata) {
		assert.Equal(t, v1beta1.ModelQuantizationNVFP4, metadata.Quantization,
			"hf_quant_config.json's NVFP4 should populate metadata.Quantization when config.json has no quantization_config")
	}
}

func TestParseModelConfigFromFiles_StandaloneHFQuantConfigWinsOverEmbedded(t *testing.T) {
	// Standalone hf_quant_config.json + embedded quantization_config:
	// standalone is the higher-priority signal (richer schema —
	// group_size, kv_cache_quant_algo, etc.) and unifying spec.Quantization
	// with the safetensors counting source prevents the NVFP4-count +
	// FP8-spec asymmetry the unification fix closes.
	//
	// Earlier this test asserted the OPPOSITE invariant (embedded wins)
	// — that was a documented bug, not a feature. Renamed for clarity.
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	configJSON := []byte(`{
		"model_type":"llama",
		"architectures":["LlamaForCausalLM"],
		"max_position_embeddings":4096,
		"quantization_config":{"quant_method":"fp8"}
	}`)
	hfQuantJSON := []byte(`{"quant_method":"modelopt","quantization":{"quant_algo":"NVFP4"}}`)

	files := []ModelConfigFileInput{
		{Path: DefaultConfigFileName, Data: configJSON},
		{Path: modelconfig.HFQuantConfigFileName, Data: hfQuantJSON},
	}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), files, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, metadata) {
		assert.Equal(t, v1beta1.ModelQuantizationNVFP4, metadata.Quantization,
			"standalone hf_quant_config.json must win — same source as parameter counting, no spec/count divergence")
	}
}

func TestParseModelConfigFromFiles_EmbeddedQuantConfigDrivesParameterCount(t *testing.T) {
	// HF-native path: config.json has quantization_config: {quant_method: mxfp4,
	// modules_to_not_convert: ["lm_head"]}, NO standalone hf_quant_config.json.
	// Verifies the embedded fallback synthesizes an HFQuantConfig and the
	// safetensors counting picks up the 2x MXFP4 multiplier on the U8 tensor.
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	// Tensors:
	//   lm_head.weight:                  F16 [4, 8] = 32 params, EXCLUDED → naive
	//   model.layers.5.mlp.gate_up.weight: U8 [8, 8] = 64 storage, MXFP4 → ×2 = 128 logical
	// Quant-aware total: 32 + 128 = 160. Naive baseline: 32 + 64 = 96.
	safetensorsBytes := safetensorsHeaderBytesForTest(t, map[string]map[string]any{
		"lm_head.weight": {
			"shape": []int64{4, 8}, "dtype": "F16", "data_offsets": []int64{0, 64},
		},
		"model.layers.5.mlp.gate_up.weight": {
			"shape": []int64{8, 8}, "dtype": "U8", "data_offsets": []int64{0, 64},
		},
	})

	files := []ModelConfigFileInput{
		{Path: DefaultConfigFileName, Data: []byte(`{
			"model_type": "gpt_oss",
			"architectures": ["GptOssForCausalLM"],
			"quantization_config": {
				"quant_method": "mxfp4",
				"modules_to_not_convert": ["lm_head"]
			}
		}`)},
		{Path: "model.safetensors", Data: safetensorsBytes},
		// NO hf_quant_config.json — proves the embedded fallback path
		// closes the gap from PR #75.
	}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), files, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	assert.Equal(t, modelconfig.FormatParamCount(160), metadata.ModelParameterSize,
		"embedded quantization_config (mxfp4) must trigger quant-aware counting just like standalone hf_quant_config.json")
	assert.Equal(t, v1beta1.ModelQuantizationMXFP4, metadata.Quantization,
		"sanity: spec.Quantization should also be set from the embedded block")
}

func TestParseModelConfigFromFiles_HFQuantConfigUnknownAlgoLeavesEmpty(t *testing.T) {
	// hf_quant_config.json with a quant_algo OME doesn't recognize
	// (e.g., a future format) → metadata.Quantization stays empty.
	// The parser should NOT write a placeholder enum value for an
	// unknown algorithm — operators should see "unset" and fix the
	// CR by hand, not see a misleading value that suggests OME knows
	// what's going on.
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{
		logger:          logger,
		loadModelConfig: modelconfig.LoadModelConfig,
	}

	configJSON := []byte(`{"model_type":"llama","architectures":["LlamaForCausalLM"]}`)
	hfQuantJSON := []byte(`{"quant_method":"modelopt","quantization":{"quant_algo":"some-future-format-ome-doesnt-know"}}`)

	files := []ModelConfigFileInput{
		{Path: DefaultConfigFileName, Data: configJSON},
		{Path: modelconfig.HFQuantConfigFileName, Data: hfQuantJSON},
	}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), files, nil, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, metadata) {
		assert.Equal(t, v1beta1.ModelQuantization(""), metadata.Quantization,
			"unknown quant_algo must NOT map to a placeholder enum value")
	}
}

func TestParseModelConfigFromFilesRejectsEscapingPath(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	metadata, err := parser.ParseModelConfigFromFiles(context.Background(), []ModelConfigFileInput{
		{Path: "../config.json", Data: []byte(`{}`)},
	}, nil, nil)

	assert.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "must stay under model root")
}

func TestFormatParamCount(t *testing.T) {
	testCases := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1000000, "1M"},
		{1500000, "1.5M"},
		{10000000, "10M"},
		{1000000000, "1B"},
		{1500000000, "1.5B"},
		{7000000000, "7B"},
		{13000000000, "13B"},
		{70000000000, "70B"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := modelconfig.FormatParamCount(tc.input)
			assert.Equal(t, tc.expected, result, "FormatParamCount failed for input %d", tc.input)
		})
	}
}

func mustReadTestFile(t *testing.T, filePath string) []byte {
	t.Helper()

	data, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	return data
}

func mustReadSafetensorsHeader(t *testing.T, filePath string) []byte {
	t.Helper()

	file, err := os.Open(filePath)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, file.Close())
	}()

	headerLenBytes := make([]byte, 8)
	_, err = io.ReadFull(file, headerLenBytes)
	assert.NoError(t, err)

	headerLen := binary.LittleEndian.Uint64(headerLenBytes)
	header := make([]byte, headerLen)
	_, err = io.ReadFull(file, header)
	assert.NoError(t, err)

	data := make([]byte, 0, 8+len(header))
	data = append(data, headerLenBytes...)
	data = append(data, header...)
	return data
}

func TestPopulateArtifactAttribute_SetsFields(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	orig := &ModelMetadata{} // empty input metadata
	sha := "abc123"
	parent := "/models/model1"
	artifact := &Artifact{
		Sha:        sha,
		ParentPath: map[string]string{"parentModel": parent},
	}

	out := parser.PopulateArtifactAttribute(artifact, orig)
	assert.NotNil(t, out, "returned metadata should not be nil")

	// Verify Artifact fields
	assert.Equal(t, sha, out.Artifact.Sha)
	assert.Equal(t, map[string]string{"parentModel": "/models/model1"}, out.Artifact.ParentPath)
	assert.Nil(t, out.Artifact.ChildrenPaths)
}

func TestPopulateArtifactAttribute_NilInput(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	// Prepare input with pre-filled artifact to ensure immutability of the input value
	original := &ModelMetadata{
		ModelType: "ClusterBaseModel",
	}

	out := parser.PopulateArtifactAttribute(nil, original)
	assert.Empty(t, out.Artifact.Sha)
	assert.Nil(t, out.Artifact.ParentPath)
	assert.Nil(t, out.Artifact.ChildrenPaths)
	assert.Equal(t, original, out, "no update should happen to original model moetadata")
}

// syntheticQuantConfig wraps a bare quant_method string in the unified
// HFQuantConfig shape so TestExtractModelMetadataFromHF (which mocks
// only the legacy GetQuantizationType()) can still drive the new
// applyQuantizationFromConfig path. Returns nil for the unquantized
// case so the helper is a no-op.
func syntheticQuantConfig(quantMethod string) *modelconfig.HFQuantConfig {
	if quantMethod == "" {
		return nil
	}
	return &modelconfig.HFQuantConfig{
		Quantization: modelconfig.HFQuantConfigDetails{QuantAlgo: quantMethod},
	}
}

func TestBuildArtifactAttribute_Basic(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := &ModelConfigParser{logger: logger}

	sha := "commit-sha-123"
	parentName := "clusterbasemodel.parentModel"
	parentPath := "/models/parent1"
	childrenPaths := []string{"/models/child1"}

	artifact := parser.BuildArtifactAttribute(sha, parentName, parentPath, childrenPaths)
	if assert.NotNil(t, artifact, "artifact should not be nil") {
		assert.Equal(t, sha, artifact.Sha, "sha should match input")
		assert.Equal(t, map[string]string{parentName: parentPath}, artifact.ParentPath, "parentPath should be a single-entry map with provided key/value")
		// ChildrenPaths should be a non-nil empty slice to allow safe appends
		assert.NotNil(t, artifact.ChildrenPaths, "childrenPaths should be non-nil")
		assert.Equal(t, childrenPaths, artifact.ChildrenPaths, "childrenPaths should be same as input")
	}
}
