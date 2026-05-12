package modelparser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/client/clientset/versioned"
	"github.com/ome-projects/ome/pkg/modelconfig"
)

const (
	// DefaultConfigFileName Default config file name used by Hugging Face models
	DefaultConfigFileName = "config.json"
	// DefaultModelIndexFileName Default model index file name used by diffusers models
	DefaultModelIndexFileName = "model_index.json"
)

// ModelConfigFileInput provides one file to a model config parse operation.
// Path must be relative to the logical model root and use slash separators.
type ModelConfigFileInput struct {
	Path string
	Data []byte
}

// modelConfigLoader is a function type for loading model configurations
// This allows for easy mocking in tests
type modelConfigLoader func(configPath string) (modelconfig.HuggingFaceModel, error)

// ModelConfigParser is responsible for parsing model config files
// and updating the corresponding Model CRD
type ModelConfigParser struct {
	logger          *zap.SugaredLogger
	omeClient       versioned.Interface
	loadModelConfig modelConfigLoader // Function to load model configs
}

// NewModelConfigParser creates a new model config parser
func NewModelConfigParser(omeClient versioned.Interface, logger *zap.SugaredLogger) *ModelConfigParser {
	return &ModelConfigParser{
		logger:          logger,
		omeClient:       omeClient,
		loadModelConfig: modelconfig.LoadModelConfig, // Use the real implementation by default
	}
}

// ParseModelConfig reads model_index.json (if present) or config.json from the model directory and extracts metadata
// without updating any resources. This allows the caller to control when and how updates happen.
func (p *ModelConfigParser) ParseModelConfig(modelDir string, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) (*ModelMetadata, error) {
	return p.parseModelConfigDir(modelDir, nil, baseModel, clusterBaseModel)
}

// ParseModelConfigFromFiles reads model metadata from an explicit set of files.
// Config JSON and safetensors headers are parsed directly from bytes by
// pkg/modelconfig.
func (p *ModelConfigParser) ParseModelConfigFromFiles(ctx context.Context, files []ModelConfigFileInput, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) (*ModelMetadata, error) {
	if shouldSkip := p.shouldSkipConfigParsing(baseModel, clusterBaseModel); shouldSkip {
		p.logger.Infof("Skipping config parsing due to annotation")
		return nil, nil
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("model config file input set is empty")
	}

	configInput, safetensorsFiles, hfQuantConfigBytes, err := splitModelConfigFileInputs(ctx, files)
	if err != nil {
		return nil, err
	}
	if configInput == nil {
		return nil, fmt.Errorf("no model_index.json or config.json found in file input set")
	}

	// Parse hf_quant_config.json early so the safetensors counting
	// downstream can be quant-aware (NVFP4 stored as U8[out, in/2]
	// requires a 2x multiplier; INT4 packed in I32 requires 8x; etc.).
	// Pre-parsing also lets the spec.Quantization fallback below use
	// the already-parsed value.
	var hfQuantConfig *modelconfig.HFQuantConfig
	if len(hfQuantConfigBytes) > 0 {
		parsed, perr := modelconfig.ParseHFQuantConfig(hfQuantConfigBytes)
		if perr != nil {
			p.logger.Warnf("Failed to parse %s: %v", modelconfig.HFQuantConfigFileName, perr)
		} else {
			hfQuantConfig = parsed
		}
	}

	var parameterCountOverride *int64
	if len(safetensorsFiles) > 0 {
		parameterCount, err := modelconfig.FindAndParseSafetensorsFilesWithQuantConfig(safetensorsFiles, hfQuantConfig)
		if err != nil {
			return nil, fmt.Errorf("parse safetensors metadata from file inputs: %w", err)
		}
		parameterCountOverride = &parameterCount
	}

	hfModel, err := modelconfig.ParseModelConfig(*configInput)
	if err != nil {
		return nil, fmt.Errorf("failed to parse model config file input %s with hf_model_config: %w", configInput.Path, err)
	}

	metadata := p.extractModelMetadataFromHFWithParameterCount(hfModel, parameterCountOverride)

	// hf_quant_config.json fallback for spec.Quantization. Override
	// only when config.json was silent — config.json wins when both
	// are present.
	if metadata.Quantization == "" && hfQuantConfig != nil {
		if mapped := QuantAlgoToOMEEnum(hfQuantConfig.Quantization.QuantAlgo); mapped != "" {
			metadata.Quantization = mapped
			p.logger.Infof("Setting quantization to %s from %s (quant_algo=%s)",
				mapped, modelconfig.HFQuantConfigFileName, hfQuantConfig.Quantization.QuantAlgo)
		} else if hfQuantConfig.Quantization.QuantAlgo != "" {
			p.logger.Warnf("%s declared quant_algo=%q which OME does not yet recognize; spec.Quantization will be left unset",
				modelconfig.HFQuantConfigFileName, hfQuantConfig.Quantization.QuantAlgo)
		}
	}

	p.updateParsedModelMetadata(baseModel, clusterBaseModel, metadata)
	return &metadata, nil
}

func (p *ModelConfigParser) parseModelConfigDir(modelDir string, parameterCountOverride *int64, baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) (*ModelMetadata, error) {
	p.logger.Infof("Parsing model config at: %s", modelDir)

	// Skip if the directory doesn't exist
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		p.logger.Warnf("Model directory doesn't exist: %s", modelDir)
		return nil, nil
	}

	// Check if model should skip config parsing
	if shouldSkip := p.shouldSkipConfigParsing(baseModel, clusterBaseModel); shouldSkip {
		p.logger.Infof("Skipping config parsing due to annotation")
		return nil, nil
	}

	// Read hf_quant_config.json early so it can drive both the
	// parameter-count override AND the spec.Quantization fallback.
	// loadHFQuantConfig returns nil for both "missing" and "parse
	// failed" — either way the rest of the function falls back to the
	// legacy naive count, which preserves behavior on non-quant models.
	hfQuantConfig := p.loadHFQuantConfig(modelDir)

	// Quant-aware parameter counting: when there's a quant config and
	// the caller didn't provide an override, recompute the count from
	// the safetensors directory using QuantPackingFactor + scale-tensor
	// skip + exclude_modules. The legacy hfModel.GetParameterCount()
	// does a naive sum of shape products which under-counts NVFP4 by
	// ~2x and over-counts FP16 excluded modules; correcting up-front
	// lets every downstream consumer see the right number.
	if parameterCountOverride == nil && hfQuantConfig != nil {
		if configPath, ferr := p.findConfigFile(modelDir); ferr == nil {
			if count, cerr := modelconfig.FindAndParseSafetensorsWithQuantConfig(configPath, hfQuantConfig); cerr == nil && count > 0 {
				countCopy := count
				parameterCountOverride = &countCopy
				p.logger.Infof("Using quant-aware safetensors count: %d (algo=%s)",
					count, hfQuantConfig.Quantization.QuantAlgo)
			}
		}
	}

	var metadata ModelMetadata
	var hasMetadata bool

	// Prefer model_index.json for diffusion models (skip config.json search if present)
	modelIndexPath, err := p.findModelIndexFile(modelDir)
	if err == nil {
		p.logger.Infof("Found model_index.json at: %s", modelIndexPath)
		hfModel, loadErr := p.loadModelConfig(modelIndexPath)
		if loadErr != nil {
			return nil, fmt.Errorf("failed to parse model_index.json with hf_model_config: %w", loadErr)
		}

		metadata = p.extractModelMetadataFromHFWithParameterCount(hfModel, parameterCountOverride)
		hasMetadata = true
	} else {
		p.logger.Infof("model_index.json not found: %v", err)

		// Look for the config.json file - it could be at the root level or a subdirectory
		configPath, findErr := p.findConfigFile(modelDir)
		if findErr != nil {
			p.logger.Infof("Config file not found: %v", findErr)
		} else {
			p.logger.Infof("Found config file at: %s", configPath)

			// Parse the config.json file using the hfutil.model_config module
			hfModel, loadErr := p.loadModelConfig(configPath)
			if loadErr != nil {
				return nil, fmt.Errorf("failed to parse config file with hf_model_config: %w", loadErr)
			}

			// Use the HuggingFaceModel interface to extract metadata
			metadata = p.extractModelMetadataFromHFWithParameterCount(hfModel, parameterCountOverride)
			hasMetadata = true
			p.logger.Infof("Extracted metadata: %+v", metadata)
		}
	}

	if !hasMetadata {
		return nil, fmt.Errorf("no model_index.json or config.json found in %s", modelDir)
	}

	// hf_quant_config.json fallback for spec.Quantization. Uses the
	// already-parsed value from the early load above. Override only
	// when config.json was silent — config.json wins when both are
	// present.
	if metadata.Quantization == "" && hfQuantConfig != nil {
		if mapped := QuantAlgoToOMEEnum(hfQuantConfig.Quantization.QuantAlgo); mapped != "" {
			metadata.Quantization = mapped
			p.logger.Infof("Setting quantization to %s from %s (quant_algo=%s)",
				mapped, modelconfig.HFQuantConfigFileName, hfQuantConfig.Quantization.QuantAlgo)
		} else if hfQuantConfig.Quantization.QuantAlgo != "" {
			p.logger.Warnf("%s declared quant_algo=%q which OME does not yet recognize; spec.Quantization will be left unset",
				modelconfig.HFQuantConfigFileName, hfQuantConfig.Quantization.QuantAlgo)
		}
	}

	p.updateParsedModelMetadata(baseModel, clusterBaseModel, metadata)
	return &metadata, nil
}

func (p *ModelConfigParser) updateParsedModelMetadata(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel, metadata ModelMetadata) {
	if baseModel != nil {
		if err := p.updateBaseModel(baseModel, metadata); err != nil {
			p.logger.Errorf("Failed to update BaseModel: %v", err)
		}
	} else if clusterBaseModel != nil {
		if err := p.updateClusterBaseModel(clusterBaseModel, metadata); err != nil {
			p.logger.Errorf("Failed to update ClusterBaseModel: %v", err)
		}
	}
}

type normalizedModelConfigFileInput struct {
	Path string
	Data []byte
}

func splitModelConfigFileInputs(ctx context.Context, files []ModelConfigFileInput) (*modelconfig.ModelConfigInput, []modelconfig.SafetensorsFileInput, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	normalized := make([]normalizedModelConfigFileInput, 0, len(files))
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}

		relativePath, err := cleanInputFilePath(file.Path)
		if err != nil {
			return nil, nil, nil, err
		}
		normalized = append(normalized, normalizedModelConfigFileInput{
			Path: relativePath,
			Data: file.Data,
		})
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Path < normalized[j].Path
	})

	var modelIndexInput *modelconfig.ModelConfigInput
	var configInput *modelconfig.ModelConfigInput
	var hfQuantConfigBytes []byte
	safetensorsFiles := make([]modelconfig.SafetensorsFileInput, 0)
	for _, file := range normalized {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		if isSafetensorsFileInput(file.Path) {
			safetensorsFiles = append(safetensorsFiles, modelconfig.SafetensorsFileInput{
				Path: file.Path,
				Data: file.Data,
			})
			continue
		}
		switch path.Base(file.Path) {
		case DefaultModelIndexFileName:
			if modelIndexInput == nil {
				input := modelconfig.ModelConfigInput{
					Path: file.Path,
					Data: file.Data,
				}
				modelIndexInput = &input
			}
		case DefaultConfigFileName:
			if configInput == nil {
				input := modelconfig.ModelConfigInput{
					Path: file.Path,
					Data: file.Data,
				}
				configInput = &input
			}
		case modelconfig.HFQuantConfigFileName:
			if hfQuantConfigBytes == nil {
				hfQuantConfigBytes = file.Data
			}
		}
	}

	if modelIndexInput != nil {
		return modelIndexInput, safetensorsFiles, hfQuantConfigBytes, nil
	}
	return configInput, safetensorsFiles, hfQuantConfigBytes, nil
}

func cleanInputFilePath(filePath string) (string, error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return "", fmt.Errorf("model parser input file path is required")
	}
	filePath = strings.TrimLeft(filePath, "/")
	filePath = path.Clean(filePath)
	if filePath == "." || filePath == "" {
		return "", fmt.Errorf("model parser input file path is invalid: %q", filePath)
	}
	if filePath == ".." || strings.HasPrefix(filePath, "../") || filepath.IsAbs(filePath) {
		return "", fmt.Errorf("model parser input file path must stay under model root: %q", filePath)
	}
	return filePath, nil
}

func isSafetensorsFileInput(filePath string) bool {
	base := path.Base(filePath)
	return base == "model.safetensors.index.json" || strings.HasSuffix(base, ".safetensors")
}

// findConfigFile searches for the config.json file in the model directory
// It checks the root directory and common subdirectories
func (p *ModelConfigParser) findConfigFile(modelDir string) (string, error) {
	// Check the root directory first
	rootConfigPath := filepath.Join(modelDir, DefaultConfigFileName)
	if _, err := os.Stat(rootConfigPath); err == nil {
		return rootConfigPath, nil
	}

	// Common places where config.json might be located
	possiblePaths := []string{
		filepath.Join(modelDir, "safetensors", DefaultConfigFileName),
		filepath.Join(modelDir, "pytorch_model", DefaultConfigFileName),
		filepath.Join(modelDir, "model", DefaultConfigFileName),
	}

	// Check if config.json exists in any of the possible paths
	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}

	// If not found in common locations, do a recursive search (limited to avoid deep searching)
	var configPath string
	err := filepath.Walk(modelDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip error files
		}
		if !info.IsDir() && info.Name() == DefaultConfigFileName {
			configPath = path
			return filepath.SkipDir // Found it, stop searching
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if configPath == "" {
		return "", fmt.Errorf("config.json not found in %s", modelDir)
	}
	return configPath, nil
}

// findHFQuantConfigFile looks for hf_quant_config.json in the same
// places as config.json. NVIDIA ModelOpt and TensorRT-LLM emit it as a
// sibling file when they don't modify config.json's quantization_config
// block. Returns "" + nil if absent — the file is optional.
func (p *ModelConfigParser) findHFQuantConfigFile(modelDir string) string {
	rootPath := filepath.Join(modelDir, modelconfig.HFQuantConfigFileName)
	if _, err := os.Stat(rootPath); err == nil {
		return rootPath
	}
	for _, sub := range []string{"safetensors", "pytorch_model", "model"} {
		p := filepath.Join(modelDir, sub, modelconfig.HFQuantConfigFileName)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// loadHFQuantConfig reads + parses an hf_quant_config.json file from
// disk. Returns nil + nil if absent so callers can use the
// "optional file" pattern without distinguishing between "not present"
// and "present but empty".
func (p *ModelConfigParser) loadHFQuantConfig(modelDir string) *modelconfig.HFQuantConfig {
	path := p.findHFQuantConfigFile(modelDir)
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		p.logger.Warnf("Found %s at %s but failed to read it: %v", modelconfig.HFQuantConfigFileName, path, err)
		return nil
	}
	cfg, err := modelconfig.ParseHFQuantConfig(data)
	if err != nil {
		p.logger.Warnf("Failed to parse %s at %s: %v", modelconfig.HFQuantConfigFileName, path, err)
		return nil
	}
	return cfg
}

// findModelIndexFile searches for the model_index.json file in the model directory
// It checks the root directory and common subdirectories
func (p *ModelConfigParser) findModelIndexFile(modelDir string) (string, error) {
	// Check the root directory first
	rootIndexPath := filepath.Join(modelDir, DefaultModelIndexFileName)
	if _, err := os.Stat(rootIndexPath); err == nil {
		return rootIndexPath, nil
	}

	// If not found in rootdir, do a recursive search (limited to avoid deep searching)
	var indexPath string
	err := filepath.Walk(modelDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip error files
		}
		if !info.IsDir() && info.Name() == DefaultModelIndexFileName {
			indexPath = path
			return filepath.SkipDir // Found it, stop searching
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if indexPath == "" {
		return "", fmt.Errorf("model_index.json not found in %s", modelDir)
	}
	return indexPath, nil
}

// updateModel is a generic function to update either a BaseModel or ClusterBaseModel
func (p *ModelConfigParser) updateModel(model interface{}, metadata ModelMetadata) error {
	// Get model info for logging
	var modelInfo string
	var namespace, name string

	// Update model spec fields from the extracted metadata
	switch m := model.(type) {
	case *v1beta1.BaseModel:
		p.updateModelSpec(&m.Spec, metadata)
		namespace, name = m.Namespace, m.Name
		modelInfo = fmt.Sprintf("BaseModel %s/%s", namespace, name)
	case *v1beta1.ClusterBaseModel:
		p.updateModelSpec(&m.Spec, metadata)
		name = m.Name
		modelInfo = fmt.Sprintf("ClusterBaseModel %s", name)
	default:
		return fmt.Errorf("unsupported model type: %T", model)
	}

	p.logger.Infof("Updating %s with extracted metadata", modelInfo)

	// Update the model in Kubernetes
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest interface{}
		var err error

		// Get the latest version of the model
		switch model.(type) {
		case *v1beta1.BaseModel:
			latest, err = p.omeClient.OmeV1beta1().BaseModels(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		case *v1beta1.ClusterBaseModel:
			latest, err = p.omeClient.OmeV1beta1().ClusterBaseModels().Get(context.TODO(), name, metav1.GetOptions{})
		}

		if err != nil {
			return fmt.Errorf("failed to get latest model: %w", err)
		}

		// Update the spec with our changes (reapply them to the latest version)
		switch m := latest.(type) {
		case *v1beta1.BaseModel:
			p.updateModelSpec(&m.Spec, metadata)
		case *v1beta1.ClusterBaseModel:
			p.updateModelSpec(&m.Spec, metadata)
		}

		// Update the model
		switch m := latest.(type) {
		case *v1beta1.BaseModel:
			_, err = p.omeClient.OmeV1beta1().BaseModels(namespace).Update(context.TODO(), m, metav1.UpdateOptions{})
		case *v1beta1.ClusterBaseModel:
			_, err = p.omeClient.OmeV1beta1().ClusterBaseModels().Update(context.TODO(), m, metav1.UpdateOptions{})
		}

		if err != nil {
			return fmt.Errorf("failed to update model: %w", err)
		}

		p.logger.Debugf("Successfully updated %s", modelInfo)
		return nil
	})
}

// updateBaseModel updates the BaseModel CRD with information from the extracted metadata
func (p *ModelConfigParser) updateBaseModel(model *v1beta1.BaseModel, metadata ModelMetadata) error {
	return p.updateModel(model, metadata)
}

// updateClusterBaseModel updates the ClusterBaseModel CRD with information from the extracted metadata
func (p *ModelConfigParser) updateClusterBaseModel(model *v1beta1.ClusterBaseModel, metadata ModelMetadata) error {
	return p.updateModel(model, metadata)
}

// extractModelMetadataFromHF extracts relevant metadata using the HuggingFaceModel interface
func (p *ModelConfigParser) extractModelMetadataFromHF(hfModel modelconfig.HuggingFaceModel) ModelMetadata {
	return p.extractModelMetadataFromHFWithParameterCount(hfModel, nil)
}

func (p *ModelConfigParser) extractModelMetadataFromHFWithParameterCount(hfModel modelconfig.HuggingFaceModel, parameterCountOverride *int64) ModelMetadata {
	p.logger.Infof("Extracting metadata from HuggingFace model: type=%s, architecture=%s",
		hfModel.GetModelType(), hfModel.GetArchitecture())

	var diffusionModel *modelconfig.DiffusionPipelineSpec
	if dm, ok := hfModel.(modelconfig.HuggingFaceDiffusionModel); ok {
		diffusionModel = dm.GetDiffusionModel()
	}
	isDiffusion := diffusionModel != nil
	modelType := hfModel.GetModelType()
	var parameterCount int64
	if parameterCountOverride != nil {
		parameterCount = *parameterCountOverride
	} else {
		parameterCount = hfModel.GetParameterCount()
	}

	metadata := ModelMetadata{
		ModelType:          modelType,
		ModelArchitecture:  hfModel.GetArchitecture(),
		ModelParameterSize: modelconfig.FormatParamCount(parameterCount),
		MaxTokens:          int32(hfModel.GetContextLength()),
		ModelCapabilities:  capabilitiesAsStrings(hfModel, p.logger),
	}

	if isDiffusion {
		metadata.ModelFormat = v1beta1.ModelFormat{
			Name:    "diffusers",
			Version: &diffusionModel.DiffusersVersion,
		}
		metadata.ModelFramework = &v1beta1.ModelFrameworkSpec{
			Name:    "diffusers",
			Version: &diffusionModel.DiffusersVersion,
		}
		metadata.DiffusionPipeline = convertDiffusionPipelineSpec(diffusionModel)
	} else {
		// Set the model format (most models use SafeTensors)
		version := "1.0.0"
		metadata.ModelFormat = v1beta1.ModelFormat{
			Name:    "safetensors",
			Version: &version,
		}

		metadata.ModelFramework = &v1beta1.ModelFrameworkSpec{
			Name: "transformers",
		}
	}

	// Set transformer version if available
	if !isDiffusion {
		transformerVersion := hfModel.GetTransformerVersion()
		if transformerVersion != "" {
			metadata.ModelFramework.Version = &transformerVersion
			p.logger.Infof("Setting transformer version: %s", transformerVersion)
		}
	}

	// Extract quantization information from config.json's
	// quantization_config block (the HF-native convention). If config.json
	// doesn't carry one, callers that have it can override metadata.Quantization
	// from a sibling hf_quant_config.json file (NVIDIA ModelOpt convention).
	if quantType := hfModel.GetQuantizationType(); quantType != "" {
		p.logger.Infof("Detected quantization type from config.json: %s", quantType)
		if mapped := QuantAlgoToOMEEnum(quantType); mapped != "" {
			metadata.Quantization = mapped
			p.logger.Infof("Setting quantization to %s", mapped)
		}
	}

	// Get the model size in bytes
	var modelSizeBytes int64
	if parameterCountOverride != nil {
		modelSizeBytes = modelconfig.EstimateModelSizeBytes(parameterCount, hfModel.GetTorchDtype())
	} else {
		modelSizeBytes = hfModel.GetModelSizeBytes()
	}
	if modelSizeBytes > 0 {
		p.logger.Infof("Model size in bytes: %d (%.2f GB)",
			modelSizeBytes, float64(modelSizeBytes)/1000000000.0)
	}

	// Get the raw JSON configuration for status
	configJSON, err := json.Marshal(struct {
		ModelType          string `json:"model_type"`
		Architecture       string `json:"architecture"`
		ContextLength      int    `json:"context_length"`
		ParameterCount     string `json:"parameter_count"`
		HasVision          bool   `json:"has_vision"`
		IsEmbedding        bool   `json:"is_embedding"`
		TransformerVersion string `json:"transformers_version"`
		TorchDtype         string `json:"torch_dtype"`
		ModelSizeBytes     int64  `json:"model_size_bytes"`
	}{
		ModelType:          hfModel.GetModelType(),
		Architecture:       hfModel.GetArchitecture(),
		ContextLength:      hfModel.GetContextLength(),
		ParameterCount:     modelconfig.FormatParamCount(parameterCount),
		HasVision:          hfModel.HasVision(),
		IsEmbedding:        hfModel.IsEmbedding(),
		TransformerVersion: hfModel.GetTransformerVersion(),
		TorchDtype:         hfModel.GetTorchDtype(),
		ModelSizeBytes:     modelSizeBytes,
	})
	if err == nil {
		metadata.ModelConfiguration = configJSON
	} else {
		p.logger.Warnf("Failed to marshal model configuration: %v", err)
	}

	p.logger.Infof("Extracted metadata: type=%s, architecture=%s, size=%s, maxTokens=%d, capabilities=%v",
		metadata.ModelType, metadata.ModelArchitecture, metadata.ModelParameterSize,
		metadata.MaxTokens, metadata.ModelCapabilities)

	return metadata
}

func (p *ModelConfigParser) updateModelSpec(spec *v1beta1.BaseModelSpec, metadata ModelMetadata) bool {
	p.logger.Info("Updating model spec with extracted metadata")

	updated := false

	// Use a helper function for updating fields
	updateField := func(current interface{}, new interface{}, fieldName string) bool {
		isUpdated := false
		switch c := current.(type) {
		case **string:
			if *c == nil && new != nil {
				val := new.(string)
				*c = &val
				isUpdated = true
			}
		case *[]string:
			if len(*c) == 0 && len(new.([]string)) > 0 {
				*c = new.([]string)
				isUpdated = true
			}
		case *v1beta1.ModelFormat:
			if c.Name == "" {
				*c = new.(v1beta1.ModelFormat)
				isUpdated = true
			}
		case **v1beta1.ModelFrameworkSpec:
			if *c == nil && new != nil {
				*c = new.(*v1beta1.ModelFrameworkSpec)
				isUpdated = true
			}
		case **int32:
			if *c == nil {
				if val, ok := new.(int32); ok && val > 0 {
					*c = &val
					isUpdated = true
				}
			}
		case **v1beta1.DiffusionPipelineSpec:
			if *c == nil && new != nil {
				*c = new.(*v1beta1.DiffusionPipelineSpec)
				isUpdated = true
			}
		case **v1beta1.ModelQuantization:
			if *c == nil && new.(v1beta1.ModelQuantization) != "" {
				val := new.(v1beta1.ModelQuantization)
				*c = &val
				isUpdated = true
			}
		case *runtime.RawExtension:
			if new != nil && len(new.([]byte)) > 0 && !bytes.Equal(c.Raw, new.([]byte)) {
				c.Raw = append(c.Raw[:0], new.([]byte)...)
				isUpdated = true
			}
		}

		if isUpdated {
			p.logger.Debugf("Setting %s: %v", fieldName, new)
		} else if current != nil {
			p.logger.Debugf("%s already set, not updating", fieldName)
		}

		return isUpdated
	}

	// Update each field using the helper function
	updated = updateField(&spec.ModelType, metadata.ModelType, "ModelType") || updated
	updated = updateField(&spec.ModelArchitecture, metadata.ModelArchitecture, "ModelArchitecture") || updated
	updated = updateField(&spec.ModelFramework, metadata.ModelFramework, "ModelFramework") || updated
	updated = updateField(&spec.ModelFormat, metadata.ModelFormat, "ModelFormat") || updated
	updated = updateField(&spec.ModelParameterSize, metadata.ModelParameterSize, "ModelParameterSize") || updated
	updated = updateField(&spec.MaxTokens, metadata.MaxTokens, "MaxTokens") || updated
	updated = updateField(&spec.ModelCapabilities, metadata.ModelCapabilities, "ModelCapabilities") || updated
	updated = updateField(&spec.ApiCapabilities, metadata.ApiCapabilities, "ApiCapabilities") || updated
	updated = updateField(&spec.Quantization, metadata.Quantization, "Quantization") || updated
	updated = updateField(&spec.ModelConfiguration, metadata.ModelConfiguration, "ModelConfiguration") || updated
	updated = updateField(&spec.DiffusionPipeline, metadata.DiffusionPipeline, "DiffusionPipeline") || updated

	p.logger.Info("Model spec update complete", "updated", updated)
	return updated
}

// ApplyModelMetadata updates unset BaseModel spec fields from parsed metadata.
func (p *ModelConfigParser) ApplyModelMetadata(spec *v1beta1.BaseModelSpec, metadata ModelMetadata) bool {
	if spec == nil {
		return false
	}
	return p.updateModelSpec(spec, metadata)
}

func convertDiffusionPipelineSpec(pipeline *modelconfig.DiffusionPipelineSpec) *v1beta1.DiffusionPipelineSpec {
	if pipeline == nil {
		return nil
	}

	spec := &v1beta1.DiffusionPipelineSpec{}
	if pipeline.ClassName != "" {
		className := pipeline.ClassName
		spec.ClassName = &className
	}

	spec.Scheduler = convertDiffusionComponent(pipeline.Scheduler)
	spec.TextEncoder = convertDiffusionComponent(pipeline.TextEncoder)
	spec.Tokenizer = convertDiffusionComponent(pipeline.Tokenizer)
	spec.Transformer = convertDiffusionComponent(pipeline.Transformer)
	spec.VAE = convertDiffusionComponent(pipeline.VAE)

	if len(pipeline.AdditionalComponents) > 0 {
		additional := make(map[string]v1beta1.DiffusionComponentSpec, len(pipeline.AdditionalComponents))
		for key, value := range pipeline.AdditionalComponents {
			additional[key] = v1beta1.DiffusionComponentSpec{Library: value.Library, Type: value.Type}
		}
		spec.AdditionalComponents = additional
	}

	return spec
}

func convertDiffusionComponent(component *modelconfig.DiffusionComponentSpec) *v1beta1.DiffusionComponentSpec {
	if component == nil {
		return nil
	}
	return &v1beta1.DiffusionComponentSpec{Library: component.Library, Type: component.Type}
}

// shouldSkipConfigParsing checks if config parsing should be skipped for this model
func (p *ModelConfigParser) shouldSkipConfigParsing(baseModel *v1beta1.BaseModel, clusterBaseModel *v1beta1.ClusterBaseModel) bool {
	// Check base model annotations
	if baseModel != nil {
		if value, exists := baseModel.Annotations[ConfigParsingAnnotation]; exists {
			if strings.ToLower(value) == "true" {
				p.logger.Infof("Skipping config parsing for BaseModel %s/%s due to annotation", baseModel.Namespace, baseModel.Name)
				return true
			}
		}
	}

	// Check cluster base model annotations
	if clusterBaseModel != nil {
		if value, exists := clusterBaseModel.Annotations[ConfigParsingAnnotation]; exists {
			if strings.ToLower(value) == "true" {
				p.logger.Infof("Skipping config parsing for ClusterBaseModel %s due to annotation", clusterBaseModel.Name)
				return true
			}
		}
	}

	return false
}

// PopulateArtifactAttribute returns a pointer to an updated copy of currentModelMetadata
// where the Artifact field is set to the provided artifact pointer
//
// Parameters:
//   - artifact: pointer to built artifact based on reusability
//   - currentModelMetadata: pointer of current model metadata
//
// Returns:
//   - *ModelMetadata: pointer to the updated metadata with Artifact populated
func (p *ModelConfigParser) PopulateArtifactAttribute(artifact *Artifact, currentModelMetadata *ModelMetadata) *ModelMetadata {
	if artifact != nil {
		currentModelMetadata.Artifact = *artifact
		p.logger.Infof("current artifact is :%s", currentModelMetadata.Artifact)
	}
	return currentModelMetadata
}

/*
BuildArtifactAttribute constructs an Artifact instance from the provided SHA and parent information.

Fields set:
  - Sha: set to the provided commit SHA identifying the artifact version
  - ParentPath: a single-entry map {matchedParentName: parentPath} pointing to the parent artifact location
  - ChildrenPaths: initialized to an empty (non-nil) slice so it can be safely appended to later

Returns a pointer to the created Artifact.
*/
func (p *ModelConfigParser) BuildArtifactAttribute(sha string, matchedParentName string, parentPath string, childrenPaths []string) *Artifact {
	artifact := Artifact{
		Sha:           sha,
		ParentPath:    map[string]string{matchedParentName: parentPath},
		ChildrenPaths: childrenPaths,
	}
	return &artifact
}
