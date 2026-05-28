package runtimeselector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	modelVer "github.com/sgl-project/ome/pkg/modelver"
)

// DefaultRuntimeMatcher implements RuntimeMatcher with comprehensive compatibility checking.
type DefaultRuntimeMatcher struct {
	config *Config
}

// NewDefaultRuntimeMatcher creates a new DefaultRuntimeMatcher.
func NewDefaultRuntimeMatcher(config *Config) RuntimeMatcher {
	return &DefaultRuntimeMatcher{
		config: config,
	}
}

// IsCompatible checks if a runtime can serve a model.
func (m *DefaultRuntimeMatcher) IsCompatible(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (bool, error) {
	// Quick checks first
	if runtime.IsDisabled() {
		return false, &RuntimeDisabledError{RuntimeName: runtimeName}
	}

	// Check if any supported format matches
	for _, format := range runtime.SupportedModelFormats {
		if m.compareSupportedModelFormats(model, format) {
			// Found a matching format, now check model size if specified
			if err := m.checkModelSize(runtime, model, runtimeName); err == nil {
				return true, nil
			}
			// If model size check failed, continue checking other formats
		}
	}

	// No matching format found
	return false, nil
}

// GetCompatibilityDetails returns detailed compatibility information.
func (m *DefaultRuntimeMatcher) GetCompatibilityDetails(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (*CompatibilityReport, error) {
	ctx := context.Background()
	logger := log.FromContext(ctx)

	report := &CompatibilityReport{
		IsCompatible:           false,
		IncompatibilityReasons: []string{},
		Warnings:               []string{},
		MatchDetails:           MatchDetails{},
	}

	// Check if runtime is disabled
	if runtime.IsDisabled() {
		report.IncompatibilityReasons = append(report.IncompatibilityReasons, "runtime is disabled")
		return report, nil
	}

	// Check supported formats (mimics original RuntimeSupportsModel logic)
	formatSupported := false
	var formatMismatchReasons []string
	for _, format := range runtime.SupportedModelFormats {
		if m.compareSupportedModelFormats(model, format) {
			formatSupported = true
			match := m.evaluateFormatMatch(model, format)
			report.MatchDetails = match
			break
		}
		// Collect mismatch reasons for debugging
		formatMismatchReasons = append(formatMismatchReasons, m.getFormatMismatchReason(model, format))
	}

	if !formatSupported {
		if len(formatMismatchReasons) > 0 {
			report.IncompatibilityReasons = append(report.IncompatibilityReasons,
				fmt.Sprintf("model format '%s' not in supported formats: %s",
					getModelFormatLabel(model), strings.Join(formatMismatchReasons, "; ")))
		} else {
			report.IncompatibilityReasons = append(report.IncompatibilityReasons,
				fmt.Sprintf("model format '%s' not in supported formats: no supported formats defined",
					getModelFormatLabel(model)))
		}
		return report, nil
	}

	// Check model size compatibility
	if model.ModelParameterSize != nil && runtime.ModelSizeRange != nil {
		modelSize := parseModelSize(*model.ModelParameterSize)
		minSize := parseModelSize(*runtime.ModelSizeRange.Min)
		maxSize := parseModelSize(*runtime.ModelSizeRange.Max)

		if modelSize < minSize || modelSize > maxSize {
			report.IncompatibilityReasons = append(report.IncompatibilityReasons,
				fmt.Sprintf("model size %s is outside supported range [%s, %s]",
					*model.ModelParameterSize, *runtime.ModelSizeRange.Min, *runtime.ModelSizeRange.Max))
			report.MatchDetails.SizeMatch = false
			return report, nil
		}
		report.MatchDetails.SizeMatch = true
	}

	// At this point, runtime supports the model
	report.IsCompatible = true

	// Auto-select is informational only — it doesn't affect compatibility.
	if !runtimeHasAutoSelectFormat(runtime) {
		report.Warnings = append(report.Warnings,
			"runtime does not have auto-select enabled for any supported format")
	}

	// Add other warnings
	if model.ModelParameterSize == nil && runtime.ModelSizeRange != nil {
		report.Warnings = append(report.Warnings,
			"model does not specify size, but runtime has size constraints")
	}

	logger.V(1).Info("Compatibility check completed",
		"runtime", runtimeName,
		"model", model.ModelFormat.Name,
		"compatible", report.IsCompatible,
		"reasons", len(report.IncompatibilityReasons))

	return report, nil
}

// evaluateFormatMatch evaluates how well a supported format matches a model.
func (m *DefaultRuntimeMatcher) evaluateFormatMatch(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) MatchDetails {
	match := MatchDetails{
		FormatMatch:             false,
		FrameworkMatch:          false,
		ArchitectureMatch:       true, // Default to true if not specified
		DiffusionPipelineMatch:  true, // Default to true if not specified
		QuantizationMatch:       true, // Default to true if not specified
		ModelCacheProviderMatch: true, // Default to true unless the model uses sharded distribution
		SizeMatch:               true, // Will be checked separately
		Priority:                m.config.DefaultPriority,
		Weight:                  0,
		Reasons:                 []string{},
	}

	// Set priority
	if format.Priority != nil {
		match.Priority = *format.Priority
	}

	// Set auto-select
	if format.AutoSelect != nil {
		match.AutoSelectEnabled = *format.AutoSelect
	}

	if modelRequiresCacheProvider(model) {
		match.ModelCacheProviderMatch = supportedFormatSupportsModelCacheProvider(format, m.config.ModelCacheProvider)
		if !match.ModelCacheProviderMatch {
			match.Reasons = append(match.Reasons, modelCacheProviderMismatchReason(m.config.ModelCacheProvider, format))
		}
	}

	pipelineMatch, pipelineReason := m.compareDiffusionPipeline(model.DiffusionPipeline, format.DiffusionPipeline)
	match.DiffusionPipelineMatch = pipelineMatch
	if !pipelineMatch && pipelineReason != "" {
		match.Reasons = append(match.Reasons, pipelineReason)
	}

	match.ArchitectureMatch = optionalEqual(model.ModelArchitecture, format.ModelArchitecture)
	if !match.ArchitectureMatch {
		if model.ModelArchitecture != nil && format.ModelArchitecture != nil {
			match.Reasons = append(match.Reasons,
				fmt.Sprintf("architecture mismatch: model=%s, runtime=%s",
					*model.ModelArchitecture, *format.ModelArchitecture))
		} else {
			match.Reasons = append(match.Reasons, "architecture requirement mismatch")
		}
	}

	match.QuantizationMatch = optionalEqual(model.Quantization, format.Quantization)
	if !match.QuantizationMatch {
		if model.Quantization != nil && format.Quantization != nil {
			match.Reasons = append(match.Reasons,
				fmt.Sprintf("quantization mismatch: model=%s, runtime=%s",
					*model.Quantization, *format.Quantization))
		} else {
			match.Reasons = append(match.Reasons, "quantization requirement mismatch")
		}
	}

	// Check model format
	if format.ModelFormat != nil && format.ModelFormat.Name == model.ModelFormat.Name {
		match.FormatMatch = matchOptionalVersions(model.ModelFormat.Version, format.ModelFormat.Version, func() bool {
			return m.compareModelFormatVersions(format.ModelFormat, &model.ModelFormat)
		})
		if match.FormatMatch && format.ModelFormat.Weight > 0 {
			match.Weight += format.ModelFormat.Weight * int64(match.Priority)
		}
	}

	if format.ModelFramework != nil && model.ModelFramework != nil {
		if format.ModelFramework.Name == model.ModelFramework.Name {
			match.FrameworkMatch = matchOptionalVersions(model.ModelFramework.Version, format.ModelFramework.Version, func() bool {
				return m.compareModelFrameworkVersions(format.ModelFramework, model.ModelFramework)
			})
			if match.FrameworkMatch && format.ModelFramework.Weight > 0 {
				match.Weight += format.ModelFramework.Weight * int64(match.Priority)
			}
		}
	} else if format.ModelFramework == nil && model.ModelFramework == nil {
		match.FrameworkMatch = true
	}

	return match
}

// compareSupportedModelFormats checks if a model matches a supported format.
func (m *DefaultRuntimeMatcher) compareSupportedModelFormats(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) bool {
	if modelRequiresCacheProvider(model) && !supportedFormatSupportsModelCacheProvider(format, m.config.ModelCacheProvider) {
		return false
	}

	if ok, _ := m.compareDiffusionPipeline(model.DiffusionPipeline, format.DiffusionPipeline); !ok {
		return false
	}

	if !optionalEqual(model.ModelArchitecture, format.ModelArchitecture) {
		return false
	}
	if !optionalEqual(model.Quantization, format.Quantization) {
		return false
	}

	// Check model format (required: format.ModelFormat must be set).
	if format.ModelFormat == nil || format.ModelFormat.Name != model.ModelFormat.Name {
		return false
	}
	if !matchOptionalVersions(model.ModelFormat.Version, format.ModelFormat.Version, func() bool {
		return m.compareModelFormatVersions(format.ModelFormat, &model.ModelFormat)
	}) {
		return false
	}

	// Check model framework
	if format.ModelFramework != nil && model.ModelFramework != nil {
		if format.ModelFramework.Name != model.ModelFramework.Name {
			return false
		}
		if !matchOptionalVersions(model.ModelFramework.Version, format.ModelFramework.Version, func() bool {
			return m.compareModelFrameworkVersions(format.ModelFramework, model.ModelFramework)
		}) {
			return false
		}
	} else if (format.ModelFramework != nil) != (model.ModelFramework != nil) {
		return false
	}

	return true
}

// getFormatMismatchReason returns a human-readable reason why a model doesn't match a supported format.
// Example outputs:
//   - "architecture mismatch (model=LlamaForCausalLM, runtime=MistralForCausalLM)"
//   - "quantization mismatch (model=fp8, runtime=int4)"
//   - "format name mismatch (model=pytorch, runtime=safetensors), quantization mismatch (model=fp8, runtime=int4)"
func (m *DefaultRuntimeMatcher) getFormatMismatchReason(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) string {
	var reasons []string

	if modelRequiresCacheProvider(model) && !supportedFormatSupportsModelCacheProvider(format, m.config.ModelCacheProvider) {
		reasons = append(reasons, modelCacheProviderMismatchReason(m.config.ModelCacheProvider, format))
	}

	if ok, reason := m.compareDiffusionPipeline(model.DiffusionPipeline, format.DiffusionPipeline); !ok {
		if reason != "" {
			reasons = append(reasons, reason)
		} else {
			reasons = append(reasons, "diffusion pipeline mismatch")
		}
	}

	// Check architecture mismatch
	if model.ModelArchitecture != nil && format.ModelArchitecture != nil {
		if *model.ModelArchitecture != *format.ModelArchitecture {
			reasons = append(reasons, fmt.Sprintf("architecture mismatch (model=%s, runtime=%s)",
				*model.ModelArchitecture, *format.ModelArchitecture))
		}
	} else if (model.ModelArchitecture == nil) != (format.ModelArchitecture == nil) {
		if model.ModelArchitecture == nil {
			reasons = append(reasons, fmt.Sprintf("model has no architecture but runtime requires %s",
				*format.ModelArchitecture))
		} else {
			reasons = append(reasons, fmt.Sprintf("model has architecture %s but runtime has no architecture requirement",
				*model.ModelArchitecture))
		}
	}

	// Check quantization mismatch
	if model.Quantization != nil && format.Quantization != nil {
		if *model.Quantization != *format.Quantization {
			reasons = append(reasons, fmt.Sprintf("quantization mismatch (model=%s, runtime=%s)",
				*model.Quantization, *format.Quantization))
		}
	} else if (model.Quantization == nil) != (format.Quantization == nil) {
		if model.Quantization == nil {
			reasons = append(reasons, fmt.Sprintf("model has no quantization but runtime requires %s",
				*format.Quantization))
		} else {
			reasons = append(reasons, fmt.Sprintf("model has quantization %s but runtime has no quantization requirement",
				*model.Quantization))
		}
	}

	// Check model format mismatch
	// Note: model.ModelFormat is a non-pointer struct, always present
	if format.ModelFormat != nil {
		if format.ModelFormat.Name != model.ModelFormat.Name {
			reasons = append(reasons, fmt.Sprintf("format name mismatch (model=%s, runtime=%s)",
				model.ModelFormat.Name, format.ModelFormat.Name))
		} else if format.ModelFormat.Version != nil && model.ModelFormat.Version != nil {
			if !m.compareModelFormatVersions(format.ModelFormat, &model.ModelFormat) {
				reasons = append(reasons, fmt.Sprintf("format version mismatch (model=%s, runtime=%s)",
					*model.ModelFormat.Version, *format.ModelFormat.Version))
			}
		} else if (format.ModelFormat.Version == nil) != (model.ModelFormat.Version == nil) {
			if model.ModelFormat.Version == nil {
				reasons = append(reasons, fmt.Sprintf("model has no format version but runtime requires %s",
					*format.ModelFormat.Version))
			} else {
				reasons = append(reasons, "model has format version but runtime has no version requirement")
			}
		}
	}

	// Check model framework mismatch
	if format.ModelFramework != nil && model.ModelFramework != nil {
		if format.ModelFramework.Name != model.ModelFramework.Name {
			reasons = append(reasons, fmt.Sprintf("framework name mismatch (model=%s, runtime=%s)",
				model.ModelFramework.Name, format.ModelFramework.Name))
		} else if format.ModelFramework.Version != nil && model.ModelFramework.Version != nil {
			if !m.compareModelFrameworkVersions(format.ModelFramework, model.ModelFramework) {
				reasons = append(reasons, fmt.Sprintf("framework version mismatch (model=%s, runtime=%s)",
					*model.ModelFramework.Version, *format.ModelFramework.Version))
			}
		} else if (format.ModelFramework.Version == nil) != (model.ModelFramework.Version == nil) {
			if model.ModelFramework.Version == nil {
				reasons = append(reasons, fmt.Sprintf("model has no framework version but runtime requires %s",
					*format.ModelFramework.Version))
			} else {
				reasons = append(reasons, "model has framework version but runtime has no version requirement")
			}
		}
	} else if (format.ModelFramework != nil) != (model.ModelFramework != nil) {
		if model.ModelFramework == nil {
			reasons = append(reasons, fmt.Sprintf("model has no framework but runtime requires %s",
				format.ModelFramework.Name))
		} else {
			reasons = append(reasons, fmt.Sprintf("model has framework %s but runtime has no framework requirement",
				model.ModelFramework.Name))
		}
	}

	if len(reasons) == 0 {
		return "unknown mismatch"
	}
	return strings.Join(reasons, ", ")
}

func (m *DefaultRuntimeMatcher) compareDiffusionPipeline(model *v1beta1.DiffusionPipelineSpec, format *v1beta1.DiffusionPipelineSpec) (bool, string) {
	if format == nil {
		return true, ""
	}

	if model == nil {
		return false, "diffusion pipeline required by runtime but not specified in model"
	}

	// At this point both are non-nil
	if format.ClassName != nil {
		if model.ClassName == nil || *format.ClassName != *model.ClassName {
			modelClassName := "<nil>"
			if model.ClassName != nil {
				modelClassName = *model.ClassName
			}
			return false, fmt.Sprintf("pipeline class mismatch (model=%s, runtime=%s)",
				modelClassName, *format.ClassName)
		}
	}

	componentChecks := []struct {
		name          string
		model, format *v1beta1.DiffusionComponentSpec
	}{
		{name: "scheduler", model: model.Scheduler, format: format.Scheduler},
		{name: "textEncoder", model: model.TextEncoder, format: format.TextEncoder},
		{name: "tokenizer", model: model.Tokenizer, format: format.Tokenizer},
		{name: "transformer", model: model.Transformer, format: format.Transformer},
		{name: "vae", model: model.VAE, format: format.VAE},
	}

	for _, check := range componentChecks {
		if ok, reason := compareDiffusionComponent(check.name, check.model, check.format); !ok {
			return false, reason
		}
	}

	if len(format.AdditionalComponents) > 0 {
		if len(model.AdditionalComponents) == 0 {
			return false, "diffusion pipeline missing required additional components"
		}
		for key, formatComponent := range format.AdditionalComponents {
			modelComponent, exists := model.AdditionalComponents[key]
			if !exists {
				return false, fmt.Sprintf("diffusion component %s missing in model", key)
			}
			runtimeComponent := formatComponent
			if ok, reason := compareDiffusionComponent(key, &modelComponent, &runtimeComponent); !ok {
				return false, reason
			}
		}
	}

	return true, ""
}

func compareDiffusionComponent(name string, model *v1beta1.DiffusionComponentSpec, runtime *v1beta1.DiffusionComponentSpec) (bool, string) {
	if runtime == nil {
		return true, ""
	}

	if model == nil {
		return false, fmt.Sprintf("component %s required by runtime but not specified in model", name)
	}

	if runtime.Library != "" && runtime.Library != model.Library {
		return false, fmt.Sprintf("%s library mismatch (model=%s, runtime=%s)",
			name, model.Library, runtime.Library)
	}

	if runtime.Type != "" && runtime.Type != model.Type {
		return false, fmt.Sprintf("%s type mismatch (model=%s, runtime=%s)",
			name, model.Type, runtime.Type)
	}

	return true, ""
}

// compareModelFormatVersions compares model format versions based on operator.
func (m *DefaultRuntimeMatcher) compareModelFormatVersions(supportedFormat *v1beta1.ModelFormat, modelFormat *v1beta1.ModelFormat) bool {
	return compareVersionsWithOperator(*modelFormat.Version, *supportedFormat.Version, supportedFormat.Operator)
}

// compareModelFrameworkVersions compares model framework versions based on operator.
func (m *DefaultRuntimeMatcher) compareModelFrameworkVersions(supportedFramework *v1beta1.ModelFrameworkSpec, modelFramework *v1beta1.ModelFrameworkSpec) bool {
	return compareVersionsWithOperator(*modelFramework.Version, *supportedFramework.Version, supportedFramework.Operator)
}

// compareVersionsWithOperator parses the model and supported version strings and
// applies the comparison operator. Returns false if either version fails to parse.
// Unofficial (pre-release) versions always force an Equal comparison regardless
// of the operator.
func compareVersionsWithOperator(modelVersion, supportedVersion string, op *v1beta1.RuntimeSelectorOperator) bool {
	base, err := modelVer.Parse(modelVersion)
	if err != nil {
		return false
	}
	supported, err := modelVer.Parse(supportedVersion)
	if err != nil {
		return false
	}

	operator := getRuntimeSelectorOperator(op)

	// Unofficial versions or explicit Equal: fall back to equality.
	if operator == "Equal" ||
		modelVer.ContainsUnofficialVersion(base) ||
		modelVer.ContainsUnofficialVersion(supported) {
		return modelVer.Equal(supported, base)
	}

	// Differing precision or major prefix can't satisfy ordering comparisons.
	if base.Precision != supported.Precision || base.MajorPrefix != supported.MajorPrefix {
		return false
	}

	switch operator {
	case "GreaterThan":
		return modelVer.GreaterThan(supported, base)
	case "GreaterThanOrEqual":
		return modelVer.GreaterThanOrEqual(supported, base)
	default:
		return modelVer.Equal(supported, base)
	}
}

// checkModelSize verifies if the model size is within the runtime's supported range.
func (m *DefaultRuntimeMatcher) checkModelSize(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, runtimeName string) error {
	if model.ModelParameterSize == nil || runtime.ModelSizeRange == nil {
		return nil
	}

	modelSize := parseModelSize(*model.ModelParameterSize)
	minSize := parseModelSize(*runtime.ModelSizeRange.Min)
	maxSize := parseModelSize(*runtime.ModelSizeRange.Max)

	if modelSize < minSize || modelSize > maxSize {
		return &RuntimeCompatibilityError{
			RuntimeName: runtimeName,
			ModelName:   "", // Will be filled by caller if available
			ModelFormat: model.ModelFormat.Name,
			Reason: fmt.Sprintf("model size %s is outside supported range [%s, %s]",
				*model.ModelParameterSize, *runtime.ModelSizeRange.Min, *runtime.ModelSizeRange.Max),
		}
	}

	return nil
}

// Helper functions

// getRuntimeSelectorOperator returns a string representation of the RuntimeSelectorOperator.
func getRuntimeSelectorOperator(operator *v1beta1.RuntimeSelectorOperator) string {
	if operator == nil {
		return string(v1beta1.RuntimeSelectorOpEqual)
	}
	return string(*operator)
}

// runtimeHasAutoSelectFormat returns true if any supported format has AutoSelect=true.
func runtimeHasAutoSelectFormat(runtime *v1beta1.ServingRuntimeSpec) bool {
	for _, format := range runtime.SupportedModelFormats {
		if format.AutoSelect != nil && *format.AutoSelect {
			return true
		}
	}
	return false
}

// optionalEqual reports whether two optional fields are compatible: both nil,
// or both non-nil with equal dereferenced values. A mixed presence (one side
// requires the field, the other doesn't specify it) is considered a mismatch.
func optionalEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// matchOptionalVersions applies the standard tri-state version match: both
// versions set -> compare via cmp, both nil -> match, exactly one set -> no
// match. Used for ModelFormat.Version and ModelFrameworkSpec.Version which
// share the same shape.
func matchOptionalVersions(modelVersion, supportedVersion *string, cmp func() bool) bool {
	if modelVersion != nil && supportedVersion != nil {
		return cmp()
	}
	return modelVersion == nil && supportedVersion == nil
}

// parseModelSize converts a model size string (e.g., "7B", "13B", "70B") to a float64 value.
func parseModelSize(sizeStr string) float64 {
	var multiplier float64 = 1

	switch {
	case strings.HasSuffix(sizeStr, "T"):
		multiplier = 1_000_000_000_000
		sizeStr = strings.TrimSuffix(sizeStr, "T")
	case strings.HasSuffix(sizeStr, "B"):
		multiplier = 1_000_000_000
		sizeStr = strings.TrimSuffix(sizeStr, "B")
	case strings.HasSuffix(sizeStr, "M"):
		multiplier = 1_000_000
		sizeStr = strings.TrimSuffix(sizeStr, "M")
	}

	size, err := strconv.ParseFloat(sizeStr, 64)
	if err != nil {
		return 0
	}

	return size * multiplier
}

// getModelFormatLabel creates a standardized label string for model formats.
func getModelFormatLabel(model *v1beta1.BaseModelSpec) string {
	label := "mt:" + model.ModelFormat.Name
	if model.ModelFormat.Version != nil {
		label += ":" + *model.ModelFormat.Version
	}
	if model.ModelArchitecture != nil {
		label += ":" + *model.ModelArchitecture
	}
	if model.Quantization != nil {
		label += ":" + string(*model.Quantization)
	}
	if model.ModelFramework != nil {
		label += ":" + model.ModelFramework.Name
		if model.ModelFramework.Version != nil {
			label += ":" + *model.ModelFramework.Version
		}
	}
	return label
}
