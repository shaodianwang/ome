package modelparser

import (
	"strings"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/modelconfig"
)

// capabilityRule classifies a model into a list of v1beta1.ModelCapability
// values. A nil return means "this rule does not match — try the next one."
// Order matters; the first non-nil match wins.
type capabilityRule func(modelType, architecture string, hf modelconfig.HuggingFaceModel) []v1beta1.ModelCapability

// capabilityRules is the ordered classifier table. Order matches the
// behavior of the previous if/else chain: diffusion first (its
// architecture-string sniffing only applies when the model is a
// diffusion pipeline), then NemotronH_Nano's special case, then the
// vision short-circuit (vision-capable text models always classify
// as ImageTextToText regardless of other architecture clues), then
// omni and ASR (which trigger on architecture substrings only when
// vision didn't match), then embedding. Default: TextToText.
var capabilityRules = []capabilityRule{
	diffusionRule,
	nemotronHNanoRule,
	visionRule,
	omniRule,
	asrRule,
	embeddingRule,
}

// determineCapabilities returns the v1beta1.ModelCapability list for a
// HuggingFaceModel. Defaults to TextToText if no rule matches.
func determineCapabilities(hf modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	mt := strings.ToLower(hf.GetModelType())
	arch := strings.ToLower(hf.GetArchitecture())
	for _, rule := range capabilityRules {
		if caps := rule(mt, arch, hf); caps != nil {
			return caps
		}
	}
	return []v1beta1.ModelCapability{v1beta1.ModelCapabilityTextToText}
}

func diffusionRule(_, architecture string, hf modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	dm, ok := hf.(modelconfig.HuggingFaceDiffusionModel)
	if !ok || dm.GetDiffusionModel() == nil {
		return nil
	}
	switch {
	case containsAny(architecture, "imageedit", "pix2pix", "img2img", "inpaint"):
		return []v1beta1.ModelCapability{v1beta1.ModelCapabilityImageTextToImage}
	case containsAny(architecture, "image", "pix", "stablediffusion"):
		return []v1beta1.ModelCapability{v1beta1.ModelCapabilityTextToImage}
	case containsAny(architecture, "texttovideo", "t2v"):
		return []v1beta1.ModelCapability{v1beta1.ModelCapabilityTextToVideo}
	case strings.Contains(architecture, "video"):
		return []v1beta1.ModelCapability{v1beta1.ModelCapabilityImageTextToVideo}
	}
	// Diffusion sub-interface but no architecture clue — return an
	// empty (non-nil) slice so the dispatcher stops here. This
	// matches today's behavior in determineModelCapabilitiesFromHF
	// where an unmatched diffusion model returned an empty list.
	return []v1beta1.ModelCapability{}
}

func nemotronHNanoRule(modelType, _ string, _ modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	if !strings.Contains(modelType, "nemotronh_nano") {
		return nil
	}
	return []v1beta1.ModelCapability{
		v1beta1.ModelCapabilityImageTextToText,
		v1beta1.ModelCapabilityTextToText,
		v1beta1.ModelCapabilityAudioToText,
	}
}

func visionRule(_, _ string, hf modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	if !hf.HasVision() {
		return nil
	}
	return []v1beta1.ModelCapability{v1beta1.ModelCapabilityImageTextToText}
}

func omniRule(_, architecture string, _ modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	if !strings.Contains(architecture, "omni") {
		return nil
	}
	return []v1beta1.ModelCapability{
		v1beta1.ModelCapabilityTextToAudio,
		v1beta1.ModelCapabilityImageTextToAudio,
		v1beta1.ModelCapabilityVideoTextToAudio,
		v1beta1.ModelCapabilityAudioToText,
		v1beta1.ModelCapabilityAudioToAudio,
	}
}

func asrRule(_, architecture string, _ modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	if !containsAny(architecture, "whisper", "wav2vec2", "hubert", "fortc", "forspeechtotext") {
		return nil
	}
	return []v1beta1.ModelCapability{v1beta1.ModelCapabilityAudioToText}
}

func embeddingRule(modelType, architecture string, hf modelconfig.HuggingFaceModel) []v1beta1.ModelCapability {
	if hf.IsEmbedding() ||
		strings.Contains(architecture, "embedding") ||
		strings.Contains(architecture, "sentence") ||
		strings.Contains(modelType, "bert") ||
		(strings.Contains(modelType, "mistral") && strings.Contains(architecture, "mistralmodel")) {
		return []v1beta1.ModelCapability{v1beta1.ModelCapabilityEmbedding}
	}
	return nil
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
