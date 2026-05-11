package modelconfig

import (
	"strings"
	"unicode"
)

// Capability identifies the kind of inference task a model serves.
// Values mirror v1beta1.ModelCapability so the modelparser mapping
// is a 1:1 string-keyed lookup; modelconfig itself stays free of
// any OME-API dependency.
type Capability string

const (
	CapabilityUnknown          Capability = ""
	CapabilityTextToText       Capability = "TEXT_TO_TEXT"
	CapabilityImageTextToText  Capability = "IMAGE_TEXT_TO_TEXT"
	CapabilityTextToImage      Capability = "TEXT_TO_IMAGE"
	CapabilityImageTextToImage Capability = "IMAGE_TEXT_TO_IMAGE"
	CapabilityTextToVideo      Capability = "TEXT_TO_VIDEO"
	CapabilityImageTextToVideo Capability = "IMAGE_TEXT_TO_VIDEO"
	CapabilityTextToAudio      Capability = "TEXT_TO_AUDIO"
	CapabilityImageTextToAudio Capability = "IMAGE_TEXT_TO_AUDIO"
	CapabilityVideoTextToAudio Capability = "VIDEO_TEXT_TO_AUDIO"
	CapabilityAudioToText      Capability = "AUDIO_TO_TEXT"
	CapabilityAudioToAudio     Capability = "AUDIO_TO_AUDIO"
	CapabilityEmbedding        Capability = "EMBEDDING"
)

// hasArchToken reports whether arch contains the given token as a
// distinct CamelCase or underscore-separated segment. Splits on
// underscore boundaries and on the boundary BEFORE each uppercase
// letter (when the previous rune is not also uppercase), so
// "Qwen3OmniMoeForConditionalGeneration" yields tokens including
// "Omni" and hasArchToken(..., "Omni") reports true. "Demonic"
// does not match "Omni" because "Demonic" splits to ["Demonic"]
// (single token), and the comparison is exact-token-match, not
// substring.
//
// Numbers stay attached to adjacent letters: "Qwen3" is a single
// token. Comparison is case-sensitive — HF architecture names are
// stable in their original casing.
func hasArchToken(arch, token string) bool {
	if arch == "" || token == "" {
		return false
	}
	for _, segment := range splitArchTokens(arch) {
		if segment == token {
			return true
		}
	}
	return false
}

// splitArchTokens splits an architecture string on underscores and
// on the boundary BEFORE each uppercase letter, except when the
// previous rune is also uppercase (so "BERT" / "MLP" stay as one
// token). Digits and lowercase letters both trigger a split before
// the next uppercase: "Qwen3Omni" → ["Qwen3", "Omni"], "fooBar" →
// ["foo", "Bar"].
func splitArchTokens(arch string) []string {
	var out []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			out = append(out, string(current))
			current = current[:0]
		}
	}
	runes := []rune(arch)
	for i, r := range runes {
		if r == '_' {
			flush()
			continue
		}
		if i > 0 && unicode.IsUpper(r) && !unicode.IsUpper(runes[i-1]) {
			flush()
		}
		current = append(current, r)
	}
	flush()
	return out
}

// hasAnySuffix reports whether arch ends with any of the suffixes
// (case-sensitive). HF architecture names are stable in their
// original casing, so HasSuffix is the right primitive for
// identity-style matches like "ForCausalLM", "ForCTC".
func hasAnySuffix(arch string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(arch, suffix) {
			return true
		}
	}
	return false
}

// containsAny reports whether s contains any of the substrings.
// Used only for diffusion pipeline class names, where substring
// is genuinely the right primitive because pipeline names are
// composite ("StableDiffusionImg2ImgPipeline").
func containsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// capabilityRule examines a model and returns the capabilities it
// classifies it as. Returning nil means "this rule does not apply
// to this model — try the next one." The dispatcher walks rules
// in order and returns the first non-nil result.
type capabilityRule func(hf HuggingFaceModel) []Capability

// capabilityRules is the ordered list of classifiers. First non-nil
// result wins. Order is load-bearing — comments below call out the
// constraints that drove it.
var capabilityRules = []capabilityRule{
	// Diffusion first: the diffusion sub-interface is a strong,
	// structural signal that beats any architecture-suffix
	// coincidence.
	diffusionRule,

	// NemotronH-Nano next: this is genuinely multi-capability
	// (image+text, text, audio outputs). MUST beat the vision
	// short-circuit, or it'd collapse to ImageTextToText only.
	nemotronHNanoRule,

	// Omni next: same reasoning — omni implies audio-output
	// capabilities, not just whatever the vision detection would
	// say. Deliberate behavior change vs today's modelparser
	// (which had vision before omni).
	omniRule,

	// Vision short-circuit: a vision-capable LLM with no omni /
	// nemotron signal classifies as ImageTextToText. Beats ASR
	// matching: a vision model with "ForConditionalGeneration"
	// suffix should not become AudioToText just because of
	// suffix coincidence.
	visionRule,

	// ASR family: stable suffix patterns (ForCTC, ForSpeechToText,
	// the exact Whisper class).
	asrRule,

	// Embedding: combination of the explicit IsEmbedding flag,
	// embedding-y architecture tokens, and the e5-mistral special
	// case (MistralModel without a CausalLM head).
	embeddingRule,
}

// generativeTextSuffixes is the catch-all for unrecognized models
// whose architecture name signals a generative-text head. Used only
// after every rule returned nil.
var generativeTextSuffixes = []string{
	"ForCausalLM",
	"ForConditionalGeneration",
	"LMHeadModel",
}

// classifyCapabilities is the dispatcher. First matching rule wins;
// if no rule matches, fall back to TextToText for recognized text
// LLMs, otherwise Unknown.
func classifyCapabilities(hf HuggingFaceModel) []Capability {
	for _, rule := range capabilityRules {
		if caps := rule(hf); caps != nil {
			return caps
		}
	}
	if hasAnySuffix(hf.GetArchitecture(), generativeTextSuffixes...) {
		return []Capability{CapabilityTextToText}
	}
	return []Capability{CapabilityUnknown}
}

func diffusionRule(hf HuggingFaceModel) []Capability {
	dm, ok := hf.(HuggingFaceDiffusionModel)
	if !ok || dm.GetDiffusionModel() == nil {
		return nil
	}
	cls := dm.GetDiffusionModel().ClassName
	switch {
	case containsAny(cls, "ImageEdit", "Pix2Pix", "Img2Img", "Inpaint"):
		return []Capability{CapabilityImageTextToImage}
	case containsAny(cls, "TextToVideo", "T2V"):
		return []Capability{CapabilityTextToVideo}
	case strings.Contains(cls, "Video"):
		// Catch-all after the T2V check above.
		return []Capability{CapabilityImageTextToVideo}
	case containsAny(cls, "Image", "Pix", "StableDiffusion", "Flux"):
		return []Capability{CapabilityTextToImage}
	}
	// Diffusion sub-interface but no recognized pipeline class.
	// Return Unknown so the warning fires and we add a rule.
	return []Capability{CapabilityUnknown}
}

func nemotronHNanoRule(hf HuggingFaceModel) []Capability {
	if !strings.Contains(strings.ToLower(hf.GetModelType()), "nemotronh_nano") {
		return nil
	}
	return []Capability{
		CapabilityImageTextToText,
		CapabilityTextToText,
		CapabilityAudioToText,
	}
}

func omniRule(hf HuggingFaceModel) []Capability {
	if !hasArchToken(hf.GetArchitecture(), "Omni") {
		return nil
	}
	return []Capability{
		CapabilityTextToAudio,
		CapabilityImageTextToAudio,
		CapabilityVideoTextToAudio,
		CapabilityAudioToText,
		CapabilityAudioToAudio,
	}
}

func visionRule(hf HuggingFaceModel) []Capability {
	if !hf.HasVision() {
		return nil
	}
	return []Capability{CapabilityImageTextToText}
}

func asrRule(hf HuggingFaceModel) []Capability {
	arch := hf.GetArchitecture()
	if hasAnySuffix(arch, "ForCTC", "ForSpeechToText") ||
		arch == "WhisperForConditionalGeneration" {
		return []Capability{CapabilityAudioToText}
	}
	return nil
}

func embeddingRule(hf HuggingFaceModel) []Capability {
	if hf.IsEmbedding() {
		return []Capability{CapabilityEmbedding}
	}
	arch := hf.GetArchitecture()
	// BERT-architecture-named models (BertModel, BertForMaskedLM,
	// BertForSequenceClassification, ...). Today's modelconfig
	// IsEmbedding() always returns false in real configs, so this
	// architecture token check is the practical embedding detector
	// for the BERT family.
	if hasArchToken(arch, "Bert") {
		return []Capability{CapabilityEmbedding}
	}
	if hasArchToken(arch, "Embedding") || hasArchToken(arch, "Sentence") {
		return []Capability{CapabilityEmbedding}
	}
	// e5-mistral and similar use MistralModel (no CausalLM head)
	// for embedding output. The model_type is "mistral" but the
	// architecture is the bare encoder.
	if strings.ToLower(hf.GetModelType()) == "mistral" && arch == "MistralModel" {
		return []Capability{CapabilityEmbedding}
	}
	return nil
}
