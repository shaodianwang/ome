package modelparser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/hfutil/modelconfig"
)

// TestDetermineCapabilities pins down the table-driven capability
// classifier. Cases mirror what the previous if/else chain did,
// including its ordering quirks: the vision short-circuit beats the
// omni and ASR architecture sniffs (so a vision-flagged model with
// "Omni" in its architecture still classifies as ImageTextToText).
func TestDetermineCapabilities(t *testing.T) {
	testCases := []struct {
		name                 string
		mockModel            modelconfig.HuggingFaceModel
		expectedCapabilities []v1beta1.ModelCapability
	}{
		{
			name: "Text Generation Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "llama",
				architecture: "LlamaForCausalLM",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityTextToText},
		},
		{
			name: "Vision Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "clip",
				architecture: "CLIPModel",
				hasVision:    true,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityImageTextToText},
		},
		{
			name: "Text Embedding Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "bert",
				architecture: "BertModel",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityEmbedding},
		},
		{
			name: "Sentence Transformer Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "sentence-transformer",
				architecture: "SentenceTransformerModel",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityEmbedding},
		},
		{
			name: "Special Case Mistral Embedding Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "mistral",
				architecture: "MistralModel", // Not MistralForCausalLM
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityEmbedding},
		},
		{
			name: "Vision-capable LLM",
			mockModel: &mockHuggingFaceModel{
				modelType:    "gemma",
				architecture: "GemmaForCausalLM",
				hasVision:    true,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityImageTextToText},
		},
		{
			name: "Diffusion Model",
			mockModel: &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "StableDiffusionPipeline",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{ClassName: "StableDiffusionPipeline"},
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityTextToImage},
		},
		{
			name: "Diffusion QwenImagePipeline",
			mockModel: &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "QwenImagePipeline",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{ClassName: "QwenImagePipeline"},
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityTextToImage},
		},
		{
			name: "Diffusion QwenImageEditPlus",
			mockModel: &mockDiffusionModel{
				mockHuggingFaceModel: mockHuggingFaceModel{
					modelType:    "diffusers",
					architecture: "QwenImageEditPlus",
					hasVision:    true,
				},
				diffusionModel: &modelconfig.DiffusionPipelineSpec{ClassName: "QwenImageEditPlus"},
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityImageTextToImage},
		},
		{
			name: "Transformer Qwen3 Omni",
			mockModel: &mockHuggingFaceModel{
				modelType:    "qwen",
				architecture: "Qwen3OmniMoeForConditionalGeneration",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{
				v1beta1.ModelCapabilityTextToAudio,
				v1beta1.ModelCapabilityImageTextToAudio,
				v1beta1.ModelCapabilityVideoTextToAudio,
				v1beta1.ModelCapabilityAudioToText,
				v1beta1.ModelCapabilityAudioToAudio,
			},
		},
		{
			name: "NemotronH_Nano Omni Model - falls through to vision due to case mismatch",
			mockModel: &mockHuggingFaceModel{
				modelType:    "NemotronH_Nano_Omni_Reasoning_V3",
				architecture: "NemotronH_Nano_Omni_Reasoning_V3",
				hasVision:    true,
			},
			expectedCapabilities: []v1beta1.ModelCapability{
				v1beta1.ModelCapabilityImageTextToText,
				v1beta1.ModelCapabilityTextToText,
				v1beta1.ModelCapabilityAudioToText,
			},
		},
		{
			name: "Whisper ASR Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "whisper",
				architecture: "WhisperForConditionalGeneration",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityAudioToText},
		},
		{
			name: "Wav2Vec2 ASR Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "wav2vec2",
				architecture: "Wav2Vec2ForCTC",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityAudioToText},
		},
		{
			name: "HuBERT ASR Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "hubert",
				architecture: "HubertForCTC",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityAudioToText},
		},
		{
			name: "Generic ForSpeechToText Model",
			mockModel: &mockHuggingFaceModel{
				modelType:    "seamless_m4t",
				architecture: "SeamlessM4TForSpeechToText",
				hasVision:    false,
			},
			expectedCapabilities: []v1beta1.ModelCapability{v1beta1.ModelCapabilityAudioToText},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capabilities := determineCapabilities(tc.mockModel)
			assert.Equal(t, tc.expectedCapabilities, capabilities,
				"Capability mismatch for test case %s", tc.name)
		})
	}
}
