package modelconfig

import "testing"

// TestGenericVisionShapeDetection covers the shape signals the
// generic resolver looks for to flag a model as multimodal:
//
//   - vision_config block (most VL models)
//   - mm_vision_tower (LLaVA v1.5 style — pre-vision_config convention)
//   - image_token_id / image_token_index (Qwen2-VL, Qwen3-VL, Gemma3,
//     Llama4, MLlama, LLaVA-1.5-HF — usually paired with vision_config
//     but one of these alone is enough)
//
// Pure JSON-shape; no model_type allowlist.
func TestGenericVisionShapeDetection(t *testing.T) {
	cases := []struct {
		name string
		json string
		want bool
	}{
		// Unknown model_types so the generic resolver — not a per-model
		// parser — is what runs. These tests pin down GenericModelConfig's
		// HasVision behavior.
		{
			name: "vision_config present",
			json: `{"model_type":"unknown_vl","vision_config":{"hidden_size":1024}}`,
			want: true,
		},
		{
			name: "mm_vision_tower present (LLaVA v1.5 style)",
			json: `{"model_type":"unknown_mm","mm_vision_tower":"openai/clip-vit"}`,
			want: true,
		},
		{
			name: "image_token_id present (Qwen2-VL style)",
			json: `{"model_type":"unknown_qwen","image_token_id":151655}`,
			want: true,
		},
		{
			name: "image_token_index present (Gemma3 style)",
			json: `{"model_type":"unknown_gemma","image_token_index":262144}`,
			want: true,
		},
		{
			name: "img_processor present (Phi-3 Vision style)",
			json: `{"model_type":"unknown_phi","img_processor":{"name":"clip_vision_model"}}`,
			want: true,
		},
		{
			name: "no vision signals",
			json: `{"model_type":"unknown_text","hidden_size":4096}`,
			want: false,
		},
		{
			name: "unrelated mm_ field is not enough",
			json: `{"model_type":"unknown_text","mm_use_im_start_end":false}`,
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, err := ParseModelConfig(ModelConfigInput{
				Path: "config.json",
				Data: []byte(tc.json),
			})
			if err != nil {
				t.Fatalf("ParseModelConfig: %v", err)
			}
			if got := model.HasVision(); got != tc.want {
				t.Errorf("HasVision() = %v, want %v", got, tc.want)
			}
		})
	}
}
