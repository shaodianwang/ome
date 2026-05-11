package modelconfig

import "testing"

// TestGenericContextLengthCascade pins down the order in which
// GenericModelConfig.GetContextLength resolves a context length:
// max_position_embeddings, then max_sequence_length, then seq_length
// (ChatGLM family), then model_max_length (Baichuan family). First
// non-zero wins. Nested LLM-config fallback is covered by
// probeNestedConfig and tested elsewhere.
func TestGenericContextLengthCascade(t *testing.T) {
	cases := []struct {
		name string
		json string
		want int
	}{
		{
			name: "max_position_embeddings wins when present",
			json: `{"model_type":"unknown","max_position_embeddings":4096,"max_sequence_length":2048,"seq_length":1024,"model_max_length":512}`,
			want: 4096,
		},
		{
			name: "falls back to max_sequence_length",
			json: `{"model_type":"unknown","max_sequence_length":2048,"seq_length":1024}`,
			want: 2048,
		},
		{
			name: "falls back to seq_length (ChatGLM)",
			json: `{"model_type":"unknown","seq_length":8192}`,
			want: 8192,
		},
		{
			name: "falls back to model_max_length (Baichuan)",
			json: `{"model_type":"unknown","model_max_length":4096}`,
			want: 4096,
		},
		{
			name: "all absent => zero",
			json: `{"model_type":"unknown","hidden_size":4096}`,
			want: 0,
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
			if got := model.GetContextLength(); got != tc.want {
				t.Errorf("GetContextLength() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestGenericNestedDtypeRescue covers nested text_config.dtype being
// promoted to top-level TorchDtype when no top-level torch_dtype is
// present (qwen3_5 family stores dtype under text_config.dtype).
func TestGenericNestedDtypeRescue(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{
			name: "top-level torch_dtype wins",
			json: `{"model_type":"unknown","torch_dtype":"bfloat16","text_config":{"dtype":"float16"}}`,
			want: "bfloat16",
		},
		{
			name: "rescues nested text_config.dtype",
			json: `{"model_type":"unknown","text_config":{"dtype":"float16"}}`,
			want: "float16",
		},
		{
			name: "rescues nested text_config.torch_dtype too (existing)",
			json: `{"model_type":"unknown","text_config":{"torch_dtype":"float16"}}`,
			want: "float16",
		},
		{
			name: "neither present => empty",
			json: `{"model_type":"unknown","hidden_size":4096}`,
			want: "",
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
			if got := model.GetTorchDtype(); got != tc.want {
				t.Errorf("GetTorchDtype() = %q, want %q", got, tc.want)
			}
		})
	}
}
