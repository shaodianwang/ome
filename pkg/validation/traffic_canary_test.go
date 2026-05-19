package validation

import (
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestValidateCanaryPercent(t *testing.T) {
	pct := func(v int64) *int64 { return &v }

	tests := []struct {
		name         string
		spec         *v1beta1.InferenceServiceSpec
		wantOK       bool
		wantContains string
	}{
		{
			name:   "nil spec",
			spec:   nil,
			wantOK: true,
		},
		{
			name:   "no components",
			spec:   &v1beta1.InferenceServiceSpec{},
			wantOK: true,
		},
		{
			name: "engine canary in range",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(10)},
				},
			},
			wantOK: true,
		},
		{
			name: "engine canary boundary 0 is OK (blue-green standby)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(0)},
				},
			},
			wantOK: true,
		},
		{
			name: "engine canary boundary 100 is OK (promote)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(100)},
				},
			},
			wantOK: true,
		},
		{
			name: "engine canary negative",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(-1)},
				},
			},
			wantContains: "InvalidCanaryPercent",
		},
		{
			name: "decoder canary over 100",
			spec: &v1beta1.InferenceServiceSpec{
				Decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(101)},
				},
			},
			wantContains: "InvalidCanaryPercent",
		},
		{
			name: "router canary out of range",
			spec: &v1beta1.InferenceServiceSpec{
				Router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(150)},
				},
			},
			wantContains: "InvalidCanaryPercent",
		},
		{
			name: "PD engine+decoder equal is OK",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(25)},
				},
				Decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(25)},
				},
			},
			wantOK: true,
		},
		{
			name: "PD engine+decoder mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(10)},
				},
				Decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(20)},
				},
			},
			wantContains: "MismatchedCanaryPercent",
		},
		{
			name: "engine-only canary in PD-shaped spec is OK (defaulter mirrors decoder later)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(10)},
				},
				Decoder: &v1beta1.DecoderSpec{},
			},
			wantOK: true,
		},
		{
			name: "router canary independent of engine in PD is OK",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(10)},
				},
				Decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(10)},
				},
				Router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{CanaryTrafficPercent: pct(50)},
				},
			},
			wantOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCanaryPercent(tc.spec)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected ok, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantContains)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantContains, err)
			}
		})
	}
}
