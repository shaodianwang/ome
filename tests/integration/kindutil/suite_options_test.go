package kindutil

import "testing"

func TestShouldApplyCRDs(t *testing.T) {
	tests := []struct {
		name string
		opts SuiteOpts
		want bool
	}{
		{
			name: "in-process defaults to OME CRDs",
			want: true,
		},
		{
			name: "in-process applies explicit CRDs",
			opts: SuiteOpts{CRDPaths: []string{"example.yaml"}},
			want: true,
		},
		{
			name: "deployed chart owns OME CRDs",
			opts: SuiteOpts{Deployed: &DeployedOME{}},
			want: false,
		},
		{
			name: "deployed suite applies optional CRDs",
			opts: SuiteOpts{
				Deployed: &DeployedOME{},
				CRDPaths: []string{"optional.yaml"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldApplyCRDs(tt.opts); got != tt.want {
				t.Fatalf("shouldApplyCRDs() = %t, want %t", got, tt.want)
			}
		})
	}
}
