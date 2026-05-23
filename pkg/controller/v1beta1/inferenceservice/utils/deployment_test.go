package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestDetermineDeploymentModes_PerComponentAnnotations(t *testing.T) {
	omeAnnot := map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	rawAnnot := map[string]string{
		constants.DeploymentMode: string(constants.RawDeployment),
	}

	tests := []struct {
		name           string
		engine         *v1beta1.EngineSpec
		decoder        *v1beta1.DecoderSpec
		router         *v1beta1.RouterSpec
		wantEngineMode constants.DeploymentModeType
		wantDecoder    constants.DeploymentModeType
		wantRouter     constants.DeploymentModeType
	}{
		{
			name: "Engine-only OMENative; absent Decoder/Router default to Raw",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Decoder annotation = OMENative is respected (foundation for PD)",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Router annotation = OMENative is respected",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			router: &v1beta1.RouterSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.OMENative,
		},
		{
			name: "Three-way PD: Engine + Decoder + Router all opt into OMENative",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			router: &v1beta1.RouterSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.OMENative,
		},
		{
			name: "Mixed: Engine OMENative, Decoder Raw (explicit)",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: rawAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Decoder with Leader/Worker but no annotation falls back to MultiNode",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: rawAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{},
			},
			wantEngineMode: constants.RawDeployment,
			wantDecoder:    constants.MultiNode,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Decoder annotation takes precedence over Leader/Worker presence",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
				Leader:                 &v1beta1.LeaderSpec{},
				Worker:                 &v1beta1.WorkerSpec{},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.RawDeployment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engineMode, decoderMode, routerMode, err := DetermineDeploymentModes(tt.engine, tt.decoder, tt.router, nil)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantEngineMode, engineMode, "engine mode mismatch")
			assert.Equal(t, tt.wantDecoder, decoderMode, "decoder mode mismatch")
			assert.Equal(t, tt.wantRouter, routerMode, "router mode mismatch")
		})
	}
}

func TestDetermineDeploymentModes_NilEngineRejected(t *testing.T) {
	_, _, _, err := DetermineDeploymentModes(nil, nil, nil, nil)
	assert.Error(t, err)
}
