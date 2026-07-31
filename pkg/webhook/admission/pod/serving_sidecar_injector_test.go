package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestNewServingSidecarInjector(t *testing.T) {
	tests := []struct {
		name        string
		configData  map[string]string
		expectError bool
	}{
		{
			name: "valid config",
			configData: map[string]string{
				servingSidecarConfigMapKeyName: `{"image":"ome/agent:1","compartmentId":"c1","authType":"InstancePrincipal"}`,
			},
			expectError: false,
		},
		{
			name:        "missing key yields zero-value injector",
			configData:  map[string]string{},
			expectError: false,
		},
		{
			name: "malformed JSON returns error instead of panicking",
			configData: map[string]string{
				servingSidecarConfigMapKeyName: `{"image": not-json`,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configMap := &v1.ConfigMap{Data: tt.configData}
			var injector *ServingSidecarInjector
			var err error
			assert.NotPanics(t, func() {
				injector, err = newServingSidecarInjector(configMap)
			})
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, injector)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, injector)
			}
		})
	}
}
