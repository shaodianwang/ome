package controllerconfig

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ome-projects/ome/pkg/constants"
)

// TestNewLifecycleConfig pins the load contract for the "lifecycle"
// ConfigMap key: present + valid parses into LifecycleConfig; absent
// yields (nil, nil) — unconfigured, the caller fails safe (Held) —
// never an error and never a fabricated default.
func TestNewLifecycleConfig(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		expectNil     bool
		expectedError bool
		validate      func(*testing.T, *LifecycleConfig)
	}{
		{
			name: "valid updateRetry block",
			configMapData: map[string]string{
				LifecycleConfigName: `{"updateRetry":{"maxAttempts":3,"initialDelay":"1m","maxDelay":"30m","multiplier":2.0}}`,
			},
			validate: func(t *testing.T, cfg *LifecycleConfig) {
				require.NotNil(t, cfg.UpdateRetry)
				assert.Equal(t, int32(3), cfg.UpdateRetry.MaxAttempts)
				assert.Equal(t, "1m", cfg.UpdateRetry.InitialDelay)
				assert.Equal(t, "30m", cfg.UpdateRetry.MaxDelay)
				assert.Equal(t, 2.0, cfg.UpdateRetry.Multiplier)
			},
		},
		{
			// Absent lifecycle key: unconfigured, NOT an error. The nil
			// config is the fail-safe-Held signal for the IR reconciler.
			name:          "absent lifecycle key yields nil config, no error",
			configMapData: map[string]string{},
			expectNil:     true,
		},
		{
			name: "lifecycle key without updateRetry yields config with nil UpdateRetry",
			configMapData: map[string]string{
				LifecycleConfigName: `{}`,
			},
			validate: func(t *testing.T, cfg *LifecycleConfig) {
				assert.Nil(t, cfg.UpdateRetry)
			},
		},
		{
			name: "malformed json is an error",
			configMapData: map[string]string{
				LifecycleConfigName: `{not-json`,
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			configMap := &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Data: tt.configMapData,
			}
			_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
			require.NoError(t, err)

			cfg, err := NewLifecycleConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.expectNil {
				assert.Nil(t, cfg)
				return
			}
			require.NotNil(t, cfg)
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

// TestUpdateRetryConfig_ToPolicy pins the validation contract: the chart
// defaults convert 1:1, and every violation is an error (the caller
// treats an invalid policy as unconfigured — fail-safe Held — rather
// than silently patching it with fallback numbers).
func TestUpdateRetryConfig_ToPolicy(t *testing.T) {
	t.Run("happy path matches chart defaults", func(t *testing.T) {
		cfg := &UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 2.0}
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, int32(3), policy.MaxAttempts)
		assert.Equal(t, time.Minute, policy.InitialDelay)
		assert.Equal(t, 30*time.Minute, policy.MaxDelay)
		assert.Equal(t, 2.0, policy.Multiplier)
	})

	invalid := []struct {
		name string
		cfg  UpdateRetryConfig
	}{
		{"maxAttempts zero", UpdateRetryConfig{MaxAttempts: 0, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 2.0}},
		{"invalid initialDelay duration", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "not-a-duration", MaxDelay: "30m", Multiplier: 2.0}},
		{"invalid maxDelay duration", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "30 minutes", Multiplier: 2.0}},
		{"multiplier below one", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 0.5}},
		{"zero initialDelay", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "0s", MaxDelay: "30m", Multiplier: 2.0}},
		{"zero maxDelay", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "1m", MaxDelay: "0s", Multiplier: 2.0}},
		{"maxDelay below initialDelay", UpdateRetryConfig{MaxAttempts: 3, InitialDelay: "10m", MaxDelay: "1m", Multiplier: 2.0}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := tt.cfg.ToPolicy()
			assert.Error(t, err)
			assert.Nil(t, policy)
		})
	}
}
