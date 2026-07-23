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

// TestLifecycleConfig_ToGracePeriod pins the validation contract:
// chart default 60s parses cleanly, absent key yields zero (not an
// error), invalid duration is an error.
func TestLifecycleConfig_ToGracePeriod(t *testing.T) {
	t.Run("happy path matches chart default", func(t *testing.T) {
		cfg := &LifecycleConfig{StuckPodGracePeriod: "60s"}
		grace, err := cfg.ToGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, 60*time.Second, grace)
	})

	t.Run("absent key yields zero, no error", func(t *testing.T) {
		cfg := &LifecycleConfig{StuckPodGracePeriod: ""}
		grace, err := cfg.ToGracePeriod()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), grace)
	})

	invalid := []struct {
		name  string
		value string
	}{
		{"invalid duration", "not-a-duration"},
		{"zero duration", "0s"},
		{"negative duration", "-5s"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &LifecycleConfig{StuckPodGracePeriod: tt.value}
			grace, err := cfg.ToGracePeriod()
			assert.Error(t, err)
			assert.Equal(t, time.Duration(0), grace)
		})
	}
}

// TestAutoMigrateConfig_Validate pins the validation contract: the
// chart default converts cleanly, zero/negative maxAttempts is an error.
func TestAutoMigrateConfig_Validate(t *testing.T) {
	t.Run("happy path matches chart default", func(t *testing.T) {
		cfg := &AutoMigrateConfig{MaxAttempts: 3}
		err := cfg.Validate()
		require.NoError(t, err)
	})

	invalid := []struct {
		name        string
		maxAttempts int32
	}{
		{"zero maxAttempts", 0},
		{"negative maxAttempts", -1},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &AutoMigrateConfig{MaxAttempts: tt.maxAttempts}
			err := cfg.Validate()
			assert.Error(t, err)
		})
	}
}

// TestNewLifecycleConfig_StuckPodGracePeriod pins the load contract
// for the stuckPodGracePeriod key: present + valid parses, absent
// leaves the field empty (no error).
func TestNewLifecycleConfig_StuckPodGracePeriod(t *testing.T) {
	t.Run("valid stuckPodGracePeriod block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"stuckPodGracePeriod":"60s"}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "60s", cfg.StuckPodGracePeriod)
	})

	t.Run("lifecycle key without stuckPodGracePeriod yields config with empty field", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "", cfg.StuckPodGracePeriod)
	})
}

// TestForceDeleteConfig_ToPolicy pins the validation contract: a nil
// receiver (absent block) is unconfigured — nil policy, no error — the
// chart example converts 1:1, and every violation (either field missing,
// unparseable, or non-positive) is an error the caller treats as
// unconfigured (escalation OFF), never patched with fallback numbers.
func TestForceDeleteConfig_ToPolicy(t *testing.T) {
	t.Run("nil receiver yields nil policy, no error", func(t *testing.T) {
		var cfg *ForceDeleteConfig
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		assert.Nil(t, policy)
	})

	t.Run("happy path matches chart example", func(t *testing.T) {
		cfg := &ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "5m"}
		policy, err := cfg.ToPolicy()
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, 2*time.Minute, policy.OverdueSlack)
		assert.Equal(t, 5*time.Minute, policy.NodeUnreachableThreshold)
	})

	invalid := []struct {
		name string
		cfg  ForceDeleteConfig
	}{
		{"missing overdueSlack", ForceDeleteConfig{OverdueSlack: "", NodeUnreachableThreshold: "5m"}},
		{"missing nodeUnreachableThreshold", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: ""}},
		{"invalid overdueSlack duration", ForceDeleteConfig{OverdueSlack: "not-a-duration", NodeUnreachableThreshold: "5m"}},
		{"invalid nodeUnreachableThreshold duration", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "5 minutes"}},
		{"zero overdueSlack", ForceDeleteConfig{OverdueSlack: "0s", NodeUnreachableThreshold: "5m"}},
		{"zero nodeUnreachableThreshold", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "0s"}},
		{"negative overdueSlack", ForceDeleteConfig{OverdueSlack: "-2m", NodeUnreachableThreshold: "5m"}},
		{"negative nodeUnreachableThreshold", ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "-5m"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := tt.cfg.ToPolicy()
			assert.Error(t, err)
			assert.Nil(t, policy)
		})
	}
}

// TestNewLifecycleConfig_ForceDelete pins the load contract for the
// forceDelete key: present + valid parses, absent leaves the field nil
// (unconfigured — the escalation does not exist).
func TestNewLifecycleConfig_ForceDelete(t *testing.T) {
	t.Run("valid forceDelete block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"forceDelete":{"overdueSlack":"2m","nodeUnreachableThreshold":"5m"}}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.ForceDelete)
		assert.Equal(t, "2m", cfg.ForceDelete.OverdueSlack)
		assert.Equal(t, "5m", cfg.ForceDelete.NodeUnreachableThreshold)
	})

	t.Run("lifecycle key without forceDelete yields config with nil ForceDelete", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Nil(t, cfg.ForceDelete)
	})
}

// TestNewLifecycleConfig_AutoMigrate pins the load contract for the
// autoMigrate key: present + valid parses, absent leaves the field nil.
func TestNewLifecycleConfig_AutoMigrate(t *testing.T) {
	t.Run("valid autoMigrate block", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{"autoMigrate":{"maxAttempts":3}}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.AutoMigrate)
		assert.Equal(t, int32(3), cfg.AutoMigrate.MaxAttempts)
	})

	t.Run("lifecycle key without autoMigrate yields config with nil AutoMigrate", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				LifecycleConfigName: `{}`,
			},
		}
		_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
		require.NoError(t, err)

		cfg, err := NewLifecycleConfig(clientset)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Nil(t, cfg.AutoMigrate)
	})
}
