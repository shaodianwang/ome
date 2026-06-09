package isvc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
)

// =============================================================================
// Helper Functions
// =============================================================================

// createTestScheme creates a runtime scheme with v1beta1 types for testing
func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	return scheme
}

// createFakeClient creates a fake client with optional ClusterBaseModel or BaseModel objects
func createFakeClient(t *testing.T, models ...client.Object) client.Client {
	scheme := createTestScheme()
	objects := []client.Object{}
	objects = append(objects, models...)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

// Helper function to create int pointers
func intPtr(i int) *int {
	return &i
}

// createBasicInferenceService creates a basic InferenceService for testing
func createBasicInferenceService(name, namespace string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.InferenceServiceSpec{},
	}
}

// =============================================================================
// Main DefaultInferenceService Tests
// =============================================================================

func TestDefaultInferenceService(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		deployConfig    *controllerconfig.DeployConfig
		wantAnnotations map[string]string
	}{
		{
			name:            "empty InferenceService should have no annotations",
			isvc:            createBasicInferenceService("test-isvc", "default"),
			deployConfig:    nil,
			wantAnnotations: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := createFakeClient(t)
			err := DefaultInferenceService(ctx, c, tt.isvc, tt.deployConfig)
			require.NoError(t, err)

			if tt.wantAnnotations == nil {
				if tt.isvc.Annotations != nil {
					assert.Empty(t, tt.isvc.Annotations, "Expected no annotations")
				}
				return
			}
			require.NotNil(t, tt.isvc.Annotations)
			for key, expectedVal := range tt.wantAnnotations {
				actualVal, exists := tt.isvc.Annotations[key]
				assert.True(t, exists, "expected annotation %q", key)
				assert.Equal(t, expectedVal, actualVal)
			}
		})
	}
}

// =============================================================================
// Deployment Mode Detection Tests
// =============================================================================

func TestDeploymentModeDetection(t *testing.T) {
	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		deployConfig *controllerconfig.DeployConfig
		expectedMode string
	}{
		{
			name: "engine and decoder should set PDDisaggregated",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
				},
			},
			deployConfig: nil,
			expectedMode: string(constants.PDDisaggregated),
		},
		{
			name: "engine with leader and worker should set OMENative",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{},
						Worker: &v1beta1.WorkerSpec{Size: intPtr(2)},
					},
				},
			},
			deployConfig: nil,
			expectedMode: string(constants.OMENative),
		},
		{
			name: "engine without leader/worker should default to RawDeployment",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			deployConfig: &controllerconfig.DeployConfig{DefaultDeploymentMode: string(constants.RawDeployment)},
			expectedMode: string(constants.RawDeployment),
		},
		{
			name: "engine with worker size zero should default to RawDeployment",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{},
						Worker: &v1beta1.WorkerSpec{Size: intPtr(0)},
					},
				},
			},
			deployConfig: &controllerconfig.DeployConfig{DefaultDeploymentMode: string(constants.RawDeployment)},
			expectedMode: string(constants.RawDeployment),
		},
		{
			// A pre-existing annotation is preserved verbatim — even a
			// legacy "MultiNode" value the defaulter no longer stamps.
			// This pins the upgrade-safety contract: admission does not
			// rewrite or reject persisted annotations.
			name: "existing deployment mode should not be overridden",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.DeploymentMode: "MultiNode",
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
				},
			},
			deployConfig: nil,
			expectedMode: "MultiNode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := createFakeClient(t)
			ctx := context.Background()
			err := DefaultInferenceService(ctx, c, tt.isvc, tt.deployConfig)
			require.NoError(t, err)

			require.NotNil(t, tt.isvc.ObjectMeta.Annotations, "Annotations should exist")
			mode, exists := tt.isvc.ObjectMeta.Annotations[constants.DeploymentMode]
			assert.True(t, exists, "Deployment mode annotation should exist")
			assert.Equal(t, tt.expectedMode, mode, "Expected deployment mode should match")
		})
	}
}

func TestDefaultComponents(t *testing.T) {
	t.Run("defaultEngine", func(t *testing.T) {
		tests := []struct {
			name            string
			engine          *v1beta1.EngineSpec
			wantMinReplicas int
			wantMaxReplicas int
		}{
			{
				name:            "nil MinReplicas should be set to 1",
				engine:          &v1beta1.EngineSpec{},
				wantMinReplicas: 1,
				wantMaxReplicas: 3,
			},
			{
				name: "existing values should be preserved",
				engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
						MaxReplicas: 5,
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 5,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defaultEngine(tt.engine, nil)
				require.NotNil(t, tt.engine.MinReplicas)
				assert.Equal(t, tt.wantMinReplicas, *tt.engine.MinReplicas)
				assert.Equal(t, tt.wantMaxReplicas, tt.engine.MaxReplicas)
			})
		}
	})

	t.Run("defaultDecoder", func(t *testing.T) {
		tests := []struct {
			name            string
			decoder         *v1beta1.DecoderSpec
			wantMinReplicas int
			wantMaxReplicas int
		}{
			{
				name:            "nil MinReplicas should be set to 1",
				decoder:         &v1beta1.DecoderSpec{},
				wantMinReplicas: 1,
				wantMaxReplicas: 3,
			},
			{
				name: "existing values should be preserved",
				decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
						MaxReplicas: 5,
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 5,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defaultDecoder(tt.decoder, nil)
				require.NotNil(t, tt.decoder.MinReplicas)
				assert.Equal(t, tt.wantMinReplicas, *tt.decoder.MinReplicas)
				assert.Equal(t, tt.wantMaxReplicas, tt.decoder.MaxReplicas)
			})
		}
	})

	t.Run("defaultRouter", func(t *testing.T) {
		tests := []struct {
			name            string
			router          *v1beta1.RouterSpec
			wantMinReplicas int
			wantMaxReplicas int
		}{
			{
				name:            "nil MinReplicas should be set to 1",
				router:          &v1beta1.RouterSpec{},
				wantMinReplicas: 1,
				wantMaxReplicas: 2,
			},
			{
				name: "existing values should be preserved",
				router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
						MaxReplicas: 5,
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 5,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defaultRouter(tt.router, nil)
				require.NotNil(t, tt.router.MinReplicas)
				assert.Equal(t, tt.wantMinReplicas, *tt.router.MinReplicas)
				assert.Equal(t, tt.wantMaxReplicas, tt.router.MaxReplicas)
			})
		}
	})
}

func int32Ptr(v int32) *int32 { return &v }

func TestDefaultOMENativeComponent(t *testing.T) {
	omenativeMode := map[string]string{constants.DeploymentMode: string(constants.OMENative)}

	t.Run("not OMENative — omenative block untouched", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)},
		}
		defaultOMENativeComponent(ext, true, nil)
		assert.Nil(t, ext.Lifecycle)
	})

	t.Run("no annotation — omenative block untouched", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{}
		defaultOMENativeComponent(ext, true, nil)
		assert.Nil(t, ext.Lifecycle)
	})

	t.Run("OMENative + multi-pod — full defaults applied", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenativeMode}
		defaultOMENativeComponent(ext, true, nil)

		require.NotNil(t, ext.Lifecycle)
		spec := ext.Lifecycle

		require.NotNil(t, spec.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *spec.RestartPolicy)

		require.NotNil(t, spec.UpdateStrategy)
		assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, spec.UpdateStrategy.Type)
		require.NotNil(t, spec.UpdateStrategy.InPlaceUpdateStrategy)
		require.NotNil(t, spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds)
		assert.Equal(t, int32(30), *spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds)
		require.NotNil(t, spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)
		assert.True(t, *spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)

		require.NotNil(t, spec.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *spec.ReadyPolicy)

		require.NotNil(t, spec.InstanceReadyTimeout)
		assert.Equal(t, 30*time.Minute, spec.InstanceReadyTimeout.Duration)

		require.NotNil(t, spec.MigrationPolicy)
		assert.Equal(t, v1beta1.MigrationPolicyModeAuto, spec.MigrationPolicy.Mode)
	})

	t.Run("OMENative + single-pod — restart/ready default to None", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenativeMode}
		defaultOMENativeComponent(ext, false, nil)

		require.NotNil(t, ext.Lifecycle)
		require.NotNil(t, ext.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *ext.Lifecycle.RestartPolicy)
		require.NotNil(t, ext.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyNone, *ext.Lifecycle.ReadyPolicy)
	})

	t.Run("OMENative + existing values — preserved, nil siblings filled", func(t *testing.T) {
		customRestart := v1beta1.InstanceRestartPolicyNone
		customTimeout := metav1.Duration{Duration: 5 * time.Minute}
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				RestartPolicy:        &customRestart,
				InstanceReadyTimeout: &customTimeout,
			},
		}
		defaultOMENativeComponent(ext, true, nil)

		// preserved
		require.NotNil(t, ext.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *ext.Lifecycle.RestartPolicy)
		assert.Equal(t, 5*time.Minute, ext.Lifecycle.InstanceReadyTimeout.Duration)

		// filled
		require.NotNil(t, ext.Lifecycle.UpdateStrategy)
		assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type)
		require.NotNil(t, ext.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *ext.Lifecycle.ReadyPolicy)
		require.NotNil(t, ext.Lifecycle.MigrationPolicy)
		assert.Equal(t, v1beta1.MigrationPolicyModeAuto, ext.Lifecycle.MigrationPolicy.Mode)
	})

	t.Run("OMENative + partial UpdateStrategy — only nil leaves filled", func(t *testing.T) {
		customType := v1beta1.UpdateStrategyRecreatePod
		customGrace := int32(60)
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					Type: customType,
					InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
						GracePeriodSeconds: &customGrace,
					},
				},
			},
		}
		defaultOMENativeComponent(ext, true, nil)

		assert.Equal(t, v1beta1.UpdateStrategyRecreatePod, ext.Lifecycle.UpdateStrategy.Type)
		assert.Equal(t, int32(60), *ext.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds)
		// MarkNotReadyDuringLifecycle was nil → defaulted to true
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)
		assert.True(t, *ext.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)
	})

	t.Run("OMENative + ProgressDeadlineSeconds set — MaxAutoRetries defaults to 3", func(t *testing.T) {
		deadline := int32(60)
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				MigrationPolicy: &v1beta1.MigrationPolicy{
					ProgressDeadlineSeconds: &deadline,
				},
			},
		}
		defaultOMENativeComponent(ext, false, nil)
		require.NotNil(t, ext.Lifecycle.MigrationPolicy.MaxAutoRetries)
		assert.Equal(t, int32(3), *ext.Lifecycle.MigrationPolicy.MaxAutoRetries)
		// ProgressDeadlineSeconds preserved.
		require.NotNil(t, ext.Lifecycle.MigrationPolicy.ProgressDeadlineSeconds)
		assert.Equal(t, int32(60), *ext.Lifecycle.MigrationPolicy.ProgressDeadlineSeconds)
	})

	t.Run("OMENative + ProgressDeadlineSeconds nil — MaxAutoRetries stays nil", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
		}
		defaultOMENativeComponent(ext, false, nil)
		require.NotNil(t, ext.Lifecycle.MigrationPolicy)
		assert.Nil(t, ext.Lifecycle.MigrationPolicy.ProgressDeadlineSeconds, "no auto-migration opt-in → no deadline")
		assert.Nil(t, ext.Lifecycle.MigrationPolicy.MaxAutoRetries, "no auto-migration opt-in → no cap (cap is meaningless without trigger)")
	})

	t.Run("OMENative + ProgressDeadlineSeconds set + MaxAutoRetries set — both preserved", func(t *testing.T) {
		deadline := int32(60)
		custom := int32(7)
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				MigrationPolicy: &v1beta1.MigrationPolicy{
					ProgressDeadlineSeconds: &deadline,
					MaxAutoRetries:          &custom,
				},
			},
		}
		defaultOMENativeComponent(ext, false, nil)
		require.NotNil(t, ext.Lifecycle.MigrationPolicy.MaxAutoRetries)
		assert.Equal(t, int32(7), *ext.Lifecycle.MigrationPolicy.MaxAutoRetries, "explicit MaxAutoRetries must be preserved")
	})
}

func TestEngineIsMultiPod(t *testing.T) {
	tests := []struct {
		name string
		spec *v1beta1.EngineSpec
		want bool
	}{
		{name: "nil", spec: nil, want: false},
		{name: "empty", spec: &v1beta1.EngineSpec{}, want: false},
		{name: "leader only", spec: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}}, want: true},
		{
			name: "worker size 0",
			spec: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 0; return &v }()}},
			want: false,
		},
		{
			name: "worker size 1",
			spec: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 1; return &v }()}},
			want: true,
		},
		{
			name: "worker size nil",
			spec: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, engineIsMultiPod(tt.spec))
		})
	}
}

func TestDefaultEngine_OMENativeAnnotationTriggersDefaults(t *testing.T) {
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 3; return &v }()},
	}
	defaultEngine(engine, nil)
	require.NotNil(t, engine.Lifecycle)
	require.NotNil(t, engine.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *engine.Lifecycle.RestartPolicy)
}

func TestDefaultRouter_OMENativeAlwaysSinglePod(t *testing.T) {
	router := &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	defaultRouter(router, nil)
	require.NotNil(t, router.Lifecycle)
	require.NotNil(t, router.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *router.Lifecycle.RestartPolicy)
}

// TestDefaultWorkerSize_MultiPodWithoutSize covers the OEP-0007
// Phase-3 defaulter rule that Worker.Size = nil with Leader set is
// treated as "minimum-viable multi-pod" (Size=1). The downstream
// engineIsMultiPod / decoderIsMultiPod detection sees the defaulted
// Size and the gang lifecycle policies kick in.
func TestDefaultWorkerSize_MultiPodWithoutSize(t *testing.T) {
	t.Run("engine leader + worker without size — defaults Size=1", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{},
		}
		defaultEngine(engine, nil)
		require.NotNil(t, engine.Worker.Size)
		assert.Equal(t, 1, *engine.Worker.Size)
		// downstream multi-pod detection should kick in:
		require.NotNil(t, engine.Lifecycle)
		require.NotNil(t, engine.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *engine.Lifecycle.RestartPolicy)
		require.NotNil(t, engine.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *engine.Lifecycle.ReadyPolicy)
	})

	t.Run("engine leader + worker with size=3 — preserved", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: intPtr(3)},
		}
		defaultEngine(engine, nil)
		require.NotNil(t, engine.Worker.Size)
		assert.Equal(t, 3, *engine.Worker.Size)
	})

	t.Run("engine leader + worker with size=0 — preserved (validator rejects)", func(t *testing.T) {
		// The defaulter must NOT silently turn a 0 into a 1; that
		// would erase a clear operator mistake. Validator rejects
		// Size <= 0 with WorkerSizeMustBePositive.
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: intPtr(0)},
		}
		defaultEngine(engine, nil)
		require.NotNil(t, engine.Worker.Size)
		assert.Equal(t, 0, *engine.Worker.Size)
	})

	t.Run("engine leader-only — no worker created (validator rejects)", func(t *testing.T) {
		// Pure leader-without-worker is not the multi-pod pairing the
		// defaulter handles. The validator catches it with
		// LeaderRequiresWorker.
		engine := &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}}
		defaultEngine(engine, nil)
		assert.Nil(t, engine.Worker)
	})

	t.Run("engine worker-only — Size left untouched (validator rejects)", func(t *testing.T) {
		// Pure worker-without-leader: the defaulter sees no pairing
		// to fill. The validator catches it with
		// WorkerRequiresLeader; the malformed Size remains for the
		// error message to surface.
		engine := &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{}}
		defaultEngine(engine, nil)
		assert.Nil(t, engine.Worker.Size)
	})

	t.Run("decoder leader + worker without size — defaults Size=1", func(t *testing.T) {
		decoder := &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{},
		}
		defaultDecoder(decoder, nil)
		require.NotNil(t, decoder.Worker.Size)
		assert.Equal(t, 1, *decoder.Worker.Size)
		require.NotNil(t, decoder.Lifecycle)
		require.NotNil(t, decoder.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *decoder.Lifecycle.RestartPolicy)
	})

	t.Run("single-pod engine — Worker field unset, no panic", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, nil)
		assert.Nil(t, engine.Worker)
		assert.Nil(t, engine.Leader)
	})
}

// TestDefaultOMENativeComponent_MultiPodPhase3 pins the OEP-0007
// Phase-3 multi-pod defaulter rules as a single explicit before/after
// test, complementing TestDefaultOMENativeComponent which already
// covers the underlying mechanic.
//
// Rules pinned:
//   - RestartPolicy → RecreateInstanceOnPodRestart
//   - ReadyPolicy   → AllPodReady
//   - UpdateStrategy.Type → SurgeThenDrain (same as single-pod;
//     execution falls back to recreate per Phase-1 sign-off)
func TestDefaultOMENativeComponent_MultiPodPhase3(t *testing.T) {
	ext := &v1beta1.ComponentExtensionSpec{
		Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
	}

	// before
	assert.Nil(t, ext.Lifecycle)

	defaultOMENativeComponent(ext, true /* multiPod */, nil)

	// after
	require.NotNil(t, ext.Lifecycle)
	require.NotNil(t, ext.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *ext.Lifecycle.RestartPolicy,
		"multi-pod must default to RecreateInstanceOnPodRestart per OEP-0007 §Restart Policy")

	require.NotNil(t, ext.Lifecycle.ReadyPolicy)
	assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *ext.Lifecycle.ReadyPolicy,
		"multi-pod must default to AllPodReady so Instance Ready iff every gang pod Ready")

	require.NotNil(t, ext.Lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type,
		"multi-pod must default to SurgeThenDrain — same as single-pod — so Phase 5 lights up auto")
}

// TestDefaultOMENativeComponent_SinglePodPhase3 confirms the
// Phase-3 rules do NOT affect single-pod defaulting.
func TestDefaultOMENativeComponent_SinglePodPhase3(t *testing.T) {
	ext := &v1beta1.ComponentExtensionSpec{
		Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
	}
	defaultOMENativeComponent(ext, false /* singlePod */, nil)

	require.NotNil(t, ext.Lifecycle)
	require.NotNil(t, ext.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *ext.Lifecycle.RestartPolicy,
		"single-pod must keep RestartPolicy=None — no gang restart needed for one pod")
	require.NotNil(t, ext.Lifecycle.ReadyPolicy)
	assert.Equal(t, v1beta1.InstanceReadyPolicyNone, *ext.Lifecycle.ReadyPolicy,
		"single-pod must keep ReadyPolicy=None — no aggregation needed for one pod")
	require.NotNil(t, ext.Lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type,
		"single-pod UpdateStrategy unchanged — Phase 3 must not regress this")
}

// silence unused-helper lint if int32Ptr isn't referenced elsewhere
var _ = int32Ptr

// =============================================================================
// Webhook Integration Tests
// =============================================================================

func TestDefault(t *testing.T) {
	t.Run("conversion error", func(t *testing.T) {
		// Create an object that cannot be converted to InferenceService
		invalidObj := &v1.Pod{}
		defaulter := &InferenceServiceDefaulter{
			Client: createFakeClient(t),
		}

		err := defaulter.Default(context.Background(), invalidObj)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected an InferenceService object but got")
	})
}

func TestDefaultRolloutCoordination_FillsPacingDefaults(t *testing.T) {
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing:     &v1beta1.CoordinationPacing{},
			},
		},
	}
	defaultRolloutCoordination(spec)
	g := spec.Groups[0]
	if g.Pacing.Type != v1beta1.CoordinationPacingPerComponent {
		t.Errorf("Type default: got %q want PerComponent", g.Pacing.Type)
	}
	if g.Pacing.MaxSurge == nil || g.Pacing.MaxSurge.StrVal != "25%" {
		t.Errorf("MaxSurge default: got %+v want 25%%", g.Pacing.MaxSurge)
	}
	if g.Pacing.MaxUnavailable == nil || g.Pacing.MaxUnavailable.IntValue() != 0 {
		t.Errorf("MaxUnavailable default: got %+v want 0", g.Pacing.MaxUnavailable)
	}
	if g.Pacing.RatioTolerancePercent != nil {
		t.Errorf("PerComponent should not set RatioTolerancePercent: got %+v", g.Pacing.RatioTolerancePercent)
	}
}

func TestDefaultRolloutCoordination_RatioBalancedSetsRatioTolerance(t *testing.T) {
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing:     &v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced},
			},
		},
	}
	defaultRolloutCoordination(spec)
	if spec.Groups[0].Pacing.RatioTolerancePercent == nil {
		t.Fatalf("RatioBalanced should default RatioTolerancePercent")
	}
	if *spec.Groups[0].Pacing.RatioTolerancePercent != 5 {
		t.Errorf("RatioTolerancePercent: got %d want 5", *spec.Groups[0].Pacing.RatioTolerancePercent)
	}
}

func TestDefaultRolloutCoordination_AbsentPacingNotPopulated(t *testing.T) {
	// pacing nil → defaulter leaves it nil (coordination is opt-in via
	// declaration; the runtime resolver supplies the same defaults).
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyIndependent,
			},
		},
	}
	defaultRolloutCoordination(spec)
	if spec.Groups[0].Pacing != nil {
		t.Errorf("absent pacing should stay nil: got %+v", spec.Groups[0].Pacing)
	}
}

func TestDefaultRolloutCoordination_PreservesOperatorOverrides(t *testing.T) {
	ms := intstr.FromInt(7)
	mu := intstr.FromInt(3)
	tol := int32(15)
	spec := &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:                  v1beta1.CoordinationPacingRatioBalanced,
					MaxSurge:              &ms,
					MaxUnavailable:        &mu,
					RatioTolerancePercent: &tol,
				},
			},
		},
	}
	defaultRolloutCoordination(spec)
	g := spec.Groups[0]
	if g.Pacing.MaxSurge.IntValue() != 7 {
		t.Errorf("MaxSurge preserved: got %+v want 7", g.Pacing.MaxSurge)
	}
	if g.Pacing.MaxUnavailable.IntValue() != 3 {
		t.Errorf("MaxUnavailable preserved: got %+v want 3", g.Pacing.MaxUnavailable)
	}
	if *g.Pacing.RatioTolerancePercent != 15 {
		t.Errorf("RatioTolerancePercent preserved: got %d want 15", *g.Pacing.RatioTolerancePercent)
	}
}

func TestDefaultRolloutCoordination_NilSpec(t *testing.T) {
	// Must not panic.
	defaultRolloutCoordination(nil)
}
