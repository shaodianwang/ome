package validation

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestValidateInferenceServiceName(t *testing.T) {
	tests := []struct {
		name     string
		isvcName string
		wantErr  bool
	}{
		{name: "valid lowercase", isvcName: "my-model", wantErr: false},
		{name: "single char", isvcName: "a", wantErr: false},
		{name: "alphanumeric with hyphens", isvcName: "llama-3-70b", wantErr: false},
		{name: "starts with number", isvcName: "3model", wantErr: true},
		{name: "uppercase", isvcName: "MyModel", wantErr: true},
		{name: "ends with hyphen", isvcName: "model-", wantErr: true},
		{name: "empty string", isvcName: "", wantErr: true},
		{name: "underscore", isvcName: "my_model", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInferenceServiceName(tc.isvcName)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for name %q, got nil", tc.isvcName)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for name %q: %v", tc.isvcName, err)
			}
		})
	}
}

func TestValidateEngineDecoderConfig(t *testing.T) {
	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{
			name: "no decoder or engine",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine without decoder",
			spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		},
		{
			name: "engine with decoder",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
			},
		},
		{
			name:    "decoder without engine",
			spec:    &v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEngineDecoderConfig(tc.spec)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateEngineDecoderDeploymentMode(t *testing.T) {
	withMode := func(mode string) *v1beta1.ComponentExtensionSpec {
		return &v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
		}
	}
	engineWith := func(ext *v1beta1.ComponentExtensionSpec) *v1beta1.EngineSpec {
		if ext == nil {
			return &v1beta1.EngineSpec{}
		}
		return &v1beta1.EngineSpec{ComponentExtensionSpec: *ext}
	}
	decoderWith := func(ext *v1beta1.ComponentExtensionSpec) *v1beta1.DecoderSpec {
		if ext == nil {
			return &v1beta1.DecoderSpec{}
		}
		return &v1beta1.DecoderSpec{ComponentExtensionSpec: *ext}
	}

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{
			name: "no engine, no decoder",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine only (no decoder) — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{Engine: engineWith(withMode(string(constants.OMENative)))},
		},
		{
			name: "both present, no annotations — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{Engine: engineWith(nil), Decoder: decoderWith(nil)},
		},
		{
			name: "both OMENative — match",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.OMENative))),
				Decoder: decoderWith(withMode(string(constants.OMENative))),
			},
		},
		{
			name: "engine OMENative, decoder unset — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.OMENative))),
				Decoder: decoderWith(nil),
			},
			wantErr: true,
		},
		{
			name: "engine unset, decoder OMENative — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(nil),
				Decoder: decoderWith(withMode(string(constants.OMENative))),
			},
			wantErr: true,
		},
		{
			name: "engine OMENative, decoder MultiNode — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.OMENative))),
				Decoder: decoderWith(withMode(string(constants.MultiNode))),
			},
			wantErr: true,
		},
		{
			name: "both MultiNode (no OMENative) — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.MultiNode))),
				Decoder: decoderWith(withMode(string(constants.MultiNode))),
			},
		},
		{
			name: "engine MultiNode, decoder RawDeployment (no OMENative) — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.MultiNode))),
				Decoder: decoderWith(withMode(string(constants.RawDeployment))),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEngineDecoderDeploymentMode(tc.spec)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateLeaderWorkerPairing(t *testing.T) {
	sizePtr := func(v int) *int { return &v }
	withWorker := func(size *int) *v1beta1.WorkerSpec { return &v1beta1.WorkerSpec{Size: size} }

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
		errSub  string
	}{
		{
			name: "no engine, no decoder",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine: neither leader nor worker — single-pod OK",
			spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		},
		{
			name: "engine: leader + worker(size=3) — valid multi-pod",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: withWorker(sizePtr(3)),
				},
			},
		},
		{
			name: "engine: leader without worker — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
			},
			wantErr: true,
			errSub:  "engine.leader is set but engine.worker is not",
		},
		{
			name: "engine: worker without leader — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Worker: withWorker(sizePtr(3))},
			},
			wantErr: true,
			errSub:  "engine.worker is set but engine.leader is not",
		},
		{
			name: "engine: worker with nil size — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: withWorker(nil),
				},
			},
			wantErr: true,
			errSub:  "worker.size is not a positive integer",
		},
		{
			name: "engine: worker with size=0 — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: withWorker(sizePtr(0)),
				},
			},
			wantErr: true,
			errSub:  "worker.size is not a positive integer",
		},
		{
			name: "decoder: leader without worker — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{Leader: &v1beta1.LeaderSpec{}},
			},
			wantErr: true,
			errSub:  "decoder.leader is set but decoder.worker is not",
		},
		{
			name: "engine OK, decoder OK — both single-pod, both pass",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
			},
		},
		{
			name: "engine fails first — engine error surfaces (decoder not checked)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
				Decoder: &v1beta1.DecoderSpec{Worker: withWorker(sizePtr(2))},
			},
			wantErr: true,
			errSub:  "engine.leader",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaderWorkerPairing(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.errSub != "" && !strings.Contains(err.Error(), tc.errSub) {
					t.Fatalf("expected error to contain %q, got %v", tc.errSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateReplicaBounds(t *testing.T) {
	intPtr := func(v int) *int { return &v }

	tests := []struct {
		name    string
		min     *int
		max     *int
		wantErr bool
	}{
		{name: "nil both", min: nil, max: nil},
		{name: "valid range", min: intPtr(1), max: intPtr(3)},
		{name: "min equals max", min: intPtr(2), max: intPtr(2)},
		{name: "min zero", min: intPtr(0), max: intPtr(1)},
		{name: "negative min", min: intPtr(-1), max: intPtr(1), wantErr: true},
		{name: "zero max", min: intPtr(0), max: intPtr(0), wantErr: true},
		{name: "min greater than max", min: intPtr(5), max: intPtr(2), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateReplicaBounds(tc.min, tc.max)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateAutoscalerConfig(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{
			name:        "no autoscaler annotation",
			annotations: map[string]string{},
		},
		{
			name: "valid HPA class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
			},
		},
		{
			name: "valid HPA with cpu metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "cpu",
			},
		},
		{
			name: "HPA with invalid metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "bogus",
			},
			wantErr: true,
		},
		{
			name: "unsupported autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: "unsupported",
			},
			wantErr: true,
		},
		{
			name: "KEDA class accepted (no validation in phase 1)",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
			},
		},
		{
			name: "valid external class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassExternal),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
			}
			_, err := ValidateAutoscalerConfig(isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateLeaderWorkerPairing_ReasonConstants confirms the
// OEP-0007 Phase-3 reason constants appear in the error strings the
// validator emits. Operators and the M-E webhook test suite grep for
// these exact tokens.
func TestValidateLeaderWorkerPairing_ReasonConstants(t *testing.T) {
	sizePtr := func(v int) *int { return &v }

	tests := []struct {
		name           string
		spec           *v1beta1.InferenceServiceSpec
		wantReasons    []string
		notWantReasons []string
	}{
		{
			name: "leader without worker — LeaderRequiresWorker",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonLeaderRequiresWorker},
		},
		{
			name: "worker without leader — WorkerRequiresLeader",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)}},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonWorkerRequiresLeader},
		},
		{
			name: "leader + worker(size=0) — WorkerSizeMustBePositive",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(0)},
				},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonWorkerSizeMustBePositive},
		},
		{
			name: "leader + worker(nil size) — WorkerSizeMustBePositive",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{},
				},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonWorkerSizeMustBePositive},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaderWorkerPairing(tc.spec)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			for _, want := range tc.wantReasons {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("expected error to contain %q, got %v", want, err)
				}
			}
			for _, notWant := range tc.notWantReasons {
				if strings.Contains(err.Error(), notWant) {
					t.Errorf("expected error to NOT contain %q, got %v", notWant, err)
				}
			}
		})
	}
}

// TestValidateLeaderSize is structurally a no-op against the current
// API surface (LeaderSpec has no Size field; see ValidateLeaderSize
// doc). The test pins the reason constant + entry point so any
// future API evolution that adds a leader Size field surfaces a test
// failure rather than silently passing.
func TestValidateLeaderSize(t *testing.T) {
	sizePtr := func(v int) *int { return &v }

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{name: "nil spec", spec: nil},
		{name: "empty spec", spec: &v1beta1.InferenceServiceSpec{}},
		{name: "single-pod engine", spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}},
		{
			name: "multi-pod engine — leader implicitly size=1 by type",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)},
				},
			},
		},
		{
			name: "multi-pod decoder — leader implicitly size=1 by type",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaderSize(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), ReasonLeaderSizeMustBeOne) {
					t.Errorf("expected reason %q, got %v", ReasonLeaderSizeMustBeOne, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateSequentialMultiPodOrder covers the OEP-0007 Phase-3
// rule that a Sequential coordination group containing a multi-pod
// Component must declare explicit Order over its Components.
func TestValidateSequentialMultiPodOrder(t *testing.T) {
	sizePtr := func(v int) *int { return &v }
	multiPodEngine := &v1beta1.EngineSpec{
		Leader: &v1beta1.LeaderSpec{},
		Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)},
	}
	multiPodDecoder := &v1beta1.DecoderSpec{
		Leader: &v1beta1.LeaderSpec{},
		Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
	}

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{name: "nil spec", spec: nil},
		{
			name: "no rolloutCoordination",
			spec: &v1beta1.InferenceServiceSpec{Engine: multiPodEngine, Decoder: multiPodDecoder},
		},
		{
			name: "BlueGreen with multi-pod components, no order — allowed",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  multiPodEngine,
				Decoder: multiPodDecoder,
				RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
					Groups: []v1beta1.RolloutCoordinationGroup{
						{
							Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
							Policy:     v1beta1.CoordinationPolicyBlueGreen,
						},
					},
				},
			},
		},
		{
			name: "Sequential with single-pod components, no order — rule does not fire (other validator catches)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
				RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
					Groups: []v1beta1.RolloutCoordinationGroup{
						{
							Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
							Policy:     v1beta1.CoordinationPolicySequential,
						},
					},
				},
			},
		},
		{
			name: "Sequential with multi-pod engine, no order — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  multiPodEngine,
				Decoder: &v1beta1.DecoderSpec{},
				RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
					Groups: []v1beta1.RolloutCoordinationGroup{
						{
							Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
							Policy:     v1beta1.CoordinationPolicySequential,
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Sequential with multi-pod decoder, no order — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: multiPodDecoder,
				RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
					Groups: []v1beta1.RolloutCoordinationGroup{
						{
							Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
							Policy:     v1beta1.CoordinationPolicySequential,
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Sequential with multi-pod components, order set — allowed",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  multiPodEngine,
				Decoder: multiPodDecoder,
				RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
					Groups: []v1beta1.RolloutCoordinationGroup{
						{
							Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
							Policy:     v1beta1.CoordinationPolicySequential,
							Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
						},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSequentialMultiPodOrder(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), ReasonSequentialMultiPodRequiresOrder) {
					t.Errorf("expected reason %q in error, got %v", ReasonSequentialMultiPodRequiresOrder, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateMultiPodLeanRunner covers the OEP-0007 Phase-3 rule
// that a lean (no spec.model, no spec.runtime) multi-pod ISVC must
// declare both leader.runner.image and worker.runner.image. The
// controller has no defaulting source for those images.
func TestValidateMultiPodLeanRunner(t *testing.T) {
	sizePtr := func(v int) *int { return &v }
	withRunner := func(img string) *v1beta1.RunnerSpec {
		if img == "" {
			return &v1beta1.RunnerSpec{}
		}
		return &v1beta1.RunnerSpec{Container: corev1.Container{Image: img}}
	}
	modelRef := &v1beta1.ModelRef{Name: "llama"}
	runtimeRef := &v1beta1.ServingRuntimeRef{Name: "vllm"}

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{name: "nil spec", spec: nil},
		{
			name: "model set, no leader/worker images — allowed (controller resolves)",
			spec: &v1beta1.InferenceServiceSpec{
				Model: modelRef,
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
		},
		{
			name: "runtime set, no leader/worker images — allowed (runtime resolves)",
			spec: &v1beta1.InferenceServiceSpec{
				Runtime: runtimeRef,
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
		},
		{
			name: "lean single-pod engine — rule does not fire (no multi-pod)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
			},
		},
		{
			name: "lean multi-pod engine, both images set — allowed",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{Runner: withRunner("leader:1")},
					Worker: &v1beta1.WorkerSpec{
						Size:   sizePtr(2),
						Runner: withRunner("worker:1"),
					},
				},
			},
		},
		{
			name: "lean multi-pod engine, leader image missing — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{
						Size:   sizePtr(2),
						Runner: withRunner("worker:1"),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "lean multi-pod engine, worker image missing — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{Runner: withRunner("leader:1")},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
			wantErr: true,
		},
		{
			name: "lean multi-pod engine, both runner specs but empty images — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{Runner: withRunner("")},
					Worker: &v1beta1.WorkerSpec{
						Size:   sizePtr(2),
						Runner: withRunner(""),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "lean multi-pod decoder, missing images — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMultiPodLeanRunner(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), ReasonMultiPodLeanModelNeedsRunner) {
					t.Errorf("expected reason %q in error, got %v", ReasonMultiPodLeanModelNeedsRunner, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateAutoscalerTargetUtilizationPercentage(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{name: "no annotation", annotations: map[string]string{}},
		{name: "valid 50", annotations: map[string]string{constants.TargetUtilizationPercentage: "50"}},
		{name: "valid 1", annotations: map[string]string{constants.TargetUtilizationPercentage: "1"}},
		{name: "valid 100", annotations: map[string]string{constants.TargetUtilizationPercentage: "100"}},
		{name: "zero", annotations: map[string]string{constants.TargetUtilizationPercentage: "0"}, wantErr: true},
		{name: "101", annotations: map[string]string{constants.TargetUtilizationPercentage: "101"}, wantErr: true},
		{name: "not a number", annotations: map[string]string{constants.TargetUtilizationPercentage: "abc"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
			}
			err := ValidateAutoscalerTargetUtilizationPercentage(isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
