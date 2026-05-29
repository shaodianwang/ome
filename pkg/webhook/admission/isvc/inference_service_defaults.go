package isvc

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
)

// logger for the mutating webhook.
var mutatorLogger = logf.Log.WithName("inferenceservice-v1beta1-mutating-webhook")

// InferenceServiceDefaulter is responsible for setting default values on the InferenceService
// when created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type InferenceServiceDefaulter struct {
	Client    client.Client
	ClientSet kubernetes.Interface
}

// +kubebuilder:webhook:path=/mutate-ome-io-v1beta1-inferenceservice,mutating=true,failurePolicy=fail,groups=ome.io,resources=inferenceservices,verbs=create;update,versions=v1beta1,name=inferenceservice.ome-webhook-server.defaulter
var _ webhook.CustomDefaulter = &InferenceServiceDefaulter{}

func (d *InferenceServiceDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	isvc, err := convertToInferenceService(obj)
	if err != nil {
		mutatorLogger.Error(err, "Unable to convert object to InferenceService")
		return err
	}
	mutatorLogger.Info("Defaulting InferenceService", "namespace", isvc.Namespace, "name", isvc.Name)
	deployConfig, err := controllerconfig.NewDeployConfig(d.ClientSet)
	if err != nil {
		mutatorLogger.Error(err, "Failed to get deploy config")
		return err
	}
	if err = DefaultInferenceService(ctx, d.Client, isvc, deployConfig); err != nil {
		return err
	}
	return nil
}

// DefaultInferenceService sets default values on the InferenceService
func DefaultInferenceService(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, deployConfig *controllerconfig.DeployConfig) error {
	// Create annotations map if it doesn't exist
	if isvc.ObjectMeta.Annotations == nil {
		isvc.ObjectMeta.Annotations = map[string]string{}
	}

	// Determine deployment mode based on components
	_, modeExists := isvc.ObjectMeta.Annotations[constants.DeploymentMode]
	if !modeExists {
		// If both Engine and Decoder are specified, set the mode for PD disaggregated deployment
		if isvc.Spec.Engine != nil && isvc.Spec.Decoder != nil {
			// Use the PDDisaggregated deployment mode for PD disaggregated deployments
			isvc.ObjectMeta.Annotations[constants.DeploymentMode] = string(constants.PDDisaggregated)
		} else if isvc.Spec.Engine != nil {
			// Check for MultiNode mode: leader and worker with worker.size > 0
			if isvc.Spec.Engine.Leader != nil &&
				isvc.Spec.Engine.Worker != nil &&
				isvc.Spec.Engine.Worker.Size != nil &&
				*isvc.Spec.Engine.Worker.Size > 0 {
				isvc.ObjectMeta.Annotations[constants.DeploymentMode] = string(constants.MultiNode)
			} else if deployConfig != nil && deployConfig.DefaultDeploymentMode == string(constants.RawDeployment) {
				// Default to RawDeployment mode if not MultiNode
				isvc.ObjectMeta.Annotations[constants.DeploymentMode] = deployConfig.DefaultDeploymentMode
			}
		} else if deployConfig != nil && deployConfig.DefaultDeploymentMode == string(constants.RawDeployment) {
			// Apply default deployment mode from config if provided
			isvc.ObjectMeta.Annotations[constants.DeploymentMode] = deployConfig.DefaultDeploymentMode
		}
	}

	// Set default values for Engine component if present
	if isvc.Spec.Engine != nil {
		defaultEngine(isvc.Spec.Engine)
	}

	// Set default values for Decoder component if present
	if isvc.Spec.Decoder != nil {
		defaultDecoder(isvc.Spec.Decoder)
	}

	// Set default values for Router component if present
	if isvc.Spec.Router != nil {
		defaultRouter(isvc.Spec.Router)
	}

	// Fill cross-Component rollout coordination defaults
	// (pacing.type, maxSurge, maxUnavailable, ratioTolerance).
	defaultRolloutCoordination(isvc.Spec.RolloutCoordination)
	return nil
}

// defaultEngine sets default values for the Engine component
func defaultEngine(engine *v1beta1.EngineSpec) {
	// Set default replica values if not set
	if engine.MinReplicas == nil {
		minReplicas := 1 // MinReplicas is *int, not *int32
		engine.MinReplicas = &minReplicas
	}

	// MaxReplicas is not a pointer type, so check if it's 0 (default value)
	if engine.MaxReplicas == 0 {
		engine.MaxReplicas = 3
	}

	// OEP-0007 multi-node Phase 3: when Worker is declared alongside
	// Leader but no Size is set, default Size to 1 (minimum-viable
	// multi-pod). The user can override; the validator
	// (ValidateLeaderWorkerPairing) still rejects Size <= 0.
	defaultWorkerSize(engine.Leader, engine.Worker)

	defaultOMENativeComponent(&engine.ComponentExtensionSpec, engineIsMultiPod(engine))
}

// defaultDecoder sets default values for the Decoder component
func defaultDecoder(decoder *v1beta1.DecoderSpec) {
	// Set default replica values if not set
	if decoder.MinReplicas == nil {
		minReplicas := 1 // MinReplicas is *int, not *int32
		decoder.MinReplicas = &minReplicas
	}

	// MaxReplicas is not a pointer type, so check if it's 0 (default value)
	if decoder.MaxReplicas == 0 {
		decoder.MaxReplicas = 3
	}

	// OEP-0007 multi-node Phase 3: see defaultEngine comment.
	defaultWorkerSize(decoder.Leader, decoder.Worker)

	defaultOMENativeComponent(&decoder.ComponentExtensionSpec, decoderIsMultiPod(decoder))
}

// defaultWorkerSize fills Worker.Size=1 when a multi-pod pairing is
// declared (Leader set AND Worker set) but Worker.Size is nil. Acts
// only when both halves of the pairing are present — pure
// leader-without-worker or worker-without-leader specs are left
// untouched (the validator rejects them later with the appropriate
// reason).
//
// Treats nil Size as the operator-friendly "minimum-viable
// multi-pod" default; if the operator wants more workers they set
// Size explicitly. A Size of 0 is preserved (and rejected at
// validation as WorkerSizeMustBePositive) so we don't quietly turn
// the operator's "no workers, please" mistake into a 2-pod gang.
//
// Must run BEFORE engineIsMultiPod / decoderIsMultiPod so the
// downstream multi-pod detection sees the defaulted Size and stamps
// the gang lifecycle policies (RecreateInstance restart, AllPodReady
// readiness, SurgeThenDrain update).
func defaultWorkerSize(leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) {
	if leader == nil || worker == nil {
		return
	}
	if worker.Size != nil {
		return
	}
	size := 1
	worker.Size = &size
}

// defaultRouter sets default values for the Router component
func defaultRouter(router *v1beta1.RouterSpec) {
	// Set default replica values if not set
	if router.MinReplicas == nil {
		minReplicas := 1 // MinReplicas is *int, not *int32
		router.MinReplicas = &minReplicas
	}

	// MaxReplicas is not a pointer type, so check if it's 0 (default value)
	if router.MaxReplicas == 0 {
		router.MaxReplicas = 2
	}

	// Router has no Leader/Worker; always single-pod.
	defaultOMENativeComponent(&router.ComponentExtensionSpec, false)
}

// engineIsMultiPod reports whether the Engine spawns more than one pod per
// Instance — true when a Leader is declared or when Worker.Size > 0.
func engineIsMultiPod(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}
	if engine.Leader != nil {
		return true
	}
	return engine.Worker != nil && engine.Worker.Size != nil && *engine.Worker.Size > 0
}

// decoderIsMultiPod reports whether the Decoder spawns more than one pod per
// Instance — true when a Leader is declared or when Worker.Size > 0.
func decoderIsMultiPod(decoder *v1beta1.DecoderSpec) bool {
	if decoder == nil {
		return false
	}
	if decoder.Leader != nil {
		return true
	}
	return decoder.Worker != nil && decoder.Worker.Size != nil && *decoder.Worker.Size > 0
}

// defaultOMENativeComponent fills in OEP-0007 lifecycle-policy defaults on
// the Component's omenative sub-block. Only acts when the Component is
// annotated ome.io/deploymentMode=OMENative; for any other mode the block is
// left untouched. multiPod toggles the restart/ready policy defaults: a
// multi-pod Instance defaults to group restart + AllPodReady aggregation;
// a single-pod Instance defaults to None for both.
//
// OEP-0007 multi-node Phase 3 multi-pod defaults (re-stated for
// documentation; the rules below were already in place but Phase 3
// pins them by explicit test):
//
//   - RestartPolicy = RecreateInstanceOnPodRestart (gang restart;
//     OEP-0007 §"Restart Policy")
//   - ReadyPolicy = AllPodReady (Instance Ready iff every pod Ready)
//   - UpdateStrategy.Type = SurgeThenDrain (same default as single-pod;
//     the execution falls back to recreate for multi-pod per Phase-1
//     sign-off in #242, and lights up automatically when Phase 5
//     multi-pod surge ships — no operator spec churn needed)
func defaultOMENativeComponent(ext *v1beta1.ComponentExtensionSpec, multiPod bool) {
	if ext == nil {
		return
	}
	if ext.Annotations[constants.DeploymentMode] != string(constants.OMENative) {
		return
	}
	if ext.Lifecycle == nil {
		ext.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	spec := ext.Lifecycle

	if spec.RestartPolicy == nil {
		rp := v1beta1.InstanceRestartPolicyNone
		if multiPod {
			rp = v1beta1.InstanceRestartPolicyRecreateInstance
		}
		spec.RestartPolicy = &rp
	}

	if spec.UpdateStrategy == nil {
		spec.UpdateStrategy = &v1beta1.UpdateStrategy{}
	}
	if spec.UpdateStrategy.Type == "" {
		// SurgeThenDrain is the default. It currently delegates to
		// recreate (no zero-downtime surge yet), which is still safer
		// than in-place because the dispatcher's MaxUnavailable gate
		// throttles drains.
		spec.UpdateStrategy.Type = v1beta1.UpdateStrategySurgeThenDrain
	}
	if spec.UpdateStrategy.InPlaceUpdateStrategy == nil {
		spec.UpdateStrategy.InPlaceUpdateStrategy = &v1beta1.InPlaceUpdateStrategy{}
	}
	if spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds == nil {
		grace := int32(30)
		spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds = &grace
	}
	if spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle == nil {
		mark := true
		spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &mark
	}

	if spec.ReadyPolicy == nil {
		rp := v1beta1.InstanceReadyPolicyNone
		if multiPod {
			rp = v1beta1.InstanceReadyPolicyAllPodReady
		}
		spec.ReadyPolicy = &rp
	}

	if spec.InstanceReadyTimeout == nil {
		spec.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	}

	if spec.MigrationPolicy == nil {
		spec.MigrationPolicy = &v1beta1.MigrationPolicy{}
	}
	if spec.MigrationPolicy.Mode == "" {
		spec.MigrationPolicy.Mode = v1beta1.MigrationPolicyModeAuto
	}
}

// defaultRolloutCoordination fills in the documented pacing defaults
// for every group that has a pacing block declared. Groups without a
// pacing block keep their nil pacing — the runtime resolver supplies
// the same defaults again so absence vs default-everything is
// equivalent.
//
// Per-group defaults:
//   - pacing.type:           PerComponent (when pacing is set without
//     a type)
//   - pacing.maxSurge:       "25%"
//   - pacing.maxUnavailable: 0
//   - pacing.ratioTolerance: 5 (only for RatioBalanced)
//
// The defaulter never populates groups[]; coordination is opt-in via
// explicit operator declaration.
func defaultRolloutCoordination(spec *v1beta1.RolloutCoordinationSpec) {
	if spec == nil {
		return
	}
	for i := range spec.Groups {
		g := &spec.Groups[i]
		if g.Pacing == nil {
			continue
		}
		if g.Pacing.Type == "" {
			g.Pacing.Type = v1beta1.CoordinationPacingPerComponent
		}
		if g.Pacing.MaxSurge == nil {
			ms := intstr.FromString("25%")
			g.Pacing.MaxSurge = &ms
		}
		if g.Pacing.MaxUnavailable == nil {
			mu := intstr.FromInt(0)
			g.Pacing.MaxUnavailable = &mu
		}
		if g.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced && g.Pacing.RatioTolerance == nil {
			tol := int32(5)
			g.Pacing.RatioTolerance = &tol
		}
	}
}
