package components

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/utils"
)

var _ Component = &Decoder{}
var _ ComponentConfig = &Decoder{}

// Decoder reconciles resources for the decoder component
type Decoder struct {
	BaseComponentFields
	decoderSpec          *v1beta1.DecoderSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
}

// NewDecoder creates a new Decoder component instance
func NewDecoder(
	client client.Client,
	clientset kubernetes.Interface,
	apiReader client.Reader,
	expectations *omenative.Expectations,
	recorder record.EventRecorder,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	decoderSpec *v1beta1.DecoderSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	supportedModelFormat *v1beta1.SupportedModelFormat,
	overlays []isvcutils.ResolvedOverlay,
) Component {
	base := BaseComponentFields{
		Client:                 client,
		Clientset:              clientset,
		APIReader:              apiReader,
		Expectations:           expectations,
		Recorder:               recorder,
		Scheme:                 scheme,
		InferenceServiceConfig: inferenceServiceConfig,
		DeploymentMode:         deploymentMode,
		BaseModel:              baseModel,
		BaseModelMeta:          baseModelMeta,
		Runtime:                runtime,
		RuntimeName:            runtimeName,
		StatusManager:          status.NewStatusReconciler(),
		Log:                    ctrl.Log.WithName("DecoderReconciler"),
		SupportedModelFormat:   supportedModelFormat,
		Overlays:               overlays,
	}

	return &Decoder{
		BaseComponentFields: base,
		decoderSpec:         decoderSpec,
		deploymentReconciler: &common.DeploymentReconciler{
			Client:        client,
			Clientset:     clientset,
			Scheme:        scheme,
			StatusManager: base.StatusManager,
			Log:           base.Log,
		},
		podSpecReconciler: &common.PodSpecReconciler{
			Log: base.Log,
		},
	}
}

// Reconcile implements the Component interface for Decoder
func (d *Decoder) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	d.Log.Info("Reconciling decoder component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate decoder spec
	if d.decoderSpec == nil {
		return ctrl.Result{}, errors.New("decoder spec is nil")
	}

	// Reconcile fine-tuned weights if specified
	if isvc.Spec.Model != nil && len(isvc.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&d.BaseComponentFields, isvc); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile fine-tuned weights")
		}
	}

	// Reconcile object metadata
	objectMeta, err := d.reconcileObjectMeta(isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}

	// Reconcile pod spec
	podSpec, err := d.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Reconcile worker pod spec if needed
	workerPodSpec, err := d.reconcileWorkerPodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile worker pod spec")
	}

	// Get worker size
	size := d.getWorkerSize()

	// Reconcile deployment based on deployment mode. The deployment
	// reconciler's RequeueAfter MUST be preserved through the rest of
	// this function — OMENative's per-Instance dispatcher uses it to
	// re-run after a Sequential / Ratio gate denial OR to poll an
	// in-flight Update / Create. Discarding it strands the rollout
	// until an unrelated watch event happens to fire another reconcile
	// (the post-decoder Sequential stall, see PR #240 follow-up).
	deploymentResult, err := d.reconcileDeployment(ctx, isvc, objectMeta, podSpec, size, workerPodSpec)
	if err != nil {
		return deploymentResult, err
	}

	// Update decoder status
	if err := d.updateDecoderStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return deploymentResult, nil
}

// getWorkerSize returns the worker size for multi-node deployments
func (d *Decoder) getWorkerSize() int {
	var size int

	// Prioritize sizes in order: Decoder.Worker -> default
	switch {
	case d.decoderSpec.Worker != nil && d.decoderSpec.Worker.Size != nil:
		size = *d.decoderSpec.Worker.Size
	default:
		size = 0 // Default value
	}

	return size
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (d *Decoder) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec) (ctrl.Result, error) {
	switch d.DeploymentMode {
	case constants.RawDeployment:
		return d.deploymentReconciler.ReconcileRawDeployment(isvc, objectMeta, podSpec, &d.decoderSpec.ComponentExtensionSpec, v1beta1.DecoderComponent)
	case constants.MultiNode:
		return d.deploymentReconciler.ReconcileMultiNodeDeployment(isvc, objectMeta, podSpec, workerSize, workerPodSpec, &d.decoderSpec.ComponentExtensionSpec, v1beta1.DecoderComponent)
	case constants.OMENative:
		// Admission rejects orphan Leader/Worker and Worker.Size <= 0, so
		// the only valid multi-pod shape has both fields set.
		multiPod := d.decoderSpec.Leader != nil && d.decoderSpec.Worker != nil
		// OMENative-mode Components dispatch through the InferenceReplica
		// path by default. See the comment on the engine dispatch for the
		// full design — fallback to the omenative direct path is gated on
		// the cluster-wide --disable-ir-managed-omenative manager flag.
		if irprojector.IsIRManagedComponent(d.DeploymentMode, irprojector.IsManagerDisableFlagSet()) {
			if _, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
				ISVC:          isvc,
				Component:     v1beta1.DecoderComponent,
				ComponentExt:  &d.decoderSpec.ComponentExtensionSpec,
				ObjectMeta:    objectMeta,
				PodSpec:       podSpec,
				WorkerPodSpec: workerPodSpec,
				WorkerSize:    workerSize,
				MultiPod:      multiPod,
				Client:        d.Client,
			}); err != nil {
				return ctrl.Result{}, errors.Wrap(err, "failed to project InferenceReplica for decoder")
			}
			return ctrl.Result{}, nil
		}
		return omenative.ReconcileComponent(ctx, omenative.ReconcileParams{
			ISVC:                    isvc,
			ObjectMeta:              objectMeta,
			PodSpec:                 podSpec,
			Component:               v1beta1.DecoderComponent,
			ComponentExt:            &d.decoderSpec.ComponentExtensionSpec,
			WorkerSize:              workerSize,
			WorkerPodSpec:           workerPodSpec,
			MultiPod:                multiPod,
			GangSchedulingAvailable: d.GangSchedulingAvailable,
			Client:                  d.Client,
			APIReader:               d.APIReader,
			Expectations:            d.Expectations,
			Recorder:                d.Recorder,
		})
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for decoder")
	}
}

// updateDecoderStatus updates the status of the decoder
func (d *Decoder) updateDecoderStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&d.BaseComponentFields, isvc, v1beta1.DecoderComponent, objectMeta, d.getPodLabelInfo)
}

// getPodLabelInfo returns the pod label key and value used to filter the
// decoder's pods. All surviving deployment modes (raw, multi-node, OMENative)
// emit pods carrying the raw-deployment app label.
func (d *Decoder) getPodLabelInfo(rawDeployment bool, objectMeta metav1.ObjectMeta, statusSpec v1beta1.ComponentStatusSpec) (string, string) {
	_ = rawDeployment
	_ = statusSpec
	return constants.RawDeploymentAppLabel, constants.GetRawServiceLabel(objectMeta.Name)
}

// reconcileObjectMeta creates the object metadata for the decoder component
func (d *Decoder) reconcileObjectMeta(isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	decoderName, err := d.determineDecoderName(isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	annotations, err := d.processAnnotations(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      decoderName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := d.processLabels(isvc)

	if err != nil {
		return metav1.ObjectMeta{
			Name:        decoderName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        decoderName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// processAnnotations processes the annotations for the decoder
func (d *Decoder) processAnnotations(isvc *v1beta1.InferenceService) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	// Merge with decoder annotations
	mergedAnnotations := annotations
	if d.decoderSpec != nil {
		decoderAnnotations := d.decoderSpec.Annotations
		mergedAnnotations = utils.Union(annotations, decoderAnnotations)
	}

	// Use common function for base annotations processing
	processedAnnotations, err := ProcessBaseAnnotations(&d.BaseComponentFields, isvc, mergedAnnotations)
	if err != nil {
		return nil, err
	}

	return processedAnnotations, nil
}

// processLabels processes the labels for the decoder
func (d *Decoder) processLabels(isvc *v1beta1.InferenceService) (map[string]string, error) {
	mergedLabels := isvc.Labels
	if d.decoderSpec != nil {
		decoderLabels := d.decoderSpec.Labels
		mergedLabels = utils.Union(isvc.Labels, decoderLabels)
	}

	// Use common function for base labels processing
	return ProcessBaseLabels(&d.BaseComponentFields, isvc, v1beta1.DecoderComponent, mergedLabels)
}

// determineDecoderName determines the name of the decoder service
func (d *Decoder) determineDecoderName(isvc *v1beta1.InferenceService) (string, error) {
	// For decoder, we'll use a pattern similar to predictor but with "-decoder" suffix
	defaultDecoderName := isvc.Name + "-decoder"

	// For decoder, we'll use a pattern similar to predictor but with "-decoder" suffix
	if d.DeploymentMode != constants.MultiNode {
		existing := &v1.Service{}
		if err := d.Client.Get(context.TODO(), types.NamespacedName{Name: defaultDecoderName, Namespace: isvc.Namespace}, existing); err == nil {
			return defaultDecoderName, nil
		}
	}

	// If the default name doesn't exist, use it
	return defaultDecoderName, nil
}

// reconcilePodSpec creates the pod spec for the decoder component
func (d *Decoder) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Get the appropriate pod spec and runner based on deployment mode
	deploymentMode := d.DeploymentMode

	var basePodSpec v1beta1.PodSpec
	var runnerSpec *v1beta1.RunnerSpec

	switch deploymentMode {
	case constants.MultiNode:
		// For multi-node, use leader spec
		if d.decoderSpec.Leader != nil {
			basePodSpec = d.decoderSpec.Leader.PodSpec
			runnerSpec = d.decoderSpec.Leader.Runner
		} else {
			// Fallback to decoder spec if leader is not defined
			basePodSpec = d.decoderSpec.PodSpec
			runnerSpec = d.decoderSpec.Runner
		}
	default:
		// For raw deployment and serverless, use decoder spec
		basePodSpec = d.decoderSpec.PodSpec
		runnerSpec = d.decoderSpec.Runner
	}

	if runnerSpec != nil {
		UpdateEnvVariables(&d.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(&d.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		MergeDecoderResources(&d.BaseComponentFields, isvc, &runnerSpec.Container)
		d.setParallelismEnvVarForDecoder(&runnerSpec.Container, d.getWorkerSize())
	}

	// Use common pod spec reconciler for base logic
	podSpec, err := d.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(&d.BaseComponentFields, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(&d.BaseComponentFields, isvc, podSpec, v1beta1.DecoderComponent)

	d.Log.Info("Decoder PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// reconcileWorkerPodSpec reconciles the worker pod spec for multi-node deployments
func (d *Decoder) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Return nil if no worker spec is defined
	if d.decoderSpec.Worker == nil {
		return nil, nil
	}

	// Get leader runner spec if available
	var workerRunner *v1beta1.RunnerSpec
	if d.decoderSpec.Worker != nil {
		workerRunner = d.decoderSpec.Worker.Runner
		if workerRunner != nil {
			UpdateVolumeMounts(&d.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			UpdateEnvVariables(&d.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			MergeDecoderResources(&d.BaseComponentFields, isvc, &workerRunner.Container)
			d.setParallelismEnvVarForDecoder(&workerRunner.Container, d.getWorkerSize())
		}
	}

	// Use common reconciler for worker pod spec
	workerPodSpec, err := d.podSpecReconciler.ReconcileWorkerPodSpec(isvc, objectMeta, &d.decoderSpec.Worker.PodSpec, workerRunner)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&d.BaseComponentFields, isvc, workerPodSpec, objectMeta)
	UpdatePodSpecNodeSelector(&d.BaseComponentFields, isvc, workerPodSpec, v1beta1.DecoderComponent)

	d.Log.Info("Decoder Worker PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return workerPodSpec, nil
}

// setParallelismEnvVarForDecoder calculates and sets the PARALLELISM_SIZE environment variable for the decoder's container.
func (d *Decoder) setParallelismEnvVarForDecoder(container *v1.Container, workerReplicas int) {
	if container == nil || d.decoderSpec == nil {
		d.Log.Info("Cannot set parallelism: container or decoderSpec is nil")
		return
	}

	numGPUsPerPod := int64(isvcutils.GetGpuCountFromContainer(container))
	numLeaders := int64(0)
	numWorkers := int64(workerReplicas)

	// Determine leader presence
	if d.decoderSpec.Leader != nil {
		numLeaders = 1
	} else if d.decoderSpec.Runner != nil { // Raw deployment or single pod considered as leader
		numLeaders = 1
	}

	// Only proceed if there are GPUs and some form of parallelism (leaders or workers)
	if numGPUsPerPod > 0 && (numLeaders > 0 || numWorkers > 0) {
		parallelismSize := numGPUsPerPod * (numLeaders + numWorkers)
		if parallelismSize > 0 {
			envVar := v1.EnvVar{Name: constants.ParallelismSizeEnvVarKey, Value: strconv.FormatInt(parallelismSize, 10)}
			isvcutils.UpdateEnvVars(container, &envVar)
			d.Log.Info("Added parallelism env variable to decoder container", "value", parallelismSize, "containerName", container.Name)
		} else {
			d.Log.Info("Calculated parallelism is zero, not adding env var", "containerName", container.Name)
		}
	} else {
		d.Log.Info("Conditions not met for parallelism (no GPUs or no leaders/workers)", "containerName", container.Name, "gpus", numGPUsPerPod, "leaders", numLeaders, "workers", numWorkers)
	}
}

// GetComponentType implements ComponentConfig interface
func (d *Decoder) GetComponentType() v1beta1.ComponentType {
	return v1beta1.DecoderComponent
}

// GetComponentSpec implements ComponentConfig interface
func (d *Decoder) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if d.decoderSpec == nil {
		return nil
	}
	return &d.decoderSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (d *Decoder) GetServiceSuffix() string {
	return "-decoder"
}

// ValidateSpec implements ComponentConfig interface
func (d *Decoder) ValidateSpec() error {
	if d.decoderSpec == nil {
		return errors.New("decoder spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
