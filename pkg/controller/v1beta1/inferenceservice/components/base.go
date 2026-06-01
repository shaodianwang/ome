package components

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/utils"
)

// BaseComponentFields contains common fields for all components
type BaseComponentFields struct {
	Client    client.Client
	Clientset kubernetes.Interface
	// APIReader is the live (uncached) reader, typically mgr.GetAPIReader().
	// Use it for reads where cache lag would be a correctness problem —
	// OMENative passes it through to ReconcileParams.APIReader for revision
	// bookkeeping, audit ledger dedup, and EndpointSlice drain checks.
	APIReader client.Reader
	// Expectations is the OMENative per-controller create/delete
	// bookkeeping cache. Threaded through ReconcileParams so the
	// per-reconcile dispatch and the SetupWithManager-registered Pod
	// event handler share the same instance. nil for non-OMENative
	// modes — they don't consult it.
	Expectations *omenative.Expectations
	// Recorder is the parent controller's event recorder. Threaded
	// through ReconcileParams so OMENative ops emit K8s Events against
	// the parent ISVC at lifecycle transitions.
	Recorder record.EventRecorder
	// GangSchedulingAvailable is the cluster-discovery boolean threaded
	// from the InferenceServiceReconciler — true when the cluster has
	// the scheduler-plugins `scheduling.x-k8s.io/v1alpha1/PodGroup` CRD
	// installed. OMENative's reconciler reads this via
	// ReconcileParams.GangSchedulingAvailable to decide whether to call
	// podgroup.EnsurePodGroup for each multi-pod Instance. Other
	// deployment modes (Raw / MultiNode / PD) ignore this field.
	GangSchedulingAvailable           bool
	Scheme                            *runtime.Scheme
	InferenceServiceConfig            *controllerconfig.InferenceServicesConfig
	DeploymentMode                    constants.DeploymentModeType
	BaseModel                         *v1beta1.BaseModelSpec
	BaseModelMeta                     *metav1.ObjectMeta
	Runtime                           *v1beta1.ServingRuntimeSpec
	RuntimeName                       string
	FineTunedServing                  bool
	FineTunedServingWithMergedWeights bool
	FineTunedWeights                  []*v1beta1.FineTunedWeight
	StatusManager                     *status.StatusReconciler
	Log                               logr.Logger
	SupportedModelFormat              *v1beta1.SupportedModelFormat
	// Overlays is the resolver output for spec.model.overlays; nil when
	// none declared. Consumed by UpdateVolumeMounts / UpdateEnvVariables
	// / UpdatePodSpecVolumes.
	Overlays []isvcutils.ResolvedOverlay
}

// Common methods as functions that operate on BaseComponentFields

// ReconcileFineTunedWeights reconciles fine-tuned weights for any component
func ReconcileFineTunedWeights(b *BaseComponentFields, isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil {
		return nil
	}
	numOfFineTunedWeights := len(isvc.Spec.Model.FineTunedWeights)
	if numOfFineTunedWeights == 0 {
		return nil
	}

	b.Log.Info("FT serving mode", "Number of fine-tuned weights", numOfFineTunedWeights)
	b.FineTunedServing = true

	// TODO: lift here when start supporting stacked FT serving
	if numOfFineTunedWeights > 1 {
		return fmt.Errorf("stacked fine-tuned serving is not supported yet")
	}

	allFineTunedWeights := make([]*v1beta1.FineTunedWeight, 0)

	for _, fineTunedWeightName := range isvc.Spec.Model.FineTunedWeights {
		fineTunedWeight, err := isvcutils.GetFineTunedWeight(b.Client, fineTunedWeightName)
		if err != nil {
			return err
		}
		allFineTunedWeights = append(allFineTunedWeights, fineTunedWeight)
	}

	// Determine if loading merged fine-tuned weights
	loadingMergedFineTunedWeights, err := isvcutils.LoadingMergedFineTunedWeight(allFineTunedWeights)
	if err != nil {
		b.Log.Error(err, "Failed to determine if loading merged fine-tuned weights")
		return err
	}
	b.FineTunedServingWithMergedWeights = loadingMergedFineTunedWeights
	b.FineTunedWeights = allFineTunedWeights

	return nil
}

// UpdateVolumeMounts updates volume mounts for the container
func UpdateVolumeMounts(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	if container == nil {
		b.Log.Error(errors.New("container is nil"), "UpdateVolumeMounts: container is nil")
		return
	}

	// Add model volume mount if base model is specified and it's necessary
	if b.BaseModel != nil && !isShardedModel(b.BaseModel) && b.BaseModel.Storage != nil && b.BaseModelMeta != nil {
		if isvcutils.IsOriginalModelVolumeMountNecessary(objectMeta.Annotations) {
			if pvc := parsePVCComponents(b); pvc != nil {
				vm := corev1.VolumeMount{
					Name:      b.BaseModelMeta.Name,
					MountPath: constants.ModelDefaultMountPath,
					SubPath:   pvc.SubPath,
					ReadOnly:  true,
				}
				isvcutils.AppendVolumeMount(container, &vm)
			} else if b.BaseModel.Storage.Path != nil {
				vm := corev1.VolumeMount{
					Name:      b.BaseModelMeta.Name,
					MountPath: *b.BaseModel.Storage.Path,
					ReadOnly:  true,
				}
				isvcutils.AppendVolumeMount(container, &vm)
			}
		}
	}

	AppendOverlayVolumeMounts(b, b.Overlays, container)

	// Add fine-tuned serving volume mounts
	if b.FineTunedServing {
		defaultModelVolumeMount := corev1.VolumeMount{
			Name:      constants.ModelEmptyDirVolumeName,
			MountPath: constants.ModelDefaultMountPath,
		}
		isvcutils.AppendVolumeMountIfNotExist(container, &defaultModelVolumeMount)

		if isvcutils.IsCohereCommand1TFewFTServing(objectMeta) {
			// Update to have `base` sub-path in model volume mount for cohere tfew stacked serving case
			defaultModelVolumeMountWithSubPath := corev1.VolumeMount{
				Name:      constants.ModelEmptyDirVolumeName,
				MountPath: filepath.Join(constants.ModelDefaultMountPath, objectMeta.Annotations[constants.BaseModelFormat]),
				SubPath:   constants.BaseModelVolumeMountSubPath,
			}
			isvcutils.UpdateVolumeMount(container, &defaultModelVolumeMountWithSubPath)

			tfewFineTunedWeightVolumeMount := corev1.VolumeMount{
				Name:      constants.ModelEmptyDirVolumeName,
				MountPath: filepath.Join(constants.CohereTFewFineTunedWeightVolumeMountPath, objectMeta.Annotations[constants.BaseModelFormat]),
				ReadOnly:  true,
				SubPath:   constants.FineTunedWeightVolumeMountSubPath,
			}
			isvcutils.AppendVolumeMount(container, &tfewFineTunedWeightVolumeMount)
		}
	}
}

// UpdateEnvVariables updates environment variables for the container
func UpdateEnvVariables(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	if container == nil {
		b.Log.Error(errors.New("container is nil"), "UpdateEnvVariables: container is nil")
		return
	}

	if !b.FineTunedServing {
		// Base model serving - add MODEL_PATH env variable if necessary
		if modelPath, ok := modelPathEnvValue(b, objectMeta); ok {
			b.Log.Info("Base model serving - adding MODEL_PATH env variable if not provided", "inference service", isvc.Name, "namespace", isvc.Namespace)
			isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
				{Name: constants.ModelPathEnvVarKey, Value: modelPath},
			})
		}
		// Cluster_cache envs fire when primary OR any overlay is Sharded
		// (cross-distribution: PVC primary + Sharded overlay still needs
		// the daemon client wired in).
		if envVars, err := modelCacheEnvVarsForOverlays(b); err != nil {
			b.Log.Error(err, "Failed to build model cache environment variables", "inferenceService", isvc.Name, "namespace", isvc.Namespace)
		} else if len(envVars) > 0 {
			isvcutils.AppendEnvVarsIfNotExist(container, &envVars)
		}
		AppendOverlayEnvVars(b, b.Overlays, container)
	} else {
		// Fine-tuned serving - add vendor-specific environment variables
		if b.BaseModel != nil && b.BaseModel.Vendor != nil {
			if *b.BaseModel.Vendor == string(constants.Meta) {
				// Llama/Meta vendor specific env vars
				isvcutils.UpdateEnvVars(container, &corev1.EnvVar{
					Name: constants.ServedModelNameEnvVarKey,
					Value: filepath.Join(
						constants.LLamaVllmFTServingServedModelNamePrefix,
						objectMeta.Annotations[constants.FineTunedAdapterInjectionKey],
					),
				})
				isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
					{Name: constants.ModelPathEnvVarKey, Value: constants.ModelDefaultMountPath},
				})
			} else if *b.BaseModel.Vendor == string(constants.Cohere) {
				// Cohere vendor specific env vars
				if isvcutils.IsCohereCommand1TFewFTServing(objectMeta) {
					isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
						{Name: constants.TFewWeightPathEnvVarKey, Value: constants.CohereTFewFineTunedWeightDefaultPath},
					})
				}
			}
		} else {
			b.Log.Info("Warning: no vendor given in base model spec - no env var added/updated")
		}
	}

}

// UpdatePodSpecNodeSelector updates pod spec with node selector for model scheduling
func UpdatePodSpecNodeSelector(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, componentType v1beta1.ComponentType) {
	// Only add node selector if we have a base model
	if b.BaseModel == nil || b.BaseModelMeta == nil {
		return
	}

	// Skip node selector for fine-tuned serving with merged weights
	// as they don't need the base model on the node
	if b.FineTunedServingWithMergedWeights {
		b.Log.Info("Skipping node selector for fine-tuned serving with merged weights",
			"inferenceService", isvc.Name, "namespace", isvc.Namespace)
		return
	}

	// Skip node selector for PVC-backed models. The model agent does not
	// label nodes for PVC storage (PVCs aren't tied to specific nodes), so
	// the K8s scheduler handles placement based on PVC accessibility.
	if isPVCBaseModel(b) {
		b.Log.Info("Skipping model node selector for PVC-backed BaseModel",
			"inferenceService", isvc.Name, "namespace", isvc.Namespace)
		mergedNodeSelector := isvcutils.MergeNodeSelector(b.Runtime, isvc, componentType)
		if len(mergedNodeSelector) > 0 {
			if podSpec.NodeSelector == nil {
				podSpec.NodeSelector = make(map[string]string)
			}
			for k, v := range mergedNodeSelector {
				podSpec.NodeSelector[k] = v
			}
		}
		return
	}

	// Add preferred node affinity for model readiness using the shared utility function
	if isShardedModel(b.BaseModel) {
		b.Log.Info("Skipping per-node model readiness selector for sharded model",
			"modelName", b.BaseModelMeta.Name,
			"namespace", b.BaseModelMeta.Namespace,
			"inferenceService", isvc.Name)
	} else {
		isvcutils.AddNodeSelectorForModelReadyNode(podSpec, b.BaseModelMeta)
	}

	// Merge node selectors from runtime + ISVC (engine/decoder only).
	mergedNodeSelector := isvcutils.MergeNodeSelector(b.Runtime, isvc, componentType)
	if len(mergedNodeSelector) > 0 {
		if podSpec.NodeSelector == nil {
			podSpec.NodeSelector = make(map[string]string)
		}
		for k, v := range mergedNodeSelector {
			podSpec.NodeSelector[k] = v
		}
	}

	if !isShardedModel(b.BaseModel) {
		b.Log.Info("Added preferred node affinity for model scheduling",
			"modelName", b.BaseModelMeta.Name,
			"namespace", b.BaseModelMeta.Namespace,
			"inferenceService", isvc.Name)
	}
}

// UpdatePodSpecVolumes updates pod spec with common volumes
func UpdatePodSpecVolumes(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta) {
	// Add model volume if base model is specified.
	if b.BaseModel != nil && !isShardedModel(b.BaseModel) && b.BaseModel.Storage != nil && b.BaseModelMeta != nil {
		if pvc := parsePVCComponents(b); pvc != nil {
			modelVolume := corev1.Volume{
				Name: b.BaseModelMeta.Name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.PVCName,
						ReadOnly:  true,
					},
				},
			}
			podSpec.Volumes = append(podSpec.Volumes, modelVolume)
		} else if b.BaseModel.Storage.Path != nil {
			modelVolume := corev1.Volume{
				Name: b.BaseModelMeta.Name,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: *b.BaseModel.Storage.Path,
					},
				},
			}
			podSpec.Volumes = append(podSpec.Volumes, modelVolume)
		}
	}

	AppendOverlayVolumes(b, b.Overlays, podSpec)

	// Add empty model directory volume if required for fine-tuned serving
	if isvcutils.IsEmptyModelDirVolumeRequired(objectMeta.Annotations) {
		emptyModelDirVolume := corev1.Volume{
			Name: constants.ModelEmptyDirVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				},
			},
		}
		podSpec.Volumes = utils.AppendVolumeIfNotExists(podSpec.Volumes, emptyModelDirVolume)
	}
}

func isShardedModel(model *v1beta1.BaseModelSpec) bool {
	return model != nil && model.Distribution != nil && *model.Distribution == v1beta1.DistributionSharded
}

func modelPathEnvValue(b *BaseComponentFields, objectMeta *metav1.ObjectMeta) (string, bool) {
	if b == nil || b.BaseModel == nil || b.BaseModel.Storage == nil {
		return "", false
	}
	if isShardedModel(b.BaseModel) {
		if b.BaseModel.Storage.StorageUri == nil || *b.BaseModel.Storage.StorageUri == "" {
			return "", false
		}
		return *b.BaseModel.Storage.StorageUri, true
	}
	if objectMeta == nil || !isvcutils.IsOriginalModelVolumeMountNecessary(objectMeta.Annotations) {
		return "", false
	}
	if isPVCBaseModel(b) {
		return constants.ModelDefaultMountPath, true
	}
	if b.BaseModel.Storage.Path == nil || *b.BaseModel.Storage.Path == "" {
		return "", false
	}
	return *b.BaseModel.Storage.Path, true
}

// modelCacheEnvVarsForOverlays fires when primary OR any overlay is
// Sharded — covers the cross-distribution PVC-primary + Sharded-overlay
// case where the pod still needs the cluster_cache client.
func modelCacheEnvVarsForOverlays(b *BaseComponentFields) ([]corev1.EnvVar, error) {
	if b == nil {
		return nil, nil
	}
	if isShardedModel(b.BaseModel) || AnyOverlayIsSharded(b.Overlays) {
		return clusterCacheEnvVars(b)
	}
	return nil, nil
}

func clusterCacheEnvVars(b *BaseComponentFields) ([]corev1.EnvVar, error) {
	if b.InferenceServiceConfig == nil {
		return nil, nil
	}
	config := b.InferenceServiceConfig.ModelCache
	provider := strings.TrimSpace(config.Provider)
	if provider == "" {
		return nil, nil
	}

	envVars := []corev1.EnvVar{
		{Name: constants.ModelCacheProviderEnvVarKey, Value: provider},
	}
	if endpoint := strings.TrimSpace(config.Endpoint); endpoint != "" {
		envVars = append(envVars, corev1.EnvVar{Name: constants.ModelCacheEndpointEnvVarKey, Value: endpoint})
		if provider == sharded.ProviderClusterCache {
			envVars = append(envVars, corev1.EnvVar{
				Name:  constants.ClusterCacheHeadlessServiceEnvVarKey,
				Value: modelCacheEndpointHostPort(endpoint),
			})
		}
	}

	options := config.Options
	if options == nil {
		options = map[string]string{}
	}
	optionsJSON, err := json.Marshal(options)
	if err != nil {
		return nil, err
	}
	envVars = append(envVars, corev1.EnvVar{Name: constants.ModelCacheOptionsEnvVarKey, Value: string(optionsJSON)})
	return envVars, nil
}

func modelCacheEndpointHostPort(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		return parsed.Host
	}
	if parsed, err := url.Parse("//" + endpoint); err == nil && parsed.Host != "" {
		return parsed.Host
	}
	return endpoint
}

// isResourcesUnspecified checks if the resource requirements are unspecified
func isResourcesUnspecified(resources corev1.ResourceRequirements) bool {
	return resources.Limits == nil && resources.Requests == nil && len(resources.Claims) == 0
}

// MergeEngineResources fills the engine container's resource requests/
// limits from the runtime when the operator hasn't set them on the
// InferenceService spec.
func MergeEngineResources(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container) {
	if isvc.Spec.Engine != nil &&
		(isvc.Spec.Engine.Runner == nil ||
			isResourcesUnspecified(isvc.Spec.Engine.Runner.Container.Resources)) {
		b.Log.Info("Merging resources for engine container as user did not specify resources in InferenceService")
		isvcutils.MergeResource(container, b.Runtime)
	}
}

// MergeDecoderResources fills the decoder container's resource requests/
// limits from the runtime when the operator hasn't set them on the
// InferenceService spec.
func MergeDecoderResources(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container) {
	if isvc.Spec.Decoder != nil &&
		(isvc.Spec.Decoder.Runner == nil ||
			isResourcesUnspecified(isvc.Spec.Decoder.Runner.Container.Resources)) {
		b.Log.Info("Merging resources for decoder container as user did not specify resources in InferenceService")
		isvcutils.MergeResource(container, b.Runtime)
	}
}

// ProcessBaseAnnotations processes common annotations
func ProcessBaseAnnotations(b *BaseComponentFields, isvc *v1beta1.InferenceService, annotations map[string]string) (map[string]string, error) {
	// Add fine-tuned weight annotations if applicable
	if b.FineTunedServing && len(b.FineTunedWeights) > 0 {
		// Inject ft adapter for single/non-stacked fine-tuned weight downloading
		annotations[constants.FineTunedAdapterInjectionKey] = b.FineTunedWeights[0].Name

		// Add fine-tuned weight ft strategy
		fineTunedWeightFTStrategy, err := isvcutils.GetValueFromRawExtension(b.FineTunedWeights[0].Spec.HyperParameters, constants.StrategyConfigKey)
		if err != nil || fineTunedWeightFTStrategy == nil {
			b.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", b.FineTunedWeights[0].Name, "namespace", isvc.Namespace)
			return nil, err
		}
		annotations[constants.FineTunedWeightFTStrategyKey] = fineTunedWeightFTStrategy.(string)
	}

	if b.FineTunedServingWithMergedWeights {
		b.Log.Info("Fine-tuned serving with merged weights", "namespace", isvc.Namespace)
		annotations[constants.FTServingWithMergedWeightsAnnotationKey] = "true"
	}

	// Add base model specific annotations
	if b.BaseModel != nil && b.BaseModelMeta != nil {
		annotations[constants.BaseModelName] = b.BaseModelMeta.Name
		if b.BaseModel.Vendor != nil {
			annotations[constants.BaseModelVendorAnnotationKey] = *b.BaseModel.Vendor
		}
		annotations[constants.BaseModelFormat] = b.BaseModel.ModelFormat.Name
		if b.BaseModel.ModelFormat.Version != nil {
			annotations[constants.BaseModelFormatVersion] = *b.BaseModel.ModelFormat.Version
		}
	}

	if b.RuntimeName != "" {
		annotations[constants.ServingRuntimeKeyName] = b.RuntimeName
	}

	return annotations, nil
}

// ProcessBaseLabels processes common labels
func ProcessBaseLabels(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, labels map[string]string) (map[string]string, error) {
	baseModelCategory := "SMALL"
	if b.BaseModelMeta != nil {
		if category, ok := b.BaseModelMeta.Annotations[constants.ModelCategoryAnnotation]; ok {
			baseModelCategory = category
		}
	}

	baseLabels := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(componentType),
		constants.ServingRuntimeLabelKey:      b.RuntimeName,
		constants.FTServingLabelKey:           strconv.FormatBool(b.FineTunedServing),
	}

	// Merge with provided labels
	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range baseLabels {
		labels[k] = v
	}

	if b.BaseModelMeta != nil {
		labels[constants.InferenceServiceBaseModelNameLabelKey] = b.BaseModelMeta.Name
		labels[constants.InferenceServiceBaseModelSizeLabelKey] = baseModelCategory
		labels[constants.BaseModelTypeLabelKey] = string(constants.ServingBaseModel)
	}

	if b.BaseModel != nil && b.BaseModel.Vendor != nil {
		labels[constants.BaseModelVendorLabelKey] = *b.BaseModel.Vendor
	}

	// Add fine-tuned serving related labels
	if b.FineTunedServing && len(b.FineTunedWeights) > 0 {
		ftStrategyParameter, err := isvcutils.GetValueFromRawExtension(b.FineTunedWeights[0].Spec.HyperParameters, constants.StrategyConfigKey)
		if err != nil {
			b.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", b.FineTunedWeights[0].Name, "namespace", isvc.Namespace)
			return nil, err
		}

		fineTunedWeightFTStrategy := ""
		if ftStrategyParameter != nil {
			fineTunedWeightFTStrategy = ftStrategyParameter.(string)
		}
		labels[constants.FineTunedWeightFTStrategyLabelKey] = fineTunedWeightFTStrategy

		labels[constants.FTServingWithMergedWeightsLabelKey] = strconv.FormatBool(b.FineTunedServingWithMergedWeights)
	}

	return labels, nil
}

// UpdateComponentStatus updates component status based on deployment mode.
// All surviving deployment modes (RawDeployment, MultiNode, OMENative)
// emit pods carrying the raw-deployment app label — the engine / decoder
// / router previously routed the same constant pair through a per-component
// getPodLabelInfo callback; that indirection was dead and is inlined here.
func UpdateComponentStatus(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, objectMeta metav1.ObjectMeta) error {
	// Always initialize the component ready condition to ensure it's visible from the start
	// The deployment reconciler will update the condition based on the actual deployment status:
	// - MultiNode: Updates when LWS becomes available
	// - RawDeployment: Updates when Deployment becomes available
	b.StatusManager.InitializeComponentCondition(&isvc.Status, componentType)

	// OEP-0013 Slice 4: mirror the resolved per-Component autoscaler block
	// + canonical scale target onto status.components.<c>.{autoscaler,
	// scaleTargetRef}. Runs before the lean-path return so operators always
	// see the resolved class / managed-by / scale target — even on
	// model-less ISVCs that exit early below.
	//
	// Slice 3 (in flight) wires the dispatch that actually creates the
	// HPA / ScaledObject; until that lands, the live-mirror branches
	// return NotFound and counters / conditions stay zero. ManagedBy
	// still correctly reports "ome" for default class=hpa ISVCs (so
	// operators can see the resolved class), and "none" for the
	// AutoscalerNone / unknown branches.
	if err := writeComponentAutoscalerStatus(b, isvc, componentType, objectMeta); err != nil {
		return errors.Wrapf(err, "failed to write %s autoscaler status", componentType)
	}

	// Lean path: with no spec.model there is no model-loading lifecycle to
	// surface — skip the modelStatus writer so we do not stamp a
	// transitionStatus/modelRevisionStates block that will never reach
	// "UpToDate"/"Loaded". The webhook permits omitting spec.model when
	// spec.runtime is set explicitly; we honor that here by leaving
	// status.modelStatus untouched.
	if isvc.Spec.Model == nil {
		return nil
	}

	// Update model status for all deployment modes based on actual pod information
	rawDeployment := b.DeploymentMode == constants.RawDeployment
	statusSpec := isvc.Status.Components[componentType]
	podLabelKey := constants.RawDeploymentAppLabel
	podLabelValue := constants.GetRawServiceLabel(objectMeta.Name)

	pods, err := isvcutils.ListPodsByLabel(b.Client, isvc.ObjectMeta.Namespace, podLabelKey, podLabelValue)
	if err != nil {
		return errors.Wrapf(err, "failed to list %s pods by label", componentType)
	}
	b.StatusManager.PropagateModelStatus(&isvc.Status, statusSpec, pods, rawDeployment)

	return nil
}

// writeComponentAutoscalerStatus resolves the per-Component autoscaler from
// the ISVC -> runtime -> default chain (Slice 1) and stamps the resulting
// ComponentAutoscalerStatus + ScaleTargetRef onto status.components.<c>.
// Existing fields on the ComponentStatusSpec entry are preserved — only the
// .autoscaler and .scaleTargetRef sub-fields are overwritten.
//
// OEP-0013 Slice 4. See pkg/.../reconcilers/autoscaler/status.go for the
// underlying mapping + live-mirror semantics.
func writeComponentAutoscalerStatus(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, objectMeta metav1.ObjectMeta) error {
	resolved, source := autoscaler.ResolveComponentAutoscaler(b.Runtime, isvc, componentType)

	scaleTargetRef := canonicalScaleTargetRef(b.DeploymentMode, isvc.Name, objectMeta.Name, componentType)

	// For OMENative-managed Components the dispatch (Slice 3) names the
	// HPA / ScaledObject after the InferenceReplica; for RawDeployment it
	// uses the legacy component metadata Name. The writer matches that
	// lookup pattern so the live mirror finds the right object.
	objectName := objectMeta.Name
	if irprojector.IsIRManagedComponent(b.DeploymentMode, irprojector.IsManagerDisableFlagSet()) {
		objectName = irprojector.InferenceReplicaName(isvc.Name, componentType)
	}

	asStatus, stRef, err := autoscaler.WriteAutoscalerStatus(
		context.Background(),
		b.Client,
		isvc.Namespace,
		objectName,
		resolved,
		source,
		scaleTargetRef,
	)
	if err != nil {
		return err
	}

	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	existing := isvc.Status.Components[componentType]
	existing.Autoscaler = asStatus
	existing.ScaleTargetRef = stRef
	isvc.Status.Components[componentType] = existing
	return nil
}

// canonicalScaleTargetRef returns the scale target an external scaler should
// point at for the given Component. OMENative-managed (default + IR-projected)
// → InferenceReplica's /scale subresource; everything else → the underlying
// Deployment via the legacy component metadata Name.
//
// Empty values are returned when the deployment mode isn't recognized so the
// status writer can surface "no published target" cleanly (the writer drops
// an all-empty ScaleTargetRef rather than emitting an obviously-broken
// `{apiVersion:"",kind:"",name:""}` block).
func canonicalScaleTargetRef(mode constants.DeploymentModeType, isvcName, componentMetaName string, componentType v1beta1.ComponentType) v1beta1.ScaleTargetRef {
	if irprojector.IsIRManagedComponent(mode, irprojector.IsManagerDisableFlagSet()) {
		return v1beta1.ScaleTargetRef{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "InferenceReplica",
			Name:       irprojector.InferenceReplicaName(isvcName, componentType),
		}
	}
	switch mode {
	case constants.RawDeployment:
		return v1beta1.ScaleTargetRef{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       componentMetaName,
		}
	default:
		return v1beta1.ScaleTargetRef{}
	}
}

// ReconcileOMENativeSubresources ensures the per-Component stable
// Service (`<isvc>-<comp>`) and PodMonitor for an OMENative-managed
// Component (engine / decoder / router). Engine, Decoder, and Router
// share this implementation byte-for-byte — only the component enum,
// the per-Component ComponentExtensionSpec, and (implicitly via
// objectMeta.Name) the resource name differ.
//
// The base selector is the OMENative-specific three-key tuple
// (InferenceServicePodLabel + OMEComponentLabel + ManagedBy=OMENative)
// so the stable Service + PodMonitor scope to OMENative-stamped pods
// only — a same-Component mode switch (engine OMENative -> engine
// RawDeployment) doesn't strand traffic on the wrong pod set during
// the transition. PodMonitor scrape port matches the Raw fallback
// rules: prefer a port named "metrics", else the first declared port,
// else "http".
//
// Stable Service selector — for multi-pod Components, also pin to
// `pod-ordinal=0` so customer traffic lands only on the rank-0
// (leader) pod. Workers run distributed-init peers and cannot
// handle inference requests; without this filter, kube-proxy would
// round-robin to all pods of the gang and ~(Size-1)/Size of
// requests would 502.
//
// Single-pod Components keep the broader selector: SurgeThenDrain
// alternates the pod-naming ordinal between 0 and 1 across surges
// (see query.LabelPodOrdinal docstring), so pinning ordinal=0
// would zero-endpoint the Service during the surge phase. Multi-pod
// Components use RecreateInstance (not SurgeThenDrain), so the
// rank-0 pod ordinal is stable.
//
// PodMonitor intentionally uses the base selector (no pod-ordinal
// filter) so Prometheus scrapes EVERY pod of the gang — workers
// emit per-rank metrics too, and per-pod scraping is the standard
// Prometheus model.
//
// When podSpec is nil (MinReplicas=0 cold-start, no rendered template)
// both reconcilers no-op: the Service reconciler would emit a portless
// ClusterIP and the PodMonitor would have no scrape target. Both are
// restored on the next reconcile pass after scale-up.
//
// Inlined PodMonitor build (rather than via the shared podmonitor
// reconciler) because the shared reconciler hardcodes the
// `app=<name>` selector that matches Raw / MultiNode pods but NOT
// OMENative pods. CreateOrUpdate is idempotent + drift-correcting on
// the three labelSelector / NamespaceSelector / PodMetricsEndpoints
// shape fields.
func ReconcileOMENativeSubresources(
	ctx context.Context,
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentExt *v1beta1.ComponentExtensionSpec,
	objectMeta metav1.ObjectMeta,
	podSpec *corev1.PodSpec,
) error {
	if podSpec == nil {
		return nil
	}
	// Base selector — narrows to OMENative-managed pods of this (ISVC,
	// Component) pair. Used as-is for PodMonitor; augmented with a
	// `pod-ordinal=0` filter for the stable Service of multi-pod
	// Components (see function-level docstring for rationale).
	baseSelector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(componentType),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	stableSelector := baseSelector
	if isvcutils.IsMultiPodComponent(isvc, componentType) {
		stableSelector = make(map[string]string, len(baseSelector)+1)
		for k, v := range baseSelector {
			stableSelector[k] = v
		}
		stableSelector[query.LabelPodOrdinal] = "0"
	}
	componentMeta := objectMeta.DeepCopy()
	componentMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}

	// Stable Service via the SAME top-level reconciler RawDeployment and
	// MultiNode use; only the selector + owner refs are OMENative-shaped.
	sr := service.NewServiceReconciler(b.Client, b.Scheme, *componentMeta, componentExt, podSpec, stableSelector)
	if _, err := sr.Reconcile(); err != nil {
		return errors.Wrap(err, "stable service")
	}

	// PodMonitor scrape port follows the Raw fallback rules: prefer a
	// port named "metrics", else the first declared port, else "http".
	portName := "http"
	if len(podSpec.Containers) > 0 {
		ports := podSpec.Containers[0].Ports
		for _, p := range ports {
			if p.Name == "metrics" {
				portName = "metrics"
				break
			}
		}
		if portName == "http" && len(ports) > 0 && ports[0].Name != "" {
			portName = ports[0].Name
		}
	}
	target := &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectMeta.Name,
			Namespace: objectMeta.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, b.Client, target, func() error {
		target.Spec.NamespaceSelector = monitoringv1.NamespaceSelector{
			MatchNames: []string{objectMeta.Namespace},
		}
		target.Spec.Selector = metav1.LabelSelector{MatchLabels: baseSelector}
		target.Spec.PodMetricsEndpoints = []monitoringv1.PodMetricsEndpoint{{
			Port:     &portName,
			Path:     "/metrics",
			Interval: "10s",
		}}
		target.Labels = baseSelector
		target.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "pod monitor %s/%s", target.Namespace, target.Name)
	}
	return nil
}

// ReconcileComponentObjectMeta builds the common ObjectMeta block
// (Name, Namespace, Annotations, Labels) shared by engine / decoder /
// router. The per-Component name is resolved upstream because the
// fallback logic (Service-existence lookup, MultiNode gating) still
// differs across components (see section 4 of the components-dispatch
// review). The annotation / label maps are the per-Component merge
// (ISVC + componentExt.Annotations / componentExt.Labels) already
// performed by the caller.
//
// On annotation-build failure the returned ObjectMeta carries Name +
// Namespace only; on label-build failure the returned ObjectMeta
// carries Name + Namespace + Annotations — preserving the partial-
// metadata error-return shape the individual receivers used to
// surface.
func ReconcileComponentObjectMeta(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentName string,
	componentAnnotations map[string]string,
	componentLabels map[string]string,
) (metav1.ObjectMeta, error) {
	annotations, err := ProcessComponentAnnotations(b, isvc, componentAnnotations)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      componentName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := ProcessComponentLabels(b, isvc, componentType, componentLabels)
	if err != nil {
		return metav1.ObjectMeta{
			Name:        componentName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        componentName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// ProcessComponentAnnotations performs the per-Component annotation
// build: filter the ISVC-level annotations against the disallowed
// list, union them with the Component-level annotations, then hand
// off to ProcessBaseAnnotations for the FT / BaseModel / runtime
// annotations the base layer adds.
func ProcessComponentAnnotations(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentAnnotations map[string]string,
) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	mergedAnnotations := annotations
	if componentAnnotations != nil {
		mergedAnnotations = utils.Union(annotations, componentAnnotations)
	}

	return ProcessBaseAnnotations(b, isvc, mergedAnnotations)
}

// ProcessComponentLabels performs the per-Component label build:
// union the ISVC-level labels with the Component-level labels, then
// hand off to ProcessBaseLabels for the FT / BaseModel / runtime
// labels the base layer adds.
func ProcessComponentLabels(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentLabels map[string]string,
) (map[string]string, error) {
	mergedLabels := isvc.Labels
	if componentLabels != nil {
		mergedLabels = utils.Union(isvc.Labels, componentLabels)
	}

	return ProcessBaseLabels(b, isvc, componentType, mergedLabels)
}
