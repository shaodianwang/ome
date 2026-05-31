package components

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/utils"
)

var _ Component = &Engine{}
var _ ComponentConfig = &Engine{}

// Engine reconciles resources for the engine component
type Engine struct {
	BaseComponentFields
	engineSpec           *v1beta1.EngineSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
}

// NewEngine creates a new Engine component instance. apiReader is the
// live (uncached) reader threaded through to OMENative dispatch;
// expectations is the per-controller Expectations cache shared with
// the SetupWithManager-registered Pod watch; recorder emits
// ISVC-scoped lifecycle events. Any may be nil — the OMENative
// backend falls back to the cached client, the DefaultExpectations
// singleton, and a no-op event recorder respectively.
func NewEngine(
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
	engineSpec *v1beta1.EngineSpec,
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
		Log:                    ctrl.Log.WithName("EngineReconciler"),
		SupportedModelFormat:   supportedModelFormat,
		Overlays:               overlays,
	}

	return &Engine{
		BaseComponentFields: base,
		engineSpec:          engineSpec,
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

// Reconcile implements the Component interface for Engine
func (e *Engine) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	e.Log.Info("Reconciling engine component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate engine spec
	if e.engineSpec == nil {
		return ctrl.Result{}, errors.New("engine spec is nil")
	}

	// Reconcile fine-tuned weights if specified
	if isvc.Spec.Model != nil && len(isvc.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&e.BaseComponentFields, isvc); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile fine-tuned weights")
		}
	}

	// Reconcile object metadata
	objectMeta, err := e.reconcileObjectMeta(isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}

	// Reconcile pod spec
	podSpec, err := e.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Reconcile worker pod spec if needed
	workerPodSpec, err := e.reconcileWorkerPodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile worker pod spec")
	}

	// Get worker size
	size := e.getWorkerSize()

	// Reconcile deployment based on deployment mode. The deployment
	// reconciler's RequeueAfter MUST be preserved through the rest of
	// this function — OMENative's per-Instance dispatcher uses it to
	// re-run after a Sequential / Ratio gate denial OR to poll an
	// in-flight Update / Create. Discarding it strands the rollout
	// until an unrelated watch event happens to fire another reconcile
	// (the post-decoder Sequential stall, see PR #240 follow-up).
	deploymentResult, err := e.reconcileDeployment(ctx, isvc, objectMeta, podSpec, size, workerPodSpec)
	if err != nil {
		return deploymentResult, err
	}

	// Per-Component stable Service + PodMonitor for OMENative. Inlined
	// here (rather than driven from the omenative dispatch) so the same
	// pair of supporting resources is emitted for BOTH OMENative
	// sub-paths uniformly: the IR-managed path (default since PR #320,
	// which bypasses omenative.ReconcileComponent entirely) AND the
	// legacy direct omenative path. The omenative package itself manages
	// only pods + the per-Component headless Service (whose naming is
	// tied to the per-Component pod template); every other top-level
	// per-Component sub-resource is owned by the ISVC controller.
	//
	// Raw / MultiNode keep their dispatcher-side service+podmonitor calls
	// because their selectors are mode-specific (Raw: nil → pod template
	// labels; MultiNode: worker-index=0 leader-only routing); hoisting
	// them is queued as a separate follow-up.
	if e.DeploymentMode == constants.OMENative {
		if err := e.reconcileOMENativeSubresources(ctx, isvc, objectMeta, podSpec); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile engine OMENative sub-resources")
		}
	}

	// Update engine status
	if err := e.updateEngineStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return deploymentResult, nil
}

// RenderPodGroupDeploymentTemplate renders the engine pod template used by
// PodGroupDeploymentRollout without reconciling the normal engine workload.
func (e *Engine) RenderPodGroupDeploymentTemplate(ctx context.Context, isvc *v1beta1.InferenceService) (*v1.PodTemplateSpec, error) {
	_ = ctx
	if e.engineSpec == nil {
		return nil, errors.New("engine spec is nil")
	}
	if isvc.Spec.Model != nil && len(isvc.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&e.BaseComponentFields, isvc); err != nil {
			return nil, errors.Wrap(err, "failed to reconcile fine-tuned weights")
		}
	}
	objectMeta, err := e.reconcileObjectMeta(isvc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to reconcile object metadata")
	}
	podSpec, err := e.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return nil, errors.Wrap(err, "failed to reconcile pod spec")
	}
	return &v1.PodTemplateSpec{
		ObjectMeta: objectMeta,
		Spec:       *podSpec,
	}, nil
}

// reconcileOMENativeSubresources ensures the per-Component stable
// Service (`<isvc>-<comp>`) and PodMonitor for OMENative-managed
// engine. Decoder and Router have their own equivalent methods —
// repetition is accepted here over a shared helper so the omenative
// package can stay pure (pods + headless Service ONLY) without
// introducing a new abstraction.
//
// The selector is the OMENative-specific three-key tuple
// (InferenceServicePodLabel + OMEComponentLabel + ManagedBy=OMENative)
// so the stable Service + PodMonitor scope to OMENative-stamped pods
// only — a same-Component mode switch (engine OMENative → engine
// RawDeployment) doesn't strand traffic on the wrong pod set during
// the transition. PodMonitor scrape port matches the Raw fallback
// rules: prefer a port named "metrics", else the first declared port,
// else "http".
//
// When podSpec is nil (MinReplicas=0 cold-start, no rendered template)
// both reconcilers no-op: the Service reconciler would emit a portless
// ClusterIP and the PodMonitor would have no scrape target. Both are
// restored on the next reconcile pass after scale-up.
func (e *Engine) reconcileOMENativeSubresources(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) error {
	if podSpec == nil {
		return nil
	}
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	componentMeta := objectMeta.DeepCopy()
	componentMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}

	// Stable Service via the SAME top-level reconciler RawDeployment and
	// MultiNode use; only the selector + owner refs are OMENative-shaped.
	sr := service.NewServiceReconciler(e.Client, e.Scheme, *componentMeta, &e.engineSpec.ComponentExtensionSpec, podSpec, selector)
	if _, err := sr.Reconcile(); err != nil {
		return errors.Wrap(err, "stable service")
	}

	// PodMonitor — build inline (rather than via the shared podmonitor
	// reconciler) because the shared reconciler hardcodes the
	// `app=<name>` selector that matches Raw / MultiNode pods but NOT
	// OMENative pods. CreateOrUpdate is idempotent + drift-correcting on
	// the three labelSelector / NamespaceSelector / PodMetricsEndpoints
	// shape fields. Scrape port follows the Raw fallback rules: prefer a
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
	if _, err := controllerutil.CreateOrUpdate(ctx, e.Client, target, func() error {
		target.Spec.NamespaceSelector = monitoringv1.NamespaceSelector{
			MatchNames: []string{objectMeta.Namespace},
		}
		target.Spec.Selector = metav1.LabelSelector{MatchLabels: selector}
		target.Spec.PodMetricsEndpoints = []monitoringv1.PodMetricsEndpoint{{
			Port:     &portName,
			Path:     "/metrics",
			Interval: "10s",
		}}
		target.Labels = selector
		target.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "pod monitor %s/%s", target.Namespace, target.Name)
	}
	return nil
}

// getWorkerSize returns the worker size for multi-node deployments
func (e *Engine) getWorkerSize() int {
	var size int

	// Prioritize sizes in order: Engine.Worker -> default
	switch {
	case e.engineSpec.Worker != nil && e.engineSpec.Worker.Size != nil:
		size = *e.engineSpec.Worker.Size
	default:
		size = 0 // Default value
	}

	return size
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (e *Engine) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec) (ctrl.Result, error) {
	switch e.DeploymentMode {
	case constants.RawDeployment:
		return e.deploymentReconciler.ReconcileRawDeployment(isvc, objectMeta, podSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	case constants.MultiNode:
		return e.deploymentReconciler.ReconcileMultiNodeDeployment(isvc, objectMeta, podSpec, workerSize, workerPodSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	case constants.OMENative:
		// Admission rejects orphan Leader/Worker and Worker.Size <= 0, so
		// the only valid multi-pod shape has both fields set.
		multiPod := e.engineSpec.Leader != nil && e.engineSpec.Worker != nil
		// OMENative-mode Components dispatch through the InferenceReplica
		// path by default: the ISVC controller projects the desired
		// per-Component spec onto an InferenceReplica object; the IR
		// controller drives per-Instance lifecycle from there. The
		// cluster-wide --disable-ir-managed-omenative manager flag is the
		// operator escape hatch — when set, IsIRManagedComponent returns
		// false and routing falls through to the omenative direct path
		// below.
		if irprojector.IsIRManagedComponent(e.DeploymentMode, irprojector.IsManagerDisableFlagSet()) {
			// Resolve the authoritative ComponentAutoscaler from the
			// ISVC → runtime → default chain (OEP-0013 §"Inheritance
			// chain"). The resolved block is projected onto
			// ir.Spec.Autoscaler by the projector; Slice 3 then
			// dispatches HPA / KEDA / external / none against the
			// committed IR (always-on per Q7).
			resolvedAS, _ := autoscaler.ResolveComponentAutoscaler(e.Runtime, isvc, v1beta1.EngineComponent)
			ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
				ISVC:               isvc,
				Component:          v1beta1.EngineComponent,
				ComponentExt:       &e.engineSpec.ComponentExtensionSpec,
				ObjectMeta:         objectMeta,
				PodSpec:            podSpec,
				WorkerPodSpec:      workerPodSpec,
				WorkerSize:         workerSize,
				MultiPod:           multiPod,
				ResolvedAutoscaler: resolvedAS,
				Client:             e.Client,
			})
			if err != nil {
				return ctrl.Result{}, errors.Wrap(err, "failed to project InferenceReplica for engine")
			}
			// OEP-0013 Slice 3: always-on per-IR autoscaler dispatch (Q7).
			// Owner-ref the HPA / SO to the IR so foreground GC cascades
			// when the IR is deleted; ScaleTargetRef points at the IR's
			// /scale subresource. Q5: external + none are status-field
			// twins — both fall through to the dispatch's "delete both"
			// branch with no separate code path.
			if err := autoscaler.DispatchForIRComponent(ctx, autoscaler.IRDispatchInput{
				Client:             e.Client,
				Scheme:             e.Scheme,
				IR:                 ir,
				ResolvedAutoscaler: resolvedAS,
				ComponentExt:       &e.engineSpec.ComponentExtensionSpec,
			}); err != nil {
				return ctrl.Result{}, errors.Wrap(err, "failed to dispatch autoscaler for engine")
			}
			return ctrl.Result{}, nil
		}
		return omenative.ReconcileComponent(ctx, omenative.ReconcileParams{
			ISVC:                    isvc,
			ObjectMeta:              objectMeta,
			PodSpec:                 podSpec,
			Component:               v1beta1.EngineComponent,
			ComponentExt:            &e.engineSpec.ComponentExtensionSpec,
			WorkerSize:              workerSize,
			WorkerPodSpec:           workerPodSpec,
			MultiPod:                multiPod,
			GangSchedulingAvailable: e.GangSchedulingAvailable,
			Client:                  e.Client,
			APIReader:               e.APIReader,
			Expectations:            e.Expectations,
			Recorder:                e.Recorder,
		})
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for engine")
	}
}

// updateEngineStatus updates the status of the engine
func (e *Engine) updateEngineStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, objectMeta)
}

// reconcileObjectMeta creates the object metadata for the engine component
func (e *Engine) reconcileObjectMeta(isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	engineName, err := e.determineEngineName(isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	annotations, err := e.processAnnotations(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      engineName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := e.processLabels(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:        engineName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        engineName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// processAnnotations processes the annotations for the engine
func (e *Engine) processAnnotations(isvc *v1beta1.InferenceService) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	// Merge with engine annotations
	mergedAnnotations := annotations
	if e.engineSpec != nil {
		engineAnnotations := e.engineSpec.Annotations
		mergedAnnotations = utils.Union(annotations, engineAnnotations)
	}

	// Use common function for base annotations processing
	processedAnnotations, err := ProcessBaseAnnotations(&e.BaseComponentFields, isvc, mergedAnnotations)
	if err != nil {
		return nil, err
	}

	return processedAnnotations, nil
}

// processLabels processes the labels for the engine
func (e *Engine) processLabels(isvc *v1beta1.InferenceService) (map[string]string, error) {
	mergedLabels := isvc.Labels
	if e.engineSpec != nil {
		engineLabels := e.engineSpec.Labels
		mergedLabels = utils.Union(isvc.Labels, engineLabels)
	}

	// Use common function for base labels processing
	return ProcessBaseLabels(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, mergedLabels)
}

// determineEngineName determines the name of the engine service.
// The suffix is sourced from GetServiceSuffix so the engine / decoder /
// router suffix lives in exactly one place (ComponentConfig).
func (e *Engine) determineEngineName(isvc *v1beta1.InferenceService) (string, error) {
	defaultEngineName := isvc.Name + e.GetServiceSuffix()

	existing := &v1.Service{}
	if err := e.Client.Get(context.TODO(), types.NamespacedName{Name: defaultEngineName, Namespace: isvc.Namespace}, existing); err == nil {
		return defaultEngineName, nil
	}

	return defaultEngineName, nil
}

// reconcilePodSpec creates the pod spec for the engine component
func (e *Engine) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Get the appropriate pod spec and runner based on deployment mode
	deploymentMode := isvcutils.DetermineEngineDeploymentMode(e.engineSpec)

	var basePodSpec v1beta1.PodSpec
	var runnerSpec *v1beta1.RunnerSpec

	switch deploymentMode {
	case constants.MultiNode:
		// For multi-node, use leader spec
		if e.engineSpec.Leader != nil {
			basePodSpec = e.engineSpec.Leader.PodSpec
			runnerSpec = e.engineSpec.Leader.Runner
		} else {
			// Fallback to engine spec if leader is not defined
			basePodSpec = e.engineSpec.PodSpec
			runnerSpec = e.engineSpec.Runner
		}
	default:
		// For raw deployment and serverless, use engine spec
		basePodSpec = e.engineSpec.PodSpec
		runnerSpec = e.engineSpec.Runner
	}
	if runnerSpec != nil {
		UpdateEnvVariables(&e.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(&e.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		MergeEngineResources(&e.BaseComponentFields, isvc, &runnerSpec.Container)
		e.setParallelismEnvVarForEngine(&runnerSpec.Container, e.getWorkerSize())
	}

	// Use common pod spec reconciler for base logic
	podSpec, err := e.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&e.BaseComponentFields, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(&e.BaseComponentFields, isvc, podSpec, v1beta1.EngineComponent)

	e.Log.Info("Engine PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// reconcileWorkerPodSpec reconciles the worker pod spec for multi-node deployments
func (e *Engine) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Return nil if no worker spec is defined
	if e.engineSpec.Worker == nil {
		return nil, nil
	}

	// Get worker runner spec if available
	var workerRunner *v1beta1.RunnerSpec
	if e.engineSpec.Worker != nil {
		workerRunner = e.engineSpec.Worker.Runner
		if workerRunner != nil {
			UpdateVolumeMounts(&e.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			UpdateEnvVariables(&e.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			MergeEngineResources(&e.BaseComponentFields, isvc, &workerRunner.Container)
			e.setParallelismEnvVarForEngine(&workerRunner.Container, e.getWorkerSize())
		}
	}

	// Use common reconciler for worker pod spec
	workerPodSpec, err := e.podSpecReconciler.ReconcileWorkerPodSpec(isvc, objectMeta, &e.engineSpec.Worker.PodSpec, workerRunner)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&e.BaseComponentFields, isvc, workerPodSpec, objectMeta)
	UpdatePodSpecNodeSelector(&e.BaseComponentFields, isvc, workerPodSpec, v1beta1.EngineComponent)
	e.Log.Info("Engine Worker PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return workerPodSpec, nil
}

// setParallelismEnvVarForEngine calculates and sets the PARALLELISM_SIZE environment variable for the engine's container.
func (e *Engine) setParallelismEnvVarForEngine(container *v1.Container, workerReplicas int) {
	if container == nil || e.engineSpec == nil {
		e.Log.Info("Cannot set parallelism: container or engineSpec is nil")
		return
	}

	numGPUsPerPod := int64(isvcutils.GetGpuCountFromContainer(container))
	numLeaders := int64(1) // at least one leader/pod
	numWorkers := int64(workerReplicas)

	// Only proceed if there are GPUs
	if numGPUsPerPod > 0 {
		parallelismSize := numGPUsPerPod * (numLeaders + numWorkers)
		if parallelismSize > 0 {
			envVar := v1.EnvVar{Name: constants.ParallelismSizeEnvVarKey, Value: strconv.FormatInt(parallelismSize, 10)}
			isvcutils.UpdateEnvVars(container, &envVar)
			e.Log.Info("Added parallelism env variable to engine container", "value", parallelismSize, "containerName", container.Name)
		} else {
			e.Log.Info("Calculated parallelism is zero, not adding env var", "containerName", container.Name)
		}
	} else {
		e.Log.Info("Conditions not met for parallelism (no GPUs or no leaders/workers)", "containerName", container.Name, "gpus", numGPUsPerPod, "leaders", numLeaders, "workers", numWorkers)
	}
}

// GetComponentType implements ComponentConfig interface
func (e *Engine) GetComponentType() v1beta1.ComponentType {
	return v1beta1.EngineComponent
}

// GetComponentSpec implements ComponentConfig interface
func (e *Engine) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if e.engineSpec == nil {
		return nil
	}
	return &e.engineSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (e *Engine) GetServiceSuffix() string {
	return "-engine"
}

// ValidateSpec implements ComponentConfig interface
func (e *Engine) ValidateSpec() error {
	if e.engineSpec == nil {
		return errors.New("engine spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
