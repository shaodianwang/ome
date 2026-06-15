package inferencedeployment

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	knapis "knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	omev1alpha1 "github.com/ome-projects/ome/pkg/apis/ome/v1alpha1"
	omev1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

const (
	finalizerName = "ome.io/inference-deployment-finalizer"

	labelManagedBy           = "app.kubernetes.io/managed-by"
	labelDeployment          = "ome.io/inference-deployment"
	labelDeploymentNamespace = "ome.io/inference-deployment-namespace"
	labelTarget              = "ome.io/target-cluster"

	managedByValue = "inference-deployment-operator"

	defaultRuntimeName              = "default-runtime"
	checkpointEnvName               = "CC_MODEL_PATH"
	sglangDisaggregationModeArg     = "disaggregation-mode"
	sglangDisaggregationModePrefill = "prefill"
	sglangDisaggregationModeDecode  = "decode"

	inferenceServiceStatusPollInterval = 30 * time.Second
)

// InferenceDeploymentReconciler reconciles management-cluster
// InferenceDeployment objects into OME InferenceServices in workload clusters.
type InferenceDeploymentReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme

	NewRemoteClient                 RemoteClientFactory
	RemoteInferenceDeploymentEvents <-chan event.GenericEvent
}

// +kubebuilder:rbac:groups=ome.io,resources=inferencedeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferencedeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencedeployments/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list

func (r *InferenceDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("inferencedeployment", req.NamespacedName)

	deployment := &omev1alpha1.InferenceDeployment{}
	if err := r.Get(ctx, req.NamespacedName, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("InferenceDeployment not found; ignoring deleted object")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get InferenceDeployment %s: %w", req.NamespacedName, err)
	}
	log = log.WithValues("generation", deployment.Generation, "targetCount", len(deployment.Spec.Clusters))
	if deployment.Generation != deployment.Status.ObservedGeneration {
		r.Log.Info(fmt.Sprintf(
			"Reconciling changed InferenceDeployment: inferenceDeployment=%s/%s generation=%d previousObservedGeneration=%d targetCount=%d",
			deployment.Namespace,
			deployment.Name,
			deployment.Generation,
			deployment.Status.ObservedGeneration,
			len(deployment.Spec.Clusters),
		))
	} else {
		log.V(1).Info("Reconciling InferenceDeployment")
	}

	if deployment.DeletionTimestamp != nil {
		r.Log.Info(fmt.Sprintf("Reconciling InferenceDeployment deletion: inferenceDeployment=%s/%s", deployment.Namespace, deployment.Name))
		return ctrl.Result{}, r.reconcileDelete(ctx, log, deployment)
	}

	if !controllerutil.ContainsFinalizer(deployment, finalizerName) {
		base := deployment.DeepCopy()
		controllerutil.AddFinalizer(deployment, finalizerName)
		if err := r.Patch(ctx, deployment, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer to InferenceDeployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
		}
		r.Log.Info(fmt.Sprintf("Added InferenceDeployment finalizer: inferenceDeployment=%s/%s", deployment.Namespace, deployment.Name))
	}

	if len(deployment.Spec.Clusters) == 0 {
		message := "invalid InferenceDeployment spec: clusters must not be empty"
		previousReady := findReadyCondition(deployment.Status.Conditions)
		if err := r.patchStatus(ctx, deployment, nil, metav1.ConditionFalse, "InvalidSpec", message); err != nil {
			return ctrl.Result{}, err
		}
		logStatusUpdate(r.Log, previousReady, deployment, metav1.ConditionFalse, "InvalidSpec", message)
		return ctrl.Result{}, nil
	}

	errs := make([]error, 0)
	if err := r.deleteStaleTargets(ctx, log, deployment); err != nil {
		log.Error(err, "Failed to delete stale target resources")
		errs = append(errs, err)
	}

	statuses := make([]omev1alpha1.ClusterDeploymentStatus, 0, len(deployment.Spec.Clusters))
	for _, target := range deployment.Spec.Clusters {
		targetStatus, err := r.reconcileTarget(ctx, log, deployment, target)
		statuses = append(statuses, targetStatus)
		if err != nil {
			log.Error(err, "Failed to reconcile target cluster", "target", target.Name)
			errs = append(errs, err)
		}
	}

	readyStatus, readyReason, readyMessage := deploymentReadyStatus(errs, statuses)
	previousReady := findReadyCondition(deployment.Status.Conditions)
	if err := r.patchStatus(ctx, deployment, statuses, readyStatus, readyReason, readyMessage); err != nil {
		errs = append(errs, err)
	} else {
		logStatusUpdate(r.Log, previousReady, deployment, readyStatus, readyReason, readyMessage)
	}
	if err := errors.Join(errs...); err != nil {
		log.Error(err, "Finished InferenceDeployment reconcile with errors", "readyStatus", readyStatus, "readyReason", readyReason)
		return ctrl.Result{}, err
	}
	if readyReason == "InvalidSpec" {
		log.Info(fmt.Sprintf("InferenceDeployment spec is invalid: inferenceDeployment=%s/%s readyReason=%s", deployment.Namespace, deployment.Name, readyReason))
		return ctrl.Result{}, nil
	}
	if readyStatus != metav1.ConditionTrue {
		log.V(1).Info("InferenceDeployment is not ready; scheduling status poll", "readyStatus", readyStatus, "readyReason", readyReason, "requeueAfter", inferenceServiceStatusPollInterval)
		return ctrl.Result{RequeueAfter: inferenceServiceStatusPollInterval}, nil
	}
	log.V(1).Info("Finished InferenceDeployment reconcile", "readyStatus", readyStatus, "readyReason", readyReason)
	return ctrl.Result{}, nil
}

func (r *InferenceDeploymentReconciler) reconcileTarget(ctx context.Context, log logr.Logger, deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster) (omev1alpha1.ClusterDeploymentStatus, error) {
	now := metav1.Now()
	status := omev1alpha1.ClusterDeploymentStatus{
		Name:               target.Name,
		ObservedGeneration: deployment.Generation,
		LastAttemptTime:    &now,
		DesiredReplicas:    target.Prefill.MinReplicas,
	}
	namespace := targetNamespace(deployment, target)
	targetLog := log.WithValues("target", target.Name, "targetNamespace", namespace, "desiredReplicas", target.Prefill.MinReplicas)
	targetLog.V(1).Info("Reconciling target cluster")

	if err := validateTargetSpec(deployment, target); err != nil {
		setClusterReady(&status, metav1.ConditionFalse, "InvalidSpec", err.Error())
		targetLog.Info(fmt.Sprintf("Skipped target reconciliation because InferenceDeployment spec is invalid: inferenceDeployment=%s/%s targetCluster=%s targetNamespace=%s error=%s", deployment.Namespace, deployment.Name, target.Name, namespace, err.Error()))
		return status, nil
	}

	remoteClient, err := r.remoteClientFactory()(ctx, target.Name, r.Scheme)
	if err != nil {
		setClusterReady(&status, metav1.ConditionFalse, "RemoteClientFailed", err.Error())
		targetLog.Error(err, "Failed to create remote client")
		return status, err
	}

	applied, observed, operation, err := r.applyInferenceService(ctx, remoteClient, deployment, target)
	status.AppliedResource = &applied
	status.CurrentReplicas = observed.CurrentReplicas
	status.ReadyReplicas = observed.ReadyReplicas
	status.ServingReplicas = observed.ServingReplicas
	status.URL = observed.URL
	status.InferenceServiceGeneration = observed.Generation
	status.InferenceServiceObservedGeneration = observed.ObservedGeneration
	if err != nil {
		setClusterReady(&status, metav1.ConditionFalse, "ApplyFailed", err.Error())
		targetLog.Error(err, "Failed to apply remote InferenceService")
		return status, err
	}

	setClusterReadyFromInferenceService(&status, observed)
	targetReady := findReadyCondition(status.Conditions)
	logTargetReconcileResult(r.Log, deployment, target, operation, status, targetReady)
	return status, nil
}

func logTargetReconcileResult(log logr.Logger, deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster, operation controllerutil.OperationResult, status omev1alpha1.ClusterDeploymentStatus, ready *metav1.Condition) {
	message := fmt.Sprintf(
		"inferenceDeployment=%s/%s targetCluster=%s targetNamespace=%s desiredReplicas=%d readyStatus=%s readyReason=%s currentReplicas=%d readyReplicas=%d",
		deployment.Namespace,
		deployment.Name,
		target.Name,
		targetNamespace(deployment, target),
		target.Prefill.MinReplicas,
		conditionStatus(ready),
		conditionReason(ready),
		status.CurrentReplicas,
		status.ReadyReplicas,
	)
	if operation != controllerutil.OperationResultNone {
		log.Info("Applied remote InferenceService: " + message)
		return
	}
	if ready == nil || ready.Status != metav1.ConditionTrue {
		log.Info("Observed remote InferenceService is not ready: " + message)
		return
	}
	log.V(1).Info("Reconciled target cluster: " + message)
}

func (r *InferenceDeploymentReconciler) applyInferenceService(ctx context.Context, c client.Client, deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster) (omev1alpha1.AppliedResource, observedReplicas, controllerutil.OperationResult, error) {
	namespace := targetNamespace(deployment, target)

	spec, err := renderInferenceServiceSpec(deployment, target)
	if err != nil {
		return omev1alpha1.AppliedResource{}, observedReplicas{}, controllerutil.OperationResultNone, err
	}

	obj := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: deployment.Name, Namespace: namespace}}
	operation, err := controllerutil.CreateOrPatch(ctx, c, obj, func() error {
		obj.Labels = managedLabels(deployment, target, nil)
		obj.Annotations = nil
		obj.Spec = *spec
		return nil
	})
	if err != nil {
		return omev1alpha1.AppliedResource{}, observedReplicas{}, operation, fmt.Errorf("apply InferenceService %s/%s to target %s: %w", namespace, deployment.Name, target.Name, err)
	}

	latest := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: deployment.Name, Namespace: namespace}}
	if err := c.Get(ctx, client.ObjectKeyFromObject(latest), latest); err != nil {
		return omev1alpha1.AppliedResource{}, observedReplicas{}, operation, fmt.Errorf("read applied InferenceService %s/%s from target %s: %w", namespace, deployment.Name, target.Name, err)
	}
	return appliedResource("InferenceService", namespace, latest.Name, latest.ResourceVersion), replicasFromStatus(latest), operation, nil
}

func renderInferenceServiceSpec(deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster) (*omev1beta1.InferenceServiceSpec, error) {
	runtimeKind := "ClusterServingRuntime"
	spec := &omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{
			Name: defaultRuntimeName,
			Kind: &runtimeKind,
		},
		Engine: renderEngineSpec(deployment, target.Prefill, sglangDisaggregationModePrefill),
	}
	if target.Decode != nil {
		spec.Decoder = renderDecoderSpec(deployment, *target.Decode, sglangDisaggregationModeDecode)
	}
	if target.Router != nil && deployment.Spec.Router != nil {
		spec.Router = renderRouterSpec(deployment.Spec.Router, *target.Router, deployment.Spec.PriorityClassName)
	}
	return spec, nil
}

func renderEngineSpec(deployment *omev1alpha1.InferenceDeployment, scale omev1alpha1.ComponentScale, mode string) *omev1beta1.EngineSpec {
	minReplicas, maxReplicas := replicaEnvelope(scale)
	return &omev1beta1.EngineSpec{
		PodSpec: podSpec(deployment.Spec.PriorityClassName),
		ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
			MinReplicas: minReplicas,
			MaxReplicas: maxReplicas,
		},
		Runner: renderSGLangRunner(deployment, mode),
	}
}

func renderDecoderSpec(deployment *omev1alpha1.InferenceDeployment, scale omev1alpha1.ComponentScale, mode string) *omev1beta1.DecoderSpec {
	minReplicas, maxReplicas := replicaEnvelope(scale)
	return &omev1beta1.DecoderSpec{
		PodSpec: podSpec(deployment.Spec.PriorityClassName),
		ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
			MinReplicas: minReplicas,
			MaxReplicas: maxReplicas,
		},
		Runner: renderSGLangRunner(deployment, mode),
	}
}

func renderRouterSpec(router *omev1alpha1.RouterSpec, scale omev1alpha1.ComponentScale, priorityClassName string) *omev1beta1.RouterSpec {
	minReplicas, maxReplicas := replicaEnvelope(scale)
	return &omev1beta1.RouterSpec{
		PodSpec: podSpec(priorityClassName),
		ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
			MinReplicas: minReplicas,
			MaxReplicas: maxReplicas,
		},
		Runner: renderRunner(router.Image, router.CPU, router.Memory, 0, nil, envVars(router.Env), argsFromMap(router.Args, nil)),
		Config: copyMap(router.Config),
	}
}

func renderSGLangRunner(deployment *omev1alpha1.InferenceDeployment, mode string) *omev1beta1.RunnerSpec {
	override := sglangComponentOverride(deployment, mode)
	var overrideArgs map[string]string
	var overrideEnv map[string]string
	if override != nil {
		overrideArgs = override.Args
		overrideEnv = override.Env
	}
	sglangArgs := argsFromMap(deployment.Spec.SGLang.Args, overrideArgs)
	env := envVarsFromMaps(sglangDefaultEnv(deployment, sglangArgs), deployment.Spec.SGLang.Env, overrideEnv)
	command := append(sglangLaunchCommand(), sglangArgs...)
	command = append(command, "--"+sglangDisaggregationModeArg, mode)
	return renderRunner(
		deployment.Spec.SGLang.Image,
		deployment.Spec.SGLang.CPU,
		deployment.Spec.SGLang.Memory,
		deployment.Spec.SGLang.GPUs,
		command,
		env,
		nil,
	)
}

func sglangComponentOverride(deployment *omev1alpha1.InferenceDeployment, mode string) *omev1alpha1.SGLangComponentOverride {
	if deployment.Spec.SGLang.Components == nil {
		return nil
	}
	switch mode {
	case sglangDisaggregationModePrefill:
		return deployment.Spec.SGLang.Components.Prefill
	case sglangDisaggregationModeDecode:
		return deployment.Spec.SGLang.Components.Decode
	default:
		return nil
	}
}

func sglangDefaultEnv(deployment *omev1alpha1.InferenceDeployment, args []string) map[string]string {
	out := map[string]string{
		checkpointEnvName:                               deployment.Spec.Checkpoint,
		"MODEL_PATH":                                    "/opt/ml/model",
		"CLUSTER_CACHE_HEADLESS_SERVICE":                "cluster-cache-headless.clustercache.svc.cluster.local:10090",
		"CLUSTER_CACHE_GPU_DIRECT":                      "1",
		"CLUSTER_CACHE_BBOX":                            "1",
		"CLUSTER_CACHE_BARRIER_TIMEOUT_S":               "60",
		"CLUSTER_CACHE_HTTP_BATCH_REQUEST_TIMEOUT_SECS": "60",
		"CLUSTER_CACHE_OVERLOAD_RETRY_ATTEMPTS":         "3",
	}
	tpSize := argValue(args, "--tp-size")
	epSize := argValue(args, "--ep-size")
	if tpSize != "" {
		out["PARALLELISM_SIZE"] = tpSize
	}
	if tpSize != "" && epSize != "" {
		out["CLUSTER_CACHE_VERIFY_TENSOR_HASH_AGAINST"] = fmt.Sprintf("@tp%sep%s", tpSize, epSize)
	}
	return out
}

func argValue(args []string, flag string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func sglangLaunchCommand() []string {
	return []string{
		"python3",
		"-m",
		"sglang_meta.launch_server",
		"--host",
		"0.0.0.0",
		"--port",
		"30000",
		"--model-path",
		"$(CC_MODEL_PATH)",
		"--fallback-model-path",
		"$(MODEL_PATH)",
	}
}

func renderRunner(image, cpu, memory string, gpus int32, command []string, env []corev1.EnvVar, args []string) *omev1beta1.RunnerSpec {
	container := corev1.Container{
		Image:   image,
		Command: command,
		Env:     env,
		Args:    args,
	}
	resources := corev1.ResourceList{}
	if cpu != "" {
		resources[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		resources[corev1.ResourceMemory] = resource.MustParse(memory)
	}
	if gpus > 0 {
		resources[corev1.ResourceName("nvidia.com/gpu")] = resource.MustParse(strconv.FormatInt(int64(gpus), 10))
	}
	if len(resources) > 0 {
		container.Resources = corev1.ResourceRequirements{
			Requests: resources.DeepCopy(),
			Limits:   resources.DeepCopy(),
		}
	}
	return &omev1beta1.RunnerSpec{Container: container}
}

func podSpec(priorityClassName string) omev1beta1.PodSpec {
	if priorityClassName == "" {
		return omev1beta1.PodSpec{}
	}
	return omev1beta1.PodSpec{PriorityClassName: priorityClassName}
}

func replicaEnvelope(scale omev1alpha1.ComponentScale) (*int, int) {
	minReplicas := int(scale.MinReplicas)
	maxReplicas := int(scale.MaxReplicas)
	return &minReplicas, maxReplicas
}

func envVars(in map[string]string) []corev1.EnvVar {
	return envVarsFromMaps(in)
}

func envVarsFromMaps(layers ...map[string]string) []corev1.EnvVar {
	merged := map[string]string{}
	for _, layer := range layers {
		for key, value := range layer {
			merged[key] = value
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return sortedEnvVars(merged)
}

func sortedEnvVars(in map[string]string) []corev1.EnvVar {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]corev1.EnvVar, 0, len(keys))
	for _, key := range keys {
		out = append(out, corev1.EnvVar{Name: key, Value: in[key]})
	}
	return out
}

func argsFromMap(layers ...map[string]string) []string {
	merged := map[string]string{}
	order := make([]string, 0)
	for _, layer := range layers {
		keys := make([]string, 0, len(layer))
		for key := range layer {
			normalized := normalizeArgName(key)
			if normalized == "--"+sglangDisaggregationModeArg {
				continue
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			normalized := normalizeArgName(key)
			if _, ok := merged[normalized]; !ok {
				order = append(order, normalized)
			}
			merged[normalized] = layer[key]
		}
	}

	out := make([]string, 0, len(order)*2)
	for _, key := range order {
		value := merged[key]
		if strings.EqualFold(value, "false") {
			continue
		}
		out = append(out, key)
		if value == "" || strings.EqualFold(value, "true") {
			continue
		}
		out = append(out, value)
	}
	return out
}

func normalizeArgName(key string) string {
	key = strings.TrimSpace(key)
	if strings.HasPrefix(key, "--") {
		return key
	}
	return "--" + key
}

func validateTargetSpec(deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster) error {
	if deployment.Spec.Checkpoint == "" {
		return fmt.Errorf("invalid InferenceDeployment spec: checkpoint is required")
	}
	if deployment.Spec.SGLang.Image == "" {
		return fmt.Errorf("invalid InferenceDeployment spec: sglang.image is required")
	}
	if err := validateResourceQuantity("sglang.cpu", deployment.Spec.SGLang.CPU); err != nil {
		return err
	}
	if err := validateResourceQuantity("sglang.memory", deployment.Spec.SGLang.Memory); err != nil {
		return err
	}
	if len(deployment.Spec.Clusters) == 0 {
		return fmt.Errorf("invalid InferenceDeployment spec: clusters must not be empty")
	}
	if target.Name == "" {
		return fmt.Errorf("invalid InferenceDeployment spec: cluster name is required")
	}
	if err := validateScale("prefill", target.Prefill); err != nil {
		return err
	}
	if target.Decode != nil {
		if err := validateScale("decode", *target.Decode); err != nil {
			return err
		}
	}
	if target.Router != nil {
		if deployment.Spec.Router == nil {
			return fmt.Errorf("invalid InferenceDeployment spec: router must be set when cluster router replicas are set")
		}
		if deployment.Spec.Router.Image == "" {
			return fmt.Errorf("invalid InferenceDeployment spec: router.image is required when router is set")
		}
		if err := validateResourceQuantity("router.cpu", deployment.Spec.Router.CPU); err != nil {
			return err
		}
		if err := validateResourceQuantity("router.memory", deployment.Spec.Router.Memory); err != nil {
			return err
		}
		if err := validateScale("router", *target.Router); err != nil {
			return err
		}
	}
	return nil
}

func validateResourceQuantity(field, value string) error {
	if value == "" {
		return nil
	}
	if _, err := resource.ParseQuantity(value); err != nil {
		return fmt.Errorf("invalid InferenceDeployment spec: %s is not a valid Kubernetes quantity: %w", field, err)
	}
	return nil
}

func validateScale(name string, scale omev1alpha1.ComponentScale) error {
	if scale.MinReplicas < 0 || scale.MaxReplicas < 0 {
		return fmt.Errorf("invalid InferenceDeployment spec: %s replicas must be non-negative", name)
	}
	if scale.MaxReplicas < scale.MinReplicas {
		return fmt.Errorf("invalid InferenceDeployment spec: %s.maxReplicas must be greater than or equal to %s.minReplicas", name, name)
	}
	return nil
}

type observedReplicas struct {
	CurrentReplicas    int32
	ReadyReplicas      int32
	ServingReplicas    int32
	URL                string
	Generation         int64
	ObservedGeneration int64
	ReadyCondition     *metav1.Condition
	ComponentReady     []metav1.Condition
}

func replicasFromStatus(isvc *omev1beta1.InferenceService) observedReplicas {
	out := observedReplicas{}
	if isvc == nil {
		return out
	}
	out.URL = inferenceServiceURL(isvc)
	out.Generation = isvc.Generation
	out.ObservedGeneration = isvc.Status.ObservedGeneration
	out.ReadyCondition = inferenceServiceReadyCondition(isvc)
	out.ComponentReady = componentReadyConditions(isvc)

	component, ok := servingReplicaComponentStatus(isvc)
	if !ok {
		return out
	}
	if component.Lifecycle != nil {
		out.CurrentReplicas = component.Lifecycle.Replicas
		out.ReadyReplicas = component.Lifecycle.ReadyReplicas
		out.ServingReplicas = component.Lifecycle.ServingReplicas
	}
	if component.Autoscaler != nil && component.Autoscaler.CurrentReplicas > out.CurrentReplicas {
		out.CurrentReplicas = component.Autoscaler.CurrentReplicas
	}
	return out
}

func inferenceServiceURL(isvc *omev1beta1.InferenceService) string {
	if isvc.Status.URL == nil {
		return ""
	}
	return isvc.Status.URL.String()
}

func inferenceServiceReadyCondition(isvc *omev1beta1.InferenceService) *metav1.Condition {
	condition := isvc.Status.GetCondition(knapis.ConditionReady)
	if condition == nil {
		return nil
	}
	return conditionFromInferenceService(isvc, omev1alpha1.InferenceDeploymentReady, condition)
}

func componentReadyConditions(isvc *omev1beta1.InferenceService) []metav1.Condition {
	conditionTypes := make([]knapis.ConditionType, 0, 2)
	if isvc.Spec.Engine != nil {
		conditionTypes = append(conditionTypes, omev1beta1.EngineReady)
	}
	if isvc.Spec.Router != nil {
		conditionTypes = append(conditionTypes, omev1beta1.RouterReady)
	}

	out := make([]metav1.Condition, 0, len(conditionTypes))
	for _, conditionType := range conditionTypes {
		condition := isvc.Status.GetCondition(conditionType)
		if condition == nil {
			out = append(out, metav1.Condition{
				Type:               string(conditionType),
				Status:             metav1.ConditionFalse,
				Reason:             "ComponentReadyUnknown",
				Message:            fmt.Sprintf("OME InferenceService has not reported %s yet", conditionType),
				ObservedGeneration: isvc.Status.ObservedGeneration,
			})
			continue
		}
		out = append(out, *conditionFromInferenceService(isvc, string(conditionType), condition))
	}
	return out
}

func conditionFromInferenceService(isvc *omev1beta1.InferenceService, conditionType string, condition *knapis.Condition) *metav1.Condition {
	return &metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionStatus(condition.Status),
		Reason:             condition.Reason,
		Message:            condition.Message,
		ObservedGeneration: isvc.Status.ObservedGeneration,
		LastTransitionTime: condition.LastTransitionTime.Inner,
	}
}

func servingReplicaComponentStatus(isvc *omev1beta1.InferenceService) (omev1beta1.ComponentStatusSpec, bool) {
	if isvc == nil || len(isvc.Status.Components) == 0 {
		return omev1beta1.ComponentStatusSpec{}, false
	}
	for _, componentType := range []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent} {
		if component, ok := isvc.Status.Components[componentType]; ok {
			return component, true
		}
	}
	return omev1beta1.ComponentStatusSpec{}, false
}

func (r *InferenceDeploymentReconciler) deleteStaleTargets(ctx context.Context, log logr.Logger, deployment *omev1alpha1.InferenceDeployment) error {
	desired := make(map[string]omev1alpha1.WorkloadCluster, len(deployment.Spec.Clusters))
	for _, target := range deployment.Spec.Clusters {
		desired[target.Name] = target
	}

	errs := make([]error, 0)
	for _, status := range deployment.Status.Clusters {
		target, ok := desired[status.Name]
		if ok && !appliedResourceChanged(deployment, target, status.AppliedResource) {
			continue
		}
		if status.AppliedResource == nil {
			continue
		}
		staleLog := log.WithValues("target", status.Name, "remoteResource", appliedResourceKey(*status.AppliedResource))
		staleLog.Info("Deleting stale target resource")
		remoteClient, err := r.remoteClientFactory()(ctx, status.Name, r.Scheme)
		if err != nil {
			staleLog.Error(err, "Failed to create remote client for stale target")
			errs = append(errs, err)
			continue
		}
		if err := deleteInferenceService(ctx, remoteClient, status.Name, *status.AppliedResource); err != nil {
			staleLog.Error(err, "Failed to delete stale target resource")
			errs = append(errs, err)
			continue
		}
		staleLog.Info("Deleted stale target resource")
	}
	return errors.Join(errs...)
}

func appliedResourceChanged(deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster, resource *omev1alpha1.AppliedResource) bool {
	if resource == nil {
		return false
	}
	return resource.Kind != "InferenceService" ||
		resource.Namespace != targetNamespace(deployment, target) ||
		resource.Name != deployment.Name
}

func (r *InferenceDeploymentReconciler) reconcileDelete(ctx context.Context, log logr.Logger, deployment *omev1alpha1.InferenceDeployment) error {
	if !controllerutil.ContainsFinalizer(deployment, finalizerName) {
		log.V(1).Info("InferenceDeployment deletion has no finalizer")
		return nil
	}

	errs := make([]error, 0)
	for _, status := range deployment.Status.Clusters {
		if status.AppliedResource == nil {
			continue
		}
		targetLog := log.WithValues("target", status.Name, "remoteResource", appliedResourceKey(*status.AppliedResource))
		targetLog.Info("Deleting target resource for InferenceDeployment deletion")
		remoteClient, err := r.remoteClientFactory()(ctx, status.Name, r.Scheme)
		if err != nil {
			targetLog.Error(err, "Failed to create remote client during deletion")
			errs = append(errs, err)
			continue
		}
		if err := deleteInferenceService(ctx, remoteClient, status.Name, *status.AppliedResource); err != nil {
			targetLog.Error(err, "Failed to delete target resource during deletion")
			errs = append(errs, err)
			continue
		}
		targetLog.Info("Deleted target resource for InferenceDeployment deletion")
	}
	if len(deployment.Status.Clusters) == 0 {
		for _, target := range deployment.Spec.Clusters {
			namespace := targetNamespace(deployment, target)
			resource := appliedResource("InferenceService", namespace, deployment.Name, "")
			targetLog := log.WithValues("target", target.Name, "remoteResource", appliedResourceKey(resource))
			targetLog.Info("Deleting target resource for InferenceDeployment deletion without prior status")
			remoteClient, err := r.remoteClientFactory()(ctx, target.Name, r.Scheme)
			if err != nil {
				targetLog.Error(err, "Failed to create remote client during deletion")
				errs = append(errs, err)
				continue
			}
			if err := deleteInferenceService(ctx, remoteClient, target.Name, resource); err != nil {
				targetLog.Error(err, "Failed to delete target resource during deletion")
				errs = append(errs, err)
				continue
			}
			targetLog.Info("Deleted target resource for InferenceDeployment deletion")
		}
	}
	if err := errors.Join(errs...); err != nil {
		log.Error(err, "Failed to delete all target resources; keeping finalizer")
		return err
	}

	base := deployment.DeepCopy()
	controllerutil.RemoveFinalizer(deployment, finalizerName)
	if err := r.Patch(ctx, deployment, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("remove finalizer from InferenceDeployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}
	r.Log.Info(fmt.Sprintf("Removed InferenceDeployment finalizer: inferenceDeployment=%s/%s", deployment.Namespace, deployment.Name))
	return nil
}

func deleteInferenceService(ctx context.Context, c client.Client, targetName string, resource omev1alpha1.AppliedResource) error {
	if resource.Kind != "InferenceService" || resource.Name == "" || resource.Namespace == "" {
		return nil
	}
	obj := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: resource.Name, Namespace: resource.Namespace}}
	if err := c.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete InferenceService %s/%s from target %s: %w", resource.Namespace, resource.Name, targetName, err)
	}
	return nil
}

func (r *InferenceDeploymentReconciler) patchStatus(ctx context.Context, deployment *omev1alpha1.InferenceDeployment, statuses []omev1alpha1.ClusterDeploymentStatus, readyStatus metav1.ConditionStatus, reason, message string) error {
	base := deployment.DeepCopy()
	deployment.Status.ObservedGeneration = deployment.Generation
	deployment.Status.Clusters = statuses
	apimeta.SetStatusCondition(&deployment.Status.Conditions, metav1.Condition{
		Type:               omev1alpha1.InferenceDeploymentReady,
		Status:             readyStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: deployment.Generation,
	})
	if err := r.Status().Patch(ctx, deployment, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patch InferenceDeployment status %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}
	return nil
}

func (r *InferenceDeploymentReconciler) remoteClientFactory() RemoteClientFactory {
	if r.NewRemoteClient != nil {
		return r.NewRemoteClient
	}
	return func(context.Context, string, *runtime.Scheme) (client.Client, error) {
		return nil, fmt.Errorf("remote client factory is not configured")
	}
}

func managedLabels(deployment *omev1alpha1.InferenceDeployment, target omev1alpha1.WorkloadCluster, extra map[string]string) map[string]string {
	out := copyMap(extra)
	out[labelManagedBy] = managedByValue
	out[labelDeployment] = deployment.Name
	out[labelDeploymentNamespace] = deployment.Namespace
	out[labelTarget] = target.Name
	return out
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func targetNamespace(deployment *omev1alpha1.InferenceDeployment, _ omev1alpha1.WorkloadCluster) string {
	if deployment.Spec.Namespace != "" {
		return deployment.Spec.Namespace
	}
	return deployment.Namespace
}

func logStatusUpdate(log logr.Logger, previousReady *metav1.Condition, deployment *omev1alpha1.InferenceDeployment, readyStatus metav1.ConditionStatus, reason, message string) {
	logMessage := fmt.Sprintf(
		"Updated InferenceDeployment readiness status: inferenceDeployment=%s/%s readyStatus=%s readyReason=%s observedGeneration=%d",
		deployment.Namespace,
		deployment.Name,
		readyStatus,
		reason,
		deployment.Status.ObservedGeneration,
	)
	if readyConditionChanged(previousReady, readyStatus, reason, message, deployment.Generation) {
		log.Info(logMessage)
		return
	}
	log.V(1).Info(logMessage)
}

func readyConditionChanged(previous *metav1.Condition, status metav1.ConditionStatus, reason, message string, observedGeneration int64) bool {
	return previous == nil ||
		previous.Status != status ||
		previous.Reason != reason ||
		previous.Message != message ||
		previous.ObservedGeneration != observedGeneration
}

func appliedResourceKey(resource omev1alpha1.AppliedResource) string {
	if resource.Kind == "" || resource.Namespace == "" || resource.Name == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", resource.Kind, resource.Namespace, resource.Name)
}

func conditionStatus(condition *metav1.Condition) metav1.ConditionStatus {
	if condition == nil {
		return ""
	}
	return condition.Status
}

func conditionReason(condition *metav1.Condition) string {
	if condition == nil {
		return ""
	}
	return condition.Reason
}

func setClusterReady(status *omev1alpha1.ClusterDeploymentStatus, conditionStatus metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               omev1alpha1.InferenceDeploymentReady,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: status.ObservedGeneration,
	})
}

func setClusterReadyFromInferenceService(status *omev1alpha1.ClusterDeploymentStatus, observed observedReplicas) {
	for _, condition := range observed.ComponentReady {
		apimeta.SetStatusCondition(&status.Conditions, condition)
	}
	if observed.ReadyCondition == nil {
		setClusterReady(status, metav1.ConditionFalse, "InferenceServiceReadyUnknown", "OME InferenceService has not reported a Ready condition yet")
		return
	}
	condition := *observed.ReadyCondition
	if condition.Status == metav1.ConditionTrue {
		if component := firstNotReadyComponent(observed.ComponentReady); component != nil {
			setClusterReady(status, metav1.ConditionFalse, componentNotReadyReason(component), componentNotReadyMessage(component))
			return
		}
	}
	if condition.Reason == "" {
		condition.Reason = inferenceServiceReadyReason(condition.Status)
	}
	if condition.Message == "" {
		condition.Message = inferenceServiceReadyMessage(condition.Status)
	}
	apimeta.SetStatusCondition(&status.Conditions, condition)
}

func firstNotReadyComponent(conditions []metav1.Condition) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Status != metav1.ConditionTrue {
			return &conditions[i]
		}
	}
	return nil
}

func componentNotReadyReason(condition *metav1.Condition) string {
	if condition.Reason != "" {
		return condition.Reason
	}
	return fmt.Sprintf("%sNotReady", condition.Type)
}

func componentNotReadyMessage(condition *metav1.Condition) string {
	if condition.Message != "" {
		return condition.Message
	}
	return fmt.Sprintf("OME InferenceService component condition %s is not ready", condition.Type)
}

func inferenceServiceReadyReason(status metav1.ConditionStatus) string {
	switch status {
	case metav1.ConditionTrue:
		return "InferenceServiceReady"
	case metav1.ConditionUnknown:
		return "InferenceServiceReadyUnknown"
	default:
		return "InferenceServiceNotReady"
	}
}

func inferenceServiceReadyMessage(status metav1.ConditionStatus) string {
	switch status {
	case metav1.ConditionTrue:
		return "OME InferenceService is ready"
	case metav1.ConditionUnknown:
		return "OME InferenceService readiness is unknown"
	default:
		return "OME InferenceService is not ready"
	}
}

func deploymentReadyStatus(errs []error, statuses []omev1alpha1.ClusterDeploymentStatus) (metav1.ConditionStatus, string, string) {
	if len(errs) > 0 {
		return metav1.ConditionFalse, "ReconcileFailed", "One or more target clusters failed to apply"
	}
	if invalid := firstClusterReadyReason(statuses, "InvalidSpec"); invalid != nil {
		return metav1.ConditionFalse, "InvalidSpec", invalid.Message
	}
	if !allClustersReady(statuses) {
		return metav1.ConditionFalse, "InferenceServicesNotReady", "One or more target cluster InferenceServices are not ready"
	}
	return metav1.ConditionTrue, "InferenceServicesReady", "All target cluster InferenceServices are ready"
}

func firstClusterReadyReason(statuses []omev1alpha1.ClusterDeploymentStatus, reason string) *metav1.Condition {
	for _, status := range statuses {
		condition := findReadyCondition(status.Conditions)
		if condition != nil && condition.Reason == reason {
			return condition
		}
	}
	return nil
}

func allClustersReady(statuses []omev1alpha1.ClusterDeploymentStatus) bool {
	for _, status := range statuses {
		condition := findReadyCondition(status.Conditions)
		if condition == nil || condition.Status != metav1.ConditionTrue {
			return false
		}
	}
	return true
}

func findReadyCondition(conditions []metav1.Condition) *metav1.Condition {
	idx := slices.IndexFunc(conditions, func(condition metav1.Condition) bool {
		return condition.Type == omev1alpha1.InferenceDeploymentReady
	})
	if idx < 0 {
		return nil
	}
	return &conditions[idx]
}

func appliedResource(kind, namespace, name, resourceVersion string) omev1alpha1.AppliedResource {
	return omev1alpha1.AppliedResource{
		Kind:            kind,
		Namespace:       namespace,
		Name:            name,
		ResourceVersion: resourceVersion,
	}
}

func (r *InferenceDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&omev1alpha1.InferenceDeployment{})
	if r.RemoteInferenceDeploymentEvents != nil {
		builder = builder.WatchesRawSource(source.Channel(
			r.RemoteInferenceDeploymentEvents,
			&handler.EnqueueRequestForObject{},
		))
	}
	return builder.Complete(r)
}
