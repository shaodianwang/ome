package inferenceservice

import (
	"context"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	knapis "knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

var podGroupDeploymentRolloutGVK = schema.GroupVersionKind{
	Group:   "podgroup.meta.com",
	Version: "v1alpha1",
	Kind:    constants.PodGroupDeploymentRolloutKind,
}

type podGroupDeploymentTemplateRenderer interface {
	RenderPodGroupDeploymentTemplate(context.Context, *v1beta1.InferenceService) (*corev1.PodTemplateSpec, error)
}

func (r *InferenceServiceReconciler) reconcilePodGroupDeploymentRollout(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	engine *v1beta1.EngineSpec,
	decoder *v1beta1.DecoderSpec,
	engineRenderer podGroupDeploymentTemplateRenderer,
	decoderRenderer podGroupDeploymentTemplateRenderer,
) (ctrl.Result, error) {
	if engine == nil || decoder == nil {
		return ctrl.Result{}, fmt.Errorf("PodGroupDeploymentRollout mode requires both engine and decoder")
	}
	if engineRenderer == nil || decoderRenderer == nil {
		return ctrl.Result{}, fmt.Errorf("PodGroupDeploymentRollout mode requires engine and decoder template renderers")
	}

	engineTemplate, err := engineRenderer.RenderPodGroupDeploymentTemplate(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render engine podgroup template: %w", err)
	}
	decoderTemplate, err := decoderRenderer.RenderPodGroupDeploymentTemplate(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render decoder podgroup template: %w", err)
	}

	desired, err := r.buildPodGroupDeploymentRollout(isvc, engine, decoder, engineTemplate, decoderTemplate)
	if err != nil {
		return ctrl.Result{}, err
	}

	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(podGroupDeploymentRolloutGVK)
	key := client.ObjectKey{Name: desired.GetName(), Namespace: desired.GetNamespace()}
	if err := r.Get(ctx, key, current); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, fmt.Errorf("create PodGroupDeploymentRollout: %w", err)
		}
		r.setPodGroupDeploymentRolloutStatus(isvc, desired)
		return ctrl.Result{}, nil
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("get PodGroupDeploymentRollout: %w", err)
	}

	if podGroupDeploymentRolloutNeedsUpdate(current, desired) {
		updated := current.DeepCopy()
		updated.SetLabels(desired.GetLabels())
		updated.SetAnnotations(desired.GetAnnotations())
		updated.SetOwnerReferences(desired.GetOwnerReferences())
		updated.Object["spec"] = desired.Object["spec"]
		if err := r.Update(ctx, updated); err != nil {
			return ctrl.Result{}, fmt.Errorf("update PodGroupDeploymentRollout: %w", err)
		}
		r.setPodGroupDeploymentRolloutStatus(isvc, updated)
		return ctrl.Result{}, nil
	}

	r.setPodGroupDeploymentRolloutStatus(isvc, current)
	return ctrl.Result{}, nil
}

func (r *InferenceServiceReconciler) buildPodGroupDeploymentRollout(
	isvc *v1beta1.InferenceService,
	engine *v1beta1.EngineSpec,
	decoder *v1beta1.DecoderSpec,
	engineTemplate *corev1.PodTemplateSpec,
	decoderTemplate *corev1.PodTemplateSpec,
) (*unstructured.Unstructured, error) {
	engineSpec, err := podGroupDeploymentSpecMap(isvc, v1beta1.EngineComponent, "prefill", engine.MinReplicas, engine.Worker, &engine.ComponentExtensionSpec, engineTemplate)
	if err != nil {
		return nil, fmt.Errorf("build prefill spec: %w", err)
	}
	decoderSpec, err := podGroupDeploymentSpecMap(isvc, v1beta1.DecoderComponent, "decode", decoder.MinReplicas, decoder.Worker, &decoder.ComponentExtensionSpec, decoderTemplate)
	if err != nil {
		return nil, fmt.Errorf("build decode spec: %w", err)
	}

	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": podGroupDeploymentRolloutGVK.GroupVersion().String(),
		"kind":       podGroupDeploymentRolloutGVK.Kind,
		"metadata": map[string]interface{}{
			"name":      isvc.Name,
			"namespace": isvc.Namespace,
			"labels": map[string]interface{}{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			},
		},
		"spec": map[string]interface{}{
			"mode":     "disagg",
			"strategy": rolloutStrategyMap(engine.PodGroupDeployment, decoder.PodGroupDeployment),
			"prefill":  engineSpec,
			"decode":   decoderSpec,
		},
	}}
	obj.SetGroupVersionKind(podGroupDeploymentRolloutGVK)
	if err := controllerutil.SetControllerReference(isvc, obj, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner reference: %w", err)
	}
	return obj, nil
}

func podGroupDeploymentSpecMap(
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	role string,
	minReplicas *int,
	worker *v1beta1.WorkerSpec,
	ext *v1beta1.ComponentExtensionSpec,
	template *corev1.PodTemplateSpec,
) (map[string]interface{}, error) {
	if template == nil {
		return nil, fmt.Errorf("nil pod template")
	}
	cfg := ext.PodGroupDeployment
	if cfg == nil {
		cfg = &v1beta1.PodGroupDeploymentComponentSpec{}
	}

	t := template.DeepCopy()
	if t.Labels == nil {
		t.Labels = map[string]string{}
	}
	t.Labels["app.kubernetes.io/name"] = isvc.Name
	t.Labels["app.kubernetes.io/component"] = role
	t.Labels[constants.InferenceServicePodLabelKey] = isvc.Name
	t.Labels[constants.OMEComponentLabel] = string(component)

	templateMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(t)
	if err != nil {
		return nil, fmt.Errorf("convert pod template: %w", err)
	}

	out := map[string]interface{}{
		"groups":                 int64(effectivePodGroupDeploymentGroups(cfg, minReplicas)),
		"replicasPerGroup":       int64(effectivePodGroupDeploymentReplicasPerGroup(cfg, worker)),
		"topologyKey":            effectivePodGroupDeploymentTopologyKey(cfg),
		"serviceTargetOrdinal":   int64(effectivePodGroupDeploymentServiceTargetOrdinal(cfg)),
		"scheduleTimeoutSeconds": int64(effectivePodGroupDeploymentScheduleTimeoutSeconds(cfg)),
		"schedulerName":          effectivePodGroupDeploymentSchedulerName(cfg, &t.Spec),
		"updateStrategy":         updateStrategyMap(cfg.UpdateStrategy),
		"template":               templateMap,
	}
	if multiNode := multiNodeMap(cfg.MultiNode); len(multiNode) > 0 {
		out["multiNode"] = multiNode
	}
	return out, nil
}

func podGroupDeploymentRolloutNeedsUpdate(current, desired *unstructured.Unstructured) bool {
	if !reflect.DeepEqual(current.Object["spec"], desired.Object["spec"]) {
		return true
	}
	if !reflect.DeepEqual(current.GetLabels(), desired.GetLabels()) {
		return true
	}
	return !reflect.DeepEqual(current.GetAnnotations(), desired.GetAnnotations())
}

func effectivePodGroupDeploymentGroups(cfg *v1beta1.PodGroupDeploymentComponentSpec, minReplicas *int) int32 {
	if cfg != nil && cfg.Groups != nil {
		return *cfg.Groups
	}
	if minReplicas != nil {
		return int32(*minReplicas)
	}
	return 1
}

func effectivePodGroupDeploymentReplicasPerGroup(cfg *v1beta1.PodGroupDeploymentComponentSpec, worker *v1beta1.WorkerSpec) int32 {
	if cfg != nil && cfg.ReplicasPerGroup != nil {
		return *cfg.ReplicasPerGroup
	}
	if worker != nil && worker.Size != nil && *worker.Size > 0 {
		return int32(*worker.Size) + 1
	}
	return 1
}

func effectivePodGroupDeploymentTopologyKey(cfg *v1beta1.PodGroupDeploymentComponentSpec) string {
	if cfg != nil && cfg.TopologyKey != "" {
		return cfg.TopologyKey
	}
	return corev1.LabelHostname
}

func effectivePodGroupDeploymentServiceTargetOrdinal(cfg *v1beta1.PodGroupDeploymentComponentSpec) int32 {
	if cfg != nil && cfg.ServiceTargetOrdinal != nil {
		return *cfg.ServiceTargetOrdinal
	}
	return 0
}

func effectivePodGroupDeploymentScheduleTimeoutSeconds(cfg *v1beta1.PodGroupDeploymentComponentSpec) int32 {
	if cfg != nil && cfg.ScheduleTimeoutSeconds != nil {
		return *cfg.ScheduleTimeoutSeconds
	}
	return 60
}

func effectivePodGroupDeploymentSchedulerName(cfg *v1beta1.PodGroupDeploymentComponentSpec, podSpec *corev1.PodSpec) string {
	if cfg != nil && cfg.SchedulerName != "" {
		return cfg.SchedulerName
	}
	if podSpec != nil && podSpec.SchedulerName != "" {
		return podSpec.SchedulerName
	}
	return "default-scheduler"
}

func rolloutStrategyMap(engine, decoder *v1beta1.PodGroupDeploymentComponentSpec) map[string]interface{} {
	strategy := &v1beta1.PodGroupDeploymentRolloutStrategy{}
	if decoder != nil && decoder.Rollout != nil {
		strategy = decoder.Rollout
	}
	if engine != nil && engine.Rollout != nil {
		strategy = engine.Rollout
	}
	maxUnavailablePercent := int32(25)
	if strategy.MaxUnavailablePercent != nil {
		maxUnavailablePercent = *strategy.MaxUnavailablePercent
	}
	maxSkew := int32(0)
	if strategy.MaxSkew != nil {
		maxSkew = *strategy.MaxSkew
	}
	return map[string]interface{}{
		"maxUnavailablePercent": int64(maxUnavailablePercent),
		"maxSkew":               int64(maxSkew),
	}
}

func updateStrategyMap(strategy *v1beta1.PodGroupDeploymentUpdateStrategy) map[string]interface{} {
	out := map[string]interface{}{
		"type": "RecreatePod",
	}
	if strategy == nil {
		return out
	}
	if strategy.Type != "" {
		out["type"] = strategy.Type
	}
	if strategy.RollingUpdate == nil {
		return out
	}
	rolling := map[string]interface{}{}
	if strategy.RollingUpdate.MaxSurge != nil {
		rolling["maxSurge"] = int64(*strategy.RollingUpdate.MaxSurge)
	}
	if strategy.RollingUpdate.MaxUnavailable != nil {
		rolling["maxUnavailable"] = int64(*strategy.RollingUpdate.MaxUnavailable)
	}
	if strategy.RollingUpdate.MaxUnavailablePercent != nil {
		rolling["maxUnavailablePercent"] = int64(*strategy.RollingUpdate.MaxUnavailablePercent)
	}
	if strategy.RollingUpdate.ProgressDeadlineSeconds != nil {
		rolling["progressDeadlineSeconds"] = int64(*strategy.RollingUpdate.ProgressDeadlineSeconds)
	}
	if len(rolling) > 0 {
		out["rollingUpdate"] = rolling
	}
	return out
}

func multiNodeMap(multiNode *v1beta1.PodGroupDeploymentMultiNodeSpec) map[string]interface{} {
	out := map[string]interface{}{}
	if multiNode == nil {
		return out
	}
	if multiNode.Enabled {
		out["enabled"] = true
	}
	if multiNode.ContainerName != "" {
		out["containerName"] = multiNode.ContainerName
	}
	if multiNode.TensorParallelSize != nil {
		out["tensorParallelSize"] = int64(*multiNode.TensorParallelSize)
	}
	if multiNode.DistInitPort != nil {
		out["distInitPort"] = int64(*multiNode.DistInitPort)
	}
	return out
}

func (r *InferenceServiceReconciler) setPodGroupDeploymentRolloutStatus(isvc *v1beta1.InferenceService, pgdr *unstructured.Unstructured) {
	phase, _, _ := unstructured.NestedString(pgdr.Object, "status", "phase")
	currentPhase, _, _ := unstructured.NestedString(pgdr.Object, "status", "currentPhase")
	if phase == "" {
		phase = "Reconciling"
	}
	message := fmt.Sprintf("PodGroupDeploymentRollout %s", phase)
	if currentPhase != "" {
		message = fmt.Sprintf("%s (%s)", message, currentPhase)
	}

	r.setPodGroupDeploymentRolloutComponentStatus(isvc, v1beta1.EngineComponent, "prefill", pgdr, phase, message)
	r.setPodGroupDeploymentRolloutComponentStatus(isvc, v1beta1.DecoderComponent, "decode", pgdr, phase, message)
}

func (r *InferenceServiceReconciler) setPodGroupDeploymentRolloutComponentStatus(
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	role string,
	pgdr *unstructured.Unstructured,
	phase string,
	message string,
) {
	if r.StatusManager != nil {
		r.StatusManager.InitializeComponentCondition(&isvc.Status, component)
	}
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	statusSpec := isvc.Status.Components[component]
	ready := phase == "Complete" || rolloutMemberComplete(pgdr, role)
	if ready {
		statusSpec.URL = &knapis.URL{Scheme: "http", Host: fmt.Sprintf("%s-%s.%s.svc.cluster.local", isvc.Name, role, isvc.Namespace)}
		isvc.Status.SetCondition(componentReadyCondition(component), &knapis.Condition{
			Type:    componentReadyCondition(component),
			Status:  corev1.ConditionTrue,
			Reason:  "PodGroupDeploymentRolloutReady",
			Message: message,
		})
	} else {
		isvc.Status.SetCondition(componentReadyCondition(component), &knapis.Condition{
			Type:    componentReadyCondition(component),
			Status:  corev1.ConditionFalse,
			Reason:  "PodGroupDeploymentRolloutReconciling",
			Message: message,
		})
	}
	isvc.Status.Components[component] = statusSpec
}

func componentReadyCondition(component v1beta1.ComponentType) knapis.ConditionType {
	switch component {
	case v1beta1.DecoderComponent:
		return v1beta1.DecoderReady
	case v1beta1.RouterComponent:
		return v1beta1.RouterReady
	default:
		return v1beta1.EngineReady
	}
}

func rolloutMemberComplete(pgdr *unstructured.Unstructured, role string) bool {
	members, found, _ := unstructured.NestedSlice(pgdr.Object, "status", "members")
	if !found {
		return false
	}
	for _, raw := range members {
		member, ok := raw.(map[string]interface{})
		if !ok || member["role"] != role {
			continue
		}
		groups, _ := toInt64(member["groups"])
		updatedReadyGroups, _ := toInt64(member["updatedReadyGroups"])
		return groups > 0 && updatedReadyGroups >= groups
	}
	return false
}

func toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}
