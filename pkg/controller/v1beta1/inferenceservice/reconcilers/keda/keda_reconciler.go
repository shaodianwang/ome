package keda

import (
	"context"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var log = logf.Log.WithName("KEDAReconciler")

// defaultMinReplicas is the floor applied when the component spec does not
// declare MinReplicas > 0.
const defaultMinReplicas int32 = 1

// KEDAReconciler reconciles the ScaledObject resource.
//
// Phase 1 of OEP-0013 stubs the trigger path: if the component declares
// an Autoscaler.Keda block with at least one trigger, those triggers are
// passed through verbatim; otherwise no ScaledObject is emitted. Full
// verbatim pass-through (Advanced, PollingInterval, CooldownPeriod,
// IdleReplicaCount, Fallback) lands in phase 2.
type KEDAReconciler struct {
	client       client.Client
	scheme       *runtime.Scheme
	ScaledObject *kedav1.ScaledObject
	componentExt *v1beta1.ComponentExtensionSpec
}

// NewKEDAReconciler creates a new KEDAReconciler.
func NewKEDAReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
) (*KEDAReconciler, error) {

	scaledObject := createScaledObject(componentMeta, componentExt)

	return &KEDAReconciler{
		client:       client,
		scheme:       scheme,
		ScaledObject: scaledObject,
		componentExt: componentExt,
	}, nil
}

// createScaledObject builds the desired ScaledObject. When the component
// has no Autoscaler.Keda block, Triggers is left empty and Reconcile()
// will skip the apply.
func createScaledObject(
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
) *kedav1.ScaledObject {
	filteredLabels := make(map[string]string)
	for key, value := range componentMeta.Labels {
		// Exclude the label that could prevent opening the edit window through lens
		if key != "k8slens-edit-resource-version" {
			filteredLabels[key] = value
		}
	}
	minReplicas := calculateMinReplicas(componentExt)
	maxReplicas := calculateMaxReplicas(componentExt, minReplicas)

	var triggers []kedav1.ScaleTriggers
	if componentExt.Autoscaler != nil && componentExt.Autoscaler.Keda != nil {
		triggers = componentExt.Autoscaler.Keda.Triggers
	}

	return &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:        utils.GetScaledObjectName(componentMeta.Name),
			Namespace:   componentMeta.Namespace,
			Labels:      filteredLabels,
			Annotations: componentMeta.Annotations,
		},
		Spec: kedav1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1.ScaleTarget{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       componentMeta.Name,
			},
			MinReplicaCount: &minReplicas,
			MaxReplicaCount: &maxReplicas,
			Triggers:        triggers,
		},
	}
}

// calculateMinReplicas calculates the minimum replicas
func calculateMinReplicas(componentExt *v1beta1.ComponentExtensionSpec) int32 {
	if componentExt.MinReplicas != nil && *componentExt.MinReplicas > 0 {
		return int32(*componentExt.MinReplicas)
	}
	return defaultMinReplicas
}

// calculateMaxReplicas calculates the maximum replicas
func calculateMaxReplicas(componentExt *v1beta1.ComponentExtensionSpec, minReplicas int32) int32 {
	if componentExt.MaxReplicas > int(minReplicas) {
		return int32(componentExt.MaxReplicas)
	}
	return minReplicas
}

// checkScaledObjectExist checks if the ScaledObject exists and determines the action
func (r *KEDAReconciler) checkScaledObjectExist() (constants.CheckResultType, *kedav1.ScaledObject, error) {
	existingScaledObject := &kedav1.ScaledObject{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: r.ScaledObject.Namespace,
		Name:      r.ScaledObject.Name,
	}, existingScaledObject)

	if err != nil {
		if apierr.IsNotFound(err) {
			if shouldCreateScaledObject(r.ScaledObject) {
				return constants.CheckResultCreate, nil, nil
			}
			return constants.CheckResultSkipped, nil, nil
		}
		return constants.CheckResultUnknown, nil, err
	}

	if semanticScaledObjectEquals(r.ScaledObject, existingScaledObject) {
		return constants.CheckResultExisted, existingScaledObject, nil
	}
	if shouldDeleteScaledObject(r.ScaledObject) {
		return constants.CheckResultDelete, existingScaledObject, nil
	}
	return constants.CheckResultUpdate, existingScaledObject, nil
}

// semanticScaledObjectEquals checks if the desired and existing ScaledObjects are equal
func semanticScaledObjectEquals(desired, existing *kedav1.ScaledObject) bool {
	desiredAutoscalerClass := desired.Annotations[constants.AutoscalerClass]
	existingAutoscalerClass := existing.Annotations[constants.AutoscalerClass]

	autoscalerClassChanged := desiredAutoscalerClass != existingAutoscalerClass
	return equality.Semantic.DeepEqual(desired.Spec, existing.Spec) && !autoscalerClassChanged
}

// shouldDeleteScaledObject determines if the ScaledObject should be deleted
func shouldDeleteScaledObject(desired *kedav1.ScaledObject) bool {
	desiredAutoscalerClass := desired.Annotations[constants.AutoscalerClass]
	return constants.AutoscalerClassType(desiredAutoscalerClass) == constants.AutoscalerClassExternal
}

// shouldCreateScaledObject determines if the ScaledObject should be created.
// Phase 1: require at least one trigger; otherwise skip apply to avoid
// creating an invalid (zero-trigger) ScaledObject.
func shouldCreateScaledObject(desired *kedav1.ScaledObject) bool {
	if len(desired.Spec.Triggers) == 0 {
		return false
	}
	desiredAutoscalerClass := desired.Annotations[constants.AutoscalerClass]
	return desiredAutoscalerClass == "" || constants.AutoscalerClassType(desiredAutoscalerClass) == constants.AutoscalerClassKEDA
}

// Reconcile reconciles the ScaledObject resource
func (r *KEDAReconciler) Reconcile() (runtime.Object, error) {
	checkResult, existingScaledObject, err := r.checkScaledObjectExist()
	log.Info("Reconciling ScaledObject", "namespace", r.ScaledObject.Namespace, "name", r.ScaledObject.Name, "checkResult", checkResult.String())
	if err != nil {
		return nil, err
	}

	var opErr error
	switch checkResult {
	case constants.CheckResultCreate:
		opErr = r.client.Create(context.TODO(), r.ScaledObject)
	case constants.CheckResultUpdate:
		// Use the resourceVersion from the existing ScaledObject
		r.ScaledObject.ResourceVersion = existingScaledObject.ResourceVersion

		opErr = r.client.Update(context.TODO(), r.ScaledObject)
	case constants.CheckResultDelete:
		opErr = r.client.Delete(context.TODO(), r.ScaledObject)
	default:
		return existingScaledObject, nil
	}

	if opErr != nil {
		log.Error(opErr, "Failed to reconcile ScaledObject", "namespace", r.ScaledObject.Namespace, "name", r.ScaledObject.Name)
		return nil, opErr
	}

	return r.ScaledObject, nil
}

// SetControllerReferences sets the owner reference for the ScaledObject
func (r *KEDAReconciler) SetControllerReferences(owner metav1.Object, scheme *runtime.Scheme) error {
	return controllerutil.SetControllerReference(owner, r.ScaledObject, scheme)
}
