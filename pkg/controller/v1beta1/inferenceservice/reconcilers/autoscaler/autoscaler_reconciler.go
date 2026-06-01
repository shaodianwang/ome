package autoscaler

import (
	"context"
	"fmt"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	hpa "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/hpa"
	keda "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/keda"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// Autoscaler Interface implemented by all autoscalers
type Autoscaler interface {
	Reconcile() error
	SetControllerReferences(owner metav1.Object, scheme *runtime.Scheme) error
}

// NoOpAutoscaler Autoscaler that does nothing. Can be used to disable creation of autoscaler resources.
type NoOpAutoscaler struct{}

func (*NoOpAutoscaler) Reconcile() error {
	return nil
}

func (a *NoOpAutoscaler) SetControllerReferences(owner metav1.Object, scheme *runtime.Scheme) error {
	return nil
}

var log = logf.Log.WithName("AutoscalerReconciler")

// AutoscalerReconciler is the struct of Raw K8S Object
type AutoscalerReconciler struct {
	client       client.Client
	scheme       *runtime.Scheme
	Autoscaler   Autoscaler
	componentExt *v1beta1.ComponentExtensionSpec
}

func NewAutoscalerReconciler(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	inferenceServiceSpec *v1beta1.InferenceServiceSpec,
) (*AutoscalerReconciler, error) {
	as, err := createAutoscaler(client, scheme, componentMeta, inferenceServiceSpec)
	if err != nil {
		return nil, err
	}
	return &AutoscalerReconciler{
		client:       client,
		scheme:       scheme,
		Autoscaler:   as,
		componentExt: &inferenceServiceSpec.Engine.ComponentExtensionSpec,
	}, err
}

func getAutoscalerClass(metadata metav1.ObjectMeta) constants.AutoscalerClassType {
	annotations := metadata.Annotations
	if value, ok := annotations[constants.AutoscalerClass]; ok {
		return constants.AutoscalerClassType(value)
	} else {
		return constants.DefaultAutoscalerClass
	}
}

func createAutoscaler(client client.Client,
	scheme *runtime.Scheme, componentMeta metav1.ObjectMeta,
	inferenceServiceSpec *v1beta1.InferenceServiceSpec,
) (Autoscaler, error) {
	ac := getAutoscalerClass(componentMeta)

	switch ac {
	// HPA and KEDA can not coexist for the same deployment
	case constants.AutoscalerClassHPA, constants.AutoscalerClassExternal:
		// Decision Q6 (OEP-0013): KEDA scheme is registered
		// unconditionally at manager startup, so the prior
		// IsGroupRegistered probe is dead code — call the cleanup
		// directly.
		err := deleteExistingScaledObject(client, componentMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to delete existing ScaledObject: %w", err)
		}
		return hpa.NewHPAReconciler(client, scheme, componentMeta, &inferenceServiceSpec.Engine.ComponentExtensionSpec), nil
	case constants.AutoscalerClassKEDA:
		// Before creating ScaledObject, ensure any existing HPA is deleted
		err := deleteExistingHPA(client, componentMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to delete existing HPA: %w", err)
		}
		return keda.NewKEDAReconciler(client, scheme, componentMeta, &inferenceServiceSpec.Engine.ComponentExtensionSpec), nil
	default:
		return nil, fmt.Errorf("unknown autoscaler class type: %v", ac)
	}
}

// deleteExistingScaledObject deletes any existing ScaledObject for the Deployment
func deleteExistingScaledObject(client client.Client, componentMeta metav1.ObjectMeta) error {
	scaledObjectName := utils.GetScaledObjectName(componentMeta.Name)
	scaledObject := &kedav1.ScaledObject{}
	err := client.Get(context.TODO(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      scaledObjectName,
	}, scaledObject)
	if err != nil {
		if apierr.IsNotFound(err) {
			return nil
		}
		// Decision Q6 (OEP-0013): KEDA is a hard dependency, so
		// `IsNotRegisteredError` cannot fire here at steady state. We keep
		// it as a defensive guard for the narrow window where KEDA was
		// uninstalled out from under a running manager.
		if runtime.IsNotRegisteredError(err) {
			log.Info("KEDA ScaledObject type not registered in scheme, skipping cleanup",
				"namespace", componentMeta.Namespace, "name", scaledObjectName)
			return nil
		}
		return err
	}
	// Delete the existing ScaledObject
	return client.Delete(context.TODO(), scaledObject)
}

// deleteExistingHPA deletes any existing HPA for the Deployment
func deleteExistingHPA(client client.Client, componentMeta metav1.ObjectMeta) error {
	hpaName := componentMeta.Name
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err := client.Get(context.TODO(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      hpaName,
	}, hpa)
	if err != nil {
		if apierr.IsNotFound(err) {
			return nil
		}
		return err
	}
	// Delete the existing HPA
	return client.Delete(context.TODO(), hpa)
}

// Reconcile ...
func (r *AutoscalerReconciler) Reconcile() error {
	// reconcile Autoscaler
	if err := r.Autoscaler.Reconcile(); err != nil {
		return err
	}
	return nil
}
