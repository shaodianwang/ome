package common

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/raw"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
)

// DeploymentReconciler handles common deployment reconciliation logic
type DeploymentReconciler struct {
	Client        client.Client
	Clientset     kubernetes.Interface
	Scheme        *runtime.Scheme
	StatusManager *status.StatusReconciler
	Log           logr.Logger
}

// ReconcileRawDeployment handles raw Kubernetes deployment
func (r *DeploymentReconciler) ReconcileRawDeployment(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	objectMeta metav1.ObjectMeta,
	podSpec *v1.PodSpec,
	componentSpec *v1beta1.ComponentExtensionSpec,
	componentType v1beta1.ComponentType,
	resolvedAutoscaler *v1beta1.ComponentAutoscaler,
) (ctrl.Result, error) {
	// Synthetic InferenceServiceSpec carrying the component-level extension
	// spec to the downstream raw resource builders. Engine is the carrier for
	// the Deployment, Service, PDB, and PodMonitor configuration.
	inferenceServiceSpec := &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: *componentSpec,
		},
	}

	reconciler, err := raw.NewRawKubeReconciler(r.Client, r.Clientset, r.Scheme, objectMeta, inferenceServiceSpec, podSpec)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to create RawKubeReconciler for %s", componentType)
	}

	if err := r.setRawReferences(isvc, reconciler); err != nil {
		return ctrl.Result{}, err
	}

	deployment, err := reconciler.Reconcile()
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to reconcile %s", componentType)
	}
	if err := autoscaler.DispatchForRawComponent(ctx, autoscaler.RawDispatchInput{
		Client:             r.Client,
		Scheme:             r.Scheme,
		ISVC:               isvc,
		ComponentMeta:      objectMeta,
		ResolvedAutoscaler: resolvedAutoscaler,
		ComponentExt:       componentSpec,
	}); err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to dispatch autoscaler for raw %s", componentType)
	}

	r.StatusManager.PropagateRawStatus(&isvc.Status, componentType, deployment, reconciler.URL)
	return ctrl.Result{}, nil
}

// setRawReferences sets the necessary references for raw deployment
func (r *DeploymentReconciler) setRawReferences(isvc *v1beta1.InferenceService, reconciler *raw.RawKubeReconciler) error {
	if err := controllerutil.SetControllerReference(isvc, reconciler.Deployment.Deployment, r.Scheme); err != nil {
		return errors.Wrapf(err, "failed to set deployment owner reference")
	}
	if err := controllerutil.SetControllerReference(isvc, reconciler.Service.Service, r.Scheme); err != nil {
		return errors.Wrapf(err, "failed to set service owner reference")
	}
	if err := controllerutil.SetControllerReference(isvc, reconciler.PodDisruptionBudget.PDB, r.Scheme); err != nil {
		return errors.Wrapf(err, "failed to set pdb owner reference")
	}
	if err := controllerutil.SetControllerReference(isvc, reconciler.PodMonitor.PodMonitor, r.Scheme); err != nil {
		return errors.Wrapf(err, "failed to set podmonitor owner reference")
	}
	return nil
}
