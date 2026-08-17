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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/raw"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
)

// DeploymentReconciler handles common deployment reconciliation logic
type DeploymentReconciler struct {
	Client        client.Client
	APIReader     client.Reader
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
	pdbRequest pdb.Request,
) (ctrl.Result, error) {
	// The synthetic Engine spec carries component settings to the Raw
	// Deployment and Service builders.
	inferenceServiceSpec := &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: *componentSpec,
		},
	}

	reconciler, err := raw.NewRawKubeReconciler(r.Client, r.APIReader, r.Clientset, r.Scheme, pdbRequest, objectMeta, inferenceServiceSpec, podSpec)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to create RawKubeReconciler for %s", componentType)
	}

	if err := r.setRawReferences(isvc, reconciler); err != nil {
		return ctrl.Result{}, err
	}

	deployment, err := reconciler.Reconcile(ctx)
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
	if err := controllerutil.SetControllerReference(isvc, reconciler.PodMonitor.PodMonitor, r.Scheme); err != nil {
		return errors.Wrapf(err, "failed to set podmonitor owner reference")
	}
	return nil
}
