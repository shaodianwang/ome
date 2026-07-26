package raw

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/deployment"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/podmonitor"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
)

// RawKubeReconciler reconciles the Native K8S Resources
type RawKubeReconciler struct {
	client              client.Client
	scheme              *runtime.Scheme
	Deployment          *deployment.DeploymentReconciler
	Service             *service.ServiceReconciler
	Scaler              *autoscaler.AutoscalerReconciler
	PodDisruptionBudget *pdb.PDBReconciler
	PodMonitor          *podmonitor.PodMonitorReconciler
	URL                 *knapis.URL
}

// NewRawKubeReconciler creates raw kubernetes resource reconciler.
func NewRawKubeReconciler(client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	inferenceServiceSpec *v1beta1.InferenceServiceSpec,
	podSpec *corev1.PodSpec,
) (*RawKubeReconciler, error) {
	as, err := autoscaler.NewAutoscalerReconciler(client, clientset, scheme, componentMeta, inferenceServiceSpec)
	if err != nil {
		return nil, err
	}

	pdb := pdb.NewPDBReconciler(client, scheme, componentMeta, &inferenceServiceSpec.Engine.ComponentExtensionSpec)
	pm := podmonitor.NewPodMonitorReconciler(client, scheme, componentMeta, podSpec)
	url, err := createRawURL(clientset, componentMeta)
	if err != nil {
		return nil, err
	}

	// Component-level extension spec is carried on Spec.Engine by the
	// caller (deployment_reconciler.go) regardless of which actual
	// component is being reconciled.
	componentExt := &inferenceServiceSpec.Engine.ComponentExtensionSpec

	return &RawKubeReconciler{
		client:              client,
		scheme:              scheme,
		Deployment:          deployment.NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec),
		Service:             service.NewServiceReconciler(client, scheme, componentMeta, componentExt, podSpec, nil),
		Scaler:              as,
		PodDisruptionBudget: pdb,
		PodMonitor:          pm,
		URL:                 url,
	}, nil
}

func createRawURL(clientset kubernetes.Interface, metadata metav1.ObjectMeta) (*knapis.URL, error) {
	ingressConfig, err := controllerconfig.NewIngressConfig(clientset)
	if err != nil {
		return nil, err
	}
	domainService := services.NewDomainService()
	url := &knapis.URL{}
	// Honor the configured urlScheme rather than hardcoding
	// http; NewIngressConfig defaults UrlScheme when unset, so this is never empty.
	url.Scheme = ingressConfig.UrlScheme
	url.Host, err = domainService.GenerateDomainName(metadata.Name, metadata, ingressConfig)
	if err != nil {
		return nil, err
	}

	return url, nil
}

// Reconcile ...
func (r *RawKubeReconciler) Reconcile() (*appsv1.Deployment, error) {
	// reconcile Deployments
	dply, err := r.Deployment.Reconcile()
	if err != nil {
		return nil, err
	}
	// reconcile Service
	_, err = r.Service.Reconcile()
	if err != nil {
		return nil, err
	}
	// reconcile HPA
	err = r.Scaler.Reconcile()
	if err != nil {
		return nil, err
	}
	// reconcile PDB
	_, err = r.PodDisruptionBudget.Reconcile()
	if err != nil {
		return nil, err
	}
	// reconcile PodMonitor
	_, err = r.PodMonitor.Reconcile()
	if err != nil {
		return nil, err
	}
	return dply, nil
}
