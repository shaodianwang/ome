package components

import (
	"context"

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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/rbac"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/utils"
)

var _ Component = &Router{}
var _ ComponentConfig = &Router{}

// Router reconciles resources for the router component
type Router struct {
	BaseComponentFields
	routerSpec           *v1beta1.RouterSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
	rbacReconciler       *rbac.RBACReconciler
}

// NewRouter creates a new Router component instance
func NewRouter(
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
	routerSpec *v1beta1.RouterSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
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
		Log:                    ctrl.Log.WithName("RouterReconciler"),
		Overlays:               overlays,
	}

	return &Router{
		BaseComponentFields: base,
		routerSpec:          routerSpec,
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

// Reconcile implements the Component interface for Router
func (r *Router) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	r.Log.Info("Reconciling router component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate router spec
	if r.routerSpec == nil {
		return ctrl.Result{}, errors.New("router spec is nil")
	}

	// Reconcile object metadata
	objectMeta, err := r.reconcileObjectMeta(isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}

	// Reconcile RBAC resources (ServiceAccount, Role, RoleBinding)
	r.rbacReconciler = rbac.NewRBACReconciler(
		r.Client,
		r.Scheme,
		objectMeta,
		v1beta1.RouterComponent,
		isvc,
	)
	if err := r.rbacReconciler.Reconcile(); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile RBAC resources")
	}

	// Reconcile pod spec
	podSpec, err := r.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Set the service account name in the pod spec
	podSpec.ServiceAccountName = r.rbacReconciler.GetServiceAccountName()

	// Reconcile deployment based on deployment mode. The deployment
	// reconciler's RequeueAfter MUST be preserved through the rest of
	// this function — OMENative's per-Instance dispatcher uses it to
	// re-run after a Sequential / Ratio gate denial OR to poll an
	// in-flight Update / Create. Discarding it strands the rollout
	// until an unrelated watch event happens to fire another reconcile
	// (the post-decoder Sequential stall, see PR #240 follow-up).
	deploymentResult, err := r.reconcileDeployment(ctx, isvc, objectMeta, podSpec)
	if err != nil {
		return deploymentResult, err
	}

	// Per-Component stable Service + PodMonitor for OMENative. See the
	// rationale on Engine.reconcileOMENativeSubresources; this method is
	// the router equivalent.
	if r.DeploymentMode == constants.OMENative {
		if err := r.reconcileOMENativeSubresources(ctx, isvc, objectMeta, podSpec); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile router OMENative sub-resources")
		}
	}

	// Update router status
	if err := r.updateRouterStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return deploymentResult, nil
}

// reconcileOMENativeSubresources ensures the per-Component stable
// Service (`<isvc>-<comp>`) and PodMonitor for OMENative-managed
// router. Engine and Decoder have their own equivalent methods —
// repetition is accepted here over a shared helper so the omenative
// package can stay pure (pods + headless Service ONLY). See
// Engine.reconcileOMENativeSubresources for the full rationale.
func (r *Router) reconcileOMENativeSubresources(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) error {
	if podSpec == nil {
		return nil
	}
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(v1beta1.RouterComponent),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	componentMeta := objectMeta.DeepCopy()
	componentMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}

	// Stable Service via the SAME top-level reconciler RawDeployment and
	// MultiNode use; only the selector + owner refs are OMENative-shaped.
	sr := service.NewServiceReconciler(r.Client, r.Scheme, *componentMeta, &r.routerSpec.ComponentExtensionSpec, podSpec, selector)
	if _, err := sr.Reconcile(); err != nil {
		return errors.Wrap(err, "stable service")
	}

	// PodMonitor — build inline (shared podmonitor reconciler hardcodes
	// the `app=<name>` selector that matches Raw / MultiNode pods but
	// NOT OMENative pods). Scrape port: prefer "metrics", else first
	// declared port, else "http".
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
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, target, func() error {
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

// reconcileDeployment manages the deployment logic for different deployment modes
func (r *Router) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) (ctrl.Result, error) {
	switch r.DeploymentMode {
	case constants.RawDeployment:
		return r.deploymentReconciler.ReconcileRawDeployment(isvc, objectMeta, podSpec, &r.routerSpec.ComponentExtensionSpec, v1beta1.RouterComponent)
	case constants.OMENative:
		// OMENative-mode Components dispatch through the InferenceReplica
		// path by default. Router is always single-pod (MultiPod=false;
		// no Worker block) so the projector receives a nil WorkerPodSpec
		// — matches the omenative ReconcileParams projection below.
		// Fallback to the omenative direct path is gated on the
		// cluster-wide --disable-ir-managed-omenative manager flag.
		if irprojector.IsIRManagedComponent(r.DeploymentMode, irprojector.IsManagerDisableFlagSet()) {
			// Resolve the authoritative ComponentAutoscaler from the
			// ISVC → runtime → default chain (OEP-0013 §"Inheritance
			// chain"). See engine dispatch for the full design — the
			// projection is a no-op at runtime until Slice 3 dispatches
			// HPA / KEDA / external / none from ir.Spec.Autoscaler.
			resolvedAS, _ := autoscaler.ResolveComponentAutoscaler(r.Runtime, isvc, v1beta1.RouterComponent)
			if _, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
				ISVC:               isvc,
				Component:          v1beta1.RouterComponent,
				ComponentExt:       &r.routerSpec.ComponentExtensionSpec,
				ObjectMeta:         objectMeta,
				PodSpec:            podSpec,
				MultiPod:           false,
				ResolvedAutoscaler: resolvedAS,
				Client:             r.Client,
			}); err != nil {
				return ctrl.Result{}, errors.Wrap(err, "failed to project InferenceReplica for router")
			}
			return ctrl.Result{}, nil
		}
		return omenative.ReconcileComponent(ctx, omenative.ReconcileParams{
			ISVC:                    isvc,
			ObjectMeta:              objectMeta,
			PodSpec:                 podSpec,
			Component:               v1beta1.RouterComponent,
			ComponentExt:            &r.routerSpec.ComponentExtensionSpec,
			GangSchedulingAvailable: r.GangSchedulingAvailable,
			Client:                  r.Client,
			APIReader:               r.APIReader,
			Expectations:            r.Expectations,
			Recorder:                r.Recorder,
		})
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for router")
	}
}

// updateRouterStatus updates the status of the router component
func (r *Router) updateRouterStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&r.BaseComponentFields, isvc, v1beta1.RouterComponent, objectMeta, r.getPodLabelInfo)
}

// getPodLabelInfo returns the pod label key and value used to filter the
// router's pods. All surviving deployment modes (raw, multi-node, OMENative)
// emit pods carrying the raw-deployment app label.
func (r *Router) getPodLabelInfo(rawDeployment bool, objectMeta metav1.ObjectMeta, statusSpec v1beta1.ComponentStatusSpec) (string, string) {
	_ = rawDeployment
	_ = statusSpec
	return constants.RawDeploymentAppLabel, constants.GetRawServiceLabel(objectMeta.Name)
}

// reconcileObjectMeta creates the object metadata for the router component
func (r *Router) reconcileObjectMeta(isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	routerName, err := r.determineRouterName(isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	annotations, err := r.processAnnotations(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      routerName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := r.processLabels(isvc)

	if err != nil {
		return metav1.ObjectMeta{
			Name:        routerName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        routerName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// processAnnotations processes the annotations for the router
func (r *Router) processAnnotations(isvc *v1beta1.InferenceService) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	// Merge with router annotations
	mergedAnnotations := annotations
	if r.routerSpec != nil {
		routerAnnotations := r.routerSpec.Annotations
		mergedAnnotations = utils.Union(annotations, routerAnnotations)
	}

	// Use common function for base annotations processing
	processedAnnotations, err := ProcessBaseAnnotations(&r.BaseComponentFields, isvc, mergedAnnotations)
	if err != nil {
		return nil, err
	}

	return processedAnnotations, nil
}

// processLabels processes the labels for the router
func (r *Router) processLabels(isvc *v1beta1.InferenceService) (map[string]string, error) {
	mergedLabels := isvc.Labels
	if r.routerSpec != nil {
		routerLabels := r.routerSpec.Labels
		mergedLabels = utils.Union(isvc.Labels, routerLabels)
	}

	// Use common function for base labels processing
	return ProcessBaseLabels(&r.BaseComponentFields, isvc, v1beta1.RouterComponent, mergedLabels)
}

// determineRouterName determines the name of the router service
func (r *Router) determineRouterName(isvc *v1beta1.InferenceService) (string, error) {
	// For router, we'll use a pattern similar to predictor but with "-router" suffix
	defaultRouterName := isvc.Name + "-router"
	existingName := defaultRouterName

	existing := &v1.Service{}
	if err := r.Client.Get(context.TODO(), types.NamespacedName{Name: defaultRouterName, Namespace: isvc.Namespace}, existing); err == nil {
		return existingName, nil
	}

	return defaultRouterName, nil
}

// reconcilePodSpec creates the pod spec for the router component
func (r *Router) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	if r.routerSpec.Runner != nil {
		if r.routerSpec.Config != nil {
			r.Log.Info("Adding config to router env", "inference service", isvc.Name, "namespace", isvc.Namespace)
			for k, v := range r.routerSpec.Config {
				r.routerSpec.Runner.Env = append(r.routerSpec.Runner.Env, v1.EnvVar{Name: k, Value: v})
			}
		}
	}
	// Use common pod spec reconciler for base logic
	podSpec, err := r.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &r.routerSpec.PodSpec, r.routerSpec.Runner)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(&r.BaseComponentFields, isvc, podSpec, objectMeta)

	r.Log.Info("Router PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// GetComponentType implements ComponentConfig interface
func (r *Router) GetComponentType() v1beta1.ComponentType {
	return v1beta1.RouterComponent
}

// GetComponentSpec implements ComponentConfig interface
func (r *Router) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if r.routerSpec == nil {
		return nil
	}
	return &r.routerSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (r *Router) GetServiceSuffix() string {
	return "-router"
}

// ValidateSpec implements ComponentConfig interface
func (r *Router) ValidateSpec() error {
	if r.routerSpec == nil {
		return errors.New("router spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
