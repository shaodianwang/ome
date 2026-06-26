package inferenceservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	policyv1 "k8s.io/api/policy/v1"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	v1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/components"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/external_service"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	multimodelconfig "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/modelconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	traffic "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/obsmetrics"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	"github.com/ome-projects/ome/pkg/utils"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices;inferenceservices/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes;servingruntimes/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes;clusterservingruntimes/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels;basemodels/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=finetunedweights/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=finetunedweights;finetunedweights/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels;basemodels/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networking.istio.io,resources=sidecars,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=update;patch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scheduling.x-k8s.io,resources=podgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.envoyproxy.io,resources=backendtrafficpolicies,verbs=get;list;watch;create;update;patch;delete

// InferenceServiceState describes the Readiness of the InferenceService
type InferenceServiceState string

// Different InferenceServiceState an InferenceService may have.
const (
	InferenceServiceReadyState    InferenceServiceState = "InferenceServiceReady"
	InferenceServiceNotReadyState InferenceServiceState = "InferenceServiceNotReady"
)

// InferenceServiceReconciler reconciles an InferenceService object
type InferenceServiceReconciler struct {
	client.Client
	ClientConfig *rest.Config
	Clientset    kubernetes.Interface
	// APIReader is the live (uncached) reader, populated from
	// mgr.GetAPIReader() in SetupWithManager. Threaded through the
	// component builder to OMENative for reads where cache lag would
	// be a correctness problem (revision bookkeeping, audit ledger,
	// EndpointSlice drain checks).
	APIReader client.Reader
	// Expectations is the OMENative create/delete bookkeeping cache.
	// SetupWithManager initializes it (if nil) and registers the Pod
	// event handler against the same instance the component dispatch
	// path threads into ReconcileParams.
	Expectations    *omenative.Expectations
	Log             logr.Logger
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	RuntimeSelector runtimeselector.Selector
	ModelCache      sharded.Provider
	// TrafficReconciler is the backend-policy reconciler.
	// Built from the active translator at controller startup; the
	// active translator is selected by traffic/factory.New based on
	// installed Gateway-implementation CRDs.
	TrafficReconciler *traffic.Reconciler
	// GangSchedulingAvailable is the cluster-discovery boolean — true
	// when the scheduler-plugins `scheduling.x-k8s.io/v1alpha1/PodGroup`
	// CRD is installed at controller startup. Threaded through the
	// component builder factory to OMENative's reconciler so the gang-
	// scheduling code path runs only when the CRD is actually present.
	// When false, multi-pod OMENative Instances still create pods but
	// the controller stamps a `GangSchedulingUnavailable=True`
	// Component condition.
	GangSchedulingAvailable bool
	// CanarySampler runs canary analysis Prometheus queries off the reconcile
	// goroutine: reconcile reads cached results non-blocking, and a completion
	// event re-reconciles the ISVC. Built once in SetupWithManager from the
	// canaryAnalysis operator config; nil disables metric-gated steps (they hold).
	CanarySampler *canary.Sampler
	// ConfigCache memoizes the inferenceservice-config ConfigMap for a short,
	// flag-driven TTL so a single reconcile pass shares one apiserver GET across
	// the Deploy / InferenceServices / Ingress / CanaryAnalysis config loads
	// instead of issuing one uncached Clientset GET each. A nil cache (or a
	// non-positive TTL) falls through to the apiserver every call, preserving the
	// pre-cache behavior; the short TTL keeps "ConfigMap edits apply without a
	// restart" intact. Initialized in SetupWithManager.
	ConfigCache *controllerconfig.ConfigCache
	// ConfigCacheTTL is the TTL applied to ConfigCache. Flag-driven (no in-code
	// behavioral default — supplied by the manager's --config-cache-ttl flag /
	// chart value); a non-positive value disables caching (always reads the
	// apiserver).
	ConfigCacheTTL time.Duration
}

func (r *InferenceServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the InferenceService instance
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, req.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// Bind structured logging context for this reconcile pass: every
	// per-step log line and every downstream callback that pulls from
	// ctx inherits (namespace, isvc) without re-stamping per-line. The
	// existing r.Log field is preserved for sites that already key off
	// the receiver — those use the same backing logger so output stays
	// consistent.
	log := r.Log.WithValues("namespace", isvc.Namespace, "isvc", isvc.Name)
	ctx = ctrl.LoggerInto(ctx, log)
	// get annotations from isvc
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	deployConfig, err := controllerconfig.NewDeployConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create DeployConfig")
	}

	// For backward compatibility with predictor-based architecture
	deploymentMode := isvcutils.GetDeploymentMode(annotations, deployConfig)
	log.Info("InferenceService deployment mode resolved", "deploymentMode", deploymentMode)

	// name of our custom finalizer
	finalizerName := "inferenceservice.finalizers"

	// examine DeletionTimestamp to determine if object is under deletion
	if isvc.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !controllerutil.ContainsFinalizer(isvc, finalizerName) {
			controllerutil.AddFinalizer(isvc, finalizerName)
			if err := r.Update(context.Background(), isvc); err != nil {
				// Race window: between top-of-reconcile Get and this
				// Update, the ISVC may have been deleted (NotFound) or
				// transitioned into deletion (Conflict / "Operation
				// cannot be fulfilled" with a UID precondition
				// mismatch). Either way we no longer need the
				// finalizer — drop the work cleanly instead of
				// returning the error and triggering a retry storm.
				if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
					return ctrl.Result{}, nil
				}
				return ctrl.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(isvc, finalizerName) {
			// Drop this ISVC's OMENative coordination + canary metric series so
			// the per-(namespace,isvc) vectors do not leak unbounded after
			// teardown. Idempotent and runs on the finalizer-bearing delete pass.
			coordination.DeleteForISVC(isvc.Namespace, isvc.Name)
			canary.DeleteForISVC(isvc.Namespace, isvc.Name)

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(isvc, finalizerName)
			if err := r.Update(context.Background(), isvc); err != nil {
				if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
					return ctrl.Result{}, nil
				}
				return ctrl.Result{}, err
			}
		}

		// Stop reconciliation as the item is being deleted
		return ctrl.Result{}, nil
	}

	// Handle VirtualDeployment without actual reconciliation
	if deploymentMode == constants.VirtualDeployment {
		return r.handleVirtualDeployment(isvc)
	}

	// Initialize status if not already initialized
	if isvc.Status.Components == nil {
		isvc.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}

	// Setup reconcilers.
	// apiVersion is omitted: controller-runtime strips TypeMeta from
	// objects fetched via the typed cache, so isvc.APIVersion is always
	// empty here. Use v1beta1.SchemeGroupVersion.String() if a callable
	// API version is needed downstream.
	log.Info("Reconciling inference service")
	isvcConfig, err := controllerconfig.NewInferenceServicesConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create InferenceServicesConfig")
	}
	isvcConfig.ModelCache = r.ModelCache.Config

	modelConfigReconciler := multimodelconfig.NewModelConfigReconciler(r.Client, r.Clientset, r.Scheme)
	result, err := modelConfigReconciler.Reconcile(ctx, isvc) // Added ctx
	if err != nil {
		return result, err
	}

	// Initialize ComponentBuilderFactory
	// Note: isvcConfig is created a few lines above inside the Reconcile function
	// for NewInferenceServicesConfig. We will use that existing isvcConfig.
	componentBuilderFactory := components.NewComponentBuilderFactory(r.Client, r.Clientset, r.APIReader, r.Expectations, r.Recorder, r.Scheme, isvcConfig).
		WithGangSchedulingAvailable(r.GangSchedulingAvailable)

	// Determine which components to reconcile based on the spec
	var reconcilers []components.Component

	var ingressDeploymentMode constants.DeploymentModeType

	// Step 1: Reconcile model first
	baseModel, baseModelMeta, baseModelStatus, err := isvcutils.ReconcileBaseModelWithStatus(r.Client, isvc)
	if err != nil {
		r.Log.Error(err, "Failed to reconcile base model", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "ModelReconcileError", err.Error())
		return reconcile.Result{}, err
	}
	// Lean path: no spec.model. Operator must specify spec.runtime
	// directly; we skip model fetch, sharded-readiness gating, overlay
	// resolution, and runtime-vs-model validation.
	if baseModel != nil && isvcutils.IsShardedBaseModel(baseModel) {
		ready, message := isvcutils.ShardedBaseModelReady(baseModelStatus, baseModelMeta.Generation)
		if !ready {
			if message == "" {
				message = "sharded BaseModel is not ready"
			}
			r.Log.Info("Waiting for sharded BaseModel to become ready",
				"model", isvc.Spec.Model.Name,
				"namespace", isvc.Namespace,
				"message", message)
			r.Recorder.Eventf(isvc, v1.EventTypeNormal, "ModelNotReady", message)
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// Resolve overlays. Failures here (NotFound, disabled, sharded
	// NotReady) become Skipped entries — only transient client errors
	// abort the reconcile.
	resolvedOverlays, err := isvcutils.ResolveOverlays(r.Client, isvc)
	if err != nil {
		r.Log.Error(err, "Failed to resolve overlays", "isvc", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "OverlayResolveError", err.Error())
		return reconcile.Result{}, err
	}
	isvc.Status.MountedOverlays = components.MountedOverlaySummary(resolvedOverlays)
	setOverlaysReadyCondition(isvc, resolvedOverlays)

	// Step 2: Get runtime spec.
	//   - runtime explicit  → fetch by name; validate against model only
	//     if model is present.
	//   - runtime omitted  → auto-select from model; requires a model.
	var rt *v1beta1.ServingRuntimeSpec
	var rtName string
	userSpecifiedRuntime := false

	switch {
	case isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "":
		rtName = isvc.Spec.Runtime.Name
		userSpecifiedRuntime = true

		// When the user opts into pinning (autoSync=false), fetch the
		// pinned ControllerRevision rather than the live runtime. The
		// pin helper handles first-reconcile create, drift detection,
		// and ome.io/runtime-sync ack — and persists status itself when
		// children must be skipped.
		if isvc.Spec.Runtime.AutoSync != nil && !*isvc.Spec.Runtime.AutoSync {
			pin, perr := r.resolvePinnedRuntime(ctx, isvc)
			if perr != nil {
				r.Log.Error(perr, "Pin resolution failed", "runtime", rtName)
				r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimePinError", perr.Error())
				return reconcile.Result{}, perr
			}
			if pin.skipChildren {
				return reconcile.Result{}, nil
			}
			rt = pin.spec
			break
		}

		if baseModel != nil {
			if err := r.RuntimeSelector.ValidateRuntime(ctx, rtName, baseModel, isvc); err != nil {
				// The operator named this runtime explicitly, so OME should not
				// block on the runtime's *declared* supportedModelFormats: a
				// generic runtime (e.g. sglang) can serve many architectures it
				// never enumerates. Downgrade a pure compatibility mismatch
				// (format / architecture / framework) to an advisory event and
				// proceed — the deliberate choice wins over the declaration.
				//
				// This mirrors the admission webhook (#460, "explicit-runtime
				// compat is advisory"), which was only applied there. Without the
				// same downgrade here, the webhook admits the ISVC but this
				// reconcile hard-fails, so it never gets pods.
				//
				// Everything else stays a hard error: a not-found / disabled
				// runtime or a malformed model genuinely cannot run, and a sharded
				// model with no configured cache provider physically cannot load
				// (the webhook's modelRequiresCacheProvider guard is the same
				// sharded check) — keep gating those.
				if runtimeselector.IsRuntimeCompatibilityError(err) && !isvcutils.IsShardedBaseModel(baseModel) {
					r.Log.Info("Runtime named explicitly; proceeding despite declared-format mismatch",
						"runtime", rtName, "model", isvc.Spec.Model.Name, "details", err.Error())
					r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeCompatibilityAdvisory",
						"Runtime %s does not declare support for model %s (%v); proceeding because the runtime was named explicitly",
						rtName, isvc.Spec.Model.Name, err)
				} else {
					r.Log.Error(err, "Runtime validation failed", "runtime", rtName, "model", isvc.Spec.Model.Name)
					r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeValidationError",
						"Runtime %s does not support model %s: %v", rtName, isvc.Spec.Model.Name, err)
					return reconcile.Result{}, err
				}
			}
		}
		rtSpec, _, err := r.RuntimeSelector.GetRuntime(ctx, rtName, isvc.Namespace)
		if err != nil {
			r.Log.Error(err, "Failed to get runtime spec", "runtime", rtName)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeFetchError", err.Error())
			return reconcile.Result{}, err
		}
		rt = rtSpec
	case baseModel != nil:
		selection, err := r.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
		if err != nil {
			r.Log.Error(err, "Failed to auto-select runtime", "model", isvc.Spec.Model.Name)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeSelectionError",
				"Failed to find runtime for model %s: %v", isvc.Spec.Model.Name, err)
			return reconcile.Result{}, err
		}
		rt = selection.Spec
		rtName = selection.Name
		log.Info("Auto-selected runtime", "runtime", rtName, "model", isvc.Spec.Model.Name)
	default:
		// No model, no runtime — webhook should have caught this; defensive
		// guard for direct client writes that bypass admission.
		err := fmt.Errorf("InferenceService must specify spec.runtime when spec.model is omitted")
		r.Log.Error(err, "Cannot reconcile", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeSelectionError", err.Error())
		return reconcile.Result{}, err
	}

	// Step 3: Merge rt and isvc specs to get final engine, decoder, and router specs
	mergedEngine, mergedDecoder, mergedRouter, err := isvcutils.MergeRuntimeSpecs(isvc, rt, r.Log)
	if err != nil {
		r.Log.Error(err, "Failed to merge specs", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "MergeSpecsError", err.Error())
		return reconcile.Result{}, err
	}

	// Canary: when spec.rollout.canary is set, stamp the current step's
	// RollingUpdate.Partition onto each merged Component before the Component
	// reconcilers run, so the standard spec→IR→plan→HeldByPartition path holds
	// the staged old/new split. The step machine + traffic run in Step 6a below.
	if isvc.Spec.GetCanaryGroup() != nil {
		if mergedEngine != nil {
			canary.StampStepPartition(isvc, v1beta1.EngineComponent, &mergedEngine.ComponentExtensionSpec)
		}
		if mergedDecoder != nil {
			canary.StampStepPartition(isvc, v1beta1.DecoderComponent, &mergedDecoder.ComponentExtensionSpec)
		}
		if mergedRouter != nil {
			canary.StampStepPartition(isvc, v1beta1.RouterComponent, &mergedRouter.ComponentExtensionSpec)
		}
	}

	// Step 4: Determine deployment modes based on merged specs
	engineDeploymentMode, decoderDeploymentMode, routerDeploymentMode, err := isvcutils.DetermineDeploymentModes(mergedEngine, mergedDecoder, mergedRouter, rt, isvc.Spec.DeploymentMode)
	if err != nil {
		r.Log.Error(err, "Failed to determine deployment modes", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "DeploymentModeError", err.Error())
		return reconcile.Result{}, err
	}

	// If both engine and decoder exist, it's PD-disaggregated. V(1):
	// steady-state per-reconcile breadcrumb; no operator action.
	if mergedEngine != nil && mergedDecoder != nil {
		log.V(1).Info("PD-disaggregated deployment detected")
	}

	// Step 5: Create reconcilers based on merged specs. The
	// "Creating <component> reconciler" / "Determined ingress
	// deployment mode" lines fire every reconcile but observe no
	// state change — demoted to V(1) because they're hot-path /
	// steady-state breadcrumbs.
	if mergedEngine != nil {
		engineSupportedModelFormats := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel, userSpecifiedRuntime)
		log.V(1).Info("Creating engine reconciler",
			"deploymentMode", engineDeploymentMode)

		engineReconciler := componentBuilderFactory.CreateEngineComponent(
			engineDeploymentMode,
			baseModel,
			baseModelMeta,
			mergedEngine,
			rt,
			rtName,
			engineSupportedModelFormats,
			resolvedOverlays,
		)
		reconcilers = append(reconcilers, engineReconciler)
	}

	if mergedDecoder != nil {
		decoderSupportedModelFormats := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel, userSpecifiedRuntime)
		log.V(1).Info("Creating decoder reconciler",
			"deploymentMode", decoderDeploymentMode)

		decoderReconciler := componentBuilderFactory.CreateDecoderComponent(
			decoderDeploymentMode,
			baseModel,
			baseModelMeta,
			mergedDecoder,
			rt,
			rtName,
			decoderSupportedModelFormats,
			resolvedOverlays,
		)
		reconcilers = append(reconcilers, decoderReconciler)
	}

	// Add Router reconciler if merged router spec exists (using new v2 Router)
	if mergedRouter != nil {
		log.V(1).Info("Creating router reconciler",
			"deploymentMode", routerDeploymentMode)

		routerReconciler := componentBuilderFactory.CreateRouterComponent(
			routerDeploymentMode, // Using the determined router deployment mode
			baseModel,
			baseModelMeta,
			mergedRouter, // Using the merged router spec instead of isvc.Spec.Router
			rt,
			rtName,
			resolvedOverlays,
		)
		reconcilers = append(reconcilers, routerReconciler)
	}

	// Determine the correct ingress deployment mode using the same logic as ingress reconciler
	// but with the already-determined deployment modes to avoid inconsistency
	if mergedRouter != nil {
		ingressDeploymentMode = routerDeploymentMode
	} else if mergedDecoder != nil {
		ingressDeploymentMode = decoderDeploymentMode
	} else {
		ingressDeploymentMode = engineDeploymentMode
	}

	log.V(1).Info("Determined ingress deployment mode",
		"ingressDeploymentMode", ingressDeploymentMode)

	// Step 6: Run all reconcilers. Capture the max-requeue across
	// per-Component reconcilers WITHOUT short-circuiting — coordination
	// must run at the end of every reconcile pass even when individual
	// Components requeue (which they do constantly during active
	// rollouts). Closes G1: previously the loop returned on the first
	// requeuing reconciler, leaving Status.RolloutCoordination /
	// per-revision Services / Traffic[] stale until convergence.
	var pendingRequeue reconcile.Result
	for _, reconciler := range reconcilers {
		result, err := reconciler.Reconcile(ctx, isvc)
		if err != nil {
			log.Error(err, "Failed to reconcile component",
				"component", fmt.Sprintf("%T", reconciler))
			return result, err
		}
		if result.Requeue {
			pendingRequeue.Requeue = true
		}
		if result.RequeueAfter > pendingRequeue.RequeueAfter {
			pendingRequeue.RequeueAfter = result.RequeueAfter
		}
	}

	// Step 6a: Run cross-Component coordination once every Component
	// reconciler has finished. The coordination layer reads observed
	// per-revision pod counts, ensures per-revision Services, writes
	// Status.Components.<c>.Traffic[], drives the per-group state
	// machine, and stamps RolloutCoordinationReady. Independent of
	// any single Component reconciler so the ingress / traffic
	// reconciliation downstream reads the freshest producer-side
	// status.
	// Rollout dispatch (v2 groups): a canary group drives the canary step machine;
	// blueGreen/rollingUpdate groups drive the coordination engine. v2 groups
	// partition the Components into disjoint sets, so both engines may run in one
	// pass — each no-ops when its group type is absent (canary.Dispatch on no
	// canary group; coordination.Reconcile on no coordination-style group). Both
	// observe per-revision pods, ensure per-revision Services, and write
	// Status.Components.<c>.Traffic + phase for their own Components. Cross-group
	// ordering (the group sequencer) is a follow-on; today the groups run
	// concurrently on their disjoint Components.
	if isvc.Spec.GetCanaryGroup() != nil {
		// Resolve the metrics source + query timeout per-reconcile from the
		// canaryAnalysis operator config (so ConfigMap edits take effect without a
		// restart); the sampler's structural tuning is fixed at startup. Only
		// analysis steps read these — non-analysis canaries ignore them.
		analysisConfig, err := controllerconfig.NewCanaryAnalysisConfigCached(r.ConfigCache, r.Clientset)
		if err != nil {
			log.Error(err, "Failed to load canaryAnalysis config")
			return reconcile.Result{}, errors.Wrapf(err, "fails to load canaryAnalysis config")
		}
		ra, err := canary.Dispatch(ctx, canary.DispatchDeps{
			ISVC:                     isvc,
			Client:                   r.Client,
			Reader:                   r.APIReader,
			Recorder:                 r.Recorder,
			Sampler:                  r.CanarySampler,
			BundledPrometheusAddress: analysisConfig.BundledPrometheusAddress,
			QueryTimeout:             analysisConfig.QueryTimeoutDuration(),
		})
		if err != nil {
			log.Error(err, "Failed to reconcile canary rollout")
			return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile canary")
		}
		if ra > pendingRequeue.RequeueAfter {
			pendingRequeue.RequeueAfter = ra
		}
	}
	// Load the coordination tuning per-reconcile from the operator config so
	// ConfigMap edits take effect without a restart (mirrors canaryAnalysis). An
	// absent "coordination" key yields a zero deadband (no hysteresis), preserving
	// the prior write-on-any-diff behavior.
	coordinationConfig, err := controllerconfig.NewCoordinationConfig(r.Clientset)
	if err != nil {
		log.Error(err, "Failed to load coordination config")
		return reconcile.Result{}, errors.Wrapf(err, "fails to load coordination config")
	}
	if _, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
		ISVC:                         isvc,
		Client:                       r.Client,
		Reader:                       r.APIReader,
		Recorder:                     r.Recorder,
		TrafficWeightDeadbandPercent: coordinationConfig.TrafficWeightDeadbandPercent,
	}); err != nil {
		log.Error(err, "Failed to reconcile cross-Component coordination")
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile coordination")
	}

	// If any Component asked to requeue, return that signal AFTER
	// coordination has run. Ingress / traffic / external-service
	// reconciliation downstream is skipped this pass — they re-run on
	// the requeue. This preserves the prior short-circuit semantics
	// for downstream reconcilers while ensuring coordination's
	// producer-side status writes (Traffic, RolloutCoordination) land
	// every pass. updateStatus must be called BEFORE the early return
	// — coordination.Reconcile above just mutated isvc.Status in-memory
	// (Traffic[], RolloutCoordination, RolloutCoordinationReady); without
	// flushing it here those writes are dropped on every requeuing pass,
	// which is the steady state during active rollouts.
	if pendingRequeue.Requeue || pendingRequeue.RequeueAfter > 0 {
		if err := r.updateStatus(isvc, deploymentMode); err != nil {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
			return reconcile.Result{}, err
		}
		return pendingRequeue, nil
	}

	// Now reconcile ingress and external service after components have created their services
	ingressConfig, err := controllerconfig.NewIngressConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create IngressConfig")
	}

	// Resolve ingress config with annotation overrides
	resolvedIngressConfig := isvcutils.ResolveIngressConfig(ingressConfig, isvc.Annotations)

	// New architecture: ingress uses the determined ingress deployment mode
	ingressReconciler := ingress.NewIngressReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig, isvcConfig)
	log.V(1).Info("Reconciling ingress for inference service")
	if err := ingressReconciler.(*ingress.IngressReconciler).ReconcileWithDeploymentMode(ctx, isvc, ingressDeploymentMode); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile ingress")
	}

	// Reconcile external service - creates a service with the inference service name
	// when ingress is disabled to provide a stable endpoint
	externalServiceReconciler := external_service.NewExternalServiceReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig)
	log.V(1).Info("Reconciling external service for inference service")
	if err := externalServiceReconciler.Reconcile(ctx, isvc); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile external service")
	}

	// Traffic management reconciler. Skipped when the InferenceService
	// declares no traffic intent (spec.traffic or ome.io/* annotation).
	// The reconciler invokes the active translator (chosen at startup
	// by the factory), applies the emitted backend policy resource,
	// and returns the TrafficStatus to write back. A nil reconciler
	// means the controller was set up without traffic management
	// (legitimate for tests / minimal configurations).
	if r.TrafficReconciler != nil {
		targetRoutes := traffic.ComputeTargetHTTPRoutes(isvc, mergedDecoder != nil, mergedRouter != nil)
		trafficStatus, err := r.TrafficReconciler.Reconcile(ctx, isvc, targetRoutes)
		if err != nil {
			r.Log.Error(err, "Failed to reconcile traffic policy",
				"namespace", isvc.Namespace, "inferenceService", isvc.Name,
				"translator", r.TrafficReconciler.TranslatorName())
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "TrafficReconcileError", err.Error())
			// Surface the status even on error so operators see the
			// TranslationFailed reason without waiting for the next
			// successful reconcile.
			isvc.Status.Traffic = trafficStatus
			return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile traffic policy")
		}
		isvc.Status.Traffic = trafficStatus
	}

	// Set Status.Address for external service and add ingress disable annotation when ingress is disabled
	if resolvedIngressConfig.DisableIngressCreation {
		// Add annotation to InferenceService so cleanup logic knows to keep the external service
		if err := r.ensureIngressDisableAnnotation(isvc); err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to add ingress disable annotation")
		}
		if err := r.setExternalServiceURL(ctx, isvc, resolvedIngressConfig); err != nil {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
			return reconcile.Result{}, errors.Wrapf(err, "fails to set external service URL")
		}
	}

	// Clean up resources for components that no longer exist. Cleanup
	// errors don't fail reconciliation (we still want the rest of the
	// status flush to land), but we log Error so a chronic permission
	// failure on cleanup isn't fully invisible to the operator.
	if err := r.cleanupRemovedComponents(ctx, isvc, mergedEngine, mergedDecoder, mergedRouter); err != nil {
		log.Error(err, "Failed to cleanup removed components")
	}

	// Clean up status for components that no longer exist. Demoted to
	// V(1): this block runs every reconcile but in steady state every
	// declared Component still has a merged spec, so the "Cleaning up"
	// breadcrumb fires on the hot path without ever doing real work.
	// The per-Component delete lines stay at Info because they only
	// fire on a real spec change (Component removed from the ISVC).
	if isvc.Status.Components != nil {
		log.V(1).Info("Cleaning up component status",
			"mergedEngine", mergedEngine != nil,
			"mergedDecoder", mergedDecoder != nil,
			"mergedRouter", mergedRouter != nil,
			"statusComponents", len(isvc.Status.Components))

		if mergedEngine == nil {
			if _, present := isvc.Status.Components[v1beta1.EngineComponent]; present {
				delete(isvc.Status.Components, v1beta1.EngineComponent)
				log.Info("Deleted engine from status")
			}
		}
		if mergedDecoder == nil {
			if _, present := isvc.Status.Components[v1beta1.DecoderComponent]; present {
				delete(isvc.Status.Components, v1beta1.DecoderComponent)
				log.Info("Deleted decoder from status")
			}
		}
		if mergedRouter == nil {
			if _, present := isvc.Status.Components[v1beta1.RouterComponent]; present {
				delete(isvc.Status.Components, v1beta1.RouterComponent)
				log.Info("Deleted router from status")
			}
		}
	}

	// For every Component whose resolved deployment mode is OMENative
	// (i.e. flows through the IR-managed path), read the live IR.Status
	// and copy it onto ISVC.Status.Components[<component>].Lifecycle.
	// The IR controller is the sole writer of IR.Status. Components on
	// non-OMENative paths are not touched here; their status was already
	// written by the RawDeployment reconciler during the per-Component
	// dispatcher pass above.
	//
	// Errors propagate so a transient apiserver read failure triggers
	// a retry; first-reconcile NotFound is swallowed inside the
	// helper (the next reconcile picks up the IR's fresh status once
	// the cache observes it).
	componentModes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent:  engineDeploymentMode,
		v1beta1.DecoderComponent: decoderDeploymentMode,
		v1beta1.RouterComponent:  routerDeploymentMode,
	}
	if err = irprojector.AggregateIRStatus(ctx, r.Client, isvc, componentModes); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	if err = r.updateStatus(isvc, deploymentMode); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *InferenceServiceReconciler) handleVirtualDeployment(isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	// We directly set URL and inference service status to Ready in VirtualDeployment mode

	// Set URL across all Status components
	host := network.GetServiceHostname(isvc.Name, isvc.Namespace)
	openAIURL := knapis.HTTP(host)
	addressURL := &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   host,
			Scheme: "http",
		},
	}
	isvc.Status.URL = openAIURL
	isvc.Status.Address = addressURL
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			URL: openAIURL,
		},
	}

	isvc.Status.SetConditions(knapis.Conditions{{
		Type:               knapis.ConditionReady,
		Status:             v1.ConditionTrue,
		LastTransitionTime: knapis.VolatileTime{Inner: metav1.Now()},
		Reason:             "VirtualDeployment",
		Message:            "InferenceService is in VirtualDeployment mode",
	}})

	if err := r.updateStatus(isvc, constants.VirtualDeployment); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *InferenceServiceReconciler) updateStatus(desiredService *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) error {
	namespacedName := types.NamespacedName{Name: desiredService.Name, Namespace: desiredService.Namespace}

	// Mirror the existing-status snapshot OUTSIDE the retry loop —
	// "wasReady" is computed against what the caller observed at the
	// top of reconcile, which is the right baseline for the Ready /
	// NotReady transition event emission below. The retry loop only
	// needs to win the optimistic-concurrency race for the actual
	// status write.
	existingService := &v1beta1.InferenceService{}
	if err := r.Get(context.TODO(), namespacedName, existingService); err != nil {
		if apierrors.IsNotFound(err) {
			// ISVC was deleted between top-of-reconcile and the status
			// flush. The deletion path has already handled cleanup; an
			// in-flight reconcile racing the delete has nothing left to
			// write. Returning nil drops the work item cleanly instead
			// of triggering controller-runtime retries that saturate the
			// work queue and stall unrelated tests.
			return nil
		}
		return err
	}
	wasReady := inferenceServiceReadiness(existingService.Status)
	if inferenceServiceStatusEqual(existingService.Status, desiredService.Status) {
		// If we didn't change anything then don't call updateStatus.
		// This is important because the copy we loaded from the informer's
		// cache may be stale, and we don't want to overwrite a prior update
		// to status with this stale state.
		return nil
	}

	// Status().Update wrapped in retry.RetryOnConflict because
	// OMENative-driven status churn (drain / Mark / op transitions)
	// reliably 409s the outer flush and would wedge the reconcile in a
	// re-loop. On conflict: re-Get the live ISVC, copy desiredService
	// status onto the fresh ResourceVersion, then re-merge
	// ComponentStatus.Lifecycle from latest so any OMENative writes that
	// landed between our snapshot and the re-Get (Incarnation bumps,
	// Phase transitions, RunningRevision promotes) are preserved. The
	// outer reconciler doesn't own that subtree.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1beta1.InferenceService{}
		if err := r.Get(context.TODO(), namespacedName, latest); err != nil {
			return err
		}
		preserved := preserveLifecycleStatus(latest.Status.Components)
		latest.Status = desiredService.Status
		mergeLifecycleStatus(&latest.Status, preserved)
		return r.Status().Update(context.TODO(), latest)
	}); err != nil {
		// Same race as the top-of-function Get: if the ISVC vanished
		// during the retry loop, there's nothing to update — drop the
		// work item silently. Only log + retry on other errors.
		if apierrors.IsNotFound(err) {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultNotFound)
			return nil
		}
		// Exhausted RetryOnConflict still surfaces a Conflict error —
		// bucket it separately so the OMENative 409 churn hotspot stays
		// visible apart from other write failures.
		if apierrors.IsConflict(err) {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultConflict)
		} else {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultError)
		}
		r.Log.Error(err, "Failed to update InferenceService status", "InferenceService", desiredService.Name)
		r.Recorder.Eventf(desiredService, v1.EventTypeWarning, "UpdateFailed",
			"Failed to update status for InferenceService %q: %v", desiredService.Name, err)
		return errors.Wrapf(err, "fails to update InferenceService status")
	}
	obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultSuccess)

	// Ready / NotReady transition events fire only after a successful
	// status write that actually flipped readiness.
	isReady := inferenceServiceReadiness(desiredService.Status)
	if wasReady && !isReady {
		r.Recorder.Eventf(desiredService, v1.EventTypeWarning, string(InferenceServiceNotReadyState),
			fmt.Sprintf("InferenceService [%v] is no longer Ready", desiredService.GetName()))
	} else if !wasReady && isReady {
		r.Recorder.Eventf(desiredService, v1.EventTypeNormal, string(InferenceServiceReadyState),
			fmt.Sprintf("InferenceService [%v] is Ready", desiredService.GetName()))
	}
	return nil
}

func inferenceServiceReadiness(status v1beta1.InferenceServiceStatus) bool {
	return status.Conditions != nil &&
		status.GetCondition(knapis.ConditionReady) != nil &&
		status.GetCondition(knapis.ConditionReady).Status == v1.ConditionTrue
}

func inferenceServiceStatusEqual(s1, s2 v1beta1.InferenceServiceStatus) bool {
	return equality.Semantic.DeepEqual(s1, s2)
}

// preserveLifecycleStatus snapshots the OMENative ComponentStatus
// subtree per Component, so the caller can re-merge it after overwriting
// latest.Status with the reconcile-pass desired status. The OMENative
// reconciler owns Status.Components[*].Lifecycle; the outer reconciler
// only touches URL / RestURL / Address / RolloutPhase / Conditions.
// Without re-merging, OMENative writes that landed between our snapshot
// and the conflict re-Get get clobbered.
func preserveLifecycleStatus(comps map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec) map[v1beta1.ComponentType]*v1beta1.LifecycleStatus {
	if len(comps) == 0 {
		return nil
	}
	out := make(map[v1beta1.ComponentType]*v1beta1.LifecycleStatus, len(comps))
	for c, cs := range comps {
		if cs.Lifecycle != nil {
			out[c] = cs.Lifecycle.DeepCopy()
		}
	}
	return out
}

func mergeLifecycleStatus(s *v1beta1.InferenceServiceStatus, preserved map[v1beta1.ComponentType]*v1beta1.LifecycleStatus) {
	if len(preserved) == 0 {
		return
	}
	if s.Components == nil {
		s.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	for c, om := range preserved {
		cs := s.Components[c]
		cs.Lifecycle = om
		s.Components[c] = cs
	}
}

// ensureIngressDisableAnnotation adds the ome.io/ingress-disable-creation annotation to the InferenceService
// This annotation is used by the cleanup logic to determine if external service should be kept
func (r *InferenceServiceReconciler) ensureIngressDisableAnnotation(isvc *v1beta1.InferenceService) error {
	const ingressDisableAnnotation = "ome.io/ingress-disable-creation"

	// Check if annotation already exists
	if val, ok := isvc.Annotations[ingressDisableAnnotation]; ok && val == "true" {
		return nil
	}

	// Add annotation
	if isvc.Annotations == nil {
		isvc.Annotations = make(map[string]string)
	}
	isvc.Annotations[ingressDisableAnnotation] = "true"

	return nil
}

func (r *InferenceServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.ClientConfig = mgr.GetConfig()
	r.APIReader = mgr.GetAPIReader()
	if r.Expectations == nil {
		r.Expectations = omenative.NewExpectations()
	}
	// Short-TTL cache for the inferenceservice-config ConfigMap so a reconcile
	// pass shares one apiserver GET across the per-pass config loads. TTL is
	// flag-driven; a non-positive TTL disables caching (reads the apiserver every
	// call), preserving the pre-cache behavior.
	if r.ConfigCache == nil {
		r.ConfigCache = controllerconfig.NewConfigCache(r.ConfigCacheTTL)
	}

	// Initialize RuntimeSelector
	runtimeSelectorConfig := runtimeselector.NewConfig(mgr.GetClient())
	runtimeSelectorConfig.ModelCacheProvider = r.ModelCache.Name
	r.RuntimeSelector = runtimeselector.NewWithConfig(runtimeSelectorConfig)

	// KEDA is a hard dependency, not an optional CRD probe.
	// Owns(&kedav1.ScaledObject{}) is unconditional below; if KEDA is
	// absent from the cluster the controller will fail fast on the first
	// reconcile rather than silently ignoring scaler-class=keda.

	podMonitorFound, err := utils.IsCrdAvailable(r.ClientConfig, monitoringv1.SchemeGroupVersion.String(), constants.PodMonitorKind)
	if err != nil {
		return err
	}

	// scheduler-plugins PodGroup detection. Optional CRD; absence is a
	// degradation surface, not a hard fail. OMENative's reconciler reads
	// the cached flag via ReconcileParams.GangSchedulingAvailable and
	// stamps a `GangSchedulingUnavailable=True` Component Condition when
	// a multi-pod Component is requested without the CRD.
	podGroupFound, err := utils.IsCrdAvailable(r.ClientConfig, schedulingv1alpha1.SchemeGroupVersion.String(), constants.PodGroupKind)
	if err != nil {
		return err
	}
	r.GangSchedulingAvailable = podGroupFound

	// Build the canary analysis sampler once. Its concurrency cap and cache TTL are
	// fixed for the controller's lifetime (the per-sample source + query timeout are
	// resolved per reconcile from the same config). A missing/unreadable config
	// falls back to the documented defaults so the sampler is always available — a
	// genuinely broken ConfigMap surfaces per-reconcile when a canary needs it.
	analysisConfig, err := controllerconfig.NewCanaryAnalysisConfig(r.Clientset)
	if err != nil {
		r.Log.Info("canaryAnalysis config unavailable at startup; using sampler defaults", "error", err.Error())
		analysisConfig = &controllerconfig.CanaryAnalysisConfig{MaxConcurrency: controllerconfig.DefaultAnalysisMaxConcurrency}
	}
	// The wake-up channel buffer is sized to the query concurrency: at most that
	// many queries finish near-simultaneously, and a full buffer harmlessly drops a
	// wake-up (the reconcile requeue is the backstop), so it never blocks a query.
	analysisEvents := make(chan event.GenericEvent, analysisConfig.MaxConcurrency)
	r.CanarySampler = canary.NewPrometheusSampler(analysisEvents, int(analysisConfig.MaxConcurrency), analysisConfig.CacheTTLDuration())

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceService{}).
		Owns(&appsv1.Deployment{}).
		Owns(&v1.Service{}).
		Owns(&v1.ConfigMap{}).
		Owns(&v1.PersistentVolume{}).
		Owns(&v1.PersistentVolumeClaim{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		// OMENative revision history is owned by the ISVC
		// so revision-driven status writes enqueue the parent.
		Owns(&appsv1.ControllerRevision{}).
		// InferenceReplica is owned by the ISVC controller —
		// the projector at the dispatch site (irprojector.EnsureInferenceReplica)
		// stamps the ISVC as the IR's controller OwnerReference so
		// foreground GC cascades on ISVC delete, and Owns() re-enqueues
		// the parent ISVC when the IR controller flips IR.Status
		// (Replicas / ReadyReplicas / phase progression). This is what
		// lets the ISVC status aggregator pick up IR-side counter
		// updates without polling.
		Owns(&v1beta1.InferenceReplica{}).
		// EndpointSlice events for OMENative headless Services enqueue
		// the parent ISVC so drain checks (IsPodDrained / IsPodInRotation)
		// react to kube-proxy convergence without waiting on the periodic
		// requeue. The mapper rejects slices that don't target an
		// OMENative-managed Service.
		Watches(
			&discoveryv1.EndpointSlice{},
			handler.EnqueueRequestsFromMapFunc(omenative.EndpointSliceToISVC),
		).
		// KEDA ScaledObject is owned unconditionally — the scheme is
		// registered at startup and KEDA is a hard cluster-side
		// dependency for OME.
		Owns(&kedav1.ScaledObject{})

	if podMonitorFound {
		ctrlBuilder = ctrlBuilder.Owns(&monitoringv1.PodMonitor{})
	} else {
		r.Log.Info("The InferenceService controller won't watch monitoring.coreos.com/v1/PodMonitor resources because the CRD is not available.")
	}

	if podGroupFound {
		// OMENative-emitted PodGroups carry the ISVC as controller-owner;
		// owner-ref-based enqueue re-reconciles the ISVC when a PodGroup
		// is modified or deleted out from under us.
		ctrlBuilder = ctrlBuilder.Owns(&schedulingv1alpha1.PodGroup{})
	} else {
		r.Log.Info("The InferenceService controller won't watch scheduling.x-k8s.io/v1alpha1/PodGroup resources because the CRD is not available; OMENative multi-pod Instances will degrade to non-gang scheduling.")
	}

	// Register an Owns() watch on the backend policy resource type
	// the active translator emits, so changes to the policy re-enqueue
	// the owning InferenceService. The noop translator returns nil, in
	// which case there is nothing to watch.
	if r.TrafficReconciler != nil {
		if watch := r.TrafficReconciler.Watches(); watch != nil {
			ctrlBuilder = ctrlBuilder.Owns(watch)
		} else {
			r.Log.Info("The InferenceService controller won't watch a backend policy resource (active translator emits nothing)",
				"translator", r.TrafficReconciler.TranslatorName())
		}
	}

	// Add watches for ServingRuntime and ClusterServingRuntime. Beyond
	// populating the cache, runtime events fan out to any ISVC that has
	// opted into pinning (spec.runtime.autoSync=false) — without this,
	// drift detection would only fire on the next unrelated ISVC
	// reconcile, leaving operators unaware their edit was rejected.
	ctrlBuilder = ctrlBuilder.
		Watches(&v1beta1.ServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(r.pinnedISVCsReferencingRuntime)).
		Watches(&v1beta1.ClusterServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(r.pinnedISVCsReferencingRuntime))

	// Re-reconcile an ISVC when its canary analysis sample lands, so the executor
	// consumes the result immediately instead of waiting on the periodic requeue.
	// The sampler emits a GenericEvent carrying the ISVC's namespace/name.
	ctrlBuilder = ctrlBuilder.WatchesRawSource(
		source.Channel(analysisEvents, &handler.EnqueueRequestForObject{}))

	return ctrlBuilder.Complete(r)
}

// pinnedISVCsReferencingRuntime returns reconcile requests for ISVCs
// that pin to the runtime named by obj (spec.runtime.name match +
// autoSync=false). Empty when nothing pins to it — autoSync=true
// consumers don't need to be re-reconciled on runtime change because
// they always read the live spec.
func (r *InferenceServiceReconciler) pinnedISVCsReferencingRuntime(ctx context.Context, obj client.Object) []reconcile.Request {
	runtimeName := obj.GetName()
	runtimeNamespace := obj.GetNamespace() // empty for cluster-scoped

	var isvcs v1beta1.InferenceServiceList
	if err := r.List(ctx, &isvcs); err != nil {
		r.Log.Error(err, "fan-out runtime event: list InferenceServices failed")
		return nil
	}

	var reqs []reconcile.Request
	for i := range isvcs.Items {
		isvc := &isvcs.Items[i]
		if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name != runtimeName {
			continue
		}
		// autoSync=true (default) ignores runtime mid-stream changes.
		if isvc.Spec.Runtime.AutoSync == nil || *isvc.Spec.Runtime.AutoSync {
			continue
		}
		// Namespaced SR only matches same-namespace ISVCs.
		if runtimeNamespace != "" && isvc.Namespace != runtimeNamespace {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: isvc.Namespace, Name: isvc.Name,
		}})
	}
	return reqs
}

func (r *InferenceServiceReconciler) setExternalServiceURL(ctx context.Context, isvc *v1beta1.InferenceService, ingressConfig *controllerconfig.IngressConfig) error {
	// Get the external service
	externalService := &v1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, externalService); err != nil {
		return err
	}

	// Get the port from the external service
	var port int32 = constants.CommonISVCPort // default port
	if len(externalService.Spec.Ports) > 0 {
		port = externalService.Spec.Ports[0].Port
	}

	// Set the URL and Address of the external service with port
	host := network.GetServiceHostname(externalService.Name, externalService.Namespace)
	hostWithPort := fmt.Sprintf("%s:%d", host, port)

	isvc.Status.URL = &knapis.URL{
		Host:   hostWithPort,
		Scheme: "http",
	}
	isvc.Status.Address = &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   hostWithPort,
			Scheme: "http",
		},
	}

	return nil
}

// setOverlaysReadyCondition flips OverlaysReady=True when every
// declared overlay was attached; False with reasons in the Message
// when one or more were skipped. Informational — the primary still
// drives the deployment regardless.
func setOverlaysReadyCondition(isvc *v1beta1.InferenceService, overlays []isvcutils.ResolvedOverlay) {
	if isvc.Spec.Model == nil || len(isvc.Spec.Model.Overlays) == 0 {
		return
	}
	cond := knapis.Condition{
		Type:               v1beta1.OverlaysReady,
		LastTransitionTime: knapis.VolatileTime{Inner: metav1.Now()},
	}
	skipped := components.SkippedOverlayReasons(overlays)
	if len(skipped) == 0 {
		cond.Status = v1.ConditionTrue
		cond.Reason = "AllOverlaysMounted"
	} else {
		cond.Status = v1.ConditionFalse
		cond.Reason = "OverlaysSkipped"
		cond.Message = strings.Join(skipped, "; ")
	}
	isvc.Status.SetConditions(knapis.Conditions{cond})
}
