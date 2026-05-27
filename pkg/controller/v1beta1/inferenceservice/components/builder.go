package components

import (
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// ComponentBuilder helps build components with common configuration
type ComponentBuilder struct {
	client                  client.Client
	clientset               kubernetes.Interface
	apiReader               client.Reader
	expectations            *omenative.Expectations
	recorder                record.EventRecorder
	scheme                  *runtime.Scheme
	inferenceServiceConfig  *controllerconfig.InferenceServicesConfig
	deploymentMode          constants.DeploymentModeType
	baseModel               *v1beta1.BaseModelSpec
	baseModelMeta           *metav1.ObjectMeta
	runtime                 *v1beta1.ServingRuntimeSpec
	runtimeName             string
	supportedModelFormat    *v1beta1.SupportedModelFormat
	overlays                []isvcutils.ResolvedOverlay
	logger                  logr.Logger
	gangSchedulingAvailable bool
}

// NewComponentBuilder creates a new component builder. apiReader is the
// live (uncached) reader (typically mgr.GetAPIReader()); the OMENative
// backend uses it for reads where cache lag would be a correctness
// problem. expectations is the per-controller Expectations cache;
// recorder is the event recorder for ISVC-scoped lifecycle events;
// all three are threaded through to ReconcileParams. Any may be nil —
// backends fall back to the cached client / DefaultExpectations /
// no-op event emission.
func NewComponentBuilder(
	client client.Client,
	clientset kubernetes.Interface,
	apiReader client.Reader,
	expectations *omenative.Expectations,
	recorder record.EventRecorder,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
) *ComponentBuilder {
	return &ComponentBuilder{
		client:                 client,
		clientset:              clientset,
		apiReader:              apiReader,
		expectations:           expectations,
		recorder:               recorder,
		scheme:                 scheme,
		inferenceServiceConfig: inferenceServiceConfig,
		logger:                 ctrl.Log.WithName("ComponentBuilder"),
	}
}

// WithDeploymentMode sets the deployment mode
func (b *ComponentBuilder) WithDeploymentMode(mode constants.DeploymentModeType) *ComponentBuilder {
	b.deploymentMode = mode
	return b
}

// WithBaseModel sets the base model
func (b *ComponentBuilder) WithBaseModel(spec *v1beta1.BaseModelSpec, meta *metav1.ObjectMeta) *ComponentBuilder {
	b.baseModel = spec
	b.baseModelMeta = meta
	return b
}

// WithRuntime sets the runtime
func (b *ComponentBuilder) WithRuntime(spec *v1beta1.ServingRuntimeSpec, name string) *ComponentBuilder {
	b.runtime = spec
	b.runtimeName = name
	return b
}

// WithLogger sets a custom logger
func (b *ComponentBuilder) WithLogger(logger logr.Logger) *ComponentBuilder {
	b.logger = logger
	return b
}

// WithSupportedModelFormat sets the supported model format
func (b *ComponentBuilder) WithSupportedModelFormat(format *v1beta1.SupportedModelFormat) *ComponentBuilder {
	b.supportedModelFormat = format
	return b
}

// WithOverlays threads the resolver output for spec.model.overlays
// through to the built component's BaseComponentFields.
func (b *ComponentBuilder) WithOverlays(overlays []isvcutils.ResolvedOverlay) *ComponentBuilder {
	b.overlays = overlays
	return b
}

// WithGangSchedulingAvailable threads the cluster-discovery boolean
// (true when scheduler-plugins PodGroup CRD is installed) onto the
// built component's BaseComponentFields. OMENative's reconciler reads
// it via ReconcileParams.GangSchedulingAvailable to decide whether to
// emit PodGroups for multi-pod Instances; other deployment modes
// ignore the field.
func (b *ComponentBuilder) WithGangSchedulingAvailable(available bool) *ComponentBuilder {
	b.gangSchedulingAvailable = available
	return b
}

// buildBaseFields creates the common base fields
func (b *ComponentBuilder) buildBaseFields() BaseComponentFields {
	return BaseComponentFields{
		Client:                 b.client,
		Clientset:              b.clientset,
		APIReader:              b.apiReader,
		Expectations:           b.expectations,
		Recorder:               b.recorder,
		Scheme:                 b.scheme,
		InferenceServiceConfig: b.inferenceServiceConfig,
		DeploymentMode:         b.deploymentMode,
		BaseModel:              b.baseModel,
		BaseModelMeta:          b.baseModelMeta,
		Runtime:                b.runtime,
		RuntimeName:            b.runtimeName,
		StatusManager:          status.NewStatusReconciler(),
		Log:                    b.logger,
		Overlays:               b.overlays,
	}
}

// BuildEngine creates an Engine component
func (b *ComponentBuilder) BuildEngine(spec *v1beta1.EngineSpec) Component {
	comp := NewEngine(
		b.client,
		b.clientset,
		b.apiReader,
		b.expectations,
		b.recorder,
		b.scheme,
		b.inferenceServiceConfig,
		b.deploymentMode,
		b.baseModel,
		b.baseModelMeta,
		spec,
		b.runtime,
		b.runtimeName,
		b.supportedModelFormat,
		b.overlays,
	)
	// Stamp the gang-scheduling availability flag after construction.
	// NewEngine keeps its positional-arg signature (test stability);
	// the field is exported on BaseComponentFields so the builder
	// fills it in post-construction with no signature churn.
	if e, ok := comp.(*Engine); ok {
		e.BaseComponentFields.GangSchedulingAvailable = b.gangSchedulingAvailable
	}
	return comp
}

// BuildDecoder creates a Decoder component
func (b *ComponentBuilder) BuildDecoder(spec *v1beta1.DecoderSpec) Component {
	comp := NewDecoder(
		b.client,
		b.clientset,
		b.apiReader,
		b.expectations,
		b.recorder,
		b.scheme,
		b.inferenceServiceConfig,
		b.deploymentMode,
		b.baseModel,
		b.baseModelMeta,
		spec,
		b.runtime,
		b.runtimeName,
		b.supportedModelFormat,
		b.overlays,
	)
	if d, ok := comp.(*Decoder); ok {
		d.BaseComponentFields.GangSchedulingAvailable = b.gangSchedulingAvailable
	}
	return comp
}

// BuildRouter creates a Router component
func (b *ComponentBuilder) BuildRouter(spec *v1beta1.RouterSpec) Component {
	comp := NewRouter(
		b.client,
		b.clientset,
		b.apiReader,
		b.expectations,
		b.recorder,
		b.scheme,
		b.inferenceServiceConfig,
		b.deploymentMode,
		b.baseModel,
		b.baseModelMeta,
		spec,
		b.runtime,
		b.runtimeName,
		b.overlays,
	)
	// Router is always single-pod (no Leader/Worker on RouterSpec)
	// so the gang flag is informational only — set for consistency.
	if r, ok := comp.(*Router); ok {
		r.BaseComponentFields.GangSchedulingAvailable = b.gangSchedulingAvailable
	}
	return comp
}

// BuildCustomComponent creates a custom component with a strategy
func (b *ComponentBuilder) BuildCustomComponent(componentType v1beta1.ComponentType, strategy ComponentConfig) Component {
	baseFields := b.buildBaseFields()
	baseFields.Log = b.logger.WithName(string(componentType) + "Reconciler")

	// This would return a generic component that uses the strategy
	// For now, returning nil as we haven't implemented the generic component yet
	return nil
}

// ComponentBuilderFactory provides a factory for creating builders
type ComponentBuilderFactory struct {
	client                  client.Client
	clientset               kubernetes.Interface
	apiReader               client.Reader
	expectations            *omenative.Expectations
	recorder                record.EventRecorder
	scheme                  *runtime.Scheme
	inferenceServiceConfig  *controllerconfig.InferenceServicesConfig
	gangSchedulingAvailable bool
}

// NewComponentBuilderFactory creates a new factory. apiReader is the
// live API reader (typically mgr.GetAPIReader()); expectations is
// the per-controller Expectations cache shared with the
// SetupWithManager-registered Pod watch; recorder emits ISVC-scoped
// lifecycle events. All plumbed through to the OMENative backend;
// any may be nil.
func NewComponentBuilderFactory(
	client client.Client,
	clientset kubernetes.Interface,
	apiReader client.Reader,
	expectations *omenative.Expectations,
	recorder record.EventRecorder,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
) *ComponentBuilderFactory {
	return &ComponentBuilderFactory{
		client:                 client,
		clientset:              clientset,
		apiReader:              apiReader,
		expectations:           expectations,
		recorder:               recorder,
		scheme:                 scheme,
		inferenceServiceConfig: inferenceServiceConfig,
	}
}

// WithGangSchedulingAvailable updates the factory's persistent gang-
// scheduling-availability boolean. Every builder spawned afterwards
// receives the new value. Wired from InferenceServiceReconciler at
// reconcile time so the value reflects the cluster-discovery cache.
func (f *ComponentBuilderFactory) WithGangSchedulingAvailable(available bool) *ComponentBuilderFactory {
	f.gangSchedulingAvailable = available
	return f
}

// NewBuilder creates a new component builder
func (f *ComponentBuilderFactory) NewBuilder() *ComponentBuilder {
	return NewComponentBuilder(f.client, f.clientset, f.apiReader, f.expectations, f.recorder, f.scheme, f.inferenceServiceConfig).
		WithGangSchedulingAvailable(f.gangSchedulingAvailable)
}

// CreateEngineComponent is a convenience method to create an engine component
func (f *ComponentBuilderFactory) CreateEngineComponent(
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	engineSpec *v1beta1.EngineSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	supportedModelFormat *v1beta1.SupportedModelFormat,
	overlays []isvcutils.ResolvedOverlay,
) Component {
	return f.NewBuilder().
		WithDeploymentMode(deploymentMode).
		WithBaseModel(baseModel, baseModelMeta).
		WithRuntime(runtime, runtimeName).
		WithSupportedModelFormat(supportedModelFormat).
		WithOverlays(overlays).
		BuildEngine(engineSpec)
}

// CreateDecoderComponent is a convenience method to create a decoder component
func (f *ComponentBuilderFactory) CreateDecoderComponent(
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	decoderSpec *v1beta1.DecoderSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	supportedModelFormat *v1beta1.SupportedModelFormat,
	overlays []isvcutils.ResolvedOverlay,
) Component {
	return f.NewBuilder().
		WithDeploymentMode(deploymentMode).
		WithBaseModel(baseModel, baseModelMeta).
		WithRuntime(runtime, runtimeName).
		WithSupportedModelFormat(supportedModelFormat).
		WithOverlays(overlays).
		BuildDecoder(decoderSpec)
}

// CreateRouterComponent is a convenience method to create a router component
func (f *ComponentBuilderFactory) CreateRouterComponent(
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	routerSpec *v1beta1.RouterSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	overlays []isvcutils.ResolvedOverlay,
) Component {
	return f.NewBuilder().
		WithDeploymentMode(deploymentMode).
		WithBaseModel(baseModel, baseModelMeta).
		WithRuntime(runtime, runtimeName).
		WithOverlays(overlays).
		BuildRouter(routerSpec)
}
