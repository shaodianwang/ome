package hpa

import (
	"context"
	"strconv"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
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
)

var log = logf.Log.WithName("HPAReconciler")

// HPAReconciler reconciles the HorizontalPodAutoscaler resource
type HPAReconciler struct {
	client       client.Client
	scheme       *runtime.Scheme
	HPA          *autoscalingv2.HorizontalPodAutoscaler
	componentExt *v1beta1.ComponentExtensionSpec
}

// NewHPAReconciler builds an HPAReconciler whose generated HPA targets the
// caller-supplied Deployment. Used by the Raw Deployment dispatch path. The
// HPA bounds (MinReplicas / MaxReplicas) and metric block come from the
// ComponentExtensionSpec:
//
//   - Bounds: ComponentExtensionSpec.MinReplicas / MaxReplicas (the
//     canonical replica bound location; OEP-0013 keeps these here).
//   - Metric: Autoscaler.HPA.Metrics when set, else CPU=80% default.
//     ome.io/targetUtilizationPercentage annotation overrides the
//     default CPU target.
//
// See NewHPAReconcilerForTarget for the IR-managed dispatch path.
func NewHPAReconciler(client client.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec) *HPAReconciler {

	return &HPAReconciler{
		client:       client,
		scheme:       scheme,
		HPA:          createHPAFromComponentExt(componentMeta, componentExt),
		componentExt: componentExt,
	}
}

// NewHPAReconcilerForTarget builds an HPAReconciler whose generated HPA
// targets the caller-supplied scaleTargetRef (typically an InferenceReplica
// for the IR-managed path). hpaSpec carries the resolved HPAAutoscaler
// block (resolver output); nil / empty Metrics expands to the OEP-0013
// default of a single CPU=80% Resource metric per README 731 + 1148.
//
// It is the entry point for OEP-0013's per-IR dispatch and stays free of
// any ComponentExtensionSpec concerns so the two callers are completely
// decoupled.
func NewHPAReconcilerForTarget(
	client client.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	scaleTargetRef autoscalingv2.CrossVersionObjectReference,
	hpaSpec *v1beta1.HPAAutoscaler,
	minReplicas int32,
	maxReplicas int32,
) *HPAReconciler {
	return &HPAReconciler{
		client: client,
		scheme: scheme,
		HPA:    createHPA(componentMeta, scaleTargetRef, hpaSpec, minReplicas, maxReplicas),
		// componentExt is intentionally nil — the IR-managed path doesn't
		// consult it. Keep the field on the struct for shape compatibility
		// with the Raw Deployment constructor; the Reconcile() path doesn't
		// read it.
		componentExt: nil,
	}
}

// createHPA builds the desired HorizontalPodAutoscaler from explicit inputs.
//
// scaleTargetRef is forwarded verbatim to the HPA spec — callers are expected
// to supply `{apps/v1, Deployment, <name>}` for Raw Deployment dispatch and
// `{ome.io/v1beta1, InferenceReplica, <ir-name>}` for the IR-managed path.
//
// hpaSpec / minReplicas / maxReplicas come from the resolver or the Raw
// Deployment shim (createHPAFromComponentExt). When hpaSpec is nil or
// hpaSpec.Metrics is empty the generator emits a single CPU=80% Resource
// metric per the OEP-0013 default contract (README 731, 1148).
func createHPA(
	componentMeta metav1.ObjectMeta,
	scaleTargetRef autoscalingv2.CrossVersionObjectReference,
	hpaSpec *v1beta1.HPAAutoscaler,
	minReplicas int32,
	maxReplicas int32,
) *autoscalingv2.HorizontalPodAutoscaler {
	metrics := defaultHPAMetrics()
	var behavior *autoscalingv2.HorizontalPodAutoscalerBehavior
	if hpaSpec != nil {
		if len(hpaSpec.Metrics) > 0 {
			metrics = hpaSpec.Metrics
		}
		if hpaSpec.Behavior != nil {
			behavior = hpaSpec.Behavior
		}
	}
	if behavior == nil {
		behavior = &autoscalingv2.HorizontalPodAutoscalerBehavior{}
	}

	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: componentMeta,
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: scaleTargetRef,
			MinReplicas:    &minReplicas,
			MaxReplicas:    maxReplicas,
			Metrics:        metrics,
			Behavior:       behavior,
		},
	}
}

// createHPAFromComponentExt is the bridge from the Raw Deployment
// (componentMeta, componentExt) caller to the parameterized createHPA.
// Reads Autoscaler.HPA when present (the canonical authoring location)
// and falls back to the CPU=80% default (overridden by the
// ome.io/targetUtilizationPercentage annotation if set) for ISVCs that
// don't declare a per-Component autoscaler block at all.
func createHPAFromComponentExt(
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
) *autoscalingv2.HorizontalPodAutoscaler {
	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       componentMeta.Name,
	}
	minReplicas := calculateMinReplicas(componentExt)
	maxReplicas := calculateMaxReplicas(componentExt, minReplicas)

	var hpaSpec *v1beta1.HPAAutoscaler
	if componentExt.Autoscaler != nil && componentExt.Autoscaler.HPA != nil {
		hpaSpec = componentExt.Autoscaler.HPA
	} else if utilization, ok := utilizationFromAnnotation(componentMeta); ok {
		// Preserve the ome.io/targetUtilizationPercentage annotation
		// override for ISVCs that don't declare an Autoscaler.HPA block.
		// The annotation is the only knob left for the CPU target now
		// that the legacy ScaleTarget field is gone.
		hpaSpec = &v1beta1.HPAAutoscaler{
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               "Utilization",
							AverageUtilization: &utilization,
						},
					},
				},
			},
		}
	}

	return createHPA(componentMeta, scaleTargetRef, hpaSpec, minReplicas, maxReplicas)
}

// defaultHPAMetrics returns the default metric list: a single
// Resource{cpu, 80%} entry. Used when the resolver / shim hands back an
// HPAAutoscaler with no explicit Metrics.
func defaultHPAMetrics() []autoscalingv2.MetricSpec {
	utilization := constants.DefaultCPUUtilization
	return []autoscalingv2.MetricSpec{
		{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               "Utilization",
					AverageUtilization: &utilization,
				},
			},
		},
	}
}

func calculateMinReplicas(componentExt *v1beta1.ComponentExtensionSpec) int32 {
	if componentExt.MinReplicas == nil || *componentExt.MinReplicas < constants.DefaultMinReplicas {
		return int32(constants.DefaultMinReplicas)
	}
	return int32(*componentExt.MinReplicas)
}

func calculateMaxReplicas(componentExt *v1beta1.ComponentExtensionSpec, minReplicas int32) int32 {
	maxReplicas := int32(componentExt.MaxReplicas)
	if maxReplicas < minReplicas {
		maxReplicas = minReplicas
	}
	return maxReplicas
}

// utilizationFromAnnotation reads the legacy
// ome.io/targetUtilizationPercentage annotation as an int32; returns
// (utilization, true) when present and parseable, else (0, false).
func utilizationFromAnnotation(metadata metav1.ObjectMeta) (int32, bool) {
	value, ok := metadata.Annotations[constants.TargetUtilizationPercentage]
	if !ok {
		return 0, false
	}
	utilization, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	return int32(utilization), true // #nosec G109
}

func (r *HPAReconciler) checkHPAExist() (constants.CheckResultType, *autoscalingv2.HorizontalPodAutoscaler, error) {
	existingHPA := &autoscalingv2.HorizontalPodAutoscaler{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: r.HPA.Namespace,
		Name:      r.HPA.Name,
	}, existingHPA)

	if err != nil {
		if apierr.IsNotFound(err) {
			if shouldCreateHPA(r.HPA) {
				return constants.CheckResultCreate, nil, nil
			}
			return constants.CheckResultSkipped, nil, nil
		}
		return constants.CheckResultUnknown, nil, err
	}

	if semanticHPAEquals(r.HPA, existingHPA) {
		return constants.CheckResultExisted, existingHPA, nil
	}
	if shouldDeleteHPA(r.HPA) {
		return constants.CheckResultDelete, existingHPA, nil
	}
	return constants.CheckResultUpdate, existingHPA, nil
}

func semanticHPAEquals(desired, existing *autoscalingv2.HorizontalPodAutoscaler) bool {
	desiredAutoscalerClass := desired.Annotations[constants.AutoscalerClass]
	existingAutoscalerClass := existing.Annotations[constants.AutoscalerClass]

	autoscalerClassChanged := desiredAutoscalerClass != existingAutoscalerClass
	return equality.Semantic.DeepEqual(desired.Spec, existing.Spec) && !autoscalerClassChanged
}

func shouldDeleteHPA(desired *autoscalingv2.HorizontalPodAutoscaler) bool {
	desiredAutoscalerClass := desired.Annotations[constants.AutoscalerClass]
	return constants.AutoscalerClassType(desiredAutoscalerClass) == constants.AutoscalerClassExternal
}

func shouldCreateHPA(desired *autoscalingv2.HorizontalPodAutoscaler) bool {
	desiredAutoscalerClass := desired.Annotations[constants.AutoscalerClass]
	return desiredAutoscalerClass == "" || constants.AutoscalerClassType(desiredAutoscalerClass) == constants.AutoscalerClassHPA
}

func (r *HPAReconciler) Reconcile() (runtime.Object, error) {
	checkResult, existingHPA, err := r.checkHPAExist()
	log.Info("Reconciling HPA", "namespace", r.HPA.Namespace, "name", r.HPA.Name, "checkResult", checkResult.String())
	if err != nil {
		return nil, err
	}

	var opErr error
	switch checkResult {
	case constants.CheckResultCreate:
		opErr = r.client.Create(context.TODO(), r.HPA)
	case constants.CheckResultUpdate:
		opErr = r.client.Update(context.TODO(), r.HPA)
	case constants.CheckResultDelete:
		opErr = r.client.Delete(context.TODO(), r.HPA)
	default:
		return existingHPA, nil
	}

	if opErr != nil {
		log.Error(opErr, "Failed to reconcile HPA", "namespace", r.HPA.Namespace, "name", r.HPA.Name)
		return nil, opErr
	}

	return r.HPA, nil
}

func (r *HPAReconciler) SetControllerReferences(owner metav1.Object, scheme *runtime.Scheme) error {
	return controllerutil.SetControllerReference(owner, r.HPA, scheme)
}
