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
// caller-supplied Deployment. Preserves the legacy ComponentExtensionSpec
// behaviour: ScaleMetric / ScaleTarget drive the synthesized metric and
// MinReplicas / MaxReplicas drive the bounds. OEP-0013 Slice 3 introduces
// the sibling NewHPAReconcilerForTarget constructor that targets an
// InferenceReplica (or any caller-supplied scaleTargetRef) using the
// resolver's typed HPAAutoscaler block.
func NewHPAReconciler(client client.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec) *HPAReconciler {

	return &HPAReconciler{
		client:       client,
		scheme:       scheme,
		HPA:          createHPAFromLegacyComponentExt(componentMeta, componentExt),
		componentExt: componentExt,
	}
}

// NewHPAReconcilerForTarget builds an HPAReconciler whose generated HPA
// targets the caller-supplied scaleTargetRef (typically an InferenceReplica
// for the OEP-0013 IR-managed path). hpaSpec carries the resolved
// HPAAutoscaler block (resolver output from Slice 1); nil / empty Metrics
// expands to the OEP-0013 default of a single CPU=80% Resource metric per
// README 731 + 1148.
//
// Unlike NewHPAReconciler, this constructor does NOT consult the legacy
// ScaleTarget / ScaleMetric / componentExt fields. It is the entry point
// for OEP-0013's per-IR dispatch (Slice 3) and stays free of legacy
// concerns so Slice 6's cleanup can drop the legacy constructor wholesale
// without touching this path.
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
		// with the legacy constructor; the Reconcile() path doesn't read it.
		componentExt: nil,
	}
}

// createHPA builds the desired HorizontalPodAutoscaler from explicit inputs.
//
// scaleTargetRef is forwarded verbatim to the HPA spec — callers are expected
// to supply `{apps/v1, Deployment, <name>}` for Raw Deployment dispatch and
// `{ome.io/v1beta1, InferenceReplica, <ir-name>}` for the IR-managed path
// introduced by OEP-0013 Slice 3.
//
// hpaSpec / minReplicas / maxReplicas come from the resolver (Slice 1) or the
// legacy shim (createHPAFromLegacyComponentExt) so that this function has no
// knowledge of the legacy ScaleTarget / ScaleMetric fields. When hpaSpec is
// nil or hpaSpec.Metrics is empty the generator emits a single CPU=80%
// Resource metric per the OEP-0013 default contract (README 731, 1148).
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

// createHPAFromLegacyComponentExt is the byte-identical bridge from the old
// (componentMeta, componentExt) → HPA generator to the parameterized createHPA.
// Raw Deployment callers reach the HPA generator through this shim so that
// the existing OEP-0013 callers (which still read the legacy ScaleTarget /
// ScaleMetric / MinReplicas / MaxReplicas fields) keep producing byte-identical
// HPAs. Slice 3 wires the new (resolver-driven) path; Slice 6 deletes the
// legacy ScaleTarget / ScaleMetric fields and this shim along with them.
func createHPAFromLegacyComponentExt(
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

	// Synthesize an HPAAutoscaler from the legacy fields so createHPA only
	// ever sees the new shape. Note: getTargetUtilization also reads the
	// `ome.io/targetUtilizationPercentage` annotation, preserving today's
	// behaviour.
	utilization := getTargetUtilization(componentMeta, componentExt)
	hpaSpec := &v1beta1.HPAAutoscaler{
		Metrics: []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: getResourceName(componentExt),
					Target: autoscalingv2.MetricTarget{
						Type:               "Utilization",
						AverageUtilization: &utilization,
					},
				},
			},
		},
	}

	return createHPA(componentMeta, scaleTargetRef, hpaSpec, minReplicas, maxReplicas)
}

// defaultHPAMetrics returns the OEP-0013 default metric list: a single
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

// getTargetUtilization derives the legacy HPA target utilisation from the
// componentExt + annotations. Deprecated: only the legacy shim
// (createHPAFromLegacyComponentExt) calls this. OEP-0013 Slice 6 deletes the
// underlying ScaleTarget field and this helper with it.
func getTargetUtilization(metadata metav1.ObjectMeta, componentExt *v1beta1.ComponentExtensionSpec) int32 {
	if value, ok := metadata.Annotations[constants.TargetUtilizationPercentage]; ok {
		utilization, _ := strconv.Atoi(value)
		return int32(utilization) // #nosec G109
	}
	if componentExt.ScaleTarget != nil {
		return int32(*componentExt.ScaleTarget)
	}
	return constants.DefaultCPUUtilization
}

// getResourceName derives the legacy HPA resource name from the componentExt.
// Deprecated: only the legacy shim (createHPAFromLegacyComponentExt) calls
// this. OEP-0013 Slice 6 deletes the underlying ScaleMetric field and this
// helper with it.
func getResourceName(componentExt *v1beta1.ComponentExtensionSpec) corev1.ResourceName {
	if componentExt.ScaleMetric != nil {
		return corev1.ResourceName(*componentExt.ScaleMetric)
	}
	return corev1.ResourceCPU
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
