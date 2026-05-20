package omenative

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// Label keys stamped on every OMENative-managed pod. Two existing keys
// from pkg/constants are reused so legacy queries
// (e.g., cleanup logic keyed on `component=engine`) keep matching.
// The omenative-specific keys are private to this package until a second
// consumer needs them.
const (
	labelManagedBy     = "ome.io/managed-by"
	labelInstanceIdx   = "ome.io/instance-index"
	labelRunner        = "ome.io/runner"
	managedByOMENative = "OMENative"
)

// servingConditionType is the controller-owned readiness gate that
// OMENative flips during drain, update, and migration.
const servingConditionType corev1.PodConditionType = "ome.io/serving"

// Render builds the corev1.Pod for one ordinal of one Runner of one
// Instance, derived from a shared rendered PodSpec template.
//
// Single-pod scope: the caller passes plan with one Runner per Instance
// of size 1. Multi-pod Runner layout (leader + N workers per Instance)
// is added in a follow-up.
//
// The returned Pod carries:
//   - stable name <isvc>-<comp>-<inst-idx>-<runner>-<ordinal>
//   - hostname + subdomain so peer DNS resolves under the Component's
//     headless service
//   - five OMENative labels (inferenceservice / component /
//     instance-index / runner / managed-by)
//   - controller-owned `ome.io/serving` readiness gate appended after
//     any user-declared gates
//   - OME_* environment variables injected into every container
//     (OMENative values override user-supplied ones with the same Name)
//   - one OwnerReference back to the parent InferenceService
func Render(
	isvc *v1beta1.InferenceService,
	podSpec *corev1.PodSpec,
	plan ComponentPlan,
	inst InstancePlan,
	runner RunnerPlan,
	ordinal int32,
) (*corev1.Pod, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	if podSpec == nil {
		return nil, fmt.Errorf("nil PodSpec")
	}

	name := podName(isvc.Name, plan.Component, inst.Index, runner.Name, ordinal)
	subdomain := headlessServiceName(isvc.Name, plan.Component)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: isvc.Namespace,
			Labels:    podLabels(isvc.Name, plan.Component, inst.Index, runner.Name),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: *podSpec.DeepCopy(),
	}

	pod.Spec.Hostname = name
	pod.Spec.Subdomain = subdomain
	pod.Spec.ReadinessGates = append(pod.Spec.ReadinessGates, corev1.PodReadinessGate{
		ConditionType: servingConditionType,
	})

	injectOMENativeEnv(pod, plan, inst, runner, ordinal, isvc.Name)

	return pod, nil
}

// isvcGroupVersionKind identifies the InferenceService owner reference
// used when stamping child pods.
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

func podName(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string, ordinal int32) string {
	return fmt.Sprintf("%s-%s-%d-%s-%d", isvc, component, instanceIdx, runner, ordinal)
}

func headlessServiceName(isvc string, component v1beta1.ComponentType) string {
	return fmt.Sprintf("%s-%s-headless", isvc, component)
}

func instanceSubdomain(isvc string, component v1beta1.ComponentType, instanceIdx int32) string {
	return fmt.Sprintf("%s-%s-%d", isvc, component, instanceIdx)
}

func podLabels(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		labelInstanceIdx:                      fmt.Sprintf("%d", instanceIdx),
		labelRunner:                           runner,
		labelManagedBy:                        managedByOMENative,
	}
}

// injectOMENativeEnv overlays OMENative's OME_* env vars onto every
// container. If a container declared a variable with the same Name,
// OMENative's value replaces it (the controller-owned values are the
// authoritative source for these names).
func injectOMENativeEnv(pod *corev1.Pod, plan ComponentPlan, inst InstancePlan, runner RunnerPlan, ordinal int32, isvcName string) {
	env := buildOMENativeEnv(plan, inst, runner, ordinal, isvcName)
	owned := make(map[string]bool, len(env))
	for _, e := range env {
		owned[e.Name] = true
	}
	for i := range pod.Spec.Containers {
		kept := pod.Spec.Containers[i].Env[:0]
		for _, e := range pod.Spec.Containers[i].Env {
			if !owned[e.Name] {
				kept = append(kept, e)
			}
		}
		pod.Spec.Containers[i].Env = append(kept, env...)
	}
}

func buildOMENativeEnv(plan ComponentPlan, inst InstancePlan, runner RunnerPlan, ordinal int32, isvcName string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "OME_INFERENCESERVICE_NAME", Value: isvcName},
		{Name: "OME_COMPONENT", Value: string(plan.Component)},
		{Name: "OME_COMPONENT_REPLICAS", Value: fmt.Sprintf("%d", plan.Replicas)},
		{Name: "OME_INSTANCE_INDEX", Value: fmt.Sprintf("%d", inst.Index)},
		{Name: "OME_RUNNER", Value: runner.Name},
		{Name: "OME_RUNNER_SIZE", Value: fmt.Sprintf("%d", runner.Size)},
		{Name: "OME_RUNNER_INDEX", Value: fmt.Sprintf("%d", ordinal)},
		{Name: "OME_INSTANCE_SUBDOMAIN", Value: instanceSubdomain(isvcName, plan.Component, inst.Index)},
	}
	// OME_LEADER_ADDRESS is emitted only for multi-pod Instances (leader
	// + worker). Single-pod Instances have one "default" runner with no
	// leader, so the variable is omitted.
}
