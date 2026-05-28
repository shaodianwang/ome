package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type rolloutChild struct {
	member rolloutMember
	spec   PodGroupDeploymentSpec
}

func (c *Controller) reconcileRolloutChildren(ctx context.Context, pgdr *PodGroupDeploymentRollout) ([]rolloutMember, error) {
	if !rolloutSpecManagesChildren(pgdr.Spec) {
		return rolloutMembersForMode(pgdr.Name, pgdr.Spec.Mode)
	}

	children, err := desiredRolloutChildren(pgdr)
	if err != nil {
		return nil, err
	}
	members := make([]rolloutMember, 0, len(children))
	for _, child := range children {
		if err := c.upsertRolloutChild(ctx, pgdr, child); err != nil {
			return members, err
		}
		members = append(members, child.member)
	}
	return members, nil
}

func rolloutSpecManagesChildren(spec PodGroupDeploymentRolloutSpec) bool {
	return spec.Prefill != nil || spec.Decode != nil || spec.Unified != nil
}

func desiredRolloutChildren(pgdr *PodGroupDeploymentRollout) ([]rolloutChild, error) {
	switch pgdr.Spec.Mode {
	case "disagg":
		if pgdr.Spec.Prefill == nil || pgdr.Spec.Decode == nil {
			return nil, fmt.Errorf("spec.prefill and spec.decode are required when spec.mode is disagg")
		}
		prefillSpec, err := mergedRolloutChildSpec(pgdr.Spec.Base, pgdr.Spec.Prefill)
		if err != nil {
			return nil, fmt.Errorf("spec.prefill: %w", err)
		}
		prefill, err := defaultAndValidateRolloutChildSpec(prefillSpec)
		if err != nil {
			return nil, fmt.Errorf("spec.prefill: %w", err)
		}
		decodeSpec, err := mergedRolloutChildSpec(pgdr.Spec.Base, pgdr.Spec.Decode)
		if err != nil {
			return nil, fmt.Errorf("spec.decode: %w", err)
		}
		decode, err := defaultAndValidateRolloutChildSpec(decodeSpec)
		if err != nil {
			return nil, fmt.Errorf("spec.decode: %w", err)
		}
		return []rolloutChild{
			{member: rolloutMember{Role: "prefill", Name: pgdr.Name + "-prefill"}, spec: prefill},
			{member: rolloutMember{Role: "decode", Name: pgdr.Name + "-decode"}, spec: decode},
		}, nil
	case "unified":
		if pgdr.Spec.Unified == nil {
			return nil, fmt.Errorf("spec.unified is required when spec.mode is unified")
		}
		unifiedSpec, err := mergedRolloutChildSpec(pgdr.Spec.Base, pgdr.Spec.Unified)
		if err != nil {
			return nil, fmt.Errorf("spec.unified: %w", err)
		}
		unified, err := defaultAndValidateRolloutChildSpec(unifiedSpec)
		if err != nil {
			return nil, fmt.Errorf("spec.unified: %w", err)
		}
		return []rolloutChild{
			{member: rolloutMember{Role: "unified", Name: pgdr.Name + "-unified"}, spec: unified},
		}, nil
	default:
		return nil, fmt.Errorf("spec.mode must be disagg or unified")
	}
}

func copyPodGroupDeploymentSpec(spec PodGroupDeploymentSpec) PodGroupDeploymentSpec {
	out := spec
	out.Template = *spec.Template.DeepCopy()
	return out
}

func defaultAndValidateRolloutChildSpec(spec PodGroupDeploymentSpec) (PodGroupDeploymentSpec, error) {
	hasRoleWindow := spec.UpdateStrategy.RollingUpdate.MaxSurge != 0 ||
		spec.UpdateStrategy.RollingUpdate.MaxUnavailable != 0 ||
		spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent != 0
	out, err := defaultAndValidateSpec(spec)
	if err != nil {
		return out, err
	}
	if !hasRoleWindow {
		out.UpdateStrategy.RollingUpdate.MaxUnavailablePercent = 0
	}
	return out, nil
}

func mergedRolloutChildSpec(base, override *PodGroupDeploymentSpec) (PodGroupDeploymentSpec, error) {
	if override == nil {
		return PodGroupDeploymentSpec{}, fmt.Errorf("role spec is required")
	}
	if base == nil {
		return copyPodGroupDeploymentSpec(*override), nil
	}
	merged := copyPodGroupDeploymentSpec(*base)
	merged.Groups = 0
	merged.ReplicasPerGroup = 0
	mergePodGroupDeploymentSpec(&merged, copyPodGroupDeploymentSpec(*override))
	return merged, nil
}

func mergePodGroupDeploymentSpec(base *PodGroupDeploymentSpec, override PodGroupDeploymentSpec) {
	if override.Groups != 0 {
		base.Groups = override.Groups
	}
	if override.ReplicasPerGroup != 0 {
		base.ReplicasPerGroup = override.ReplicasPerGroup
	}
	mergeUpdateStrategy(&base.UpdateStrategy, override.UpdateStrategy)
	mergeMultiNodeSpec(&base.MultiNode, override.MultiNode)
	if override.TopologyKey != "" {
		base.TopologyKey = override.TopologyKey
	}
	if override.ServiceTargetOrdinal != 0 {
		base.ServiceTargetOrdinal = override.ServiceTargetOrdinal
	}
	if override.ScheduleTimeoutSeconds != 0 {
		base.ScheduleTimeoutSeconds = override.ScheduleTimeoutSeconds
	}
	if override.SchedulerName != "" {
		base.SchedulerName = override.SchedulerName
	}
	mergePodTemplateSpec(&base.Template, override.Template)
}

func mergeUpdateStrategy(base *UpdateStrategy, override UpdateStrategy) {
	if override.Type != "" {
		base.Type = override.Type
	}
	if override.RollingUpdate.MaxSurge != 0 {
		base.RollingUpdate.MaxSurge = override.RollingUpdate.MaxSurge
	}
	if override.RollingUpdate.MaxUnavailable != 0 {
		base.RollingUpdate.MaxUnavailable = override.RollingUpdate.MaxUnavailable
	}
	if override.RollingUpdate.MaxUnavailablePercent != 0 {
		base.RollingUpdate.MaxUnavailablePercent = override.RollingUpdate.MaxUnavailablePercent
	}
	if override.RollingUpdate.ProgressDeadlineSeconds != 0 {
		base.RollingUpdate.ProgressDeadlineSeconds = override.RollingUpdate.ProgressDeadlineSeconds
	}
}

func mergeMultiNodeSpec(base *MultiNodeSpec, override MultiNodeSpec) {
	if override.Enabled {
		base.Enabled = true
	}
	if override.ContainerName != "" {
		base.ContainerName = override.ContainerName
	}
	if override.TensorParallelSize != 0 {
		base.TensorParallelSize = override.TensorParallelSize
	}
	if override.DistInitPort != 0 {
		base.DistInitPort = override.DistInitPort
	}
}

func mergePodTemplateSpec(base *corev1.PodTemplateSpec, override corev1.PodTemplateSpec) {
	base.Labels = mergeStringMaps(base.Labels, override.Labels)
	base.Annotations = mergeStringMaps(base.Annotations, override.Annotations)
	mergePodSpec(&base.Spec, override.Spec)
}

func mergePodSpec(base *corev1.PodSpec, override corev1.PodSpec) {
	if override.ServiceAccountName != "" {
		base.ServiceAccountName = override.ServiceAccountName
	}
	if override.PriorityClassName != "" {
		base.PriorityClassName = override.PriorityClassName
	}
	if override.RestartPolicy != "" {
		base.RestartPolicy = override.RestartPolicy
	}
	if override.TerminationGracePeriodSeconds != nil {
		base.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}
	if len(override.ImagePullSecrets) > 0 {
		base.ImagePullSecrets = override.ImagePullSecrets
	}
	base.NodeSelector = mergeStringMaps(base.NodeSelector, override.NodeSelector)
	if len(override.Tolerations) > 0 {
		base.Tolerations = override.Tolerations
	}
	if override.Affinity != nil {
		base.Affinity = override.Affinity
	}
	if len(override.Containers) > 0 {
		base.Containers = mergeContainers(base.Containers, override.Containers)
	}
	if len(override.Volumes) > 0 {
		base.Volumes = mergeVolumes(base.Volumes, override.Volumes)
	}
}

func mergeContainers(base, overrides []corev1.Container) []corev1.Container {
	out := append([]corev1.Container(nil), base...)
	for _, override := range overrides {
		index := containerIndexByName(out, override.Name)
		if index < 0 {
			out = append(out, override)
			continue
		}
		mergeContainer(&out[index], override)
	}
	return out
}

func mergeContainer(base *corev1.Container, override corev1.Container) {
	if override.Image != "" {
		base.Image = override.Image
	}
	if override.ImagePullPolicy != "" {
		base.ImagePullPolicy = override.ImagePullPolicy
	}
	if len(override.Command) > 0 {
		base.Command = mergeCommandOverride(base.Command, override.Command)
	}
	if len(override.Args) > 0 {
		base.Args = mergeCommandOverride(base.Args, override.Args)
	}
	if len(override.Env) > 0 {
		base.Env = mergeEnvVars(base.Env, override.Env)
	}
	if len(override.Ports) > 0 {
		base.Ports = mergeContainerPorts(base.Ports, override.Ports)
	}
	if !reflect.DeepEqual(override.Resources, corev1.ResourceRequirements{}) {
		base.Resources = override.Resources
	}
	if override.SecurityContext != nil {
		base.SecurityContext = override.SecurityContext
	}
	if override.StartupProbe != nil {
		base.StartupProbe = override.StartupProbe
	}
	if override.ReadinessProbe != nil {
		base.ReadinessProbe = override.ReadinessProbe
	}
	if override.LivenessProbe != nil {
		base.LivenessProbe = override.LivenessProbe
	}
	if len(override.VolumeMounts) > 0 {
		base.VolumeMounts = mergeVolumeMounts(base.VolumeMounts, override.VolumeMounts)
	}
}

func mergeCommandOverride(base, override []string) []string {
	if len(base) > 0 && isFlagOnlyOverride(override) {
		return mergeFlagArgs(base, override)
	}
	return append([]string(nil), override...)
}

func isFlagOnlyOverride(args []string) bool {
	return len(args) > 0 && strings.HasPrefix(args[0], "--")
}

func mergeFlagArgs(base, override []string) []string {
	out := append([]string(nil), base...)
	for i := 0; i < len(override); i++ {
		flag := override[i]
		if !strings.HasPrefix(flag, "--") {
			out = append(out, flag)
			continue
		}
		if eq := strings.Index(flag, "="); eq > 0 {
			name := flag[:eq]
			value := flag[eq+1:]
			if !replaceFlagValue(&out, name, value) {
				out = append(out, name, value)
			}
			continue
		}
		if i+1 < len(override) && !strings.HasPrefix(override[i+1], "--") {
			value := override[i+1]
			if !replaceFlagValue(&out, flag, value) {
				out = append(out, flag, value)
			}
			i++
			continue
		}
		if !hasFlag(out, flag) {
			out = append(out, flag)
		}
	}
	return out
}

func hasFlag(args []string, flag string) bool {
	if _, ok := flagValue(args, flag); ok {
		return true
	}
	for _, arg := range args {
		if arg == flag {
			return true
		}
	}
	return false
}

func containerIndexByName(containers []corev1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}

func mergeEnvVars(base, overrides []corev1.EnvVar) []corev1.EnvVar {
	out := append([]corev1.EnvVar(nil), base...)
	for _, override := range overrides {
		index := -1
		for i := range out {
			if out[i].Name == override.Name {
				index = i
				break
			}
		}
		if index >= 0 {
			out[index] = override
		} else {
			out = append(out, override)
		}
	}
	return out
}

func mergeContainerPorts(base, overrides []corev1.ContainerPort) []corev1.ContainerPort {
	out := append([]corev1.ContainerPort(nil), base...)
	for _, override := range overrides {
		index := -1
		for i := range out {
			if out[i].Name == override.Name && override.Name != "" {
				index = i
				break
			}
			if out[i].ContainerPort == override.ContainerPort && out[i].Protocol == override.Protocol {
				index = i
				break
			}
		}
		if index >= 0 {
			out[index] = override
		} else {
			out = append(out, override)
		}
	}
	return out
}

func mergeVolumeMounts(base, overrides []corev1.VolumeMount) []corev1.VolumeMount {
	out := append([]corev1.VolumeMount(nil), base...)
	for _, override := range overrides {
		index := -1
		for i := range out {
			if out[i].Name == override.Name {
				index = i
				break
			}
		}
		if index >= 0 {
			out[index] = override
		} else {
			out = append(out, override)
		}
	}
	return out
}

func mergeVolumes(base, overrides []corev1.Volume) []corev1.Volume {
	out := append([]corev1.Volume(nil), base...)
	for _, override := range overrides {
		index := -1
		for i := range out {
			if out[i].Name == override.Name {
				index = i
				break
			}
		}
		if index >= 0 {
			out[index] = override
		} else {
			out = append(out, override)
		}
	}
	return out
}

func (c *Controller) upsertRolloutChild(ctx context.Context, pgdr *PodGroupDeploymentRollout, child rolloutChild) error {
	desired := &PodGroupDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: operatorAPIVersion,
			Kind:       operatorKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      child.member.Name,
			Namespace: pgdr.Namespace,
			Labels: map[string]string{
				labelManagedBy:   managerName,
				labelName:        "pod-group-deployment",
				labelRollout:     pgdr.Name,
				labelRolloutRole: child.member.Role,
			},
			OwnerReferences: rolloutOwnerRef(pgdr),
		},
		Spec: child.spec,
	}
	client := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace)
	obj, err := podGroupDeploymentToUnstructured(desired)
	if err != nil {
		return err
	}

	current, err := client.Get(ctx, child.member.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		logPGDR(pgdr, "creating child_podgroupdeployment=%s role=%s groups=%d replicas_per_group=%d", child.member.Name, child.member.Role, child.spec.Groups, child.spec.ReplicasPerGroup)
		_, err = client.Create(ctx, obj, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}

	currentPGD, err := decodePodGroupDeployment(current)
	if err != nil {
		return err
	}
	desired.Labels = mergeStringMaps(currentPGD.Labels, desired.Labels)
	desired.Annotations = currentPGD.Annotations
	desired.ResourceVersion = currentPGD.ResourceVersion
	desired.UID = currentPGD.UID
	desired.CreationTimestamp = currentPGD.CreationTimestamp

	if rolloutChildMatches(currentPGD, desired) {
		return nil
	}
	obj, err = podGroupDeploymentToUnstructured(desired)
	if err != nil {
		return err
	}
	logPGDR(pgdr, "updating child_podgroupdeployment=%s role=%s groups=%d replicas_per_group=%d", child.member.Name, child.member.Role, child.spec.Groups, child.spec.ReplicasPerGroup)
	_, err = client.Update(ctx, obj, metav1.UpdateOptions{})
	return err
}

func rolloutChildMatches(current, desired *PodGroupDeployment) bool {
	return reflect.DeepEqual(current.Spec, desired.Spec) &&
		stringMapsEqual(current.Labels, desired.Labels) &&
		reflect.DeepEqual(current.OwnerReferences, desired.OwnerReferences)
}

func podGroupDeploymentToUnstructured(pgd *PodGroupDeployment) (*unstructured.Unstructured, error) {
	raw, err := json.Marshal(pgd)
	if err != nil {
		return nil, err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}
