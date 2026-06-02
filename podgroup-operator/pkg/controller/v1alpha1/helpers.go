package v1alpha1

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ownerRef(pgd *PodGroupDeployment) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion: operatorAPIVersion,
		Kind:       operatorKind,
		Name:       pgd.Name,
		UID:        pgd.UID,
		Controller: ptr(true),
	}}
}

func rolloutOwnerRef(pgdr *PodGroupDeploymentRollout) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion: operatorAPIVersion,
		Kind:       operatorRolloutKind,
		Name:       pgdr.Name,
		UID:        pgdr.UID,
		Controller: ptr(true),
	}}
}

func baseLabels(pgd *PodGroupDeployment, group int32, groupInstance, revision string) map[string]string {
	labels := map[string]string{
		labelManagedBy:  managerName,
		labelName:       "pod-group-deployment",
		labelInstance:   pgd.Name,
		labelGroupIndex: strconv.Itoa(int(group)),
	}
	if rollout := pgd.Labels[labelRollout]; rollout != "" {
		labels[labelRollout] = rollout
	}
	if role := pgd.Labels[labelRolloutRole]; role != "" {
		labels[labelRolloutRole] = role
	}
	if groupInstance != "" {
		labels[labelGroupInstance] = groupInstance
	}
	if revision != "" {
		labels[labelRevision] = revision
	}
	return labels
}

func podLabels(pgd *PodGroupDeployment, group, ordinal int32, groupInstance, revision string) map[string]string {
	labels := baseLabels(pgd, group, groupInstance, revision)
	labels[labelOrdinal] = strconv.Itoa(int(ordinal))
	labels[labelSchedulerPG] = groupInstance
	if ordinal == pgd.Spec.ServiceTargetOrdinal {
		labels[labelServiceTarget] = "true"
	} else {
		labels[labelServiceTarget] = "false"
	}
	return labels
}

func mergeStringMaps(base map[string]string, overlays ...map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for _, overlay := range overlays {
		for k, v := range overlay {
			out[k] = v
		}
	}
	return out
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func stableGroupInstance(base string, group int32) string {
	return fmt.Sprintf("%s-%d", base, group)
}

func revisionedGroupInstance(base string, group int32, revision string) string {
	return fmt.Sprintf("%s-%s-%d", base, revision, group)
}

func podName(groupInstance string, ordinal int32) string {
	return fmt.Sprintf("%s-%d", groupInstance, ordinal)
}

func templateRevision(template *corev1.PodTemplateSpec) (string, error) {
	raw, err := json.Marshal(template)
	if err != nil {
		return "", err
	}
	hash := fnv.New32a()
	if _, err := hash.Write(raw); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", hash.Sum32()), nil
}

func deploymentRevision(spec *PodGroupDeploymentSpec) (string, error) {
	if !spec.MultiNode.Enabled {
		return templateRevision(&spec.Template)
	}
	raw, err := json.Marshal(struct {
		Template  corev1.PodTemplateSpec `json:"template"`
		MultiNode MultiNodeSpec          `json:"multiNode,omitempty"`
	}{
		Template:  spec.Template,
		MultiNode: spec.MultiNode,
	})
	if err != nil {
		return "", err
	}
	hash := fnv.New32a()
	if _, err := hash.Write(raw); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", hash.Sum32()), nil
}

func templateRecreateRevision(template *corev1.PodTemplateSpec) (string, error) {
	normalized := template.DeepCopy()
	normalized.Labels = nil
	normalized.Annotations = nil
	for i := range normalized.Spec.Containers {
		normalized.Spec.Containers[i].Image = ""
	}
	return templateRevision(normalized)
}

func deploymentRecreateRevision(spec *PodGroupDeploymentSpec) (string, error) {
	if !spec.MultiNode.Enabled {
		return templateRecreateRevision(&spec.Template)
	}
	normalized := spec.Template.DeepCopy()
	normalized.Labels = nil
	normalized.Annotations = nil
	for i := range normalized.Spec.Containers {
		normalized.Spec.Containers[i].Image = ""
	}
	raw, err := json.Marshal(struct {
		Template  corev1.PodTemplateSpec `json:"template"`
		MultiNode MultiNodeSpec          `json:"multiNode,omitempty"`
	}{
		Template:  *normalized,
		MultiNode: spec.MultiNode,
	})
	if err != nil {
		return "", err
	}
	hash := fnv.New32a()
	if _, err := hash.Write(raw); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", hash.Sum32()), nil
}

func ptr[T any](v T) *T {
	return &v
}
