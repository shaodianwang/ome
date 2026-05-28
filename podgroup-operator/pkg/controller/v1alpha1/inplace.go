package v1alpha1

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func groupInPlaceUpdateEligible(pgd *PodGroupDeployment, group int32, groupInstance string, pods []corev1.Pod, desiredRecreateRevision string) bool {
	if int32(len(pods)) != pgd.Spec.ReplicasPerGroup {
		return false
	}

	seen := map[int32]struct{}{}
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false
		}
		if pod.Annotations[annotationRecreateRevision] != desiredRecreateRevision {
			return false
		}
		ordinal, ok := parseInt32Label(pod.Labels, labelOrdinal)
		if !ok || ordinal < 0 || ordinal >= pgd.Spec.ReplicasPerGroup {
			return false
		}
		if !podContainersPatchable(pod.Spec.Containers, pgd.Spec.Template.Spec.Containers) {
			return false
		}
		seen[ordinal] = struct{}{}
	}
	return int32(len(seen)) == pgd.Spec.ReplicasPerGroup
}

func rolloutMemberInPlaceEligible(pgd *PodGroupDeployment, pods []corev1.Pod, desiredRevision string) (bool, error) {
	if !shouldTryInPlaceUpdate(pgd.Spec) {
		return false, nil
	}
	desiredRecreateRevision, err := deploymentRecreateRevision(&pgd.Spec)
	if err != nil {
		return false, err
	}
	podsByInstance := map[string][]corev1.Pod{}
	for _, pod := range pods {
		instance := pod.Labels[labelGroupInstance]
		if instance != "" {
			podsByInstance[instance] = append(podsByInstance[instance], pod)
		}
	}

	hasOutdatedGroup := false
	for group := int32(0); group < pgd.Spec.Groups; group++ {
		status, ok := groupStatusByIndex(pgd.Status.Groups, group)
		if !ok || status.GroupInstance == "" {
			return false, nil
		}
		groupPods := podsByInstance[status.GroupInstance]
		activeRevision := instanceRevision(groupPods, "")
		if activeRevision == "" || activeRevision == desiredRevision {
			continue
		}
		hasOutdatedGroup = true
		if !groupInPlaceUpdateEligible(pgd, group, status.GroupInstance, groupPods, desiredRecreateRevision) {
			if pgd.Spec.UpdateStrategy.Type == updateStrategyInPlaceOnly {
				return false, fmt.Errorf("group %d cannot be updated in place: template change requires recreate", group)
			}
			return false, nil
		}
	}
	return hasOutdatedGroup, nil
}

func groupStatusByIndex(groups []GroupStatus, index int32) (GroupStatus, bool) {
	for _, group := range groups {
		if group.Index == index {
			return group, true
		}
	}
	return GroupStatus{}, false
}

func podContainersPatchable(current, desired []corev1.Container) bool {
	if len(current) != len(desired) {
		return false
	}
	desiredNames := map[string]struct{}{}
	for _, container := range desired {
		desiredNames[container.Name] = struct{}{}
	}
	for _, container := range current {
		if _, ok := desiredNames[container.Name]; !ok {
			return false
		}
	}
	return true
}

func (c *Controller) updateGroupInPlace(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance, desiredRevision string) error {
	for ordinal := int32(0); ordinal < pgd.Spec.ReplicasPerGroup; ordinal++ {
		if err := c.updatePodInPlace(ctx, pgd, group, ordinal, groupInstance, desiredRevision); err != nil {
			return err
		}
	}
	if err := c.upsertSchedulerPodGroup(ctx, pgd, group, groupInstance, desiredRevision); err != nil {
		return err
	}
	return c.upsertService(ctx, pgd, group, groupInstance)
}

func (c *Controller) updatePodInPlace(ctx context.Context, pgd *PodGroupDeployment, group, ordinal int32, groupInstance, desiredRevision string) error {
	desired := podForGroup(pgd, group, ordinal, groupInstance, desiredRevision)
	client := c.kube.CoreV1().Pods(pgd.Namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	updated := current.DeepCopy()
	updated.Labels = mergeStringMaps(updated.Labels, desired.Labels)
	updated.Annotations = mergeStringMaps(updated.Annotations, desired.Annotations)
	updated.OwnerReferences = desired.OwnerReferences

	desiredContainers := map[string]corev1.Container{}
	for _, container := range desired.Spec.Containers {
		desiredContainers[container.Name] = container
	}
	for i := range updated.Spec.Containers {
		if desiredContainer, ok := desiredContainers[updated.Spec.Containers[i].Name]; ok {
			updated.Spec.Containers[i].Image = desiredContainer.Image
		}
	}

	logPGD(pgd, "updating pod in place pod=%s group=%d ordinal=%d instance=%s revision=%s", updated.Name, group, ordinal, groupInstance, desiredRevision)
	_, err = client.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}
