package v1alpha1

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (c *Controller) deleteExtraObjects(ctx context.Context, pgd *PodGroupDeployment, desiredRevision string, activeInstances map[int32]string, podsByInstance map[string][]corev1.Pod) error {
	selector := labels.Set{
		labelManagedBy: managerName,
		labelInstance:  pgd.Name,
	}.String()
	allowedInstances := allowedGroupInstances(pgd, desiredRevision, activeInstances, podsByInstance)

	pods, err := c.kube.CoreV1().Pods(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if podOwnedByCurrentSpec(pgd, pod.Labels, allowedInstances) {
			continue
		}
		if err := c.kube.CoreV1().Pods(pgd.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		} else if err == nil {
			logPGD(pgd, "deleted extra pod=%s", pod.Name)
		}
	}

	services, err := c.kube.CoreV1().Services(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	for i := range services.Items {
		svc := &services.Items[i]
		if svc.Name == pgd.Name {
			continue
		}
		group, ok := parseInt32Label(svc.Labels, labelGroupIndex)
		if ok && group >= 0 && group < pgd.Spec.Groups && svc.Name == stableGroupInstance(pgd.Name, group) {
			continue
		}
		if err := c.kube.CoreV1().Services(pgd.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		} else if err == nil {
			logPGD(pgd, "deleted extra service=%s", svc.Name)
		}
	}

	schedulerPGs, err := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	for i := range schedulerPGs.Items {
		obj := &schedulerPGs.Items[i]
		group, ok := parseInt32Label(obj.GetLabels(), labelGroupIndex)
		instance := obj.GetLabels()[labelGroupInstance]
		if instance == "" {
			instance = obj.GetName()
		}
		if ok && group >= 0 && group < pgd.Spec.Groups && isAllowedGroupInstance(allowedInstances, instance) && obj.GetName() == instance {
			continue
		}
		if err := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		} else if err == nil {
			logPGD(pgd, "deleted extra scheduler_podgroup=%s", obj.GetName())
		}
	}

	return nil
}

func allowedGroupInstances(pgd *PodGroupDeployment, desiredRevision string, activeInstances map[int32]string, podsByInstance map[string][]corev1.Pod) map[string]struct{} {
	allowed := map[string]struct{}{}
	for group := int32(0); group < pgd.Spec.Groups; group++ {
		desiredInstance := revisionedGroupInstance(pgd.Name, group, desiredRevision)
		activeInstance := activeInstances[group]
		if activeInstance == "" {
			activeInstance = desiredInstance
		}
		allowed[activeInstance] = struct{}{}
		if activeInstance == desiredInstance || instanceRevision(podsByInstance[activeInstance], "") != desiredRevision {
			allowed[desiredInstance] = struct{}{}
		}
	}
	return allowed
}

func podOwnedByCurrentSpec(pgd *PodGroupDeployment, podLabels map[string]string, allowedInstances map[string]struct{}) bool {
	group, ok := parseInt32Label(podLabels, labelGroupIndex)
	if !ok || group < 0 || group >= pgd.Spec.Groups {
		return false
	}
	ordinal, ok := parseInt32Label(podLabels, labelOrdinal)
	if !ok || ordinal < 0 || ordinal >= pgd.Spec.ReplicasPerGroup {
		return false
	}
	instance := podLabels[labelGroupInstance]
	if instance == "" {
		instance = stableGroupInstance(podLabels[labelInstance], group)
	}
	return isAllowedGroupInstance(allowedInstances, instance)
}

func isAllowedGroupInstance(allowedInstances map[string]struct{}, instance string) bool {
	_, ok := allowedInstances[instance]
	return ok
}

func parseInt32Label(labelSet map[string]string, key string) (int32, bool) {
	raw := labelSet[key]
	if raw == "" {
		return 0, false
	}
	return parseInt32String(raw)
}

func parseInt32String(raw string) (int32, bool) {
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	return int32(n), err == nil
}

func (c *Controller) podsByGroupInstance(ctx context.Context, pgd *PodGroupDeployment) (map[string][]corev1.Pod, error) {
	pods, err := c.podsForDeployment(ctx, pgd)
	if err != nil {
		return nil, err
	}
	byInstance := map[string][]corev1.Pod{}
	for _, pod := range pods {
		instance := pod.Labels[labelGroupInstance]
		if instance == "" {
			group, ok := parseInt32Label(pod.Labels, labelGroupIndex)
			if !ok {
				continue
			}
			instance = stableGroupInstance(pgd.Name, group)
		}
		byInstance[instance] = append(byInstance[instance], pod)
	}
	return byInstance, nil
}

func (c *Controller) podsForDeployment(ctx context.Context, pgd *PodGroupDeployment) ([]corev1.Pod, error) {
	selector := labels.Set{
		labelManagedBy: managerName,
		labelInstance:  pgd.Name,
	}.String()
	pods, err := c.kube.CoreV1().Pods(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func (c *Controller) activeGroupInstances(ctx context.Context, pgd *PodGroupDeployment) (map[int32]string, error) {
	selector := labels.Set{
		labelManagedBy: managerName,
		labelInstance:  pgd.Name,
	}.String()
	services, err := c.kube.CoreV1().Services(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	active := map[int32]string{}
	for i := range services.Items {
		service := &services.Items[i]
		group, ok := parseInt32Label(service.Labels, labelGroupIndex)
		if !ok {
			continue
		}
		instance := service.Spec.Selector[labelGroupInstance]
		if instance == "" {
			instance = stableGroupInstance(pgd.Name, group)
		}
		active[group] = instance
	}
	return active, nil
}

func groupReady(pods []corev1.Pod, replicas int32) bool {
	if int32(len(pods)) != replicas {
		return false
	}
	ordinals := map[int32]struct{}{}
	for i := range pods {
		ordinal, ok := parseInt32Label(pods[i].Labels, labelOrdinal)
		if !ok || ordinal < 0 || ordinal >= replicas {
			return false
		}
		if !podReady(&pods[i]) {
			return false
		}
		ordinals[ordinal] = struct{}{}
	}
	return int32(len(ordinals)) == replicas
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func groupInstanceDisrupted(pods []corev1.Pod, replicas int32) bool {
	if int32(len(pods)) != replicas {
		return true
	}
	ordinals := map[int32]struct{}{}
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			return true
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return true
		}
		ordinal, ok := parseInt32Label(pod.Labels, labelOrdinal)
		if !ok || ordinal < 0 || ordinal >= replicas {
			return true
		}
		ordinals[ordinal] = struct{}{}
	}
	return int32(len(ordinals)) != replicas
}

func instanceRevision(pods []corev1.Pod, fallback string) string {
	revision := ""
	for i := range pods {
		podRevision := pods[i].Labels[labelRevision]
		if podRevision == "" {
			continue
		}
		if revision == "" {
			revision = podRevision
			continue
		}
		if revision != podRevision {
			return ""
		}
	}
	if revision != "" {
		return revision
	}
	return fallback
}

func filterPodsByInstance(pods []corev1.Pod, instance string) []corev1.Pod {
	var filtered []corev1.Pod
	for _, pod := range pods {
		podInstance := pod.Labels[labelGroupInstance]
		if podInstance == "" {
			group, ok := parseInt32Label(pod.Labels, labelGroupIndex)
			if ok {
				podInstance = stableGroupInstance(pod.Labels[labelInstance], group)
			}
		}
		if podInstance == instance {
			filtered = append(filtered, pod)
		}
	}
	return filtered
}

func (c *Controller) deleteGroupInstance(ctx context.Context, pgd *PodGroupDeployment, groupInstance string) error {
	selector := labels.Set{
		labelManagedBy:     managerName,
		labelInstance:      pgd.Name,
		labelGroupInstance: groupInstance,
	}.String()
	pods, err := c.kube.CoreV1().Pods(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	grace := int64(1)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if err := c.kube.CoreV1().Pods(pgd.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
			return err
		} else if err == nil {
			logPGD(pgd, "deleted pod=%s instance=%s", pod.Name, groupInstance)
		}
	}
	if err := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).Delete(ctx, groupInstance, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	} else if err == nil {
		logPGD(pgd, "deleted scheduler_podgroup=%s", groupInstance)
	}
	return nil
}
