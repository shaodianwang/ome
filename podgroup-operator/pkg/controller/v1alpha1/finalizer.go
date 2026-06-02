package v1alpha1

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (c *Controller) ensurePodGroupDeploymentFinalizer(ctx context.Context, pgd *PodGroupDeployment) (bool, error) {
	if hasString(pgd.Finalizers, finalizerPodGroupDeployment) {
		return false, nil
	}
	return true, c.patchPodGroupDeploymentFinalizers(ctx, pgd.Namespace, pgd.Name, appendString(pgd.Finalizers, finalizerPodGroupDeployment))
}

func (c *Controller) finalizePodGroupDeployment(ctx context.Context, pgd *PodGroupDeployment) error {
	if !hasString(pgd.Finalizers, finalizerPodGroupDeployment) {
		return nil
	}
	logPGD(pgd, "finalizing podgroupdeployment")
	done, err := c.deleteAllPodGroupDeploymentChildren(ctx, pgd)
	if err != nil {
		return err
	}
	if !done {
		return nil
	}
	c.recordNormal(pgd, reasonFinalizing, "managed Pods, Services, and scheduler PodGroups are deleted")
	return c.patchPodGroupDeploymentFinalizers(ctx, pgd.Namespace, pgd.Name, removeString(pgd.Finalizers, finalizerPodGroupDeployment))
}

func (c *Controller) deleteAllPodGroupDeploymentChildren(ctx context.Context, pgd *PodGroupDeployment) (bool, error) {
	selector := labels.Set{
		labelManagedBy: managerName,
		labelInstance:  pgd.Name,
	}.String()

	pods, err := c.kube.CoreV1().Pods(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	grace := int64(1)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if err := c.kube.CoreV1().Pods(pgd.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}

	services, err := c.kube.CoreV1().Services(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	for i := range services.Items {
		service := &services.Items[i]
		if err := c.kube.CoreV1().Services(pgd.Namespace).Delete(ctx, service.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}

	schedulerPGs, err := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	for i := range schedulerPGs.Items {
		obj := &schedulerPGs.Items[i]
		if err := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}

	remaining, err := c.podGroupDeploymentChildrenObserved(ctx, pgd, selector)
	if err != nil {
		return false, err
	}
	return !remaining, nil
}

func (c *Controller) ensurePodGroupDeploymentRolloutFinalizer(ctx context.Context, pgdr *PodGroupDeploymentRollout) (bool, error) {
	if hasString(pgdr.Finalizers, finalizerPodGroupDeploymentRollout) {
		return false, nil
	}
	return true, c.patchPodGroupDeploymentRolloutFinalizers(ctx, pgdr.Namespace, pgdr.Name, appendString(pgdr.Finalizers, finalizerPodGroupDeploymentRollout))
}

func (c *Controller) finalizePodGroupDeploymentRollout(ctx context.Context, pgdr *PodGroupDeploymentRollout) error {
	if !hasString(pgdr.Finalizers, finalizerPodGroupDeploymentRollout) {
		return nil
	}
	logPGDR(pgdr, "finalizing podgroupdeploymentrollout")
	done, err := c.deleteAllPodGroupDeploymentRolloutChildren(ctx, pgdr)
	if err != nil {
		return err
	}
	if !done {
		return nil
	}
	c.recordNormal(pgdr, reasonFinalizing, "managed PodGroupDeployments are deleted")
	return c.patchPodGroupDeploymentRolloutFinalizers(ctx, pgdr.Namespace, pgdr.Name, removeString(pgdr.Finalizers, finalizerPodGroupDeploymentRollout))
}

func (c *Controller) deleteAllPodGroupDeploymentRolloutChildren(ctx context.Context, pgdr *PodGroupDeploymentRollout) (bool, error) {
	selector := labels.Set{
		labelManagedBy: managerName,
		labelRollout:   pgdr.Name,
	}.String()
	list, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	for i := range list.Items {
		obj := &list.Items[i]
		if !rolloutChildControlledBy(pgdr, obj.GetOwnerReferences()) {
			continue
		}
		if err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	remaining, err := c.rolloutChildrenObserved(ctx, pgdr, selector)
	if err != nil {
		return false, err
	}
	return !remaining, nil
}

func (c *Controller) podGroupDeploymentChildrenObserved(ctx context.Context, pgd *PodGroupDeployment, selector string) (bool, error) {
	pods, err := c.kube.CoreV1().Pods(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	if len(pods.Items) > 0 {
		return true, nil
	}
	services, err := c.kube.CoreV1().Services(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	if len(services.Items) > 0 {
		return true, nil
	}
	schedulerPGs, err := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	return len(schedulerPGs.Items) > 0, nil
}

func (c *Controller) rolloutChildrenObserved(ctx context.Context, pgdr *PodGroupDeploymentRollout, selector string) (bool, error) {
	list, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, err
	}
	for i := range list.Items {
		if rolloutChildControlledBy(pgdr, list.Items[i].GetOwnerReferences()) {
			return true, nil
		}
	}
	return false, nil
}

func hasString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func appendString(values []string, value string) []string {
	if hasString(values, value) {
		return append([]string(nil), values...)
	}
	out := append([]string(nil), values...)
	return append(out, value)
}

func removeString(values []string, target string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			out = append(out, value)
		}
	}
	return out
}
