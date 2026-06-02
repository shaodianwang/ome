package v1alpha1

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Controller) reconcileOne(ctx context.Context, pgd *PodGroupDeployment) error {
	if !pgd.DeletionTimestamp.IsZero() {
		return c.finalizePodGroupDeployment(ctx, pgd)
	}
	finalizerAdded, err := c.ensurePodGroupDeploymentFinalizer(ctx, pgd)
	if err != nil {
		return err
	}
	if finalizerAdded {
		return nil
	}

	spec, err := defaultAndValidateSpec(pgd.Spec)
	if err != nil {
		return err
	}
	pgd.Spec = spec
	desiredRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		return err
	}
	desiredRecreateRevision, err := deploymentRecreateRevision(&pgd.Spec)
	if err != nil {
		return err
	}
	rolloutGate := rolloutGateFromAnnotations(pgd.Annotations)

	podsByInstance, err := c.podsByGroupInstance(ctx, pgd)
	if err != nil {
		return err
	}
	activeInstances, err := c.activeGroupInstances(ctx, pgd)
	if err != nil {
		return err
	}

	if isPGDRManaged(pgd) && !rolloutGate.enabled && pgdRequiresRolloutGate(pgd, desiredRevision, activeInstances, podsByInstance) {
		logPGD(pgd, "holding pgdr-managed podgroupdeployment without rollout gate revision=%s", desiredRevision)
		return c.reconcilePGDRManagedWithoutGate(ctx, pgd, desiredRevision, activeInstances, podsByInstance)
	}

	surgeInUse := int32(0)
	unavailableInUse := int32(0)
	for group := int32(0); group < spec.Groups; group++ {
		desiredInstance := revisionedGroupInstance(pgd.Name, group, desiredRevision)
		activeInstance := activeInstances[group]
		if activeInstance == "" {
			activeInstance = desiredInstance
		}
		activeRevision := instanceRevision(podsByInstance[activeInstance], "")
		if !groupReady(podsByInstance[activeInstance], spec.ReplicasPerGroup) {
			unavailableInUse++
		}
		if (activeRevision != desiredRevision || activeInstance != desiredInstance) && len(podsByInstance[desiredInstance]) > 0 {
			surgeInUse++
		}
	}

	for group := int32(0); group < spec.Groups; group++ {
		desiredInstance := revisionedGroupInstance(pgd.Name, group, desiredRevision)
		activeInstance := activeInstances[group]
		if activeInstance == "" {
			activeInstance = desiredInstance
		}
		activePods := podsByInstance[activeInstance]
		activeRevision := instanceRevision(activePods, "")
		replacementPods := podsByInstance[desiredInstance]

		if rolloutGate.enabled {
			if err := c.reconcileGatedGroup(ctx, pgd, rolloutGate, group, activeInstances[group] != "", activeInstance, activeRevision, activePods, desiredInstance, desiredRevision, replacementPods); err != nil {
				return err
			}
			continue
		}

		if len(activePods) == 0 && activeInstances[group] == "" {
			logPGD(pgd, "creating group=%d instance=%s revision=%s", group, activeInstance, desiredRevision)
			if err := c.upsertGroup(ctx, pgd, group, activeInstance, desiredRevision); err != nil {
				return err
			}
			if err := c.upsertService(ctx, pgd, group, activeInstance); err != nil {
				return err
			}
			continue
		}
		if activeInstances[group] != "" && len(activePods) == 0 {
			if len(replacementPods) > 0 {
				if groupReady(replacementPods, spec.ReplicasPerGroup) {
					logPGD(pgd, "switching service group=%d from_instance=%s to_instance=%s revision=%s", group, activeInstance, desiredInstance, desiredRevision)
					if err := c.upsertService(ctx, pgd, group, desiredInstance); err != nil {
						return err
					}
				} else if err := c.upsertReplacementGroup(ctx, pgd, group, desiredInstance, desiredRevision, replacementPods); err != nil {
					return err
				}
				continue
			}
			if activeInstance != desiredInstance {
				logPGD(pgd, "creating replacement group=%d instance=%s revision=%s old_instance=%s", group, desiredInstance, desiredRevision, activeInstance)
				if err := c.upsertGroup(ctx, pgd, group, desiredInstance, desiredRevision); err != nil {
					return err
				}
				if err := c.upsertGroupBootstrapService(ctx, pgd, group, desiredInstance); err != nil {
					return err
				}
				continue
			}
			logPGD(pgd, "recreating empty active group=%d instance=%s revision=%s", group, activeInstance, desiredRevision)
			if err := c.upsertGroup(ctx, pgd, group, activeInstance, desiredRevision); err != nil {
				return err
			}
			if err := c.upsertService(ctx, pgd, group, activeInstance); err != nil {
				return err
			}
			continue
		}
		if activeInstances[group] != "" && groupInstanceDisrupted(activePods, spec.ReplicasPerGroup) {
			logPGD(pgd, "deleting disrupted group=%d instance=%s pods=%d expected_pods=%d", group, activeInstance, len(activePods), spec.ReplicasPerGroup)
			if err := c.deleteGroupInstance(ctx, pgd, activeInstance); err != nil {
				return err
			}
			continue
		}

		if activeRevision == desiredRevision && activeRevision != "" {
			if err := c.upsertGroup(ctx, pgd, group, activeInstance, desiredRevision); err != nil {
				return err
			}
			if err := c.upsertService(ctx, pgd, group, activeInstance); err != nil {
				return err
			}
			continue
		}

		if shouldTryInPlaceUpdate(spec) && activeRevision != "" && activeRevision != desiredRevision && len(activePods) > 0 {
			if groupInPlaceUpdateEligible(pgd, group, activeInstance, activePods, desiredRecreateRevision) {
				if unavailableInUse < effectiveMaxUnavailable(spec) {
					logPGD(pgd, "updating group in place group=%d instance=%s from_revision=%s to_revision=%s", group, activeInstance, activeRevision, desiredRevision)
					if err := c.updateGroupInPlace(ctx, pgd, group, activeInstance, desiredRevision); err != nil {
						return err
					}
					unavailableInUse++
				}
				continue
			}
			if spec.UpdateStrategy.Type == updateStrategyInPlaceOnly {
				return fmt.Errorf("group %d cannot be updated in place: template change requires recreate", group)
			}
		}

		if groupReady(replacementPods, spec.ReplicasPerGroup) {
			if activeInstance != desiredInstance {
				logPGD(pgd, "switching service group=%d from_instance=%s to_instance=%s revision=%s", group, activeInstance, desiredInstance, desiredRevision)
			}
			if err := c.upsertService(ctx, pgd, group, desiredInstance); err != nil {
				return err
			}
			if activeInstance != desiredInstance {
				logPGD(pgd, "deleting old group=%d instance=%s after replacement is ready", group, activeInstance)
				if err := c.deleteGroupInstance(ctx, pgd, activeInstance); err != nil {
					return err
				}
			}
			continue
		}

		if len(replacementPods) > 0 || surgeInUse < spec.UpdateStrategy.RollingUpdate.MaxSurge {
			if len(replacementPods) == 0 {
				logPGD(pgd, "creating replacement group=%d instance=%s revision=%s old_instance=%s", group, desiredInstance, desiredRevision, activeInstance)
				surgeInUse++
			}
			if err := c.upsertReplacementGroup(ctx, pgd, group, desiredInstance, desiredRevision, replacementPods); err != nil {
				return err
			}
		} else if spec.UpdateStrategy.RollingUpdate.MaxSurge == 0 && unavailableInUse < effectiveMaxUnavailable(spec) {
			logPGD(pgd, "deleting old group=%d instance=%s before replacement max_surge=0", group, activeInstance)
			if err := c.deleteGroupInstance(ctx, pgd, activeInstance); err != nil {
				return err
			}
			unavailableInUse++
			logPGD(pgd, "creating replacement group=%d instance=%s revision=%s", group, desiredInstance, desiredRevision)
			if err := c.upsertGroup(ctx, pgd, group, desiredInstance, desiredRevision); err != nil {
				return err
			}
			if err := c.upsertGroupBootstrapService(ctx, pgd, group, desiredInstance); err != nil {
				return err
			}
			continue
		}

		if activeRevision != "" {
			if err := c.upsertGroupMetadata(ctx, pgd, group, activeInstance, activeRevision); err != nil {
				return err
			}
		}
	}

	if err := c.upsertTopLevelService(ctx, pgd); err != nil {
		return err
	}
	if err := c.deleteExtraObjects(ctx, pgd, desiredRevision, activeInstances, podsByInstance); err != nil {
		return err
	}
	return c.updateStatus(ctx, pgd, desiredRevision)
}

func isPGDRManaged(pgd *PodGroupDeployment) bool {
	if pgd == nil {
		return false
	}
	return pgd.Labels[labelRollout] != ""
}

func pgdRequiresRolloutGate(pgd *PodGroupDeployment, desiredRevision string, activeInstances map[int32]string, podsByInstance map[string][]corev1.Pod) bool {
	for group := int32(0); group < pgd.Spec.Groups; group++ {
		desiredInstance := revisionedGroupInstance(pgd.Name, group, desiredRevision)
		activeInstance := activeInstances[group]
		if activeInstance == "" {
			if pgd.Status.CurrentRevision == desiredRevision && groupPodsMatchRevision(podsByInstance, group, desiredRevision) {
				continue
			}
			return true
		}
		if activeInstance != desiredInstance {
			return true
		}
		activeRevision := instanceRevision(podsByInstance[activeInstance], "")
		if activeRevision != "" && activeRevision != desiredRevision {
			return true
		}
	}
	return false
}

func groupPodsMatchRevision(podsByInstance map[string][]corev1.Pod, group int32, revision string) bool {
	found := false
	for _, pods := range podsByInstance {
		for i := range pods {
			podGroup, ok := parseInt32Label(pods[i].Labels, labelGroupIndex)
			if !ok || podGroup != group {
				continue
			}
			found = true
			if pods[i].Labels[labelRevision] != revision {
				return false
			}
		}
	}
	return found
}

func (c *Controller) reconcilePGDRManagedWithoutGate(ctx context.Context, pgd *PodGroupDeployment, desiredRevision string, activeInstances map[int32]string, podsByInstance map[string][]corev1.Pod) error {
	for group := int32(0); group < pgd.Spec.Groups; group++ {
		desiredInstance := revisionedGroupInstance(pgd.Name, group, desiredRevision)
		activeInstance := activeInstances[group]
		if activeInstance == "" {
			continue
		}
		activePods := podsByInstance[activeInstance]
		activeRevision := instanceRevision(activePods, "")
		replacementPods := podsByInstance[desiredInstance]

		if activeInstance != desiredInstance && len(replacementPods) > 0 {
			logPGD(pgd, "deleting ungated pgdr-managed replacement group=%d instance=%s revision=%s", group, desiredInstance, desiredRevision)
			if err := c.deleteGroupInstance(ctx, pgd, desiredInstance); err != nil {
				return err
			}
		}
		if activeRevision != "" && len(activePods) > 0 {
			if err := c.upsertGroupMetadata(ctx, pgd, group, activeInstance, activeRevision); err != nil {
				return err
			}
		}
	}
	if err := c.upsertTopLevelService(ctx, pgd); err != nil {
		return err
	}
	return c.updateStatus(ctx, pgd, desiredRevision)
}

func (c *Controller) reconcileGatedGroup(ctx context.Context, pgd *PodGroupDeployment, gate parsedRolloutGate, group int32, hasActiveInstance bool, activeInstance, activeRevision string, activePods []corev1.Pod, desiredInstance, desiredRevision string, replacementPods []corev1.Pod) error {
	if !gate.allows(group) || gate.action == rolloutActionHold {
		if activeInstance != desiredInstance && len(replacementPods) > 0 {
			logPGD(pgd, "deleting gated replacement outside window group=%d instance=%s rollout=%s action=%s phase=%s", group, desiredInstance, gate.rollout, gate.action, gate.phase)
			if err := c.deleteGroupInstance(ctx, pgd, desiredInstance); err != nil {
				return err
			}
		}
		if activeRevision != "" && len(activePods) > 0 {
			if err := c.upsertGroupMetadata(ctx, pgd, group, activeInstance, activeRevision); err != nil {
				return err
			}
		}
		return nil
	}

	switch gate.action {
	case rolloutActionDeleteOld:
		if activeInstance != desiredInstance && len(replacementPods) > 0 {
			logPGD(pgd, "deleting gated replacement before delete-old group=%d instance=%s rollout=%s phase=%s", group, desiredInstance, gate.rollout, gate.phase)
			if err := c.deleteGroupInstance(ctx, pgd, desiredInstance); err != nil {
				return err
			}
		}
		if activeInstance != desiredInstance && len(activePods) > 0 {
			logPGD(pgd, "deleting gated old group=%d instance=%s rollout=%s phase=%s", group, activeInstance, gate.rollout, gate.phase)
			return c.deleteGroupInstance(ctx, pgd, activeInstance)
		}
		return nil
	case rolloutActionCreateNew:
		if groupReady(replacementPods, pgd.Spec.ReplicasPerGroup) {
			if activeInstance != desiredInstance {
				logPGD(pgd, "switching gated service group=%d from_instance=%s to_instance=%s rollout=%s phase=%s", group, activeInstance, desiredInstance, gate.rollout, gate.phase)
			}
			if err := c.upsertService(ctx, pgd, group, desiredInstance); err != nil {
				return err
			}
			if activeInstance != desiredInstance {
				logPGD(pgd, "deleting gated old group=%d instance=%s rollout=%s phase=%s", group, activeInstance, gate.rollout, gate.phase)
				return c.deleteGroupInstance(ctx, pgd, activeInstance)
			}
			return nil
		}
		if len(replacementPods) > 0 {
			return c.upsertReplacementGroup(ctx, pgd, group, desiredInstance, desiredRevision, replacementPods)
		}
		if activeInstance != desiredInstance && len(activePods) > 0 {
			logPGD(pgd, "deleting gated old group=%d instance=%s before replacement rollout=%s phase=%s", group, activeInstance, gate.rollout, gate.phase)
			return c.deleteGroupInstance(ctx, pgd, activeInstance)
		}
		logPGD(pgd, "creating gated replacement group=%d instance=%s revision=%s rollout=%s phase=%s", group, desiredInstance, desiredRevision, gate.rollout, gate.phase)
		if err := c.upsertGroup(ctx, pgd, group, desiredInstance, desiredRevision); err != nil {
			return err
		}
		return c.upsertGroupBootstrapService(ctx, pgd, group, desiredInstance)
	case rolloutActionInPlace:
		desiredRecreateRevision, err := deploymentRecreateRevision(&pgd.Spec)
		if err != nil {
			return err
		}
		if activeRevision == desiredRevision && activeRevision != "" {
			if err := c.upsertGroup(ctx, pgd, group, activeInstance, desiredRevision); err != nil {
				return err
			}
			return c.upsertService(ctx, pgd, group, activeInstance)
		}
		if !groupInPlaceUpdateEligible(pgd, group, activeInstance, activePods, desiredRecreateRevision) {
			return fmt.Errorf("group %d cannot be updated in place: template change requires recreate", group)
		}
		logPGD(pgd, "updating gated group in place group=%d instance=%s from_revision=%s to_revision=%s rollout=%s phase=%s", group, activeInstance, activeRevision, desiredRevision, gate.rollout, gate.phase)
		return c.updateGroupInPlace(ctx, pgd, group, activeInstance, desiredRevision)
	default:
		return fmt.Errorf("unsupported rollout gate action %q", gate.action)
	}
}

func (c *Controller) upsertGroup(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance, revision string) error {
	if err := c.upsertSchedulerPodGroup(ctx, pgd, group, groupInstance, revision); err != nil {
		return err
	}
	for ordinal := int32(0); ordinal < pgd.Spec.ReplicasPerGroup; ordinal++ {
		if err := c.upsertPod(ctx, pgd, group, ordinal, groupInstance, revision); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) upsertReplacementGroup(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance, revision string, existingPods []corev1.Pod) error {
	if err := c.deleteStaleUnreadyReplacementPods(ctx, pgd, group, groupInstance, existingPods); err != nil {
		return err
	}
	if err := c.upsertGroup(ctx, pgd, group, groupInstance, revision); err != nil {
		return err
	}
	return c.upsertGroupBootstrapService(ctx, pgd, group, groupInstance)
}

func (c *Controller) upsertGroupBootstrapService(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance string) error {
	if !multiNodeActive(pgd.Spec) {
		return nil
	}
	return c.upsertService(ctx, pgd, group, groupInstance)
}

func (c *Controller) deleteStaleUnreadyReplacementPods(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance string, pods []corev1.Pod) error {
	deadline := effectiveProgressDeadline(pgd.Spec)
	if deadline <= 0 {
		return nil
	}
	now := time.Now()
	grace := int64(1)
	for i := range pods {
		pod := &pods[i]
		if podReady(pod) || pod.DeletionTimestamp != nil || pod.CreationTimestamp.IsZero() {
			continue
		}
		if now.Sub(pod.CreationTimestamp.Time) < deadline {
			continue
		}
		logPGD(pgd, "deleting stale unready replacement pod=%s group=%d instance=%s age=%s progress_deadline=%s", pod.Name, group, groupInstance, now.Sub(pod.CreationTimestamp.Time).Round(time.Second), deadline)
		if err := c.kube.CoreV1().Pods(pgd.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (c *Controller) upsertGroupMetadata(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance, revision string) error {
	if err := c.upsertSchedulerPodGroup(ctx, pgd, group, groupInstance, revision); err != nil {
		return err
	}
	return c.upsertService(ctx, pgd, group, groupInstance)
}

func defaultAndValidateSpec(spec PodGroupDeploymentSpec) (PodGroupDeploymentSpec, error) {
	if spec.Groups < 0 {
		return spec, fmt.Errorf("spec.groups must be non-negative")
	}
	if spec.ReplicasPerGroup <= 0 {
		return spec, fmt.Errorf("spec.replicasPerGroup must be greater than zero")
	}
	if spec.ServiceTargetOrdinal < 0 || spec.ServiceTargetOrdinal >= spec.ReplicasPerGroup {
		return spec, fmt.Errorf("spec.serviceTargetOrdinal must be between 0 and replicasPerGroup-1")
	}
	if len(spec.Template.Spec.Containers) == 0 {
		return spec, fmt.Errorf("spec.template.spec.containers must not be empty")
	}
	if spec.TopologyKey == "" {
		spec.TopologyKey = defaultTopologyKey
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = defaultSchedulerName
	}
	if spec.Template.Spec.RestartPolicy == "" {
		spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	if spec.Template.Spec.TerminationGracePeriodSeconds == nil {
		spec.Template.Spec.TerminationGracePeriodSeconds = ptr(defaultTerminationGracePeriodSeconds)
	}
	if spec.UpdateStrategy.RollingUpdate.MaxSurge < 0 {
		return spec, fmt.Errorf("spec.updateStrategy.rollingUpdate.maxSurge must be non-negative")
	}
	if spec.UpdateStrategy.Type == "" {
		spec.UpdateStrategy.Type = updateStrategyInPlaceIfPossible
	}
	switch spec.UpdateStrategy.Type {
	case updateStrategyRecreatePod, updateStrategyInPlaceIfPossible, updateStrategyInPlaceOnly:
	default:
		return spec, fmt.Errorf("spec.updateStrategy.type must be RecreatePod, InPlaceIfPossible, or InPlaceOnly")
	}
	if spec.UpdateStrategy.RollingUpdate.MaxUnavailable < 0 {
		return spec, fmt.Errorf("spec.updateStrategy.rollingUpdate.maxUnavailable must be non-negative")
	}
	if spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent < 0 || spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent > 100 {
		return spec, fmt.Errorf("spec.updateStrategy.rollingUpdate.maxUnavailablePercent must be between 0 and 100")
	}
	if spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds < 0 {
		return spec, fmt.Errorf("spec.updateStrategy.rollingUpdate.progressDeadlineSeconds must be non-negative")
	}
	if spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds == 0 {
		spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds = defaultProgressDeadlineSeconds
	}
	if spec.MultiNode.DistInitPort == 0 {
		spec.MultiNode.DistInitPort = defaultMultiNodeDistInitPort
	}
	if err := validateMultiNodeSpec(spec); err != nil {
		return spec, err
	}
	if spec.UpdateStrategy.RollingUpdate.MaxSurge == 0 &&
		spec.UpdateStrategy.RollingUpdate.MaxUnavailable == 0 &&
		spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent == 0 {
		spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent = defaultMaxUnavailablePercent
	}
	return spec, nil
}

func effectiveMaxUnavailable(spec PodGroupDeploymentSpec) int32 {
	if spec.UpdateStrategy.RollingUpdate.MaxUnavailable > 0 {
		return spec.UpdateStrategy.RollingUpdate.MaxUnavailable
	}
	return rolloutWaveSize(spec.Groups, spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent)
}

func effectiveProgressDeadline(spec PodGroupDeploymentSpec) time.Duration {
	seconds := spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds
	if seconds == 0 {
		seconds = defaultProgressDeadlineSeconds
	}
	return time.Duration(seconds) * time.Second
}

func shouldTryInPlaceUpdate(spec PodGroupDeploymentSpec) bool {
	return spec.UpdateStrategy.Type == updateStrategyInPlaceIfPossible || spec.UpdateStrategy.Type == updateStrategyInPlaceOnly
}
