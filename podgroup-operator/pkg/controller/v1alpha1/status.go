package v1alpha1

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

func (c *Controller) updateRolloutStatus(ctx context.Context, pgdr *PodGroupDeploymentRollout, status PodGroupDeploymentRolloutStatus) error {
	current, err := c.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(pgdr.Namespace).Get(ctx, pgdr.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if currentRollout, err := decodePodGroupDeploymentRollout(current); err == nil {
		status.Conditions = currentRollout.Status.Conditions
	}
	setRolloutStatusConditions(&status)
	obj, err := rolloutStatusToUnstructured(status)
	if err != nil {
		return err
	}
	if currentStatus, found, _ := unstructured.NestedMap(current.Object, "status"); found && unstructuredMapsEqual(currentStatus, obj) {
		return nil
	}
	_ = unstructured.SetNestedField(current.Object, obj, "status")
	_, err = c.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(pgdr.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
	return err
}

func (c *Controller) updateStatus(ctx context.Context, pgd *PodGroupDeployment, currentRevision string) error {
	selector := labels.Set{
		labelManagedBy: managerName,
		labelInstance:  pgd.Name,
	}.String()
	pods, err := c.kube.CoreV1().Pods(pgd.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	activeInstances, err := c.activeGroupInstances(ctx, pgd)
	if err != nil {
		return err
	}
	nodeTopologyValues, err := c.nodeTopologyValues(ctx, pgd.Spec.TopologyKey)
	if err != nil {
		return err
	}

	status := PodGroupDeploymentStatus{
		ObservedGeneration: pgd.Generation,
		CurrentRevision:    currentRevision,
		RolloutGate:        rolloutGateFromAnnotations(pgd.Annotations).status(),
		Total: TotalStatus{
			DesiredGroups: pgd.Spec.Groups,
			DesiredPods:   pgd.Spec.Groups * pgd.Spec.ReplicasPerGroup,
		},
	}

	byGroup := map[int32][]corev1.Pod{}
	for _, pod := range pods.Items {
		group, ok := parseInt32Label(pod.Labels, labelGroupIndex)
		if !ok {
			continue
		}
		byGroup[group] = append(byGroup[group], pod)
	}

	for group := int32(0); group < pgd.Spec.Groups; group++ {
		desiredInstance := revisionedGroupInstance(pgd.Name, group, currentRevision)
		activeInstance := activeInstances[group]
		if activeInstance == "" {
			activeInstance = desiredInstance
		}
		groupPods := filterPodsByInstance(byGroup[group], activeInstance)
		gs := GroupStatus{
			Index:             group,
			SchedulerPodGroup: activeInstance,
			Service:           stableGroupInstance(pgd.Name, group),
			GroupInstance:     activeInstance,
			Revision:          instanceRevision(groupPods, ""),
			TargetPod:         podName(activeInstance, pgd.Spec.ServiceTargetOrdinal),
			Phase:             "Pending",
		}
		topologyValues := map[string]int{}
		for i := range groupPods {
			pod := &groupPods[i]
			switch pod.Status.Phase {
			case corev1.PodRunning:
				gs.Running++
				status.Total.RunningPods++
				if podReady(pod) {
					gs.Ready++
					status.Total.ReadyPods++
				}
			case corev1.PodFailed:
				gs.Failed++
				status.Total.FailedPods++
			default:
				gs.Pending++
				status.Total.PendingPods++
			}
			if pod.Spec.NodeName != "" {
				topologyValues[topologyValueForPod(pod, nodeTopologyValues)]++
			}
		}
		if groupReady(groupPods, pgd.Spec.ReplicasPerGroup) {
			gs.Phase = "Ready"
		} else if gs.Running == pgd.Spec.ReplicasPerGroup {
			gs.Phase = "Running"
		} else if gs.Failed > 0 {
			gs.Phase = "Failed"
		}
		gs.TopologyValue = dominantTopologyValue(topologyValues)
		status.Groups = append(status.Groups, gs)
	}

	current, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(ctx, pgd.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if currentPGD, err := decodePodGroupDeployment(current); err == nil {
		status.Conditions = currentPGD.Status.Conditions
	}
	setPodGroupDeploymentStatusConditions(&status)
	obj, err := statusToUnstructured(status)
	if err != nil {
		return err
	}
	if currentStatus, found, _ := unstructured.NestedMap(current.Object, "status"); found && unstructuredMapsEqual(currentStatus, obj) {
		return nil
	}
	_ = unstructured.SetNestedField(current.Object, obj, "status")
	_, err = c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
	return err
}

func setPodGroupDeploymentStatusConditions(status *PodGroupDeploymentStatus) {
	ready := status.Total.DesiredPods > 0 && status.Total.ReadyPods == status.Total.DesiredPods
	if ready {
		setCondition(&status.Conditions, status.ObservedGeneration, conditionReady, metav1.ConditionTrue, reasonAllGroupsReady, "all desired pods are ready")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionProgressing, metav1.ConditionFalse, reasonAllGroupsReady, "all desired pods are ready")
	} else {
		setCondition(&status.Conditions, status.ObservedGeneration, conditionReady, metav1.ConditionFalse, reasonReconciling, "waiting for desired pods to become ready")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionProgressing, metav1.ConditionTrue, reasonReconciling, "reconciling desired pods")
	}
	if status.RolloutGate.Rollout != "" {
		message := "rollout gate action=" + status.RolloutGate.Action
		if status.RolloutGate.Phase != "" {
			message += " phase=" + status.RolloutGate.Phase
		}
		if status.RolloutGate.PausedReason != "" {
			message += ": " + status.RolloutGate.PausedReason
		}
		setCondition(&status.Conditions, status.ObservedGeneration, conditionRolloutGated, metav1.ConditionTrue, reasonRolloutGate, message)
	} else {
		setCondition(&status.Conditions, status.ObservedGeneration, conditionRolloutGated, metav1.ConditionFalse, reasonNoRolloutGate, "no rollout gate is active")
	}
}

func setRolloutStatusConditions(status *PodGroupDeploymentRolloutStatus) {
	switch status.Phase {
	case "Complete":
		setCondition(&status.Conditions, status.ObservedGeneration, conditionReady, metav1.ConditionTrue, reasonRolloutComplete, "rollout is complete")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionProgressing, metav1.ConditionFalse, reasonRolloutComplete, "rollout is complete")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionBlocked, metav1.ConditionFalse, reasonRolloutComplete, "rollout is complete")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionDegraded, metav1.ConditionFalse, reasonRolloutComplete, "rollout is complete")
	case "Blocked":
		setCondition(&status.Conditions, status.ObservedGeneration, conditionReady, metav1.ConditionFalse, reasonRolloutBlocked, status.Reason)
		setCondition(&status.Conditions, status.ObservedGeneration, conditionProgressing, metav1.ConditionFalse, reasonRolloutBlocked, status.Reason)
		setCondition(&status.Conditions, status.ObservedGeneration, conditionBlocked, metav1.ConditionTrue, reasonRolloutBlocked, status.Reason)
		setCondition(&status.Conditions, status.ObservedGeneration, conditionDegraded, metav1.ConditionFalse, reasonRolloutBlocked, status.Reason)
	case "Degraded":
		setCondition(&status.Conditions, status.ObservedGeneration, conditionReady, metav1.ConditionFalse, reasonRolloutDegraded, status.Reason)
		setCondition(&status.Conditions, status.ObservedGeneration, conditionProgressing, metav1.ConditionFalse, reasonRolloutDegraded, status.Reason)
		setCondition(&status.Conditions, status.ObservedGeneration, conditionBlocked, metav1.ConditionFalse, reasonRolloutDegraded, status.Reason)
		setCondition(&status.Conditions, status.ObservedGeneration, conditionDegraded, metav1.ConditionTrue, reasonRolloutDegraded, status.Reason)
	default:
		setCondition(&status.Conditions, status.ObservedGeneration, conditionReady, metav1.ConditionFalse, reasonRolloutProgress, "rollout is progressing")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionProgressing, metav1.ConditionTrue, reasonRolloutProgress, "rollout is progressing")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionBlocked, metav1.ConditionFalse, reasonRolloutProgress, "rollout is progressing")
		setCondition(&status.Conditions, status.ObservedGeneration, conditionDegraded, metav1.ConditionFalse, reasonRolloutProgress, "rollout is progressing")
	}
}

func setCondition(conditions *[]metav1.Condition, observedGeneration int64, conditionType string, status metav1.ConditionStatus, reason, message string) {
	if message == "" {
		message = reason
	}
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: observedGeneration,
		Reason:             reason,
		Message:            message,
	})
}

func (c *Controller) nodeTopologyValues(ctx context.Context, topologyKey string) (map[string]string, error) {
	nodes, err := c.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		value := node.Labels[topologyKey]
		if value == "" {
			value = node.Name
		}
		values[node.Name] = value
	}
	return values, nil
}

func topologyValueForPod(pod *corev1.Pod, nodeTopologyValues map[string]string) string {
	if pod.Spec.NodeName == "" {
		return ""
	}
	if value := nodeTopologyValues[pod.Spec.NodeName]; value != "" {
		return value
	}
	return pod.Spec.NodeName
}

func statusToUnstructured(status PodGroupDeploymentStatus) (map[string]interface{}, error) {
	raw, err := json.Marshal(status)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	return out, json.Unmarshal(raw, &out)
}

func rolloutStatusToUnstructured(status PodGroupDeploymentRolloutStatus) (map[string]interface{}, error) {
	raw, err := json.Marshal(status)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	return out, json.Unmarshal(raw, &out)
}

func unstructuredMapsEqual(a, b map[string]interface{}) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	aJSON, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bJSON, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(aJSON) == string(bJSON)
}

func dominantTopologyValue(values map[string]int) string {
	type item struct {
		name  string
		count int
	}
	items := make([]item, 0, len(values))
	for name, count := range values {
		items = append(items, item{name: name, count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].name < items[j].name
		}
		return items[i].count > items[j].count
	})
	if len(items) == 0 {
		return ""
	}
	return items[0].name
}
