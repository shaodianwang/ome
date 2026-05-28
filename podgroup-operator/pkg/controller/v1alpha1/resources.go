package v1alpha1

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func (c *Controller) upsertSchedulerPodGroup(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance, revision string) error {
	name := groupInstance
	labels := baseLabels(pgd, group, groupInstance, revision)
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "scheduling.x-k8s.io/v1alpha1",
			"kind":       "PodGroup",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": pgd.Namespace,
				"labels":    labels,
			},
			"spec": map[string]interface{}{
				"minMember": int64(pgd.Spec.ReplicasPerGroup),
			},
		},
	}
	if pgd.Spec.ScheduleTimeoutSeconds > 0 {
		_ = unstructured.SetNestedField(obj.Object, int64(pgd.Spec.ScheduleTimeoutSeconds), "spec", "scheduleTimeoutSeconds")
	}
	obj.SetOwnerReferences(ownerRef(pgd))

	client := c.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace)
	current, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		logPGD(pgd, "creating scheduler_podgroup=%s group=%d min_member=%d revision=%s schedule_timeout_seconds=%d", name, group, pgd.Spec.ReplicasPerGroup, revision, pgd.Spec.ScheduleTimeoutSeconds)
		_, err = client.Create(ctx, obj, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}

	obj.SetResourceVersion(current.GetResourceVersion())
	_, err = client.Update(ctx, obj, metav1.UpdateOptions{})
	return err
}

func (c *Controller) upsertPod(ctx context.Context, pgd *PodGroupDeployment, group, ordinal int32, groupInstance, revision string) error {
	desired := podForGroup(pgd, group, ordinal, groupInstance, revision)
	client := c.kube.CoreV1().Pods(pgd.Namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		logPGD(pgd, "creating pod=%s group=%d ordinal=%d instance=%s revision=%s scheduler=%s", desired.Name, group, ordinal, groupInstance, revision, desired.Spec.SchedulerName)
		_, err = client.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if podNeedsRecreate(current, desired) {
		grace := int64(0)
		logPGD(pgd, "deleting pod for recreate pod=%s group=%d ordinal=%d instance=%s revision=%s", current.Name, group, ordinal, groupInstance, revision)
		return client.Delete(ctx, current.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	}
	if scheduledPodMetadataNeedsUpdate(current, desired) {
		if current.Spec.NodeName != "" {
			logPGD(pgd, "updating scheduled pod metadata pod=%s group=%d ordinal=%d instance=%s revision=%s node=%s", current.Name, group, ordinal, groupInstance, revision, current.Spec.NodeName)
		} else {
			logPGD(pgd, "updating pod metadata pod=%s group=%d ordinal=%d instance=%s revision=%s", current.Name, group, ordinal, groupInstance, revision)
		}
		updated := current.DeepCopy()
		updated.Labels = mergeStringMaps(updated.Labels, desired.Labels)
		updated.Annotations = mergeStringMaps(updated.Annotations, desired.Annotations)
		updated.OwnerReferences = desired.OwnerReferences
		_, err = client.Update(ctx, updated, metav1.UpdateOptions{})
		return err
	}
	return nil
}

func podForGroup(pgd *PodGroupDeployment, group, ordinal int32, groupInstance, revision string) *corev1.Pod {
	template := pgd.Spec.Template.DeepCopy()
	pod := &corev1.Pod{
		ObjectMeta: *template.ObjectMeta.DeepCopy(),
		Spec:       *template.Spec.DeepCopy(),
	}
	pod.Name = podName(groupInstance, ordinal)
	pod.Namespace = pgd.Namespace
	pod.GenerateName = ""
	pod.ResourceVersion = ""
	pod.UID = ""
	pod.CreationTimestamp = metav1.Time{}
	pod.ManagedFields = nil
	pod.OwnerReferences = ownerRef(pgd)
	pod.Labels = mergeStringMaps(pod.Labels, podLabels(pgd, group, ordinal, groupInstance, revision))
	recreateRevision, _ := deploymentRecreateRevision(&pgd.Spec)
	pod.Annotations = mergeStringMaps(pod.Annotations, map[string]string{
		annotationRecreateRevision: recreateRevision,
	})
	pod.Spec.SchedulerName = pgd.Spec.SchedulerName
	pod.Spec.RestartPolicy = pgd.Spec.Template.Spec.RestartPolicy
	pod.Spec.Affinity = withRequiredGroupAffinity(pod.Spec.Affinity, pgd, groupInstance)
	applyMultiNodeArgs(pgd, pod, group, ordinal)
	return pod
}

func scheduledPodMetadataNeedsUpdate(current, desired *corev1.Pod) bool {
	for key, value := range desired.Labels {
		if current.Labels[key] != value {
			return true
		}
	}
	for key, value := range desired.Annotations {
		if current.Annotations[key] != value {
			return true
		}
	}
	return false
}

func podNeedsRecreate(current, desired *corev1.Pod) bool {
	return current.Annotations[annotationRecreateRevision] != desired.Annotations[annotationRecreateRevision]
}

func withRequiredGroupAffinity(existing *corev1.Affinity, pgd *PodGroupDeployment, groupInstance string) *corev1.Affinity {
	affinity := &corev1.Affinity{}
	if existing != nil {
		affinity = existing.DeepCopy()
	}
	if affinity.PodAffinity == nil {
		affinity.PodAffinity = &corev1.PodAffinity{}
	}

	term := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				labelInstance:      pgd.Name,
				labelGroupInstance: groupInstance,
			},
		},
		TopologyKey: pgd.Spec.TopologyKey,
	}
	affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
		term,
	)
	return affinity
}

func (c *Controller) upsertService(ctx context.Context, pgd *PodGroupDeployment, group int32, groupInstance string) error {
	desired := serviceForGroup(pgd, group, groupInstance)
	return c.upsertServiceObject(ctx, pgd, desired)
}

func (c *Controller) upsertTopLevelService(ctx context.Context, pgd *PodGroupDeployment) error {
	desired := serviceForDeployment(pgd)
	return c.upsertServiceObject(ctx, pgd, desired)
}

func (c *Controller) upsertServiceObject(ctx context.Context, pgd *PodGroupDeployment, desired *corev1.Service) error {
	client := c.kube.CoreV1().Services(pgd.Namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		logPGD(pgd, "creating service=%s selector=%v", desired.Name, desired.Spec.Selector)
		_, err = client.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}

	desired.ResourceVersion = current.ResourceVersion
	desired.Spec.ClusterIP = current.Spec.ClusterIP
	desired.Spec.ClusterIPs = current.Spec.ClusterIPs
	desired.Spec.IPFamilies = current.Spec.IPFamilies
	desired.Spec.IPFamilyPolicy = current.Spec.IPFamilyPolicy
	_, err = client.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

func serviceForDeployment(pgd *PodGroupDeployment) *corev1.Service {
	labels := map[string]string{
		labelManagedBy: managerName,
		labelName:      "pod-group-deployment",
		labelInstance:  pgd.Name,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            pgd.Name,
			Namespace:       pgd.Namespace,
			Labels:          labels,
			OwnerReferences: ownerRef(pgd),
		},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeClusterIP,
			Ports: servicePortsFromTemplate(&pgd.Spec.Template),
			Selector: map[string]string{
				labelInstance:      pgd.Name,
				labelServiceTarget: "true",
			},
		},
	}
}

func serviceForGroup(pgd *PodGroupDeployment, group int32, groupInstance string) *corev1.Service {
	labels := baseLabels(pgd, group, groupInstance, "")
	ports := servicePortsForGroup(pgd)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            stableGroupInstance(pgd.Name, group),
			Namespace:       pgd.Namespace,
			Labels:          labels,
			OwnerReferences: ownerRef(pgd),
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			Ports:                    ports,
			PublishNotReadyAddresses: multiNodeActive(pgd.Spec),
			Selector: map[string]string{
				labelInstance:      pgd.Name,
				labelGroupIndex:    strconv.Itoa(int(group)),
				labelGroupInstance: groupInstance,
				labelServiceTarget: "true",
			},
		},
	}
}

func servicePortsForGroup(pgd *PodGroupDeployment) []corev1.ServicePort {
	ports := servicePortsFromTemplate(&pgd.Spec.Template)
	if !multiNodeActive(pgd.Spec) {
		return ports
	}
	distInitPort := effectiveMultiNodeDistInitPort(pgd.Spec)
	for _, port := range ports {
		if port.Port == distInitPort || port.Name == "dist-init" {
			return ports
		}
	}
	ports = append(ports, corev1.ServicePort{
		Name:       "dist-init",
		Port:       distInitPort,
		Protocol:   corev1.ProtocolTCP,
		TargetPort: intstr.FromInt32(distInitPort),
	})
	sort.Slice(ports, func(i, j int) bool { return ports[i].Name < ports[j].Name })
	return ports
}

func servicePortsFromTemplate(template *corev1.PodTemplateSpec) []corev1.ServicePort {
	var ports []corev1.ServicePort
	seen := map[string]struct{}{}
	for _, container := range template.Spec.Containers {
		for _, port := range container.Ports {
			name := port.Name
			if name == "" {
				name = fmt.Sprintf("port-%d", port.ContainerPort)
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			protocol := port.Protocol
			if protocol == "" {
				protocol = corev1.ProtocolTCP
			}
			ports = append(ports, corev1.ServicePort{
				Name:       name,
				Port:       port.ContainerPort,
				Protocol:   protocol,
				TargetPort: intstr.FromInt32(port.ContainerPort),
			})
		}
	}
	if len(ports) == 0 {
		return []corev1.ServicePort{{
			Name:       "http",
			Port:       80,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(80),
		}}
	}
	sort.Slice(ports, func(i, j int) bool { return ports[i].Name < ports[j].Name })
	return ports
}
