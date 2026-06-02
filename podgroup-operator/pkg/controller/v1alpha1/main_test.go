package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func TestDefaultAndValidateSpec(t *testing.T) {
	spec, err := defaultAndValidateSpec(PodGroupDeploymentSpec{
		Groups:           3,
		ReplicasPerGroup: 4,
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "worker", Image: "busybox"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("defaultAndValidateSpec() error = %v", err)
	}
	if spec.TopologyKey != corev1.LabelHostname {
		t.Fatalf("topologyKey = %s, want %s", spec.TopologyKey, corev1.LabelHostname)
	}
	if spec.SchedulerName != defaultSchedulerName {
		t.Fatalf("schedulerName = %s, want %s", spec.SchedulerName, defaultSchedulerName)
	}
	if spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restartPolicy = %s, want Never", spec.Template.Spec.RestartPolicy)
	}
	if spec.Template.Spec.TerminationGracePeriodSeconds == nil || *spec.Template.Spec.TerminationGracePeriodSeconds != 1 {
		t.Fatalf("terminationGracePeriodSeconds = %v, want 1", spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	if spec.UpdateStrategy.RollingUpdate.MaxSurge != 0 {
		t.Fatalf("maxSurge = %d, want 0", spec.UpdateStrategy.RollingUpdate.MaxSurge)
	}
	if spec.UpdateStrategy.Type != updateStrategyInPlaceIfPossible {
		t.Fatalf("updateStrategy.type = %s, want InPlaceIfPossible", spec.UpdateStrategy.Type)
	}
	if spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent != 20 {
		t.Fatalf("maxUnavailablePercent = %d, want 20", spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent)
	}
	if spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds != defaultProgressDeadlineSeconds {
		t.Fatalf("progressDeadlineSeconds = %d, want %d", spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds, defaultProgressDeadlineSeconds)
	}
	if spec.MultiNode.DistInitPort != defaultMultiNodeDistInitPort {
		t.Fatalf("multiNode.distInitPort = %d, want %d", spec.MultiNode.DistInitPort, defaultMultiNodeDistInitPort)
	}
	if got := effectiveProgressDeadline(spec); got != time.Duration(defaultProgressDeadlineSeconds)*time.Second {
		t.Fatalf("effectiveProgressDeadline = %s, want %ds", got, defaultProgressDeadlineSeconds)
	}
}

func TestDefaultAndValidateSpecAllowsMaxUnavailableOnly(t *testing.T) {
	spec, err := defaultAndValidateSpec(PodGroupDeploymentSpec{
		Groups:           4,
		ReplicasPerGroup: 4,
		UpdateStrategy: UpdateStrategy{
			RollingUpdate: RollingUpdateStrategy{
				MaxUnavailable: 2,
			},
		},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "worker", Image: "busybox"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("defaultAndValidateSpec() error = %v", err)
	}
	if spec.UpdateStrategy.RollingUpdate.MaxSurge != 0 {
		t.Fatalf("maxSurge = %d, want 0", spec.UpdateStrategy.RollingUpdate.MaxSurge)
	}
	if spec.UpdateStrategy.RollingUpdate.MaxUnavailable != 2 {
		t.Fatalf("maxUnavailable = %d, want 2", spec.UpdateStrategy.RollingUpdate.MaxUnavailable)
	}
	if effectiveMaxUnavailable(spec) != 2 {
		t.Fatalf("effectiveMaxUnavailable = %d, want 2", effectiveMaxUnavailable(spec))
	}
}

func TestDefaultAndValidateSpecDefaultsMaxUnavailablePercent(t *testing.T) {
	spec, err := defaultAndValidateSpec(PodGroupDeploymentSpec{
		Groups:           18,
		ReplicasPerGroup: 1,
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "worker", Image: "busybox"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("defaultAndValidateSpec() error = %v", err)
	}
	if got := effectiveMaxUnavailable(spec); got != 3 {
		t.Fatalf("effectiveMaxUnavailable = %d, want 3", got)
	}
}

func TestDefaultAndValidateSpecRejectsInvalidUpdateStrategyType(t *testing.T) {
	_, err := defaultAndValidateSpec(PodGroupDeploymentSpec{
		Groups:           1,
		ReplicasPerGroup: 1,
		UpdateStrategy: UpdateStrategy{
			Type: UpdateStrategyType("Invalid"),
		},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "worker", Image: "busybox"}},
			},
		},
	})
	if err == nil {
		t.Fatal("expected invalid update strategy type to fail validation")
	}
}

func TestPodForGroupUsesCoschedulingLabelAndAffinity(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.TopologyKey = "ds.coreweave.com/nvlink.domain"
	pod := podForGroup(pgd, 2, 3, "inference-aabbccdd-2", "aabbccdd")

	if pod.Name != "inference-aabbccdd-2-3" {
		t.Fatalf("pod name = %s, want inference-aabbccdd-2-3", pod.Name)
	}
	if pod.Spec.SchedulerName != defaultSchedulerName {
		t.Fatalf("schedulerName = %s, want %s", pod.Spec.SchedulerName, defaultSchedulerName)
	}
	if got := pod.Labels[labelSchedulerPG]; got != "inference-aabbccdd-2" {
		t.Fatalf("scheduler podgroup label = %s, want inference-aabbccdd-2", got)
	}
	if got := pod.Labels[labelServiceTarget]; got != "false" {
		t.Fatalf("service target label = %s, want false", got)
	}
	if got := pod.Labels[labelRevision]; got != "aabbccdd" {
		t.Fatalf("revision label = %s, want aabbccdd", got)
	}

	terms := pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("required affinity terms = %d, want 1", len(terms))
	}
	term := terms[0]
	if term.TopologyKey != "ds.coreweave.com/nvlink.domain" {
		t.Fatalf("topologyKey = %s, want ds.coreweave.com/nvlink.domain", term.TopologyKey)
	}
	if term.LabelSelector.MatchLabels[labelInstance] != "inference" || term.LabelSelector.MatchLabels[labelGroupInstance] != "inference-aabbccdd-2" {
		t.Fatalf("affinity selector = %#v", term.LabelSelector.MatchLabels)
	}
}

func TestPodForGroupMarksServiceTargetOrdinal(t *testing.T) {
	pgd := testPodGroupDeployment()
	pod := podForGroup(pgd, 1, 0, "inference-1", "aabbccdd")
	if got := pod.Labels[labelServiceTarget]; got != "true" {
		t.Fatalf("service target label = %s, want true", got)
	}
	if got := pod.Annotations[annotationRecreateRevision]; got == "" {
		t.Fatal("expected recreate revision annotation")
	}
}

func TestPodForGroupCarriesRolloutLabels(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Labels = map[string]string{
		labelRollout:     "inference",
		labelRolloutRole: "decode",
	}

	pod := podForGroup(pgd, 1, 0, "inference-aabbccdd-1", "aabbccdd")
	if got := pod.Labels[labelRollout]; got != "inference" {
		t.Fatalf("rollout label = %q, want inference", got)
	}
	if got := pod.Labels[labelRolloutRole]; got != "decode" {
		t.Fatalf("rollout role label = %q, want decode", got)
	}
}

func TestPodForGroupInjectsMultiNodeArgs(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.ReplicasPerGroup = 2
	pgd.Spec.MultiNode.Enabled = true
	pgd.Spec.Template.Spec.Containers[0].Command = []string{
		"python3",
		"-m",
		"sglang_meta.launch_server",
		"--tp-size",
		"8",
	}

	pod := podForGroup(pgd, 1, 1, "inference-new-1", "new")
	command := pod.Spec.Containers[0].Command
	if got, ok := flagValue(command, multiNodeArgNNodes); !ok || got != "2" {
		t.Fatalf("--nnodes = %q/%v, want 2/true in command %#v", got, ok, command)
	}
	if got, ok := flagValue(command, multiNodeArgNodeRank); !ok || got != "1" {
		t.Fatalf("--node-rank = %q/%v, want 1/true in command %#v", got, ok, command)
	}
	wantAddr := "inference-1.demo.svc.cluster.local:20000"
	if got, ok := flagValue(command, multiNodeArgDistInitAddr); !ok || got != wantAddr {
		t.Fatalf("--dist-init-addr = %q/%v, want %s/true in command %#v", got, ok, wantAddr, command)
	}
}

func TestPodForGroupDoesNotInjectMultiNodeArgsForTP4(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.ReplicasPerGroup = 2
	pgd.Spec.MultiNode.Enabled = true
	pgd.Spec.Template.Spec.Containers[0].Command = []string{
		"python3",
		"-m",
		"sglang_meta.launch_server",
		"--tp-size",
		"4",
	}

	pod := podForGroup(pgd, 1, 1, "inference-new-1", "new")
	if _, ok := flagValue(pod.Spec.Containers[0].Command, multiNodeArgNNodes); ok {
		t.Fatalf("did not expect multi-node args for tp-size 4: %#v", pod.Spec.Containers[0].Command)
	}
}

func TestPodForGroupUpdatesExistingMultiNodeArgs(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.ReplicasPerGroup = 4
	pgd.Spec.MultiNode.Enabled = true
	pgd.Spec.MultiNode.TensorParallelSize = 16
	pgd.Spec.MultiNode.DistInitPort = 23456
	pgd.Spec.Template.Spec.Containers[0].Args = []string{
		"--nnodes",
		"1",
		"--node-rank=0",
		"--dist-init-addr",
		"old:1",
	}

	pod := podForGroup(pgd, 2, 3, "inference-new-2", "new")
	args := pod.Spec.Containers[0].Args
	if got, ok := flagValue(args, multiNodeArgNNodes); !ok || got != "4" {
		t.Fatalf("--nnodes = %q/%v, want 4/true in args %#v", got, ok, args)
	}
	if got, ok := flagValue(args, multiNodeArgNodeRank); !ok || got != "3" {
		t.Fatalf("--node-rank = %q/%v, want 3/true in args %#v", got, ok, args)
	}
	wantAddr := "inference-2.demo.svc.cluster.local:23456"
	if got, ok := flagValue(args, multiNodeArgDistInitAddr); !ok || got != wantAddr {
		t.Fatalf("--dist-init-addr = %q/%v, want %s/true in args %#v", got, ok, wantAddr, args)
	}
}

func TestPodNeedsRecreateUsesRecreateRevision(t *testing.T) {
	current := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{annotationRecreateRevision: "same"},
	}}
	desired := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{annotationRecreateRevision: "same"},
	}}
	if podNeedsRecreate(current, desired) {
		t.Fatal("expected matching recreate revision to keep existing pod")
	}

	desired.Annotations[annotationRecreateRevision] = "different"
	if !podNeedsRecreate(current, desired) {
		t.Fatal("expected recreate revision change to recreate pod")
	}
}

func TestTemplateRecreateRevisionIgnoresImageAndMetadata(t *testing.T) {
	before := testPodGroupDeployment().Spec.Template
	after := *before.DeepCopy()
	after.Labels = map[string]string{"version": "v2"}
	after.Annotations = map[string]string{"rollout": "candidate"}
	after.Spec.Containers[0].Image = "busybox:2"

	beforeRevision, err := templateRecreateRevision(&before)
	if err != nil {
		t.Fatalf("templateRecreateRevision(before) error = %v", err)
	}
	afterRevision, err := templateRecreateRevision(&after)
	if err != nil {
		t.Fatalf("templateRecreateRevision(after) error = %v", err)
	}
	if beforeRevision != afterRevision {
		t.Fatalf("recreate revision changed for image/metadata update: %s != %s", beforeRevision, afterRevision)
	}

	after.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "MODEL", Value: "new"}}
	envRevision, err := templateRecreateRevision(&after)
	if err != nil {
		t.Fatalf("templateRecreateRevision(env) error = %v", err)
	}
	if envRevision == beforeRevision {
		t.Fatal("expected recreate revision to change for env update")
	}
}

func TestDeploymentRevisionIncludesEnabledMultiNodeConfig(t *testing.T) {
	pgd := testPodGroupDeployment()
	templateOnlyRevision, err := templateRevision(&pgd.Spec.Template)
	if err != nil {
		t.Fatalf("templateRevision() error = %v", err)
	}
	disabledRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatalf("deploymentRevision(disabled) error = %v", err)
	}
	if disabledRevision != templateOnlyRevision {
		t.Fatalf("disabled MultiNode revision = %s, want template revision %s", disabledRevision, templateOnlyRevision)
	}

	pgd.Spec.MultiNode.Enabled = true
	pgd.Spec.MultiNode.TensorParallelSize = 8
	enabledRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatalf("deploymentRevision(enabled) error = %v", err)
	}
	if enabledRevision == templateOnlyRevision {
		t.Fatal("expected enabled MultiNode config to affect deployment revision")
	}
}

func TestGroupInPlaceUpdateEligible(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.ReplicasPerGroup = 2
	recreateRevision, err := templateRecreateRevision(&pgd.Spec.Template)
	if err != nil {
		t.Fatalf("templateRecreateRevision() error = %v", err)
	}
	pods := []corev1.Pod{
		testInPlacePod(0, "old", recreateRevision),
		testInPlacePod(1, "old", recreateRevision),
	}
	if !groupInPlaceUpdateEligible(pgd, 0, "inference-old-0", pods, recreateRevision) {
		t.Fatal("expected image-only group to be in-place eligible")
	}
	pods[1].Annotations[annotationRecreateRevision] = "different"
	if groupInPlaceUpdateEligible(pgd, 0, "inference-old-0", pods, recreateRevision) {
		t.Fatal("expected recreate-revision mismatch to be ineligible")
	}
}

func TestGroupInPlaceUpdateEligibleAllowsKubeInjectedContainerFields(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.ReplicasPerGroup = 1
	recreateRevision, err := templateRecreateRevision(&pgd.Spec.Template)
	if err != nil {
		t.Fatalf("templateRecreateRevision() error = %v", err)
	}
	pod := testInPlacePod(0, "old", recreateRevision)
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
		Name:      "kube-api-access-test",
		MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
		ReadOnly:  true,
	}}

	if !groupInPlaceUpdateEligible(pgd, 0, "inference-old-0", []corev1.Pod{pod}, recreateRevision) {
		t.Fatal("expected kube-injected container fields to stay in-place eligible")
	}
}

func TestServiceForGroupSelectsOnlyTargetPod(t *testing.T) {
	pgd := testPodGroupDeployment()
	service := serviceForGroup(pgd, 1, "inference-aabbccdd-1")

	if service.Name != "inference-1" {
		t.Fatalf("service name = %s, want inference-1", service.Name)
	}
	if got := service.Spec.Selector[labelServiceTarget]; got != "true" {
		t.Fatalf("service target selector = %s, want true", got)
	}
	if got := service.Spec.Selector[labelGroupIndex]; got != "1" {
		t.Fatalf("group selector = %s, want 1", got)
	}
	if got := service.Spec.Selector[labelGroupInstance]; got != "inference-aabbccdd-1" {
		t.Fatalf("group instance selector = %s, want inference-aabbccdd-1", got)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 8080 {
		t.Fatalf("service ports = %#v, want one port 8080", service.Spec.Ports)
	}
	if service.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Fatalf("service protocol = %s, want TCP", service.Spec.Ports[0].Protocol)
	}
}

func TestServiceForGroupSupportsMultiNodeBootstrap(t *testing.T) {
	pgd := testPodGroupDeployment()
	pgd.Spec.ReplicasPerGroup = 2
	pgd.Spec.MultiNode.Enabled = true
	pgd.Spec.MultiNode.TensorParallelSize = 8

	service := serviceForGroup(pgd, 1, "inference-aabbccdd-1")
	if !service.Spec.PublishNotReadyAddresses {
		t.Fatal("expected group service to publish not-ready rank0 address for MultiNode bootstrap")
	}
	found := false
	for _, port := range service.Spec.Ports {
		if port.Name == "dist-init" && port.Port == defaultMultiNodeDistInitPort {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected dist-init service port, got %#v", service.Spec.Ports)
	}
}

func TestServiceForDeploymentSelectsAllGroupTargetPods(t *testing.T) {
	pgd := testPodGroupDeployment()
	service := serviceForDeployment(pgd)

	if service.Name != "inference" {
		t.Fatalf("service name = %s, want inference", service.Name)
	}
	if got := service.Spec.Selector[labelInstance]; got != "inference" {
		t.Fatalf("instance selector = %s, want inference", got)
	}
	if got := service.Spec.Selector[labelServiceTarget]; got != "true" {
		t.Fatalf("service target selector = %s, want true", got)
	}
	if _, ok := service.Spec.Selector[labelGroupIndex]; ok {
		t.Fatalf("top-level service must not select a single group: %#v", service.Spec.Selector)
	}
	if _, ok := service.Spec.Selector[labelGroupInstance]; ok {
		t.Fatalf("top-level service must not select a single group instance: %#v", service.Spec.Selector)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 8080 {
		t.Fatalf("service ports = %#v, want one port 8080", service.Spec.Ports)
	}
}

func TestPodOwnedByCurrentSpec(t *testing.T) {
	pgd := testPodGroupDeployment()
	allowedInstances := map[string]struct{}{
		"inference-2":          {},
		"inference-aabbccdd-2": {},
	}
	labels := map[string]string{
		labelInstance:      "inference",
		labelGroupIndex:    "2",
		labelGroupInstance: "inference-aabbccdd-2",
		labelOrdinal:       "3",
	}
	if !podOwnedByCurrentSpec(pgd, labels, allowedInstances) {
		t.Fatal("expected pod labels to be inside current spec")
	}
	labels[labelOrdinal] = "4"
	if podOwnedByCurrentSpec(pgd, labels, allowedInstances) {
		t.Fatal("expected ordinal 4 to be outside replicasPerGroup=4")
	}
	labels[labelOrdinal] = "3"
	labels[labelGroupInstance] = "inference-stale-2"
	if podOwnedByCurrentSpec(pgd, labels, allowedInstances) {
		t.Fatal("expected stale group instance to be outside current spec")
	}
}

func TestAllowedGroupInstancesIncludesActiveAndDesiredReplacement(t *testing.T) {
	pgd := testPodGroupDeployment()
	podsByInstance := map[string][]corev1.Pod{
		"inference-0":          {testRevisionPod("oldrev")},
		"inference-previous-1": {testRevisionPod("oldrev")},
	}
	allowed := allowedGroupInstances(pgd, "aabbccdd", map[int32]string{
		0: "inference-0",
		1: "inference-previous-1",
	}, podsByInstance)

	for _, instance := range []string{
		"inference-0",
		"inference-aabbccdd-0",
		"inference-previous-1",
		"inference-aabbccdd-1",
	} {
		if !isAllowedGroupInstance(allowed, instance) {
			t.Fatalf("expected %s to be allowed", instance)
		}
	}
	if isAllowedGroupInstance(allowed, "inference-old-1") {
		t.Fatal("expected stale replacement instance to be disallowed")
	}
}

func TestAllowedGroupInstancesDropsReplacementAfterStableActiveMatchesRevision(t *testing.T) {
	pgd := testPodGroupDeployment()
	allowed := allowedGroupInstances(pgd, "aabbccdd", map[int32]string{
		0: "inference-aabbccdd-0",
	}, map[string][]corev1.Pod{
		"inference-0":          {testRevisionPod("aabbccdd")},
		"inference-aabbccdd-0": {testRevisionPod("aabbccdd")},
	})

	if !isAllowedGroupInstance(allowed, "inference-aabbccdd-0") {
		t.Fatal("expected active revisioned group to be allowed")
	}
	if isAllowedGroupInstance(allowed, "inference-0") {
		t.Fatal("expected duplicate stable group to be disallowed once service points at revisioned group")
	}
}

func TestGroupInstanceDisrupted(t *testing.T) {
	pods := []corev1.Pod{
		testGroupPod(0, corev1.PodRunning, false),
		testGroupPod(1, corev1.PodRunning, false),
		testGroupPod(2, corev1.PodRunning, false),
		testGroupPod(3, corev1.PodRunning, false),
	}
	if groupInstanceDisrupted(pods, 4) {
		t.Fatal("expected complete running group to be healthy")
	}
	if !groupInstanceDisrupted(pods[:3], 4) {
		t.Fatal("expected missing pod to disrupt the whole group")
	}
	pods[2] = testGroupPod(2, corev1.PodRunning, true)
	if !groupInstanceDisrupted(pods, 4) {
		t.Fatal("expected terminating pod to disrupt the whole group")
	}
	pods[2] = testGroupPod(2, corev1.PodFailed, false)
	if !groupInstanceDisrupted(pods, 4) {
		t.Fatal("expected failed pod to disrupt the whole group")
	}
	pods[2] = testGroupPod(9, corev1.PodRunning, false)
	if !groupInstanceDisrupted(pods, 4) {
		t.Fatal("expected invalid ordinal to disrupt the whole group")
	}
}

func TestGroupReadyRequiresPodReadyCondition(t *testing.T) {
	pods := []corev1.Pod{
		testGroupPod(0, corev1.PodRunning, false),
		testGroupPod(1, corev1.PodRunning, false),
	}
	if groupReady(pods, 2) {
		t.Fatal("expected running pods without Ready condition to be unready")
	}

	pods[0] = markPodReady(pods[0])
	if groupReady(pods, 2) {
		t.Fatal("expected partial Ready condition to be unready")
	}

	pods[1] = markPodReady(pods[1])
	if !groupReady(pods, 2) {
		t.Fatal("expected all Ready pods to make the group ready")
	}
}

func TestGroupReadyRequiresExactOrdinals(t *testing.T) {
	pods := []corev1.Pod{
		markPodReady(testGroupPod(0, corev1.PodRunning, false)),
		markPodReady(testGroupPod(0, corev1.PodRunning, false)),
	}
	if groupReady(pods, 2) {
		t.Fatal("expected duplicate ordinals to prevent group readiness")
	}

	pods = append(pods, markPodReady(testGroupPod(1, corev1.PodRunning, false)))
	if groupReady(pods, 2) {
		t.Fatal("expected extra pods to prevent group readiness")
	}
}

func TestTopologyValueForPod(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node-a"}}
	nodeValues := map[string]string{"node-a": "domain-1"}
	if got := topologyValueForPod(pod, nodeValues); got != "domain-1" {
		t.Fatalf("topologyValueForPod() = %s, want domain-1", got)
	}
	pod.Spec.NodeName = "node-b"
	if got := topologyValueForPod(pod, nodeValues); got != "node-b" {
		t.Fatalf("topologyValueForPod() = %s, want node-b", got)
	}
	pod.Spec.NodeName = ""
	if got := topologyValueForPod(pod, nodeValues); got != "" {
		t.Fatalf("topologyValueForPod() = %s, want empty", got)
	}
}

func TestRolloutInstanceNamePutsRevisionBeforeGroup(t *testing.T) {
	got := revisionedGroupInstance("gpu-one", 0, "8a80854d")
	if got != "gpu-one-8a80854d-0" {
		t.Fatalf("revisionedGroupInstance() = %s, want gpu-one-8a80854d-0", got)
	}
}

func TestRolloutMembersForMode(t *testing.T) {
	disagg, err := rolloutMembersForMode("qwen", "disagg")
	if err != nil {
		t.Fatalf("rolloutMembersForMode(disagg) error = %v", err)
	}
	if len(disagg) != 2 || disagg[0].Role != "prefill" || disagg[0].Name != "qwen-prefill" || disagg[1].Role != "decode" || disagg[1].Name != "qwen-decode" {
		t.Fatalf("disagg members = %#v", disagg)
	}

	unified, err := rolloutMembersForMode("qwen", "unified")
	if err != nil {
		t.Fatalf("rolloutMembersForMode(unified) error = %v", err)
	}
	if len(unified) != 1 || unified[0].Role != "unified" || unified[0].Name != "qwen-unified" {
		t.Fatalf("unified members = %#v", unified)
	}
	if _, err := rolloutMembersForMode("qwen", "custom"); err == nil {
		t.Fatal("expected custom mode to fail")
	}
}

func TestDesiredRolloutChildrenDisagg(t *testing.T) {
	prefill := testPodGroupDeploymentWithName("prefill", 8).Spec
	decode := testPodGroupDeploymentWithName("decode", 4).Spec
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo"},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode:    "disagg",
			Prefill: &prefill,
			Decode:  &decode,
		},
	}

	children, err := desiredRolloutChildren(pgdr)
	if err != nil {
		t.Fatalf("desiredRolloutChildren() error = %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("children = %d, want 2", len(children))
	}
	if children[0].member.Role != "prefill" || children[0].member.Name != "qwen-prefill" || children[0].spec.Groups != 8 {
		t.Fatalf("prefill child = %#v", children[0])
	}
	if children[1].member.Role != "decode" || children[1].member.Name != "qwen-decode" || children[1].spec.Groups != 4 {
		t.Fatalf("decode child = %#v", children[1])
	}
	if children[0].spec.SchedulerName != defaultSchedulerName || children[1].spec.SchedulerName != defaultSchedulerName {
		t.Fatalf("expected child specs to be defaulted")
	}
	if children[0].spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent != defaultMaxUnavailablePercent || children[1].spec.UpdateStrategy.RollingUpdate.MaxUnavailablePercent != defaultMaxUnavailablePercent {
		t.Fatalf("PGDR-managed child specs should render API defaults to avoid update churn: %#v %#v", children[0].spec.UpdateStrategy, children[1].spec.UpdateStrategy)
	}
	for _, child := range children {
		if child.spec.UpdateStrategy.Type != updateStrategyInPlaceIfPossible {
			t.Fatalf("%s updateStrategy.type = %q, want API default", child.member.Name, child.spec.UpdateStrategy.Type)
		}
		if child.spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds != defaultProgressDeadlineSeconds {
			t.Fatalf("%s progressDeadlineSeconds = %d, want %d", child.member.Name, child.spec.UpdateStrategy.RollingUpdate.ProgressDeadlineSeconds, defaultProgressDeadlineSeconds)
		}
		if child.spec.MultiNode.DistInitPort != defaultMultiNodeDistInitPort {
			t.Fatalf("%s distInitPort = %d, want %d", child.member.Name, child.spec.MultiNode.DistInitPort, defaultMultiNodeDistInitPort)
		}
		if child.spec.TopologyKey != defaultTopologyKey || child.spec.SchedulerName != defaultSchedulerName || child.spec.ServiceTargetOrdinal != 0 {
			t.Fatalf("%s defaults = topology %q scheduler %q service target %d", child.member.Name, child.spec.TopologyKey, child.spec.SchedulerName, child.spec.ServiceTargetOrdinal)
		}
	}
}

func TestPodGroupDeploymentDefaultsMatchCRDDefaults(t *testing.T) {
	expected := []string{
		"default: InPlaceIfPossible",
		fmt.Sprintf("default: %d", defaultMaxUnavailablePercent),
		fmt.Sprintf("default: %d", defaultProgressDeadlineSeconds),
		"default: false",
		fmt.Sprintf("default: %d", defaultMultiNodeDistInitPort),
		"default: kubernetes.io/hostname",
		"default: 0",
		"default: default-scheduler",
	}
	for _, path := range []string{
		"../../../charts/podgroup-operator/templates/crd-podgroupdeployment.yaml",
		"../../../manifests/crd.yaml",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, want := range expected {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing %q", path, want)
			}
		}
	}
}

func TestRolloutChildMatchesAPIDefaultedSpec(t *testing.T) {
	desired := testPodGroupDeploymentWithName("qwen-prefill", 8)
	defaultPGDSpec(t, desired)

	current := desired.DeepCopy()
	current.Spec.UpdateStrategy = UpdateStrategy{}
	current.Spec.MultiNode = MultiNodeSpec{}
	current.Spec.TopologyKey = ""
	current.Spec.SchedulerName = ""
	current.Spec.Template.Spec.RestartPolicy = ""
	current.Spec.Template.Spec.TerminationGracePeriodSeconds = nil

	if !rolloutChildMatches(current, desired) {
		t.Fatalf("rolloutChildMatches() = false for semantically equal defaulted specs\ncurrent=%#v\ndesired=%#v", current.Spec, desired.Spec)
	}

	current.Spec.MultiNode.DistInitPort = 30000
	if rolloutChildMatches(current, desired) {
		t.Fatal("rolloutChildMatches() ignored non-default distInitPort drift")
	}
}

func TestInitialRolloutGateAnnotationsHoldNewChildren(t *testing.T) {
	pgdr := &PodGroupDeploymentRollout{ObjectMeta: metav1.ObjectMeta{Name: "qwen"}}
	annotations := initialRolloutGateAnnotations(pgdr)
	if annotations[annotationRollout] != "qwen" {
		t.Fatalf("rollout annotation = %q, want qwen", annotations[annotationRollout])
	}
	if annotations[annotationRolloutAction] != rolloutActionHold {
		t.Fatalf("rollout action = %q, want hold", annotations[annotationRolloutAction])
	}
	if annotations[annotationRolloutAllowedStart] != "0" || annotations[annotationRolloutAllowedEnd] != "1" {
		t.Fatalf("rollout window = [%s,%s), want [0,1)", annotations[annotationRolloutAllowedStart], annotations[annotationRolloutAllowedEnd])
	}
}

func TestRolloutChildNeedsInitialGateForUngatedSpecChange(t *testing.T) {
	current := testPodGroupDeploymentWithName("qwen-prefill", 8)
	desired := current.DeepCopy()
	desired.Spec.Template.Annotations = map[string]string{
		"podgroup.meta.com/rollout-trigger": "next",
	}

	if !rolloutChildNeedsInitialGate(current, desired) {
		t.Fatal("expected ungated child spec update to require an initial rollout gate")
	}
}

func TestRolloutChildNeedsInitialGatePreservesActiveGate(t *testing.T) {
	current := testPodGroupDeploymentWithName("qwen-prefill", 8)
	current.Annotations = map[string]string{
		annotationRollout:             "qwen",
		annotationRolloutAllowedStart: "0",
		annotationRolloutAllowedEnd:   "1",
		annotationRolloutWave:         "0",
		annotationRolloutAction:       rolloutActionCreateNew,
		annotationRolloutPhase:        rolloutPhaseCreatePrefill,
	}
	desired := current.DeepCopy()
	desired.Spec.Template.Annotations = map[string]string{
		"podgroup.meta.com/rollout-trigger": "next",
	}

	if rolloutChildNeedsInitialGate(current, desired) {
		t.Fatal("active rollout gate should be preserved instead of replaced")
	}
}

func TestUpsertRolloutChildAppliesInitialGateBeforeSpecUpdate(t *testing.T) {
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo", UID: types.UID("rollout-uid")},
	}
	current := testPodGroupDeploymentWithName("qwen-prefill", 8)
	current.Namespace = pgdr.Namespace
	current.Labels = map[string]string{
		labelManagedBy:   managerName,
		labelRollout:     pgdr.Name,
		labelRolloutRole: "prefill",
	}
	current.Spec.Template.Annotations = map[string]string{"version": "old"}
	current.Status.CurrentRevision = "old"
	obj, err := podGroupDeploymentToUnstructured(current)
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestControllerWithDynamicObjects(t, obj)

	desiredSpec := current.Spec
	desiredSpec.Template.Annotations = map[string]string{"version": "new"}
	child := rolloutChild{
		member: rolloutMember{Role: "prefill", Name: current.Name},
		spec:   desiredSpec,
	}
	changed, err := controller.upsertRolloutChild(context.Background(), pgdr, child)
	if err != nil {
		t.Fatalf("upsertRolloutChild() error = %v", err)
	}
	if !changed {
		t.Fatal("expected upsertRolloutChild to report metadata change")
	}

	updatedObj, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), current.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := decodePodGroupDeployment(updatedObj)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Template.Annotations["version"] != "old" {
		t.Fatalf("spec version = %q, want old until gate is active", updated.Spec.Template.Annotations["version"])
	}
	if !rolloutGateFromAnnotations(updated.Annotations).enabled {
		t.Fatalf("rollout gate was not applied: %#v", updated.Annotations)
	}
	if updated.Status.CurrentRevision != "old" {
		t.Fatalf("status currentRevision = %q, want old", updated.Status.CurrentRevision)
	}
}

func TestRolloutStopsAfterApplyingInitialChildGate(t *testing.T) {
	pgdr := &PodGroupDeploymentRollout{
		TypeMeta: metav1.TypeMeta{APIVersion: operatorAPIVersion, Kind: operatorRolloutKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "qwen",
			Namespace:  "demo",
			UID:        types.UID("rollout-uid"),
			Finalizers: []string{finalizerPodGroupDeploymentRollout},
		},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode: "unified",
		},
	}
	current := testPodGroupDeploymentWithName("qwen-unified", 1)
	current.Namespace = pgdr.Namespace
	current.Generation = 4
	markPGDRManaged(current, pgdr.Name, "unified")
	current.OwnerReferences = rolloutOwnerRef(pgdr)
	current.Spec.Template.Annotations = map[string]string{"version": "old"}
	defaultPGDSpec(t, current)
	oldRevision, err := deploymentRevision(&current.Spec)
	if err != nil {
		t.Fatal(err)
	}
	current.Status.ObservedGeneration = current.Generation
	current.Status.CurrentRevision = oldRevision
	current.Status.Groups = []GroupStatus{{Index: 0, Revision: oldRevision, Phase: "Ready"}}

	desiredSpec := current.Spec.DeepCopy()
	desiredSpec.Template.Annotations = map[string]string{"version": "new"}
	pgdr.Spec.Unified = &desiredSpec
	pgdrObj, err := podGroupDeploymentRolloutToUnstructured(pgdr)
	if err != nil {
		t.Fatal(err)
	}
	currentObj, err := podGroupDeploymentToUnstructured(current)
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestControllerWithDynamicObjectsList(t, []runtime.Object{pgdrObj, currentObj})

	if _, err := controller.reconcileOneRollout(context.Background(), pgdr); err != nil {
		t.Fatalf("reconcileOneRollout() error = %v", err)
	}

	updatedObj, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), current.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := decodePodGroupDeployment(updatedObj)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Template.Annotations["version"] != "old" {
		t.Fatalf("spec version = %q, want old until next reconcile", updated.Spec.Template.Annotations["version"])
	}
	gate := rolloutGateFromAnnotations(updated.Annotations)
	if !gate.enabled || gate.action != rolloutActionHold {
		t.Fatalf("initial gate = %#v, want hold", gate)
	}
	statusObj, err := controller.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(pgdr.Namespace).Get(context.Background(), pgdr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedRollout, err := decodePodGroupDeploymentRollout(statusObj)
	if err != nil {
		t.Fatal(err)
	}
	if updatedRollout.Status.Phase != "Blocked" || updatedRollout.Status.CurrentPhase != "ReconcilingChildren" {
		t.Fatalf("rollout status = %#v, want blocked reconciling children", updatedRollout.Status)
	}
}

func TestRolloutBlocksOnStaleChildGeneration(t *testing.T) {
	pgdr := &PodGroupDeploymentRollout{
		TypeMeta: metav1.TypeMeta{APIVersion: operatorAPIVersion, Kind: operatorRolloutKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "qwen",
			Namespace:  "demo",
			Finalizers: []string{finalizerPodGroupDeploymentRollout},
		},
		Spec: PodGroupDeploymentRolloutSpec{Mode: "unified"},
	}
	pgd := testPodGroupDeploymentWithName("qwen-unified", 1)
	pgd.Namespace = pgdr.Namespace
	pgd.Generation = 5
	pgd.Status.ObservedGeneration = 4
	markPGDRManaged(pgd, pgdr.Name, "unified")
	defaultPGDSpec(t, pgd)
	desiredRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	pgd.Status.CurrentRevision = desiredRevision
	pgd.Status.Groups = []GroupStatus{{Index: 0, Revision: desiredRevision, Phase: "Ready"}}
	pgdrObj, err := podGroupDeploymentRolloutToUnstructured(pgdr)
	if err != nil {
		t.Fatal(err)
	}
	pgdObj, err := podGroupDeploymentToUnstructured(pgd)
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestControllerWithDynamicObjectsList(t, []runtime.Object{pgdrObj, pgdObj})

	if _, err := controller.reconcileOneRollout(context.Background(), pgdr); err != nil {
		t.Fatalf("reconcileOneRollout() error = %v", err)
	}

	statusObj, err := controller.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(pgdr.Namespace).Get(context.Background(), pgdr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedRollout, err := decodePodGroupDeploymentRollout(statusObj)
	if err != nil {
		t.Fatal(err)
	}
	if updatedRollout.Status.Phase != "Blocked" {
		t.Fatalf("rollout phase = %s, want Blocked", updatedRollout.Status.Phase)
	}
	if len(updatedRollout.Status.Members) != 1 {
		t.Fatalf("members = %d, want 1", len(updatedRollout.Status.Members))
	}
	member := updatedRollout.Status.Members[0]
	if member.Generation != 5 || member.ObservedGeneration != 4 || member.Action != rolloutActionHold || member.Phase != "Blocked" {
		t.Fatalf("member status = %#v, want stale hold", member)
	}
	updatedPGDObj, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), pgd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedPGD, err := decodePodGroupDeployment(updatedPGDObj)
	if err != nil {
		t.Fatal(err)
	}
	gate := rolloutGateFromAnnotations(updatedPGD.Annotations)
	if !gate.enabled || gate.action != rolloutActionHold || gate.pausedReason == "" {
		t.Fatalf("stale generation gate = %#v, want hold with paused reason", gate)
	}
}

func TestStandalonePGDWithoutGateCreatesInitialGroup(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("standalone", 1)
	pgd.Spec.ReplicasPerGroup = 1
	controller := newTestController(t, pgd)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	pods, err := controller.kube.CoreV1().Pods(pgd.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want 1", len(pods.Items))
	}
}

func TestPGDFirstReconcileAddsFinalizerBeforeChildren(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("standalone", 1)
	pgd.Finalizers = nil
	pgd.Spec.ReplicasPerGroup = 1
	controller := newTestController(t, pgd)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	obj, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(context.Background(), pgd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasString(obj.GetFinalizers(), finalizerPodGroupDeployment) {
		t.Fatalf("finalizers = %#v, want PGD finalizer", obj.GetFinalizers())
	}
	pods, err := controller.kube.CoreV1().Pods(pgd.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want no children until finalizer is persisted", len(pods.Items))
	}
}

func TestDeletingPGDSkipsChildReconcile(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("deleting", 1)
	pgd.Spec.ReplicasPerGroup = 1
	now := metav1.Now()
	pgd.DeletionTimestamp = &now
	controller := newTestController(t, pgd)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	pods, err := controller.kube.CoreV1().Pods(pgd.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want 0", len(pods.Items))
	}
	services, err := controller.kube.CoreV1().Services(pgd.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(services.Items) != 0 {
		t.Fatalf("services = %d, want 0", len(services.Items))
	}
}

func TestPGDFinalizerDeletesChildrenBeforeRemoval(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("finalizing", 1)
	pgd.Spec.ReplicasPerGroup = 1
	defaultPGDSpec(t, pgd)
	pgd.Finalizers = []string{finalizerPodGroupDeployment}
	now := metav1.Now()
	pgd.DeletionTimestamp = &now
	revision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	instance := revisionedGroupInstance(pgd.Name, 0, revision)
	pod := podForGroup(pgd, 0, 0, instance, revision)
	service := serviceForGroup(pgd, 0, instance)
	schedulerPG := testSchedulerPodGroup(pgd, 0, instance, revision)
	controller := newTestControllerWithDynamicObjectsList(t, []runtime.Object{mustPodGroupDeploymentObject(t, pgd), schedulerPG}, pod, service)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("pod %s still exists", pod.Name)
	}
	if _, err := controller.kube.CoreV1().Services(pgd.Namespace).Get(context.Background(), service.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("service %s still exists", service.Name)
	}
	if _, err := controller.dynamic.Resource(schedulerPodGroupGVR).Namespace(pgd.Namespace).Get(context.Background(), schedulerPG.GetName(), metav1.GetOptions{}); err == nil {
		t.Fatalf("scheduler podgroup %s still exists", schedulerPG.GetName())
	}
	obj, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(context.Background(), pgd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasString(obj.GetFinalizers(), finalizerPodGroupDeployment) {
		t.Fatalf("finalizer was not removed: %#v", obj.GetFinalizers())
	}
}

func TestPGDRManagedPGDWithoutGateDoesNotCreateInitialGroup(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("managed", 1)
	pgd.Spec.ReplicasPerGroup = 1
	markPGDRManaged(pgd, "rollout", "decode")
	controller := newTestController(t, pgd)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	pods, err := controller.kube.CoreV1().Pods(pgd.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want 0", len(pods.Items))
	}
}

func TestPGDRManagedPGDWithoutGateDeletesUngatedReplacement(t *testing.T) {
	oldPGD := testPodGroupDeploymentWithName("managed", 1)
	oldPGD.Spec.ReplicasPerGroup = 1
	markPGDRManaged(oldPGD, "rollout", "prefill")
	defaultPGDSpec(t, oldPGD)
	oldRevision, err := deploymentRevision(&oldPGD.Spec)
	if err != nil {
		t.Fatal(err)
	}
	oldInstance := revisionedGroupInstance(oldPGD.Name, 0, oldRevision)

	pgd := oldPGD.DeepCopy()
	pgd.Spec.Template.Annotations = map[string]string{"podgroup.meta.com/rollout-trigger": "next"}
	defaultPGDSpec(t, pgd)
	desiredRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	desiredInstance := revisionedGroupInstance(pgd.Name, 0, desiredRevision)

	oldPod := podForGroup(pgd, 0, 0, oldInstance, oldRevision)
	replacementPod := podForGroup(pgd, 0, 0, desiredInstance, desiredRevision)
	activeService := serviceForGroup(pgd, 0, oldInstance)
	controller := newTestController(t, pgd, oldPod, replacementPod, activeService)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), replacementPod.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("ungated replacement pod %s still exists", replacementPod.Name)
	}
	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), oldPod.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("old active pod was not preserved: %v", err)
	}
	service, err := controller.kube.CoreV1().Services(pgd.Namespace).Get(context.Background(), activeService.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if service.Spec.Selector[labelGroupInstance] != oldInstance {
		t.Fatalf("service selector instance = %s, want %s", service.Spec.Selector[labelGroupInstance], oldInstance)
	}
}

func TestPGDRManagedPGDWithoutGateSelfHealsCurrentRevisionService(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("managed", 1)
	pgd.Spec.ReplicasPerGroup = 1
	markPGDRManaged(pgd, "rollout", "decode")
	defaultPGDSpec(t, pgd)
	desiredRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	pgd.Status.CurrentRevision = desiredRevision
	desiredInstance := revisionedGroupInstance(pgd.Name, 0, desiredRevision)
	pod := podForGroup(pgd, 0, 0, desiredInstance, desiredRevision)
	controller := newTestController(t, pgd, pod)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	service, err := controller.kube.CoreV1().Services(pgd.Namespace).Get(context.Background(), stableGroupInstance(pgd.Name, 0), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if service.Spec.Selector[labelGroupInstance] != desiredInstance {
		t.Fatalf("service selector instance = %s, want %s", service.Spec.Selector[labelGroupInstance], desiredInstance)
	}
	pods, err := controller.kube.CoreV1().Pods(pgd.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want 1", len(pods.Items))
	}
}

func TestPGDRManagedPGDHoldGateDeletesReplacementPods(t *testing.T) {
	oldPGD := testPodGroupDeploymentWithName("managed", 1)
	oldPGD.Spec.ReplicasPerGroup = 1
	markPGDRManaged(oldPGD, "rollout", "decode")
	defaultPGDSpec(t, oldPGD)
	oldRevision, err := deploymentRevision(&oldPGD.Spec)
	if err != nil {
		t.Fatal(err)
	}
	oldInstance := revisionedGroupInstance(oldPGD.Name, 0, oldRevision)

	pgd := oldPGD.DeepCopy()
	pgd.Spec.Template.Annotations = map[string]string{"podgroup.meta.com/rollout-trigger": "next"}
	pgd.Annotations = map[string]string{
		annotationRollout:             "rollout",
		annotationRolloutAllowedStart: "0",
		annotationRolloutAllowedEnd:   "1",
		annotationRolloutWave:         "0",
		annotationRolloutAction:       rolloutActionHold,
		annotationRolloutPhase:        rolloutPhaseCreateDecode,
	}
	defaultPGDSpec(t, pgd)
	desiredRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	desiredInstance := revisionedGroupInstance(pgd.Name, 0, desiredRevision)

	oldPod := podForGroup(pgd, 0, 0, oldInstance, oldRevision)
	replacementPod := podForGroup(pgd, 0, 0, desiredInstance, desiredRevision)
	activeService := serviceForGroup(pgd, 0, oldInstance)
	controller := newTestController(t, pgd, oldPod, replacementPod, activeService)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), replacementPod.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("held replacement pod %s still exists", replacementPod.Name)
	}
	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), oldPod.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("old active pod was not preserved: %v", err)
	}
}

func TestPGDRManagedPGDCreateNewGateOnlyCreatesAllowedWindow(t *testing.T) {
	oldPGD := testPodGroupDeploymentWithName("managed", 2)
	oldPGD.Spec.ReplicasPerGroup = 1
	markPGDRManaged(oldPGD, "rollout", "prefill")
	defaultPGDSpec(t, oldPGD)
	oldRevision, err := deploymentRevision(&oldPGD.Spec)
	if err != nil {
		t.Fatal(err)
	}
	oldInstance0 := revisionedGroupInstance(oldPGD.Name, 0, oldRevision)
	oldInstance1 := revisionedGroupInstance(oldPGD.Name, 1, oldRevision)

	pgd := oldPGD.DeepCopy()
	pgd.Spec.Template.Annotations = map[string]string{"podgroup.meta.com/rollout-trigger": "next"}
	pgd.Annotations = map[string]string{
		annotationRollout:             "rollout",
		annotationRolloutAllowedStart: "0",
		annotationRolloutAllowedEnd:   "1",
		annotationRolloutWave:         "0",
		annotationRolloutAction:       rolloutActionCreateNew,
		annotationRolloutPhase:        rolloutPhaseCreatePrefill,
	}
	defaultPGDSpec(t, pgd)
	desiredRevision, err := deploymentRevision(&pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	desiredInstance0 := revisionedGroupInstance(pgd.Name, 0, desiredRevision)
	desiredInstance1 := revisionedGroupInstance(pgd.Name, 1, desiredRevision)

	activeService0 := serviceForGroup(pgd, 0, oldInstance0)
	activeService1 := serviceForGroup(pgd, 1, oldInstance1)
	oldPod1 := podForGroup(pgd, 1, 0, oldInstance1, oldRevision)
	controller := newTestController(t, pgd, activeService0, activeService1, oldPod1)

	if err := controller.reconcileOne(context.Background(), pgd); err != nil {
		t.Fatalf("reconcileOne() error = %v", err)
	}

	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), podName(desiredInstance0, 0), metav1.GetOptions{}); err != nil {
		t.Fatalf("allowed window replacement was not created: %v", err)
	}
	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), podName(desiredInstance1, 0), metav1.GetOptions{}); err == nil {
		t.Fatalf("out-of-window replacement pod %s was created", podName(desiredInstance1, 0))
	}
	if _, err := controller.kube.CoreV1().Pods(pgd.Namespace).Get(context.Background(), oldPod1.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("out-of-window old pod was not preserved: %v", err)
	}
}

func TestDesiredRolloutChildrenUnified(t *testing.T) {
	unifiedSpec := testPodGroupDeploymentWithName("unified", 4).Spec
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo"},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode:    "unified",
			Unified: &unifiedSpec,
		},
	}

	children, err := desiredRolloutChildren(pgdr)
	if err != nil {
		t.Fatalf("desiredRolloutChildren() error = %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("children = %d, want 1", len(children))
	}
	if children[0].member.Role != "unified" || children[0].member.Name != "qwen-unified" || children[0].spec.Groups != 4 {
		t.Fatalf("unified child = %#v", children[0])
	}
}

func TestReconcileRolloutChildrenDeletesStaleOwnedChildren(t *testing.T) {
	unifiedSpec := testPodGroupDeploymentWithName("unified", 4).Spec
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo", UID: types.UID("rollout-uid")},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode:    "unified",
			Unified: &unifiedSpec,
		},
	}
	stale := testPodGroupDeploymentWithName("qwen-prefill", 2)
	markPGDRManaged(stale, pgdr.Name, "prefill")
	stale.OwnerReferences = rolloutOwnerRef(pgdr)
	staleObj, err := podGroupDeploymentToUnstructured(stale)
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestControllerWithDynamicObjects(t, staleObj)

	members, changed, err := controller.reconcileRolloutChildren(context.Background(), pgdr)
	if err != nil {
		t.Fatalf("reconcileRolloutChildren() error = %v", err)
	}
	if !changed {
		t.Fatal("expected reconcileRolloutChildren to report child changes")
	}
	if len(members) != 1 || members[0].Name != "qwen-unified" {
		t.Fatalf("members = %#v, want qwen-unified", members)
	}
	if _, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), "qwen-prefill", metav1.GetOptions{}); err == nil {
		t.Fatal("stale prefill child still exists")
	}
	if _, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), "qwen-unified", metav1.GetOptions{}); err != nil {
		t.Fatalf("desired unified child was not created: %v", err)
	}
}

func TestDeletingPGDRSkipsChildReconcile(t *testing.T) {
	unifiedSpec := testPodGroupDeploymentWithName("unified", 4).Spec
	now := metav1.Now()
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo", DeletionTimestamp: &now},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode:    "unified",
			Unified: &unifiedSpec,
		},
	}
	controller := newTestControllerWithDynamicObjectsList(t, nil)

	members, err := controller.reconcileOneRollout(context.Background(), pgdr)
	if err != nil {
		t.Fatalf("reconcileOneRollout() error = %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("members = %d, want 0", len(members))
	}
	if _, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), "qwen-unified", metav1.GetOptions{}); err == nil {
		t.Fatal("deleting rollout created unified child")
	}
}

func TestPGDRFirstReconcileAddsFinalizerBeforeChildren(t *testing.T) {
	unifiedSpec := testPodGroupDeploymentWithName("unified", 4).Spec
	pgdr := &PodGroupDeploymentRollout{
		TypeMeta: metav1.TypeMeta{APIVersion: operatorAPIVersion, Kind: operatorRolloutKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "qwen",
			Namespace: "demo",
			UID:       types.UID("rollout-uid"),
		},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode:    "unified",
			Unified: &unifiedSpec,
		},
	}
	controller := newTestControllerWithDynamicObjectsList(t, []runtime.Object{mustPodGroupDeploymentRolloutObject(t, pgdr)})

	members, err := controller.reconcileOneRollout(context.Background(), pgdr)
	if err != nil {
		t.Fatalf("reconcileOneRollout() error = %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("members = %d, want no children until finalizer is persisted", len(members))
	}
	obj, err := controller.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(pgdr.Namespace).Get(context.Background(), pgdr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasString(obj.GetFinalizers(), finalizerPodGroupDeploymentRollout) {
		t.Fatalf("finalizers = %#v, want PGDR finalizer", obj.GetFinalizers())
	}
	children, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(children.Items) != 0 {
		t.Fatalf("children = %d, want none until finalizer is persisted", len(children.Items))
	}
}

func TestPGDRFinalizerDeletesOwnedChildrenBeforeRemoval(t *testing.T) {
	now := metav1.Now()
	pgdr := &PodGroupDeploymentRollout{
		TypeMeta: metav1.TypeMeta{APIVersion: operatorAPIVersion, Kind: operatorRolloutKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "qwen",
			Namespace:         "demo",
			UID:               types.UID("rollout-uid"),
			Finalizers:        []string{finalizerPodGroupDeploymentRollout},
			DeletionTimestamp: &now,
		},
		Spec: PodGroupDeploymentRolloutSpec{Mode: "unified"},
	}
	child := testPodGroupDeploymentWithName("qwen-unified", 1)
	child.Namespace = pgdr.Namespace
	markPGDRManaged(child, pgdr.Name, "unified")
	child.OwnerReferences = rolloutOwnerRef(pgdr)
	controller := newTestControllerWithDynamicObjectsList(t, []runtime.Object{
		mustPodGroupDeploymentRolloutObject(t, pgdr),
		mustPodGroupDeploymentObject(t, child),
	})

	if _, err := controller.reconcileOneRollout(context.Background(), pgdr); err != nil {
		t.Fatalf("reconcileOneRollout() error = %v", err)
	}

	if _, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(context.Background(), child.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("child %s still exists", child.Name)
	}
	obj, err := controller.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(pgdr.Namespace).Get(context.Background(), pgdr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasString(obj.GetFinalizers(), finalizerPodGroupDeploymentRollout) {
		t.Fatalf("finalizer was not removed: %#v", obj.GetFinalizers())
	}
}

func TestDesiredRolloutChildrenMergesBaseAndRoleOverrides(t *testing.T) {
	base := testPodGroupDeploymentWithName("base", 3).Spec
	base.Groups = 99
	base.ReplicasPerGroup = 99
	base.Template.Labels = map[string]string{"app": "qwen"}
	base.Template.Spec.Containers[0].Command = []string{
		"python3",
		"-m",
		"server",
		"--tp-size",
		"4",
		"--shared",
		"true",
	}
	base.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "ROLE", Value: "base"}}

	prefill := PodGroupDeploymentSpec{
		Groups:           8,
		ReplicasPerGroup: 2,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"component": "prefill"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:    "worker",
					Command: []string{"--disaggregation-mode", "prefill", "--disaggregation-bootstrap-port", "8998"},
					Env:     []corev1.EnvVar{{Name: "ROLE", Value: "prefill"}},
					Ports: []corev1.ContainerPort{{
						Name:          "bootstrap",
						ContainerPort: 8998,
					}},
				}},
			},
		},
	}
	decode := PodGroupDeploymentSpec{
		Groups:           4,
		ReplicasPerGroup: 1,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"component": "decode"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:    "worker",
					Command: []string{"--disaggregation-mode", "decode"},
				}},
			},
		},
	}
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo"},
		Spec: PodGroupDeploymentRolloutSpec{
			Mode:    "disagg",
			Base:    &base,
			Prefill: &prefill,
			Decode:  &decode,
		},
	}

	children, err := desiredRolloutChildren(pgdr)
	if err != nil {
		t.Fatalf("desiredRolloutChildren() error = %v", err)
	}
	prefillSpec := children[0].spec
	if prefillSpec.Groups != 8 || prefillSpec.ReplicasPerGroup != 2 {
		t.Fatalf("prefill groups/replicas = %d/%d, want 8/2", prefillSpec.Groups, prefillSpec.ReplicasPerGroup)
	}
	if children[1].spec.Groups != 4 || children[1].spec.ReplicasPerGroup != 1 {
		t.Fatalf("decode groups/replicas = %d/%d, want 4/1", children[1].spec.Groups, children[1].spec.ReplicasPerGroup)
	}
	if prefillSpec.Template.Labels["app"] != "qwen" || prefillSpec.Template.Labels["component"] != "prefill" {
		t.Fatalf("prefill labels = %#v", prefillSpec.Template.Labels)
	}
	command := prefillSpec.Template.Spec.Containers[0].Command
	if got, ok := flagValue(command, "--tp-size"); !ok || got != "4" {
		t.Fatalf("prefill inherited --tp-size = %q/%v in %#v", got, ok, command)
	}
	if got, ok := flagValue(command, "--disaggregation-mode"); !ok || got != "prefill" {
		t.Fatalf("prefill --disaggregation-mode = %q/%v in %#v", got, ok, command)
	}
	if got, ok := flagValue(command, "--disaggregation-bootstrap-port"); !ok || got != "8998" {
		t.Fatalf("prefill bootstrap arg = %q/%v in %#v", got, ok, command)
	}
	if got := prefillSpec.Template.Spec.Containers[0].Env[0].Value; got != "prefill" {
		t.Fatalf("prefill ROLE env = %q, want prefill", got)
	}
	if len(prefillSpec.Template.Spec.Containers[0].Ports) != 2 {
		t.Fatalf("prefill ports = %#v, want inherited http plus bootstrap", prefillSpec.Template.Spec.Containers[0].Ports)
	}

	decodeCommand := children[1].spec.Template.Spec.Containers[0].Command
	if got, ok := flagValue(decodeCommand, "--disaggregation-mode"); !ok || got != "decode" {
		t.Fatalf("decode --disaggregation-mode = %q/%v in %#v", got, ok, decodeCommand)
	}
	if _, ok := flagValue(decodeCommand, "--disaggregation-bootstrap-port"); ok {
		t.Fatalf("decode should not inherit prefill bootstrap override: %#v", decodeCommand)
	}
}

func TestDesiredRolloutChildrenRequiresRoleSpecs(t *testing.T) {
	pgdr := &PodGroupDeploymentRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: "demo"},
		Spec:       PodGroupDeploymentRolloutSpec{Mode: "disagg"},
	}
	if _, err := desiredRolloutChildren(pgdr); err == nil {
		t.Fatal("expected disagg rollout without role specs to fail")
	}
}

func TestRolloutWaveSize(t *testing.T) {
	tests := []struct {
		name    string
		groups  int32
		percent int32
		want    int32
	}{
		{name: "prefill forty groups ten percent", groups: 40, percent: 10, want: 4},
		{name: "decode twenty groups ten percent", groups: 20, percent: 10, want: 2},
		{name: "small rollout still advances", groups: 3, percent: 10, want: 1},
		{name: "zero groups", groups: 0, percent: 10, want: 0},
		{name: "default percent", groups: 10, percent: 0, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rolloutWaveSize(tt.groups, tt.percent); got != tt.want {
				t.Fatalf("rolloutWaveSize(%d, %d) = %d, want %d", tt.groups, tt.percent, got, tt.want)
			}
		})
	}
}

func TestPlanRolloutWavesDisaggPreservesMemberRatio(t *testing.T) {
	progress := []rolloutMemberProgress{
		{
			member:       rolloutMember{Role: "prefill", Name: "qwen-prefill"},
			pgd:          testPodGroupDeploymentWithName("qwen-prefill", 9),
			waveSize:     rolloutWaveSize(9, 17),
			updatedReady: 0,
		},
		{
			member:       rolloutMember{Role: "decode", Name: "qwen-decode"},
			pgd:          testPodGroupDeploymentWithName("qwen-decode", 3),
			waveSize:     rolloutWaveSize(3, 17),
			updatedReady: 0,
		},
	}

	planRolloutWaves(progress)
	prefillStart, prefillEnd := rolloutMemberWindow(progress[0], 0)
	decodeStart, decodeEnd := rolloutMemberWindow(progress[1], 0)

	if progress[0].waveCount != 3 || progress[1].waveCount != 3 {
		t.Fatalf("wave counts = %d/%d, want 3/3", progress[0].waveCount, progress[1].waveCount)
	}
	if prefillStart != 0 || prefillEnd != 3 {
		t.Fatalf("prefill window = [%d,%d), want [0,3)", prefillStart, prefillEnd)
	}
	if decodeStart != 0 || decodeEnd != 1 {
		t.Fatalf("decode window = [%d,%d), want [0,1)", decodeStart, decodeEnd)
	}
}

func TestPlanRolloutWavesUsesReadyGroupsForBringup(t *testing.T) {
	prefill := testPodGroupDeploymentWithName("qwen-prefill", 8)
	prefill.Status.Groups = []GroupStatus{
		{Index: 0, Revision: "new", Phase: "Ready"},
		{Index: 1, Revision: "new", Phase: "Ready"},
	}
	decode := testPodGroupDeploymentWithName("qwen-decode", 4)
	decode.Status.Groups = []GroupStatus{
		{Index: 0, Revision: "new", Phase: "Ready"},
	}
	progress := []rolloutMemberProgress{
		{
			member:       rolloutMember{Role: "prefill", Name: "qwen-prefill"},
			pgd:          prefill,
			desired:      "new",
			waveSize:     2,
			updatedReady: updatedReadyGroups(prefill.Status.Groups, "new"),
		},
		{
			member:       rolloutMember{Role: "decode", Name: "qwen-decode"},
			pgd:          decode,
			desired:      "new",
			waveSize:     1,
			updatedReady: updatedReadyGroups(decode.Status.Groups, "new"),
		},
	}

	planRolloutWaves(progress)

	if progress[0].completed != 1 || progress[1].completed != 1 {
		t.Fatalf("completed waves = %d/%d, want 1/1", progress[0].completed, progress[1].completed)
	}
}

func TestPlanRolloutWavesResetsForNewDesiredRevision(t *testing.T) {
	prefill := testPodGroupDeploymentWithName("qwen-prefill", 24)
	prefill.Status.Groups = []GroupStatus{
		{Index: 0, Revision: "old", Phase: "Running"},
		{Index: 1, Revision: "old", Phase: "Running"},
		{Index: 2, Revision: "old", Phase: "Running"},
		{Index: 3, Revision: "old", Phase: "Running"},
		{Index: 4, Revision: "old", Phase: "Running"},
		{Index: 5, Revision: "old", Phase: "Running"},
		{Index: 6, Revision: "old", Phase: "Running"},
		{Index: 7, Revision: "old", Phase: "Running"},
		{Index: 8, Revision: "old", Phase: "Running"},
		{Index: 9, Revision: "old", Phase: "Running"},
		{Index: 10, Revision: "old", Phase: "Running"},
		{Index: 11, Revision: "old", Phase: "Running"},
		{Index: 12, Revision: "old", Phase: "Running"},
		{Index: 13, Revision: "old", Phase: "Running"},
		{Index: 14, Revision: "old", Phase: "Running"},
		{Index: 15, Revision: "old", Phase: "Pending", Pending: 1},
		{Index: 16, Revision: "old", Phase: "Pending", Pending: 1},
		{Index: 17, Revision: "old", Phase: "Pending", Pending: 1},
	}
	decode := testPodGroupDeploymentWithName("qwen-decode", 12)
	decode.Status.Groups = []GroupStatus{
		{Index: 0, Revision: "old", Phase: "Running"},
		{Index: 1, Revision: "old", Phase: "Running"},
		{Index: 2, Revision: "old", Phase: "Running"},
		{Index: 3, Revision: "old", Phase: "Running"},
		{Index: 4, Revision: "old", Phase: "Running"},
		{Index: 5, Revision: "old", Phase: "Running"},
		{Index: 6, Revision: "old", Phase: "Running"},
		{Index: 7, Revision: "old", Phase: "Running"},
		{Index: 8, Revision: "old", Phase: "Running"},
	}

	progress := []rolloutMemberProgress{
		{
			member:       rolloutMember{Role: "prefill", Name: "qwen-prefill"},
			pgd:          prefill,
			desired:      "new",
			waveSize:     rolloutWaveSize(24, 25),
			updatedReady: updatedReadyGroups(prefill.Status.Groups, "new"),
			pods: []corev1.Pod{
				testRolloutPod(0, "old"),
				testRolloutPod(1, "old"),
				testRolloutPod(2, "old"),
				testRolloutPod(3, "old"),
				testRolloutPod(4, "old"),
				testRolloutPod(5, "old"),
			},
		},
		{
			member:       rolloutMember{Role: "decode", Name: "qwen-decode"},
			pgd:          decode,
			desired:      "new",
			waveSize:     rolloutWaveSize(12, 25),
			updatedReady: updatedReadyGroups(decode.Status.Groups, "new"),
			pods: []corev1.Pod{
				testRolloutPod(0, "old"),
				testRolloutPod(1, "old"),
				testRolloutPod(2, "old"),
			},
		},
	}

	planRolloutWaves(progress)
	phase, actions := rolloutPhaseAndActions("disagg", progress, 0)

	if progress[0].completed != 0 || progress[1].completed != 0 {
		t.Fatalf("completed waves = %d/%d, want 0/0 for new desired revision", progress[0].completed, progress[1].completed)
	}
	if phase != rolloutPhaseTerminatePrefill || actions["prefill"] != rolloutActionDeleteOld {
		t.Fatalf("phase/actions = %s %#v, want wave 0 prefill termination", phase, actions)
	}
}

func TestNewGroupsReadyRequiresReadyPhase(t *testing.T) {
	groups := []GroupStatus{
		{Index: 0, Revision: "new", Phase: "Ready", Ready: 2},
		{Index: 1, Revision: "new", Phase: "Running", Running: 1},
	}

	if newGroupsReady(groups, "new", 0, 2) {
		t.Fatal("expected running-but-not-ready group to prevent ready condition")
	}

	groups[1] = GroupStatus{Index: 1, Revision: "new", Phase: "Ready", Ready: 1}
	if !newGroupsReady(groups, "new", 0, 2) {
		t.Fatal("expected all ready groups to satisfy ready condition")
	}
}

func TestCompletedRolloutWaves(t *testing.T) {
	groups := []GroupStatus{
		{Index: 0, Revision: "new", Phase: "Ready"},
		{Index: 1, Revision: "new", Phase: "Ready"},
	}
	if got := completedRolloutWaves(groups, "new", 9, 3); got != 0 {
		t.Fatalf("completed waves = %d, want 0", got)
	}
	groups = append(groups, GroupStatus{Index: 2, Revision: "new", Phase: "Ready"})
	if got := completedRolloutWaves(groups, "new", 9, 3); got != 1 {
		t.Fatalf("completed waves = %d, want 1", got)
	}
	for i := int32(3); i < 9; i++ {
		groups = append(groups, GroupStatus{Index: i, Revision: "new", Phase: "Ready"})
	}
	if got := completedRolloutWaves(groups, "new", 9, 3); got != 3 {
		t.Fatalf("completed waves = %d, want 3", got)
	}
}

func TestCompletedRolloutWavesRequiresContiguousReadyWindows(t *testing.T) {
	groups := []GroupStatus{
		{Index: 0, Revision: "new", Phase: "Ready"},
		{Index: 1, Revision: "new", Phase: "Ready"},
		{Index: 3, Revision: "new", Phase: "Ready"},
		{Index: 4, Revision: "new", Phase: "Ready"},
		{Index: 5, Revision: "new", Phase: "Ready"},
	}
	if got := completedRolloutWaves(groups, "new", 6, 2); got != 0 {
		t.Fatalf("completed waves = %d, want 0 because group 2 is not Ready", got)
	}
}

func TestRolloutGateFromAnnotations(t *testing.T) {
	gate := rolloutGateFromAnnotations(map[string]string{
		annotationRollout:             "qwen",
		annotationRolloutAllowedStart: "4",
		annotationRolloutAllowedEnd:   "8",
		annotationRolloutWave:         "1",
		annotationRolloutAction:       rolloutActionDeleteOld,
		annotationRolloutPhase:        rolloutPhaseTerminatePrefill,
		annotationRolloutPausedReason: "waiting for qwen-decode",
	})
	if !gate.enabled {
		t.Fatal("expected rollout gate to be enabled")
	}
	if gate.allows(3) || !gate.allows(4) || !gate.allows(7) || gate.allows(8) {
		t.Fatalf("unexpected gate allowance for range [%d,%d)", gate.start, gate.end)
	}
	status := gate.status()
	if status.Rollout != "qwen" || status.Wave != 1 || status.AllowedStart != 4 || status.AllowedEnd != 8 || status.Action != rolloutActionDeleteOld || status.Phase != rolloutPhaseTerminatePrefill || status.PausedReason != "waiting for qwen-decode" {
		t.Fatalf("gate status = %#v", status)
	}
}

func TestRolloutGateFromAnnotationsIgnoresInvalidRange(t *testing.T) {
	gate := rolloutGateFromAnnotations(map[string]string{
		annotationRollout:             "qwen",
		annotationRolloutAllowedStart: "8",
		annotationRolloutAllowedEnd:   "4",
	})
	if gate.enabled {
		t.Fatal("expected invalid rollout gate to be disabled")
	}
	if !gate.allows(99) {
		t.Fatal("disabled rollout gate should allow all groups")
	}
}

func TestApplyAndClearRolloutGatePatchAnnotations(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("qwen-decode", 1)
	pgd.Annotations = map[string]string{"keep": "true"}
	controller := newTestController(t, pgd)
	recorder := record.NewFakeRecorder(2)
	controller.WithEventRecorder(recorder)
	pgdr := &PodGroupDeploymentRollout{ObjectMeta: metav1.ObjectMeta{Name: "qwen", Namespace: pgd.Namespace}}

	if err := controller.applyRolloutGate(context.Background(), pgdr, pgd, 0, 1, 0, rolloutActionCreateNew, rolloutPhaseCreateDecode, "waiting"); err != nil {
		t.Fatalf("applyRolloutGate() error = %v", err)
	}
	obj, err := controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(context.Background(), pgd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	annotations := obj.GetAnnotations()
	if annotations["keep"] != "true" {
		t.Fatalf("kept annotation = %q, want true", annotations["keep"])
	}
	if annotations[annotationRollout] != "qwen" || annotations[annotationRolloutAction] != rolloutActionCreateNew {
		t.Fatalf("rollout annotations after apply = %#v", annotations)
	}

	updated, err := decodePodGroupDeployment(obj)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.clearRolloutGate(context.Background(), updated); err != nil {
		t.Fatalf("clearRolloutGate() error = %v", err)
	}
	obj, err = controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(context.Background(), pgd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	annotations = obj.GetAnnotations()
	if annotations["keep"] != "true" {
		t.Fatalf("kept annotation after clear = %q, want true", annotations["keep"])
	}
	if annotations[annotationRollout] != "" || annotations[annotationRolloutAction] != "" {
		t.Fatalf("rollout annotations after clear = %#v", annotations)
	}

	firstEvent := <-recorder.Events
	if !strings.Contains(firstEvent, reasonRolloutGate) {
		t.Fatalf("first event = %q, want rollout gate event", firstEvent)
	}
	secondEvent := <-recorder.Events
	if !strings.Contains(secondEvent, reasonNoRolloutGate) {
		t.Fatalf("second event = %q, want gate cleared event", secondEvent)
	}
}

func TestPodGroupDeploymentStatusConditions(t *testing.T) {
	status := PodGroupDeploymentStatus{
		ObservedGeneration: 7,
		Total: TotalStatus{
			DesiredPods: 4,
			ReadyPods:   4,
		},
	}
	setPodGroupDeploymentStatusConditions(&status)

	ready := conditionByType(status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != reasonAllGroupsReady || ready.ObservedGeneration != 7 {
		t.Fatalf("Ready condition = %#v", ready)
	}
	gated := conditionByType(status.Conditions, conditionRolloutGated)
	if gated == nil || gated.Status != metav1.ConditionFalse || gated.Reason != reasonNoRolloutGate {
		t.Fatalf("RolloutGated condition = %#v", gated)
	}

	transition := ready.LastTransitionTime
	setPodGroupDeploymentStatusConditions(&status)
	ready = conditionByType(status.Conditions, conditionReady)
	if !ready.LastTransitionTime.Equal(&transition) {
		t.Fatalf("Ready transition time changed without status change: got %s want %s", ready.LastTransitionTime, transition)
	}
}

func TestRolloutStatusConditions(t *testing.T) {
	status := PodGroupDeploymentRolloutStatus{
		ObservedGeneration: 3,
		Phase:              "Blocked",
		Reason:             "decode waiting",
	}
	setRolloutStatusConditions(&status)

	blocked := conditionByType(status.Conditions, conditionBlocked)
	if blocked == nil || blocked.Status != metav1.ConditionTrue || blocked.Reason != reasonRolloutBlocked || blocked.Message != "decode waiting" || blocked.ObservedGeneration != 3 {
		t.Fatalf("Blocked condition = %#v", blocked)
	}
	ready := conditionByType(status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %#v", ready)
	}
}

func TestRolloutActionsSummary(t *testing.T) {
	members := []RolloutMemberStatus{
		{Role: "prefill", Action: rolloutActionDeleteOld},
		{Role: "decode", Action: rolloutActionHold},
	}
	if got := rolloutActionsSummary(members); got != "prefill=delete-old,decode=hold" {
		t.Fatalf("rolloutActionsSummary() = %q, want prefill=delete-old,decode=hold", got)
	}
}

func TestRolloutPhaseAndActionsDisaggOrdering(t *testing.T) {
	prefill := rolloutMemberProgress{
		member:   rolloutMember{Role: "prefill", Name: "qwen-prefill"},
		pgd:      testPodGroupDeploymentWithName("qwen-prefill", 18),
		desired:  "new",
		waveSize: 3,
		pods: []corev1.Pod{
			testRolloutPod(0, "old"),
			testRolloutPod(1, "old"),
			testRolloutPod(2, "old"),
		},
	}
	decode := rolloutMemberProgress{
		member:   rolloutMember{Role: "decode", Name: "qwen-decode"},
		pgd:      testPodGroupDeploymentWithName("qwen-decode", 6),
		desired:  "new",
		waveSize: 1,
		pods:     []corev1.Pod{testRolloutPod(0, "old")},
	}

	phase, actions := rolloutPhaseAndActions("disagg", []rolloutMemberProgress{prefill, decode}, 0)
	if phase != rolloutPhaseTerminatePrefill || actions["prefill"] != rolloutActionDeleteOld || actions["decode"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want terminate prefill", phase, actions)
	}

	prefill.pods = []corev1.Pod{}
	phase, actions = rolloutPhaseAndActions("disagg", []rolloutMemberProgress{prefill, decode}, 0)
	if phase != rolloutPhaseTerminateDecode || actions["decode"] != rolloutActionDeleteOld || actions["prefill"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want terminate decode", phase, actions)
	}

	decode.pods = []corev1.Pod{}
	phase, actions = rolloutPhaseAndActions("disagg", []rolloutMemberProgress{prefill, decode}, 0)
	if phase != rolloutPhaseCreateDecode || actions["decode"] != rolloutActionCreateNew || actions["prefill"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want create decode", phase, actions)
	}

	decode.pgd.Status.Groups = []GroupStatus{{Index: 0, Revision: "new", Phase: "Running", Running: 1}}
	decode.updatedReady = 0
	phase, actions = rolloutPhaseAndActions("disagg", []rolloutMemberProgress{prefill, decode}, 0)
	if phase != rolloutPhaseCreateDecode || actions["decode"] != rolloutActionCreateNew || actions["prefill"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want to keep creating decode until group is Ready", phase, actions)
	}

	decode.pgd.Status.Groups = []GroupStatus{{Index: 0, Revision: "new", Phase: "Ready", Ready: 1}}
	phase, actions = rolloutPhaseAndActions("disagg", []rolloutMemberProgress{prefill, decode}, 0)
	if phase != rolloutPhaseCreatePrefill || actions["prefill"] != rolloutActionCreateNew || actions["decode"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want create prefill", phase, actions)
	}
}

func TestRolloutPhaseAndActionsDisaggInPlaceUpdatesDecodeBeforePrefill(t *testing.T) {
	prefill := rolloutMemberProgress{
		member:       rolloutMember{Role: "prefill", Name: "qwen-prefill"},
		pgd:          testPodGroupDeploymentWithName("qwen-prefill", 16),
		desired:      "new",
		waveSize:     2,
		updatedReady: 0,
		inPlace:      true,
	}
	decode := rolloutMemberProgress{
		member:       rolloutMember{Role: "decode", Name: "qwen-decode"},
		pgd:          testPodGroupDeploymentWithName("qwen-decode", 8),
		desired:      "new",
		waveSize:     1,
		updatedReady: 0,
		inPlace:      true,
	}
	progress := []rolloutMemberProgress{prefill, decode}
	planRolloutWaves(progress)

	phase, actions := rolloutPhaseAndActions("disagg", progress, 0)
	if phase != rolloutPhaseUpdateDecode || actions["decode"] != rolloutActionInPlace || actions["prefill"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want update decode", phase, actions)
	}

	progress[1].updatedReady = 1
	phase, actions = rolloutPhaseAndActions("disagg", progress, 0)
	if phase != rolloutPhaseUpdatePrefill || actions["prefill"] != rolloutActionInPlace || actions["decode"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want update prefill", phase, actions)
	}
}

func TestRolloutPhaseAndActionsDisaggInPlaceContinuesAfterDecodeComplete(t *testing.T) {
	prefill := rolloutMemberProgress{
		member:       rolloutMember{Role: "prefill", Name: "qwen-prefill"},
		pgd:          testPodGroupDeploymentWithName("qwen-prefill", 16),
		desired:      "new",
		waveSize:     3,
		waveCount:    6,
		updatedReady: 13,
		inPlace:      true,
	}
	decode := rolloutMemberProgress{
		member:       rolloutMember{Role: "decode", Name: "qwen-decode"},
		pgd:          testPodGroupDeploymentWithName("qwen-decode", 8),
		desired:      "new",
		waveSize:     2,
		waveCount:    6,
		updatedReady: 8,
		inPlace:      false,
	}

	phase, actions := rolloutPhaseAndActions("disagg", []rolloutMemberProgress{prefill, decode}, 5)
	if phase != rolloutPhaseUpdatePrefill || actions["prefill"] != rolloutActionInPlace || actions["decode"] != rolloutActionHold {
		t.Fatalf("phase/actions = %s %#v, want update prefill after decode complete", phase, actions)
	}
}

func TestRolloutPhaseAndActionsUnifiedOrdering(t *testing.T) {
	unified := rolloutMemberProgress{
		member:   rolloutMember{Role: "unified", Name: "qwen-unified"},
		pgd:      testPodGroupDeploymentWithName("qwen-unified", 4),
		desired:  "new",
		waveSize: 1,
		pods:     []corev1.Pod{testRolloutPod(0, "old")},
	}

	phase, actions := rolloutPhaseAndActions("unified", []rolloutMemberProgress{unified}, 0)
	if phase != rolloutPhaseTerminateUnified || actions["unified"] != rolloutActionDeleteOld {
		t.Fatalf("phase/actions = %s %#v, want terminate unified", phase, actions)
	}

	unified.pods = []corev1.Pod{}
	phase, actions = rolloutPhaseAndActions("unified", []rolloutMemberProgress{unified}, 0)
	if phase != rolloutPhaseCreateUnified || actions["unified"] != rolloutActionCreateNew {
		t.Fatalf("phase/actions = %s %#v, want create unified", phase, actions)
	}

	unified.inPlace = true
	unified.updatedReady = 0
	phase, actions = rolloutPhaseAndActions("unified", []rolloutMemberProgress{unified}, 0)
	if phase != rolloutPhaseUpdateUnified || actions["unified"] != rolloutActionInPlace {
		t.Fatalf("phase/actions = %s %#v, want update unified", phase, actions)
	}
}

func TestRolloutMemberInPlaceEligibleFalseWhenAlreadyAtDesiredRevision(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("qwen-decode", 1)
	pgd.Status.Groups = []GroupStatus{{
		Index:         0,
		GroupInstance: "qwen-decode-new-0",
		Revision:      "new",
		Phase:         "Running",
	}}
	recreateRevision, err := templateRecreateRevision(&pgd.Spec.Template)
	if err != nil {
		t.Fatalf("templateRecreateRevision() error = %v", err)
	}
	pods := []corev1.Pod{testInPlacePodFor(pgd, 0, "new", recreateRevision)}

	eligible, err := rolloutMemberInPlaceEligible(pgd, pods, "new")
	if err != nil {
		t.Fatalf("rolloutMemberInPlaceEligible() error = %v", err)
	}
	if eligible {
		t.Fatal("expected no in-place rollout when every group is already at desired revision")
	}
}

func TestRolloutMemberInPlaceEligibleHonorsRecreatePodStrategy(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("qwen-decode", 1)
	pgd.Spec.UpdateStrategy.Type = updateStrategyRecreatePod
	pgd.Status.Groups = []GroupStatus{{
		Index:         0,
		GroupInstance: "qwen-decode-old-0",
		Revision:      "old",
		Phase:         "Running",
	}}
	recreateRevision, err := templateRecreateRevision(&pgd.Spec.Template)
	if err != nil {
		t.Fatalf("templateRecreateRevision() error = %v", err)
	}
	pods := []corev1.Pod{testInPlacePodFor(pgd, 0, "old", recreateRevision)}

	eligible, err := rolloutMemberInPlaceEligible(pgd, pods, "new")
	if err != nil {
		t.Fatalf("rolloutMemberInPlaceEligible() error = %v", err)
	}
	if eligible {
		t.Fatal("expected RecreatePod strategy to disable in-place rollout")
	}
}

func TestRolloutMemberInPlaceEligibleRejectsInPlaceOnlyWhenRecreateRequired(t *testing.T) {
	pgd := testPodGroupDeploymentWithName("qwen-decode", 1)
	pgd.Spec.UpdateStrategy.Type = updateStrategyInPlaceOnly
	pgd.Status.Groups = []GroupStatus{{
		Index:         0,
		GroupInstance: "qwen-decode-old-0",
		Revision:      "old",
		Phase:         "Running",
	}}
	pods := []corev1.Pod{testInPlacePodFor(pgd, 0, "old", "different-recreate-revision")}

	eligible, err := rolloutMemberInPlaceEligible(pgd, pods, "new")
	if err == nil {
		t.Fatal("expected InPlaceOnly to reject recreate-required update")
	}
	if eligible {
		t.Fatal("rejected in-place-only update must not be eligible")
	}
}

func testGroupPod(ordinal int32, phase corev1.PodPhase, deleting bool) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				labelOrdinal: fmt.Sprintf("%d", ordinal),
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
	}
	return pod
}

func markPodReady(pod corev1.Pod) corev1.Pod {
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	})
	return pod
}

func testRevisionPod(revision string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				labelRevision: revision,
			},
		},
	}
}

func testRolloutPod(group int32, revision string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				labelGroupIndex: fmt.Sprintf("%d", group),
				labelRevision:   revision,
			},
		},
	}
}

func testInPlacePod(ordinal int32, revision, recreateRevision string) corev1.Pod {
	return testInPlacePodFor(testPodGroupDeployment(), ordinal, revision, recreateRevision)
}

func testInPlacePodFor(pgd *PodGroupDeployment, ordinal int32, revision, recreateRevision string) corev1.Pod {
	out := podForGroup(pgd, 0, ordinal, pgd.Name+"-"+revision+"-0", revision)
	out.Status.Phase = corev1.PodRunning
	out.Annotations[annotationRecreateRevision] = recreateRevision
	return *out
}

func testPodGroupDeploymentWithName(name string, groups int32) *PodGroupDeployment {
	pgd := testPodGroupDeployment()
	pgd.Name = name
	pgd.Spec.Groups = groups
	return pgd
}

func testPodGroupDeployment() *PodGroupDeployment {
	return &PodGroupDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: operatorAPIVersion,
			Kind:       operatorKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "inference",
			Namespace:  "demo",
			UID:        types.UID("uid-123"),
			Finalizers: []string{finalizerPodGroupDeployment},
		},
		Spec: PodGroupDeploymentSpec{
			Groups:               3,
			ReplicasPerGroup:     4,
			TopologyKey:          corev1.LabelHostname,
			ServiceTargetOrdinal: 0,
			SchedulerName:        defaultSchedulerName,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "worker",
						Image: "busybox",
						Ports: []corev1.ContainerPort{{
							Name:          "http",
							ContainerPort: 8080,
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU: resource.MustParse("100m"),
							},
						},
					}},
				},
			},
		},
	}
}

func markPGDRManaged(pgd *PodGroupDeployment, rollout, role string) {
	pgd.Labels = mergeStringMaps(pgd.Labels, map[string]string{
		labelManagedBy:   managerName,
		labelRollout:     rollout,
		labelRolloutRole: role,
	})
}

func defaultPGDSpec(t *testing.T, pgd *PodGroupDeployment) {
	t.Helper()
	spec, err := defaultAndValidateSpec(pgd.Spec)
	if err != nil {
		t.Fatal(err)
	}
	pgd.Spec = spec
}

func conditionByType(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func mustPodGroupDeploymentObject(t *testing.T, pgd *PodGroupDeployment) *unstructured.Unstructured {
	t.Helper()
	obj, err := podGroupDeploymentToUnstructured(pgd)
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func mustPodGroupDeploymentRolloutObject(t *testing.T, pgdr *PodGroupDeploymentRollout) *unstructured.Unstructured {
	t.Helper()
	obj, err := podGroupDeploymentRolloutToUnstructured(pgdr)
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func testSchedulerPodGroup(pgd *PodGroupDeployment, group int32, groupInstance, revision string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "scheduling.x-k8s.io/v1alpha1",
			"kind":       "PodGroup",
			"spec": map[string]interface{}{
				"minMember": int64(pgd.Spec.ReplicasPerGroup),
			},
		},
	}
	obj.SetName(groupInstance)
	obj.SetNamespace(pgd.Namespace)
	obj.SetLabels(baseLabels(pgd, group, groupInstance, revision))
	obj.SetOwnerReferences(ownerRef(pgd))
	return obj
}

func podGroupDeploymentRolloutToUnstructured(pgdr *PodGroupDeploymentRollout) (*unstructured.Unstructured, error) {
	raw, err := json.Marshal(pgdr)
	if err != nil {
		return nil, err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

func newTestController(t *testing.T, pgd *PodGroupDeployment, kubeObjects ...runtime.Object) *Controller {
	t.Helper()
	obj, err := podGroupDeploymentToUnstructured(pgd)
	if err != nil {
		t.Fatal(err)
	}
	return newTestControllerWithDynamicObjects(t, obj, kubeObjects...)
}

func newTestControllerWithDynamicObjects(t *testing.T, dynamicObject runtime.Object, kubeObjects ...runtime.Object) *Controller {
	t.Helper()
	return newTestControllerWithDynamicObjectsList(t, []runtime.Object{dynamicObject}, kubeObjects...)
}

func newTestControllerWithDynamicObjectsList(t *testing.T, dynamicObjects []runtime.Object, kubeObjects ...runtime.Object) *Controller {
	t.Helper()

	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			podGroupDeploymentGVR:        "PodGroupDeploymentList",
			podGroupDeploymentRolloutGVR: "PodGroupDeploymentRolloutList",
			schedulerPodGroupGVR:         "PodGroupList",
		},
		dynamicObjects...,
	)
	return New(kubefake.NewSimpleClientset(kubeObjects...), dynamicClient)
}
