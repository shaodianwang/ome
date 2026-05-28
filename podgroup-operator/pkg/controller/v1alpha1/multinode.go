package v1alpha1

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

const (
	multiNodeArgTPSize       = "--tp-size"
	multiNodeArgNNodes       = "--nnodes"
	multiNodeArgNodeRank     = "--node-rank"
	multiNodeArgDistInitAddr = "--dist-init-addr"
)

func validateMultiNodeSpec(spec PodGroupDeploymentSpec) error {
	multiNode := spec.MultiNode
	if !multiNode.Enabled {
		return nil
	}
	if multiNode.DistInitPort < 0 || multiNode.DistInitPort > 65535 {
		return fmt.Errorf("spec.multiNode.distInitPort must be between 0 and 65535")
	}
	if multiNode.ContainerName != "" && multiNodeContainerIndex(spec.Template.Spec.Containers, multiNode.ContainerName) < 0 {
		return fmt.Errorf("spec.multiNode.containerName %q was not found in spec.template.spec.containers", multiNode.ContainerName)
	}
	if spec.ReplicasPerGroup > 1 {
		if _, ok := multiNodeTensorParallelSize(spec); !ok {
			return fmt.Errorf("spec.multiNode requires tensorParallelSize or a --tp-size argument")
		}
	}
	return nil
}

func applyMultiNodeArgs(pgd *PodGroupDeployment, pod *corev1.Pod, group, ordinal int32) {
	if !multiNodeActive(pgd.Spec) {
		return
	}
	index := multiNodeTargetContainerIndex(pod.Spec.Containers, pgd.Spec.MultiNode)
	if index < 0 {
		return
	}

	distInitAddr := fmt.Sprintf("%s.%s.svc.cluster.local:%d", stableGroupInstance(pgd.Name, group), pgd.Namespace, effectiveMultiNodeDistInitPort(pgd.Spec))
	container := &pod.Spec.Containers[index]
	setMultiNodeArg(container, multiNodeArgNNodes, strconv.Itoa(int(pgd.Spec.ReplicasPerGroup)))
	setMultiNodeArg(container, multiNodeArgNodeRank, strconv.Itoa(int(ordinal)))
	setMultiNodeArg(container, multiNodeArgDistInitAddr, distInitAddr)
}

func multiNodeActive(spec PodGroupDeploymentSpec) bool {
	if !spec.MultiNode.Enabled || spec.ReplicasPerGroup <= 1 {
		return false
	}
	tpSize, ok := multiNodeTensorParallelSize(spec)
	return ok && tpSize > 4
}

func effectiveMultiNodeDistInitPort(spec PodGroupDeploymentSpec) int32 {
	if spec.MultiNode.DistInitPort > 0 {
		return spec.MultiNode.DistInitPort
	}
	return defaultMultiNodeDistInitPort
}

func multiNodeTensorParallelSize(spec PodGroupDeploymentSpec) (int32, bool) {
	if spec.MultiNode.TensorParallelSize > 0 {
		return spec.MultiNode.TensorParallelSize, true
	}
	index := multiNodeTargetContainerIndex(spec.Template.Spec.Containers, spec.MultiNode)
	if index < 0 {
		return 0, false
	}
	container := spec.Template.Spec.Containers[index]
	if value, ok := flagInt32(container.Command, multiNodeArgTPSize); ok {
		return value, true
	}
	return flagInt32(container.Args, multiNodeArgTPSize)
}

func multiNodeTargetContainerIndex(containers []corev1.Container, multiNode MultiNodeSpec) int {
	if multiNode.ContainerName != "" {
		return multiNodeContainerIndex(containers, multiNode.ContainerName)
	}
	for i := range containers {
		if _, ok := flagInt32(containers[i].Command, multiNodeArgTPSize); ok {
			return i
		}
		if _, ok := flagInt32(containers[i].Args, multiNodeArgTPSize); ok {
			return i
		}
	}
	if len(containers) == 1 {
		return 0
	}
	return -1
}

func multiNodeContainerIndex(containers []corev1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}

func flagInt32(args []string, flag string) (int32, bool) {
	raw, ok := flagValue(args, flag)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

func flagValue(args []string, flag string) (string, bool) {
	for i := range args {
		if args[i] == flag && i+1 < len(args) {
			return args[i+1], true
		}
		prefix := flag + "="
		if len(args[i]) > len(prefix) && args[i][:len(prefix)] == prefix {
			return args[i][len(prefix):], true
		}
	}
	return "", false
}

func setMultiNodeArg(container *corev1.Container, flag, value string) {
	if replaceFlagValue(&container.Command, flag, value) {
		return
	}
	if replaceFlagValue(&container.Args, flag, value) {
		return
	}
	if len(container.Args) > 0 {
		container.Args = append(container.Args, flag, value)
		return
	}
	if len(container.Command) > 0 {
		container.Command = append(container.Command, flag, value)
		return
	}
	container.Args = append(container.Args, flag, value)
}

func replaceFlagValue(args *[]string, flag, value string) bool {
	items := *args
	for i := range items {
		if items[i] == flag {
			if i+1 < len(items) {
				items[i+1] = value
			} else {
				items = append(items, value)
			}
			*args = items
			return true
		}
		prefix := flag + "="
		if len(items[i]) > len(prefix) && items[i][:len(prefix)] == prefix {
			items[i] = prefix + value
			*args = items
			return true
		}
	}
	return false
}
