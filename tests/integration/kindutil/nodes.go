package kindutil

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Node signal helpers. KIND nodes carry no accelerators and no health
// detector, so suites publish the signals themselves the way the real
// components do. Conditions travel as strategic merge patches on the status
// subresource, as the kubelet and node-problem-detector send them: only the
// touched condition is in the patch, so a concurrent writer's entries are
// never deleted. Extended resources (a key-wise map merge) and spec fields
// (taints carry no patch strategy) travel as plain merge patches.

// SetNodeExtendedResource advertises qty of an extended resource on nodeName
// in both capacity and allocatable.
func SetNodeExtendedResource(ctx context.Context, c client.Client, nodeName string, resName corev1.ResourceName, qty int64) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("SetNodeExtendedResource: get %s: %w", nodeName, err)
	}
	base := node.DeepCopy()
	q := *resource.NewQuantity(qty, resource.DecimalSI)
	if node.Status.Capacity == nil {
		node.Status.Capacity = corev1.ResourceList{}
	}
	if node.Status.Allocatable == nil {
		node.Status.Allocatable = corev1.ResourceList{}
	}
	node.Status.Capacity[resName] = q
	node.Status.Allocatable[resName] = q
	if err := c.Status().Patch(ctx, node, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("SetNodeExtendedResource: patch %s: %w", nodeName, err)
	}
	return nil
}

// SetNodeCondition upserts the condition of the given type on nodeName.
// LastTransitionTime is stamped only when the status changes, matching what
// a detector such as node-problem-detector reports.
func SetNodeCondition(ctx context.Context, c client.Client, nodeName string, condType corev1.NodeConditionType, status corev1.ConditionStatus, reason, message string) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("SetNodeCondition: get %s: %w", nodeName, err)
	}
	base := node.DeepCopy()
	now := metav1.Now()
	updated := false
	for i := range node.Status.Conditions {
		cond := &node.Status.Conditions[i]
		if cond.Type != condType {
			continue
		}
		if cond.Status != status {
			cond.LastTransitionTime = now
		}
		cond.Status, cond.Reason, cond.Message, cond.LastHeartbeatTime = status, reason, message, now
		updated = true
	}
	if !updated {
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type: condType, Status: status, Reason: reason, Message: message,
			LastTransitionTime: now, LastHeartbeatTime: now,
		})
	}
	if err := c.Status().Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		return fmt.Errorf("SetNodeCondition: patch %s: %w", nodeName, err)
	}
	return nil
}

// ClearNodeCondition removes every condition of the given type from nodeName.
func ClearNodeCondition(ctx context.Context, c client.Client, nodeName string, condType corev1.NodeConditionType) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("ClearNodeCondition: get %s: %w", nodeName, err)
	}
	base := node.DeepCopy()
	kept := node.Status.Conditions[:0]
	for _, cond := range node.Status.Conditions {
		if cond.Type != condType {
			kept = append(kept, cond)
		}
	}
	node.Status.Conditions = kept
	if err := c.Status().Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		return fmt.Errorf("ClearNodeCondition: patch %s: %w", nodeName, err)
	}
	return nil
}

// AddNodeTaint adds taint to nodeName unless an identical key/effect exists.
func AddNodeTaint(ctx context.Context, c client.Client, nodeName string, taint corev1.Taint) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("AddNodeTaint: get %s: %w", nodeName, err)
	}
	for _, t := range node.Spec.Taints {
		if t.Key == taint.Key && t.Effect == taint.Effect {
			return nil
		}
	}
	base := node.DeepCopy()
	node.Spec.Taints = append(node.Spec.Taints, taint)
	if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("AddNodeTaint: patch %s: %w", nodeName, err)
	}
	return nil
}

// RemoveNodeTaint removes every taint with the given key from nodeName.
func RemoveNodeTaint(ctx context.Context, c client.Client, nodeName, key string) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("RemoveNodeTaint: get %s: %w", nodeName, err)
	}
	base := node.DeepCopy()
	kept := node.Spec.Taints[:0]
	for _, t := range node.Spec.Taints {
		if t.Key != key {
			kept = append(kept, t)
		}
	}
	node.Spec.Taints = kept
	if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("RemoveNodeTaint: patch %s: %w", nodeName, err)
	}
	return nil
}

// SetNodeUnschedulable cordons (true) or uncordons (false) nodeName.
func SetNodeUnschedulable(ctx context.Context, c client.Client, nodeName string, unschedulable bool) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("SetNodeUnschedulable: get %s: %w", nodeName, err)
	}
	if node.Spec.Unschedulable == unschedulable {
		return nil
	}
	base := node.DeepCopy()
	node.Spec.Unschedulable = unschedulable
	if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("SetNodeUnschedulable: patch %s: %w", nodeName, err)
	}
	return nil
}
