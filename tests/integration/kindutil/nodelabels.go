package kindutil

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LabelClusterBaseModelReady stamps
// `models.ome.io/clusterbasemodel.<name>=Ready` on every Node in the
// cluster, so the OME pod-injection NodeSelector (which OME's
// production model-agent normally populates after the per-node model
// download completes) is satisfied under KIND. Without this, ISVC
// pods stay Pending with "didn't match Pod's node affinity/selector."
//
// Test fixtures call this after the ClusterBaseModel is applied and
// before the InferenceService is created. Idempotent — re-labeling a
// node that already carries the label is a no-op patch.
//
// For tests that exercise multi-node scheduling: pass workers > 0 to
// RunKindSuite first, then label individual nodes selectively via
// LabelNode rather than this all-nodes helper.
func LabelClusterBaseModelReady(ctx context.Context, c client.Client, cbmName string) error {
	key := fmt.Sprintf("models.ome.io/clusterbasemodel.%s", cbmName)
	return labelAllNodes(ctx, c, key, "Ready")
}

// LabelBaseModelReady is the namespaced-BaseModel analog of
// LabelClusterBaseModelReady. Same Ready-label contract; different
// label key shape (production pod-injection chooses the key based on
// whether the model is namespaced or cluster-scoped).
func LabelBaseModelReady(ctx context.Context, c client.Client, namespace, bmName string) error {
	key := fmt.Sprintf("models.ome.io/%s.basemodel.%s", namespace, bmName)
	return labelAllNodes(ctx, c, key, "Ready")
}

// LabelNode adds key=value to one specific node by name. Useful for
// multi-node suites that want to pin a model to a subset of nodes
// (testing model-availability filtering, defragmentation specs, etc.).
func LabelNode(ctx context.Context, c client.Client, nodeName, key, value string) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("LabelNode: get %s: %w", nodeName, err)
	}
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	if node.Labels[key] == value {
		return nil
	}
	node.Labels[key] = value
	return c.Update(ctx, node)
}

// labelAllNodes is the all-nodes implementation backing the
// per-model helpers above. Walks the live node list and Updates
// each that needs the label.
func labelAllNodes(ctx context.Context, c client.Client, key, value string) error {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return fmt.Errorf("labelAllNodes: list nodes: %w", err)
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Labels[key] == value {
			continue
		}
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels[key] = value
		if err := c.Update(ctx, node); err != nil {
			return fmt.Errorf("labelAllNodes: update %s with %s=%s: %w",
				node.Name, key, value, err)
		}
	}
	return nil
}
