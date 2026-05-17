package sharded

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/shared"
)

// HandleModelDeletion short-circuits the per-node ConfigMap wait for
// Sharded models. cluster_cache manages chunks daemon-side and
// model-agent's scout skips Sharded entirely (OEP-0009), so leftover
// per-node entries from a prior PerNode lifecycle never get marked
// Deleted on their own. Best-effort scrub, then drop the finalizer —
// scrub failures don't block the deletion.
func HandleModelDeletion(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, isClusterScoped bool, finalizer string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		return ctrl.Result{}, nil
	}

	modelName := obj.GetName()
	var modelNamespace string
	if !isClusterScoped {
		modelNamespace = obj.GetNamespace()
	}
	modelInfo := modelName
	if !isClusterScoped {
		modelInfo = modelNamespace + "/" + modelName
	}
	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)

	scrubbed, skipped := scrubStaleConfigMapEntries(ctx, kubeClient, log, modelKey)
	if scrubbed+skipped > 0 {
		log.Info("Scrubbed stale per-node entries for Sharded model",
			"model", modelInfo, "scrubbed", scrubbed, "skipped", skipped)
	}

	if err := shared.RetryUpdate(ctx, kubeClient, log, obj, "sharded-finalizer-remove", func(ctx context.Context, c client.Client, latest client.Object) error {
		controllerutil.RemoveFinalizer(latest, finalizer)
		return c.Update(ctx, latest)
	}); err != nil {
		log.Error(err, "Failed to remove finalizer for Sharded model", "model", modelInfo)
		return ctrl.Result{}, err
	}
	log.Info("Sharded model deletion complete", "model", modelInfo)
	return ctrl.Result{}, nil
}

// scrubStaleConfigMapEntries removes modelKey from every per-node
// status ConfigMap. Never returns an error — the caller must drop the
// finalizer regardless; a stuck deletion is worse than a leaked entry.
func scrubStaleConfigMapEntries(ctx context.Context, kubeClient client.Client, log logr.Logger, modelKey string) (scrubbed, skipped int) {
	configMaps := &corev1.ConfigMapList{}
	if err := kubeClient.List(ctx, configMaps,
		client.InNamespace(constants.OMENamespace),
		client.MatchingLabels{constants.ModelStatusConfigMapLabel: "true"},
	); err != nil {
		log.Error(err, "Failed to list per-node model-status ConfigMaps; Sharded deletion will proceed without scrub")
		return 0, 0
	}
	for i := range configMaps.Items {
		cm := &configMaps.Items[i]
		if _, exists := cm.Data[modelKey]; !exists {
			continue
		}
		delete(cm.Data, modelKey)
		if err := kubeClient.Update(ctx, cm); err != nil {
			log.Error(err, "Failed to scrub stale entry; deletion proceeds regardless",
				"configMap", cm.Name, "modelKey", modelKey)
			skipped++
			continue
		}
		scrubbed++
	}
	return scrubbed, skipped
}
